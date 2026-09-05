// Copyright 2026 StreamFusion Authors
// Licensed under the Apache License, Version 2.0

use std::cmp::Ordering;
use std::sync::Arc;

use ahash::RandomState;
use arrow::array::{Array, ArrayRef, Int8Array, UInt32Array};
use arrow::compute::take;
use arrow::datatypes::{DataType, Field, Schema, SchemaRef};
use arrow::record_batch::RecordBatch;
use arrow_row::{RowConverter, Rows, SortField};
use datafusion::error::{DataFusionError, Result};
use hashbrown::HashMap;

use crate::memory_pool::HostMemoryReservation;
use crate::planner::arrow_schema;
use crate::state::{
    decode_key_group_snapshot, KeyedState, MemoryKeyedState, RocksPluginKeyedState, StateKey,
    StateKeyRef, StateMutation,
};
use crate::{decode_plan, proto};

use super::top_n::compare::compare_rows;

const INSERT: i8 = 0;
const UPDATE_BEFORE: i8 = 1;
const UPDATE_AFTER: i8 = 2;
const DELETE: i8 = 3;
const STATE_KEY_PREFIX: u8 = 26;
const PHYSICAL_STATE_KEY_PREFIX: u8 = 27;
const INPUT_KIND_COLUMN: &str = "__streamfusion_input_row_kind";
const OUTPUT_KIND_COLUMN: &str = "__streamfusion_row_kind";
const OUTPUT_BATCH_ROWS: usize = 16_384;

#[derive(Clone, Debug, Eq, PartialEq)]
struct PhysicalHeapRow {
    sequence: u64,
    kind: i8,
    encoded: Vec<u8>,
}

fn physical_heap_bytes(rows: &[PhysicalHeapRow], capacity: usize) -> usize {
    capacity
        .saturating_mul(std::mem::size_of::<PhysicalHeapRow>())
        .saturating_add(rows.iter().map(|row| row.encoded.capacity()).sum::<usize>())
}

struct PendingSort {
    unique: RecordBatch,
    order: Vec<usize>,
    counts: Vec<u64>,
    kinds: Option<Vec<i8>>,
    order_position: usize,
    emitted_from_current: u64,
    remaining_skip: u64,
    remaining_take: Option<u64>,
}

impl PendingSort {
    fn retained_bytes(&self) -> usize {
        self.unique
            .get_array_memory_size()
            .saturating_add(
                self.order
                    .capacity()
                    .saturating_mul(std::mem::size_of::<usize>()),
            )
            .saturating_add(
                self.counts
                    .capacity()
                    .saturating_mul(std::mem::size_of::<u64>()),
            )
            .saturating_add(self.kinds.as_ref().map_or(0, |kinds| {
                kinds.capacity().saturating_mul(std::mem::size_of::<i8>())
            }))
    }

    fn drained(&self) -> bool {
        self.order_position >= self.order.len() || self.remaining_take == Some(0)
    }
}

/// Bounded global full sort over a backend-neutral counted multiset of Arrow rows.
pub(crate) struct BoundedSortProcessor {
    plan: proto::BoundedSort,
    input_schema: SchemaRef,
    output_schema: SchemaRef,
    state: Box<dyn KeyedState>,
    first_key_group: u32,
    last_key_group: u32,
    write_key_group: u32,
    row_converter: RowConverter,
    prepared_schema: Option<SchemaRef>,
    input_kind_index: Option<usize>,
    scratch: HostMemoryReservation,
    pending: Option<PendingSort>,
    drained: bool,
    next_sequence: u64,
    physical_heap: Vec<PhysicalHeapRow>,
    physical_heap_loaded: bool,
    physical_state_distributed: bool,
    physical_loaded_keys: Vec<StateKey>,
    state_read_batches: u64,
    state_write_batches: u64,
    rows_read: u64,
    rows_written: u64,
    invalid_retractions: u64,
    comparator_calls: u64,
    emitted_rows: u64,
}

impl BoundedSortProcessor {
    pub(crate) fn new(
        serialized_plan: &[u8],
        first_key_group: u32,
        last_key_group: u32,
        reservation: HostMemoryReservation,
    ) -> Result<Self> {
        let scratch = reservation.sibling("native bounded sort batch scratch and output");
        let state = Box::new(MemoryKeyedState::new(
            first_key_group,
            last_key_group,
            reservation,
        )?);
        Self::with_state(
            serialized_plan,
            first_key_group,
            last_key_group,
            state,
            scratch,
        )
    }

    pub(crate) fn new_rocksdb(
        serialized_plan: &[u8],
        first_key_group: u32,
        last_key_group: u32,
        plugin_path: &std::path::Path,
        database_path: &std::path::Path,
        memory_limit: usize,
        scratch: HostMemoryReservation,
    ) -> Result<Self> {
        let state = Box::new(RocksPluginKeyedState::open(
            plugin_path,
            database_path,
            first_key_group,
            last_key_group,
            memory_limit,
        )?);
        Self::with_state(
            serialized_plan,
            first_key_group,
            last_key_group,
            state,
            scratch,
        )
    }

    fn with_state(
        serialized_plan: &[u8],
        first_key_group: u32,
        last_key_group: u32,
        state: Box<dyn KeyedState>,
        scratch: HostMemoryReservation,
    ) -> Result<Self> {
        let root = decode_plan(serialized_plan)?
            .root
            .ok_or_else(|| DataFusionError::Plan("bounded sort plan has no root".to_string()))?;
        let plan = match root.operator {
            Some(proto::operator::Operator::BoundedSort(plan)) => *plan,
            _ => {
                return Err(DataFusionError::Plan(
                    "bounded sort handle requires a BoundedSort root".to_string(),
                ));
            }
        };
        validate_plan(&plan)?;
        let write_key_group = if plan.use_first_owned_key_group {
            first_key_group
        } else {
            0
        };
        if write_key_group < first_key_group || write_key_group > last_key_group {
            return Err(DataFusionError::Plan(format!(
                "bounded sort write key group {write_key_group} is outside owned range {first_key_group}..={last_key_group}"
            )));
        }
        let input_schema = arrow_schema(plan.input_schema.as_ref().expect("validated schema"))?;
        let row_converter = RowConverter::new(
            input_schema
                .fields()
                .iter()
                .map(|field| SortField::new(field.data_type().clone()))
                .collect(),
        )?;
        let mut output_fields = input_schema.fields().iter().cloned().collect::<Vec<_>>();
        output_fields.push(Arc::new(Field::new(
            OUTPUT_KIND_COLUMN,
            DataType::Int8,
            false,
        )));
        Ok(Self {
            plan,
            input_schema,
            output_schema: Arc::new(Schema::new(output_fields)),
            state,
            first_key_group,
            last_key_group,
            write_key_group,
            row_converter,
            prepared_schema: None,
            input_kind_index: None,
            scratch,
            pending: None,
            drained: false,
            next_sequence: 0,
            physical_heap: Vec::new(),
            physical_heap_loaded: false,
            physical_state_distributed: false,
            physical_loaded_keys: Vec::new(),
            state_read_batches: 0,
            state_write_batches: 0,
            rows_read: 0,
            rows_written: 0,
            invalid_retractions: 0,
            comparator_calls: 0,
            emitted_rows: 0,
        })
    }

    pub(crate) fn process_arrow(&mut self, batch: RecordBatch) -> Result<()> {
        if self.pending.is_some() || self.drained {
            return Err(DataFusionError::Execution(
                "bounded sort received input after terminal output started".to_string(),
            ));
        }
        self.prepare_schema(batch.schema())?;
        let visible_count = self.input_schema.fields().len();
        let heap_working = if self.plan.physical_input_semantics {
            physical_heap_bytes(&self.physical_heap, self.physical_heap.capacity())
                .saturating_mul(3)
        } else {
            0
        };
        let base = batch
            .get_array_memory_size()
            .saturating_mul(2)
            .saturating_add(batch.num_rows().saturating_mul(192))
            .saturating_add(heap_working);
        self.scratch.resize(base)?;
        let result = self.process_accounted(&batch, visible_count);
        let retained = if self.plan.physical_input_semantics {
            physical_heap_bytes(&self.physical_heap, self.physical_heap.capacity())
        } else {
            0
        };
        self.scratch.resize(retained)?;
        result
    }

    fn process_accounted(&mut self, batch: &RecordBatch, visible_count: usize) -> Result<()> {
        let encoded = self
            .row_converter
            .convert_columns(&batch.columns()[..visible_count])?;
        let kinds = batch
            .column(self.input_kind_index.expect("prepared input schema"))
            .as_any()
            .downcast_ref::<Int8Array>()
            .ok_or_else(|| {
                DataFusionError::Execution("bounded sort RowKinds are not Arrow Int8".to_string())
            })?;
        if self.plan.physical_input_semantics {
            return self.process_physical_batch(batch, kinds, &encoded);
        }
        let mut unique = HashMap::<Vec<u8>, usize, RandomState>::with_capacity_and_hasher(
            batch.num_rows().max(1),
            RandomState::new(),
        );
        let mut row_indices = Vec::with_capacity(batch.num_rows());
        for row in 0..batch.num_rows() {
            let bytes = encoded.row(row).data();
            let next = unique.len();
            let index = *unique.entry(state_key(bytes)).or_insert(next);
            row_indices.push(index);
        }
        if unique.is_empty() {
            return Ok(());
        }
        // Consume the hash table into insertion order so the same owned row bytes become the
        // backend key. This avoids retaining and copying a second row vector for every distinct
        // record in the batch.
        let mut ordered_keys = Vec::with_capacity(unique.len());
        ordered_keys.resize_with(unique.len(), || None);
        for (key, index) in unique {
            ordered_keys[index] = Some(key);
        }
        let keys = ordered_keys
            .into_iter()
            .map(|key| key.expect("bounded sort unique index is dense"))
            .collect::<Vec<_>>();
        let refs = keys
            .iter()
            .map(|key| StateKeyRef {
                key_group: self.write_key_group,
                key,
            })
            .collect::<Vec<_>>();
        let existing = self.state.get_batch(&refs)?;
        self.state_read_batches = self.state_read_batches.saturating_add(1);
        self.rows_read = self.rows_read.saturating_add(existing.len() as u64);
        let mut counts = existing
            .iter()
            .map(|value| value.as_ref().map(|value| decode_count(value)).transpose())
            .collect::<Result<Vec<_>>>()?
            .into_iter()
            .map(Option::unwrap_or_default)
            .collect::<Vec<_>>();
        for row in 0..batch.num_rows() {
            let count = &mut counts[row_indices[row]];
            match kinds.value(row) {
                INSERT | UPDATE_AFTER => {
                    *count = count.checked_add(1).ok_or_else(|| {
                        DataFusionError::Execution("bounded sort row count overflow".to_string())
                    })?;
                }
                UPDATE_BEFORE | DELETE => {
                    if *count == 0 {
                        self.invalid_retractions = self.invalid_retractions.saturating_add(1);
                        return Err(DataFusionError::Execution("RowData not exist!".to_string()));
                    }
                    *count -= 1;
                }
                kind => {
                    return Err(DataFusionError::Execution(format!(
                        "bounded sort received invalid Flink RowKind {kind}"
                    )));
                }
            }
        }
        let mutations = keys
            .into_iter()
            .zip(counts)
            .map(|(key, count)| StateMutation {
                key: StateKey {
                    key_group: self.write_key_group,
                    key,
                },
                value: (count != 0).then(|| count.to_le_bytes().to_vec()),
            })
            .collect::<Vec<_>>();
        self.rows_written = self.rows_written.saturating_add(mutations.len() as u64);
        self.state.write_batch(mutations)?;
        self.state_write_batches = self.state_write_batches.saturating_add(1);
        Ok(())
    }

    fn process_physical_batch(
        &mut self,
        batch: &RecordBatch,
        kinds: &Int8Array,
        encoded: &Rows,
    ) -> Result<()> {
        self.ensure_physical_heap()?;
        if batch.num_rows() == 0 {
            return Ok(());
        }
        let old_heap = std::mem::take(&mut self.physical_heap);
        let old_len = old_heap.len();
        let parser = self.row_converter.parser();
        let columns = self.row_converter.convert_rows(
            old_heap
                .iter()
                .map(|row| parser.parse(&row.encoded))
                .chain((0..batch.num_rows()).map(|row| parser.parse(encoded.row(row).data()))),
        )?;
        let records = RecordBatch::try_new(Arc::clone(&self.input_schema), columns)?;
        let mut heap = (0..old_len).collect::<Vec<_>>();
        let limit_end = usize::try_from(self.plan.limit_end.expect("validated SortLimit end"))
            .map_err(|_| {
                DataFusionError::Execution(
                    "bounded SortLimit end exceeds the native address space".to_string(),
                )
            })?;
        let mut input_sequences = Vec::with_capacity(batch.num_rows());
        for row in 0..batch.num_rows() {
            let sequence = self.next_sequence;
            self.next_sequence = self.next_sequence.checked_add(1).ok_or_else(|| {
                DataFusionError::Execution("bounded sort input sequence overflow".to_string())
            })?;
            input_sequences.push(sequence);
            let candidate = old_len + row;
            if heap.len() < limit_end {
                heap_push(
                    &mut heap,
                    candidate,
                    &records,
                    &self.plan,
                    &mut self.comparator_calls,
                )?;
            } else if !heap.is_empty()
                && compare_plan_rows(
                    &records,
                    heap[0],
                    candidate,
                    &self.plan,
                    &mut self.comparator_calls,
                )? == Ordering::Greater
            {
                heap_poll(&mut heap, &records, &self.plan, &mut self.comparator_calls)?;
                heap_push(
                    &mut heap,
                    candidate,
                    &records,
                    &self.plan,
                    &mut self.comparator_calls,
                )?;
            }
        }

        let mut next_heap = Vec::with_capacity(heap.len());
        for index in heap {
            if index < old_len {
                next_heap.push(old_heap[index].clone());
            } else {
                let input_row = index - old_len;
                let kind = kinds.value(input_row);
                if !matches!(kind, INSERT | UPDATE_BEFORE | UPDATE_AFTER | DELETE) {
                    return Err(DataFusionError::Execution(format!(
                        "bounded SortLimit received invalid Flink RowKind {kind}"
                    )));
                }
                next_heap.push(PhysicalHeapRow {
                    sequence: input_sequences[input_row],
                    kind,
                    encoded: encoded.row(input_row).data().to_vec(),
                });
            }
        }

        let mut mutations = Vec::new();
        if self.physical_state_distributed {
            mutations.extend(
                self.physical_loaded_keys
                    .drain(..)
                    .map(|key| StateMutation { key, value: None }),
            );
            mutations.extend(
                next_heap
                    .iter()
                    .enumerate()
                    .map(|(slot, row)| StateMutation {
                        key: StateKey {
                            key_group: self.write_key_group,
                            key: physical_state_key(slot as u64),
                        },
                        value: Some(encode_physical_heap_row(row)),
                    }),
            );
        } else {
            for slot in 0..old_heap.len().max(next_heap.len()) {
                let old = old_heap.get(slot);
                let new = next_heap.get(slot);
                if old == new {
                    continue;
                }
                mutations.push(StateMutation {
                    key: StateKey {
                        key_group: self.write_key_group,
                        key: physical_state_key(slot as u64),
                    },
                    value: new.map(encode_physical_heap_row),
                });
            }
        }
        if !mutations.is_empty() {
            self.rows_written = self.rows_written.saturating_add(mutations.len() as u64);
            self.state.write_batch(mutations)?;
            self.state_write_batches = self.state_write_batches.saturating_add(1);
        }
        self.physical_loaded_keys = (0..next_heap.len())
            .map(|slot| StateKey {
                key_group: self.write_key_group,
                key: physical_state_key(slot as u64),
            })
            .collect();
        self.physical_state_distributed = false;
        self.physical_heap = next_heap;
        Ok(())
    }

    fn ensure_physical_heap(&mut self) -> Result<()> {
        if self.physical_heap_loaded {
            return Ok(());
        }
        let mut loaded = Vec::<(u32, u64, PhysicalHeapRow)>::new();
        let mut nonempty_groups = 0usize;
        for key_group in self.first_key_group..=self.last_key_group {
            let snapshot = self.state.snapshot_key_group(key_group)?;
            let mut group_rows = Vec::new();
            for (key, value) in decode_key_group_snapshot(key_group, &snapshot)? {
                let Some(slot) = decode_physical_slot(&key)? else {
                    return Err(DataFusionError::Execution(
                        "bounded SortLimit state contains an unknown namespace".to_string(),
                    ));
                };
                self.physical_loaded_keys.push(StateKey {
                    key_group,
                    key: key.clone(),
                });
                group_rows.push((slot, decode_physical_heap_row(&value)?));
            }
            if !group_rows.is_empty() {
                nonempty_groups += 1;
                self.state_read_batches = self.state_read_batches.saturating_add(1);
                self.rows_read = self.rows_read.saturating_add(group_rows.len() as u64);
            }
            group_rows.sort_by_key(|(slot, _)| *slot);
            if group_rows
                .iter()
                .enumerate()
                .any(|(expected, (slot, _))| *slot != expected as u64)
            {
                return Err(DataFusionError::Execution(
                    "bounded SortLimit heap state has non-contiguous slots".to_string(),
                ));
            }
            loaded.extend(
                group_rows
                    .into_iter()
                    .map(|(slot, row)| (key_group, slot, row)),
            );
        }
        self.next_sequence = loaded.iter().fold(self.next_sequence, |next, (_, _, row)| {
            next.max(row.sequence.saturating_add(1))
        });
        if loaded.is_empty() {
            self.physical_heap_loaded = true;
            return Ok(());
        }
        if nonempty_groups == 1
            && loaded
                .iter()
                .all(|(group, _, _)| *group == self.write_key_group)
        {
            self.physical_heap = loaded.into_iter().map(|(_, _, row)| row).collect();
            self.scratch.try_grow(
                physical_heap_bytes(&self.physical_heap, self.physical_heap.capacity())
                    .saturating_mul(3),
            )?;
            self.physical_heap_loaded = true;
            return Ok(());
        }

        loaded.sort_by_key(|(key_group, _, row)| (*key_group, row.sequence));
        let candidates = loaded
            .into_iter()
            .map(|(_, _, row)| row)
            .collect::<Vec<_>>();
        let parser = self.row_converter.parser();
        let columns = self.row_converter.convert_rows(
            candidates
                .iter()
                .map(|row| parser.parse(row.encoded.as_slice())),
        )?;
        let records = RecordBatch::try_new(Arc::clone(&self.input_schema), columns)?;
        let limit_end = usize::try_from(self.plan.limit_end.expect("validated SortLimit end"))
            .map_err(|_| {
                DataFusionError::Execution(
                    "bounded SortLimit end exceeds the native address space".to_string(),
                )
            })?;
        let mut heap = Vec::with_capacity(limit_end.min(candidates.len()));
        for row in 0..candidates.len() {
            if heap.len() < limit_end {
                heap_push(
                    &mut heap,
                    row,
                    &records,
                    &self.plan,
                    &mut self.comparator_calls,
                )?;
            } else if !heap.is_empty()
                && compare_plan_rows(
                    &records,
                    heap[0],
                    row,
                    &self.plan,
                    &mut self.comparator_calls,
                )? == Ordering::Greater
            {
                heap_poll(&mut heap, &records, &self.plan, &mut self.comparator_calls)?;
                heap_push(
                    &mut heap,
                    row,
                    &records,
                    &self.plan,
                    &mut self.comparator_calls,
                )?;
            }
        }
        let mut candidates = candidates.into_iter().map(Some).collect::<Vec<_>>();
        self.physical_heap = heap
            .into_iter()
            .map(|index| {
                candidates[index]
                    .take()
                    .expect("physical heap index is unique")
            })
            .collect();
        self.scratch.try_grow(
            physical_heap_bytes(&self.physical_heap, self.physical_heap.capacity())
                .saturating_mul(3),
        )?;
        self.physical_state_distributed = true;
        self.physical_heap_loaded = true;
        Ok(())
    }

    pub(crate) fn finish(&mut self) -> Result<RecordBatch> {
        if self.drained {
            return Ok(RecordBatch::new_empty(Arc::clone(&self.output_schema)));
        }
        if self.pending.is_none() {
            if let Err(error) = self.prepare_output() {
                self.scratch.resize(0)?;
                return Err(error);
            }
            if self.pending.is_none() {
                self.drained = true;
                self.scratch.resize(0)?;
                return Ok(RecordBatch::new_empty(Arc::clone(&self.output_schema)));
            }
        }

        let retained = self
            .pending
            .as_ref()
            .expect("prepared output")
            .retained_bytes();
        let indices = next_output_indices(self.pending.as_mut().expect("prepared output"))?;
        let output = self.take_output(&indices)?;
        let output_bytes = output.get_array_memory_size();
        let finished = self.pending.as_ref().expect("prepared output").drained();
        if finished {
            self.pending = None;
            self.drained = true;
            self.scratch.resize(output_bytes)?;
        } else {
            self.scratch.resize(retained.saturating_add(output_bytes))?;
        }
        self.emitted_rows = self.emitted_rows.saturating_add(output.num_rows() as u64);
        self.scratch.transfer_to_arrow(output_bytes)?;
        Ok(output)
    }

    fn prepare_output(&mut self) -> Result<()> {
        if self.plan.physical_input_semantics {
            return self.prepare_physical_output();
        }
        let mut rows = HashMap::<Vec<u8>, u64, RandomState>::with_hasher(RandomState::new());
        let mut snapshot_bytes = 0usize;
        for key_group in self.first_key_group..=self.last_key_group {
            let snapshot = self.state.snapshot_key_group(key_group)?;
            snapshot_bytes = snapshot_bytes.saturating_add(snapshot.len());
            let entries = decode_key_group_snapshot(key_group, &snapshot)?;
            for (key, value) in entries {
                if key.first().copied() != Some(STATE_KEY_PREFIX) {
                    return Err(DataFusionError::Execution(
                        "bounded sort state contains an unknown namespace".to_string(),
                    ));
                }
                let count = decode_count(&value)?;
                let total = rows.entry(key[1..].to_vec()).or_default();
                *total = total.checked_add(count).ok_or_else(|| {
                    DataFusionError::Execution("bounded sort row count overflow".to_string())
                })?;
            }
        }
        self.scratch.resize(snapshot_bytes.saturating_mul(2))?;
        let (encoded_rows, counts): (Vec<_>, Vec<_>) = rows.into_iter().unzip();
        if encoded_rows.is_empty() {
            return Ok(());
        }
        let parser = self.row_converter.parser();
        let columns = self
            .row_converter
            .convert_rows(encoded_rows.iter().map(|row| parser.parse(row)))?;
        let unique = RecordBatch::try_new(Arc::clone(&self.input_schema), columns)?;
        let mut order = (0..unique.num_rows()).collect::<Vec<_>>();
        let mut compare_error = None;
        order.sort_by(|&left, &right| {
            self.comparator_calls = self.comparator_calls.saturating_add(1);
            match compare_rows(
                &unique,
                left,
                &unique,
                right,
                &self.plan.sort_key_indices,
                &self.plan.sort_ascending,
                &self.plan.sort_nulls_last,
            ) {
                Ok(ordering) => ordering,
                Err(error) => {
                    compare_error = Some(error);
                    Ordering::Equal
                }
            }
        });
        if let Some(error) = compare_error {
            return Err(error);
        }
        let pending = PendingSort {
            unique,
            order,
            counts,
            kinds: None,
            order_position: 0,
            emitted_from_current: 0,
            remaining_skip: self.plan.limit_start.unwrap_or(0),
            remaining_take: self
                .plan
                .limit_end
                .zip(self.plan.limit_start)
                .map(|(end, start)| end - start),
        };
        self.scratch.resize(pending.retained_bytes())?;
        self.pending = Some(pending);
        Ok(())
    }

    fn prepare_physical_output(&mut self) -> Result<()> {
        self.ensure_physical_heap()?;
        self.scratch.resize(
            physical_heap_bytes(&self.physical_heap, self.physical_heap.capacity())
                .saturating_mul(3),
        )?;
        let rows = std::mem::take(&mut self.physical_heap);
        if rows.is_empty() {
            return Ok(());
        }
        let parser = self.row_converter.parser();
        let columns = self
            .row_converter
            .convert_rows(rows.iter().map(|row| parser.parse(&row.encoded)))?;
        let records = RecordBatch::try_new(Arc::clone(&self.input_schema), columns)?;
        let mut heap = (0..records.num_rows()).collect::<Vec<_>>();
        if self.plan.sort_limit_global {
            let mut compare_error = None;
            heap.sort_by(|&left, &right| {
                match compare_plan_rows(
                    &records,
                    left,
                    right,
                    &self.plan,
                    &mut self.comparator_calls,
                ) {
                    Ok(ordering) => ordering,
                    Err(error) => {
                        compare_error = Some(error);
                        Ordering::Equal
                    }
                }
            });
            if let Some(error) = compare_error {
                return Err(error);
            }
        }
        let kinds = rows.iter().map(|row| row.kind).collect::<Vec<_>>();
        let counts = vec![1; records.num_rows()];
        let pending = PendingSort {
            unique: records,
            order: heap,
            counts,
            kinds: Some(kinds),
            order_position: 0,
            emitted_from_current: 0,
            remaining_skip: if self.plan.sort_limit_global {
                self.plan.limit_start.unwrap_or(0)
            } else {
                0
            },
            remaining_take: if self.plan.sort_limit_global {
                self.plan
                    .limit_end
                    .zip(self.plan.limit_start)
                    .map(|(end, start)| end - start)
            } else {
                self.plan.limit_end
            },
        };
        self.scratch.resize(pending.retained_bytes())?;
        self.pending = Some(pending);
        Ok(())
    }

    fn take_output(&self, indices: &UInt32Array) -> Result<RecordBatch> {
        let pending = self.pending.as_ref().expect("prepared output");
        let mut output = pending
            .unique
            .columns()
            .iter()
            .map(|column| take(column.as_ref(), &indices, None))
            .collect::<Result<Vec<ArrayRef>, _>>()?;
        if let Some(kinds) = &pending.kinds {
            let kind_array = Int8Array::from(kinds.clone());
            output.push(take(&kind_array, indices, None)?);
        } else {
            output.push(Arc::new(Int8Array::from_value(INSERT, indices.len())));
        }
        Ok(RecordBatch::try_new(
            Arc::clone(&self.output_schema),
            output,
        )?)
    }

    pub(crate) fn statistics(&self) -> [u64; 7] {
        [
            self.state_read_batches,
            self.state_write_batches,
            self.rows_read,
            self.rows_written,
            self.invalid_retractions,
            self.comparator_calls,
            self.emitted_rows,
        ]
    }

    pub(crate) fn snapshot_key_group(&self, key_group: u32) -> Result<Vec<u8>> {
        self.state.snapshot_key_group(key_group)
    }

    pub(crate) fn restore_key_group(&mut self, key_group: u32, bytes: &[u8]) -> Result<()> {
        self.state.restore_key_group(key_group, bytes)?;
        if self.plan.physical_input_semantics {
            self.physical_heap.clear();
            self.physical_loaded_keys.clear();
            self.physical_heap_loaded = false;
            self.physical_state_distributed = false;
            self.next_sequence = 0;
        }
        Ok(())
    }

    pub(crate) fn checkpoint(&self, directory: &std::path::Path) -> Result<()> {
        self.state.checkpoint(directory)
    }

    fn prepare_schema(&mut self, schema: SchemaRef) -> Result<()> {
        if let Some(expected) = &self.prepared_schema {
            if expected.as_ref() != schema.as_ref() {
                return Err(DataFusionError::Execution(
                    "bounded sort input schema changed while running".to_string(),
                ));
            }
            return Ok(());
        }
        let visible_count = self.input_schema.fields().len();
        if schema.fields().len() != visible_count + 1
            || !self
                .input_schema
                .fields()
                .iter()
                .zip(&schema.fields()[..visible_count])
                .all(|(expected, actual)| expected.data_type() == actual.data_type())
            || schema.field(visible_count).name() != INPUT_KIND_COLUMN
            || schema.field(visible_count).data_type() != &DataType::Int8
        {
            return Err(DataFusionError::Execution(format!(
                "bounded sort Arrow input does not match its plan: expected {:?} plus RowKind, got {schema:?}",
                self.input_schema
            )));
        }
        self.input_kind_index = Some(visible_count);
        self.prepared_schema = Some(schema);
        Ok(())
    }
}

fn next_output_indices(pending: &mut PendingSort) -> Result<UInt32Array> {
    let mut indices = Vec::with_capacity(OUTPUT_BATCH_ROWS);
    while indices.len() < OUTPUT_BATCH_ROWS && pending.order_position < pending.order.len() {
        let row = pending.order[pending.order_position];
        let count = pending.counts[row];
        let available = count
            .checked_sub(pending.emitted_from_current)
            .ok_or_else(|| {
                DataFusionError::Internal(
                    "bounded sort output cursor exceeded its row count".to_string(),
                )
            })?;
        let skipped = available.min(pending.remaining_skip);
        pending.remaining_skip -= skipped;
        pending.emitted_from_current += skipped;
        let available = available - skipped;
        if pending.emitted_from_current == count {
            pending.order_position += 1;
            pending.emitted_from_current = 0;
            continue;
        }
        if pending.remaining_take == Some(0) {
            break;
        }
        let take_count = available
            .min((OUTPUT_BATCH_ROWS - indices.len()) as u64)
            .min(pending.remaining_take.unwrap_or(u64::MAX));
        let index = u32::try_from(row).map_err(|_| {
            DataFusionError::Execution("bounded sort unique row count exceeds u32".to_string())
        })?;
        indices.extend(std::iter::repeat_n(index, take_count as usize));
        pending.emitted_from_current += take_count;
        if let Some(remaining) = &mut pending.remaining_take {
            *remaining -= take_count;
        }
        if pending.emitted_from_current == count {
            pending.order_position += 1;
            pending.emitted_from_current = 0;
        }
    }
    Ok(UInt32Array::from(indices))
}

fn state_key(row: &[u8]) -> Vec<u8> {
    let mut key = Vec::with_capacity(1 + row.len());
    key.push(STATE_KEY_PREFIX);
    key.extend_from_slice(row);
    key
}

fn physical_state_key(slot: u64) -> Vec<u8> {
    let mut key = Vec::with_capacity(9);
    key.push(PHYSICAL_STATE_KEY_PREFIX);
    key.extend_from_slice(&slot.to_le_bytes());
    key
}

fn decode_physical_slot(key: &[u8]) -> Result<Option<u64>> {
    if key.first().copied() != Some(PHYSICAL_STATE_KEY_PREFIX) {
        return Ok(None);
    }
    let bytes: [u8; 8] = key[1..].try_into().map_err(|_| {
        DataFusionError::Execution("bounded SortLimit heap key has an invalid length".to_string())
    })?;
    Ok(Some(u64::from_le_bytes(bytes)))
}

fn encode_physical_heap_row(row: &PhysicalHeapRow) -> Vec<u8> {
    let mut value = Vec::with_capacity(10 + row.encoded.len());
    value.push(1);
    value.extend_from_slice(&row.sequence.to_le_bytes());
    value.push(row.kind as u8);
    value.extend_from_slice(&row.encoded);
    value
}

fn decode_physical_heap_row(value: &[u8]) -> Result<PhysicalHeapRow> {
    if value.len() < 10 || value[0] != 1 {
        return Err(DataFusionError::Execution(
            "bounded SortLimit heap row has an unsupported state version".to_string(),
        ));
    }
    let sequence = u64::from_le_bytes(value[1..9].try_into().expect("checked heap row length"));
    let kind = value[9] as i8;
    if !matches!(kind, INSERT | UPDATE_BEFORE | UPDATE_AFTER | DELETE) {
        return Err(DataFusionError::Execution(format!(
            "bounded SortLimit state contains invalid Flink RowKind {kind}"
        )));
    }
    Ok(PhysicalHeapRow {
        sequence,
        kind,
        encoded: value[10..].to_vec(),
    })
}

fn compare_plan_rows(
    batch: &RecordBatch,
    left: usize,
    right: usize,
    plan: &proto::BoundedSort,
    comparator_calls: &mut u64,
) -> Result<Ordering> {
    *comparator_calls = comparator_calls.saturating_add(1);
    compare_rows(
        batch,
        left,
        batch,
        right,
        &plan.sort_key_indices,
        &plan.sort_ascending,
        &plan.sort_nulls_last,
    )
}

fn heap_push(
    heap: &mut Vec<usize>,
    row: usize,
    batch: &RecordBatch,
    plan: &proto::BoundedSort,
    comparator_calls: &mut u64,
) -> Result<()> {
    heap.push(row);
    let mut child = heap.len() - 1;
    while child > 0 {
        let parent = (child - 1) >> 1;
        if compare_plan_rows(batch, heap[parent], heap[child], plan, comparator_calls)?
            != Ordering::Less
        {
            break;
        }
        heap.swap(parent, child);
        child = parent;
    }
    Ok(())
}

fn heap_poll(
    heap: &mut Vec<usize>,
    batch: &RecordBatch,
    plan: &proto::BoundedSort,
    comparator_calls: &mut u64,
) -> Result<()> {
    let last = heap.pop().expect("nonempty SortLimit heap");
    if heap.is_empty() {
        return Ok(());
    }
    heap[0] = last;
    let mut parent = 0;
    let half = heap.len() >> 1;
    while parent < half {
        let mut child = (parent << 1) + 1;
        let right = child + 1;
        if right < heap.len()
            && compare_plan_rows(batch, heap[child], heap[right], plan, comparator_calls)?
                == Ordering::Less
        {
            child = right;
        }
        if compare_plan_rows(batch, heap[parent], heap[child], plan, comparator_calls)?
            != Ordering::Less
        {
            break;
        }
        heap.swap(parent, child);
        parent = child;
    }
    Ok(())
}

fn decode_count(bytes: &[u8]) -> Result<u64> {
    let bytes: [u8; 8] = bytes.try_into().map_err(|_| {
        DataFusionError::Execution("bounded sort state count is not eight bytes".to_string())
    })?;
    let count = u64::from_le_bytes(bytes);
    if count == 0 {
        return Err(DataFusionError::Execution(
            "bounded sort state contains a zero count".to_string(),
        ));
    }
    Ok(count)
}

fn validate_plan(plan: &proto::BoundedSort) -> Result<()> {
    let schema = plan.input_schema.as_ref().ok_or_else(|| {
        DataFusionError::Plan("bounded sort is missing its input schema".to_string())
    })?;
    if plan.sort_key_indices.is_empty()
        || plan.sort_key_indices.len() != plan.sort_ascending.len()
        || plan.sort_key_indices.len() != plan.sort_nulls_last.len()
    {
        return Err(DataFusionError::Plan(
            "bounded sort ordering arrays must be equally sized and non-empty".to_string(),
        ));
    }
    if plan
        .sort_key_indices
        .iter()
        .any(|&index| index as usize >= schema.fields.len())
    {
        return Err(DataFusionError::Plan(
            "bounded sort key index is outside the input schema".to_string(),
        ));
    }
    match (plan.limit_start, plan.limit_end) {
        (None, None) => {}
        (Some(start), Some(end)) if start <= end => {}
        (Some(_), Some(_)) => {
            return Err(DataFusionError::Plan(
                "bounded sort limit end precedes its start".to_string(),
            ));
        }
        _ => {
            return Err(DataFusionError::Plan(
                "bounded sort limit start and end must be present together".to_string(),
            ));
        }
    }
    if plan.physical_input_semantics && plan.limit_end.is_none() {
        return Err(DataFusionError::Plan(
            "bounded physical-input sort requires a finite SortLimit range".to_string(),
        ));
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::memory_pool::{tests_support::TestBroker, HostMemoryReservation};
    use arrow::array::{ArrayRef, Int32Array, StringArray};
    use prost::Message;

    #[test]
    fn sorts_counted_rows_and_applies_all_retraction_kinds() {
        let mut processor = new_processor();
        processor
            .process_arrow(batch(
                &[3, 1, 2, 2, 2, 4],
                &["c", "a", "b", "b", "b", "d"],
                &[INSERT, INSERT, INSERT, INSERT, DELETE, UPDATE_AFTER],
            ))
            .unwrap();
        processor
            .process_arrow(batch(&[4], &["d"], &[UPDATE_BEFORE]))
            .unwrap();
        let output = processor.finish().unwrap();
        assert_eq!(integers(&output), vec![1, 2, 3]);
        assert_eq!(strings(&output), vec!["a", "b", "c"]);
        assert_eq!(kinds(&output), vec![INSERT; 3]);
        assert_eq!(processor.statistics()[..5], [2, 2, 5, 5, 0]);
    }

    #[test]
    fn canonical_snapshot_restores_to_an_identical_terminal_sort() {
        let mut source = new_processor();
        source
            .process_arrow(batch(&[4, 1, 3], &["d", "a", "c"], &[INSERT; 3]))
            .unwrap();
        let snapshot = source.snapshot_key_group(0).unwrap();
        let mut restored = new_processor();
        restored.restore_key_group(0, &snapshot).unwrap();
        restored
            .process_arrow(batch(&[2], &["b"], &[INSERT]))
            .unwrap();
        assert_eq!(integers(&restored.finish().unwrap()), vec![1, 2, 3, 4]);
    }

    #[test]
    fn sort_limit_applies_offset_after_counting_duplicates() {
        let mut processor = BoundedSortProcessor::new(
            &limit_plan(2, 5, false),
            0,
            0,
            HostMemoryReservation::new(
                Arc::new(TestBroker::new(64 << 20)),
                "bounded sort limit test",
            ),
        )
        .unwrap();
        processor
            .process_arrow(batch(
                &[1, 1, 2, 3, 4, 5],
                &["a", "a", "b", "c", "d", "e"],
                &[INSERT; 6],
            ))
            .unwrap();
        assert_eq!(integers(&processor.finish().unwrap()), vec![2, 3, 4]);
        assert_eq!(processor.finish().unwrap().num_rows(), 0);
        assert_eq!(processor.statistics()[6], 3);
    }

    #[test]
    fn local_sort_limit_merges_rescaled_owned_key_groups() {
        let source_plan = limit_plan(0, 4, true);
        let mut left = BoundedSortProcessor::new(
            &source_plan,
            0,
            0,
            HostMemoryReservation::new(
                Arc::new(TestBroker::new(64 << 20)),
                "bounded local sort left",
            ),
        )
        .unwrap();
        left.process_arrow(batch(&[5, 1], &["e", "a"], &[INSERT; 2]))
            .unwrap();
        let left_snapshot = left.snapshot_key_group(0).unwrap();

        let mut right = BoundedSortProcessor::new(
            &source_plan,
            1,
            1,
            HostMemoryReservation::new(
                Arc::new(TestBroker::new(64 << 20)),
                "bounded local sort right",
            ),
        )
        .unwrap();
        right
            .process_arrow(batch(&[4, 2], &["d", "b"], &[INSERT; 2]))
            .unwrap();
        let right_snapshot = right.snapshot_key_group(1).unwrap();

        let mut restored = BoundedSortProcessor::new(
            &source_plan,
            0,
            1,
            HostMemoryReservation::new(
                Arc::new(TestBroker::new(64 << 20)),
                "bounded local sort rescaled",
            ),
        )
        .unwrap();
        restored.restore_key_group(0, &left_snapshot).unwrap();
        restored.restore_key_group(1, &right_snapshot).unwrap();
        restored
            .process_arrow(batch(&[3], &["c"], &[INSERT]))
            .unwrap();
        assert_eq!(integers(&restored.finish().unwrap()), vec![1, 2, 3, 4]);
    }

    #[test]
    fn physical_sort_limit_keeps_the_first_rows_at_an_equal_key_cutoff() {
        let mut processor = BoundedSortProcessor::new(
            &physical_limit_plan(0, 2, false),
            0,
            0,
            HostMemoryReservation::new(
                Arc::new(TestBroker::new(64 << 20)),
                "bounded physical sort limit",
            ),
        )
        .unwrap();
        processor
            .process_arrow(batch(
                &[1, 1, 1, 1],
                &["first", "second", "third", "fourth"],
                &[INSERT, DELETE, UPDATE_BEFORE, UPDATE_AFTER],
            ))
            .unwrap();

        let output = processor.finish().unwrap();
        assert_eq!(strings(&output), vec!["first", "second"]);
        assert_eq!(kinds(&output), vec![INSERT, DELETE]);
    }

    #[test]
    fn physical_sort_limit_persists_only_the_bounded_heap_and_skips_unchanged_writes() {
        let mut processor = BoundedSortProcessor::new(
            &physical_limit_plan(0, 2, true),
            0,
            0,
            HostMemoryReservation::new(
                Arc::new(TestBroker::new(64 << 20)),
                "bounded online physical heap",
            ),
        )
        .unwrap();
        processor
            .process_arrow(batch(
                &[1, 2, 3, 4, 5, 6],
                &["a", "b", "c", "d", "e", "f"],
                &[INSERT; 6],
            ))
            .unwrap();
        assert_eq!(
            decode_key_group_snapshot(0, &processor.snapshot_key_group(0).unwrap())
                .unwrap()
                .len(),
            2
        );
        assert_eq!(processor.statistics()[..4], [0, 1, 0, 2]);

        processor
            .process_arrow(batch(&[7, 8], &["g", "h"], &[INSERT; 2]))
            .unwrap();
        assert_eq!(processor.statistics()[..4], [0, 1, 0, 2]);
        assert_eq!(integers(&processor.finish().unwrap()), vec![2, 1]);
    }

    #[test]
    fn canonical_state_moves_between_memory_and_rocksdb_with_batched_io() {
        let Ok(plugin_path) = std::env::var("STREAMFUSION_TEST_ROCKSDB_PLUGIN") else {
            return;
        };
        let broker = Arc::new(TestBroker::new(1 << 30));
        let mut memory = BoundedSortProcessor::new(
            &plan(),
            0,
            0,
            HostMemoryReservation::new(broker.clone(), "bounded sort memory source"),
        )
        .unwrap();
        memory
            .process_arrow(batch(&[3, 1], &["c", "a"], &[INSERT, INSERT]))
            .unwrap();
        assert_eq!(memory.statistics()[..4], [1, 1, 2, 2]);
        let snapshot = memory.snapshot_key_group(0).unwrap();

        let directory = tempfile::tempdir().unwrap();
        let mut rocks = BoundedSortProcessor::new_rocksdb(
            &plan(),
            0,
            0,
            std::path::Path::new(&plugin_path),
            directory.path(),
            64 << 20,
            HostMemoryReservation::new(broker.clone(), "bounded sort RocksDB scratch"),
        )
        .unwrap();
        rocks.restore_key_group(0, &snapshot).unwrap();
        assert_eq!(rocks.snapshot_key_group(0).unwrap(), snapshot);
        rocks.process_arrow(batch(&[2], &["b"], &[INSERT])).unwrap();
        assert_eq!(rocks.statistics()[..4], [1, 1, 1, 1]);
        let rocks_snapshot = rocks.snapshot_key_group(0).unwrap();

        let mut restored = BoundedSortProcessor::new(
            &plan(),
            0,
            0,
            HostMemoryReservation::new(broker, "bounded sort memory restore"),
        )
        .unwrap();
        restored.restore_key_group(0, &rocks_snapshot).unwrap();
        assert_eq!(integers(&restored.finish().unwrap()), vec![1, 2, 3]);
    }

    #[test]
    fn missing_retraction_fails_with_flinks_contract() {
        let mut processor = new_processor();
        let error = processor
            .process_arrow(batch(&[9], &["missing"], &[DELETE]))
            .unwrap_err();
        assert!(error.to_string().contains("RowData not exist!"));
        assert_eq!(processor.statistics()[4], 1);
    }

    #[test]
    fn drains_terminal_output_in_managed_batches_without_splitting_row_counts() {
        let rows = OUTPUT_BATCH_ROWS + 3_616;
        let mut processor = new_processor();
        processor
            .process_arrow(batch(
                &vec![7; rows],
                &vec!["same"; rows],
                &vec![INSERT; rows],
            ))
            .unwrap();

        let first = processor.finish().unwrap();
        let second = processor.finish().unwrap();
        let exhausted = processor.finish().unwrap();
        assert_eq!(first.num_rows(), OUTPUT_BATCH_ROWS);
        assert_eq!(second.num_rows(), 3_616);
        assert_eq!(exhausted.num_rows(), 0);
        assert!(integers(&first).iter().all(|&value| value == 7));
        assert!(integers(&second).iter().all(|&value| value == 7));
        assert_eq!(processor.statistics()[6], rows as u64);
    }

    #[test]
    fn accounts_state_scratch_and_output_and_releases_on_failure() {
        let broker = Arc::new(TestBroker::new(64 << 20));
        let mut processor = BoundedSortProcessor::new(
            &plan(),
            0,
            0,
            HostMemoryReservation::new(broker.clone(), "bounded sort accounting"),
        )
        .unwrap();
        let empty_state = broker.reserved();
        processor
            .process_arrow(batch(&[2, 1], &["second", "first"], &[INSERT; 2]))
            .unwrap();
        assert!(broker.reserved() > empty_state);
        let output = processor.finish().unwrap();
        assert_eq!(output.num_rows(), 2);
        drop(output);
        drop(processor);
        assert_eq!(broker.reserved(), 0);

        let constrained = Arc::new(TestBroker::new(1 << 10));
        let mut processor = BoundedSortProcessor::new(
            &plan(),
            0,
            0,
            HostMemoryReservation::new(constrained.clone(), "bounded sort constrained"),
        )
        .unwrap();
        let payload = "x".repeat(8 << 10);
        let error = processor
            .process_arrow(batch(&[1], &[payload.as_str()], &[INSERT]))
            .unwrap_err();
        assert!(matches!(error, DataFusionError::ResourcesExhausted(_)));
        drop(processor);
        assert_eq!(constrained.reserved(), 0);
    }

    fn new_processor() -> BoundedSortProcessor {
        BoundedSortProcessor::new(
            &plan(),
            0,
            0,
            HostMemoryReservation::new(Arc::new(TestBroker::new(64 << 20)), "bounded sort test"),
        )
        .unwrap()
    }

    fn plan() -> Vec<u8> {
        limit_plan_values(None, None, false)
    }

    fn limit_plan(start: u64, end: u64, local: bool) -> Vec<u8> {
        limit_plan_values(Some(start), Some(end), local)
    }

    fn physical_limit_plan(start: u64, end: u64, local: bool) -> Vec<u8> {
        limit_plan_values_with_semantics(Some(start), Some(end), local, true)
    }

    fn limit_plan_values(start: Option<u64>, end: Option<u64>, local: bool) -> Vec<u8> {
        limit_plan_values_with_semantics(start, end, local, false)
    }

    fn limit_plan_values_with_semantics(
        start: Option<u64>,
        end: Option<u64>,
        local: bool,
        physical_input_semantics: bool,
    ) -> Vec<u8> {
        let schema = proto::Schema {
            fields: vec![
                field(
                    "number",
                    proto::logical_type::Type::Integer(proto::EmptyType::default()),
                ),
                field(
                    "label",
                    proto::logical_type::Type::Varchar(proto::EmptyType::default()),
                ),
            ],
        };
        proto::NativePlan {
            protocol_version: crate::PLAN_PROTOCOL_VERSION,
            root: Some(proto::Operator {
                plan_node_id: 0,
                operator: Some(proto::operator::Operator::BoundedSort(Box::new(
                    proto::BoundedSort {
                        input: None,
                        input_schema: Some(schema),
                        sort_key_indices: vec![0],
                        sort_ascending: vec![true],
                        sort_nulls_last: vec![false],
                        limit_start: start,
                        limit_end: end,
                        use_first_owned_key_group: local,
                        physical_input_semantics,
                        sort_limit_global: !local,
                    },
                ))),
            }),
        }
        .encode_to_vec()
    }

    fn field(name: &str, r#type: proto::logical_type::Type) -> proto::Field {
        proto::Field {
            name: name.to_string(),
            r#type: Some(proto::LogicalType {
                nullable: true,
                r#type: Some(r#type),
            }),
        }
    }

    fn batch(numbers: &[i32], labels: &[&str], row_kinds: &[i8]) -> RecordBatch {
        RecordBatch::try_from_iter(vec![
            (
                "number",
                Arc::new(Int32Array::from(numbers.to_vec())) as ArrayRef,
            ),
            (
                "label",
                Arc::new(StringArray::from(labels.to_vec())) as ArrayRef,
            ),
            (
                INPUT_KIND_COLUMN,
                Arc::new(Int8Array::from(row_kinds.to_vec())) as ArrayRef,
            ),
        ])
        .unwrap()
    }

    fn integers(batch: &RecordBatch) -> Vec<i32> {
        batch
            .column(0)
            .as_any()
            .downcast_ref::<Int32Array>()
            .unwrap()
            .values()
            .to_vec()
    }

    fn strings(batch: &RecordBatch) -> Vec<&str> {
        let values = batch
            .column(1)
            .as_any()
            .downcast_ref::<StringArray>()
            .unwrap();
        (0..values.len()).map(|row| values.value(row)).collect()
    }

    fn kinds(batch: &RecordBatch) -> Vec<i8> {
        batch
            .column(2)
            .as_any()
            .downcast_ref::<Int8Array>()
            .unwrap()
            .values()
            .to_vec()
    }
}
