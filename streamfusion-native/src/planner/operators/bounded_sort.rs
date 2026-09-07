// Copyright 2026 StreamFusion Authors
// Licensed under the Apache License, Version 2.0

use std::cmp::Ordering;
use std::sync::Arc;

mod spill;

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
    spillable: Option<spill::SpillableSort>,
    spill_directory: std::path::PathBuf,
    spill_statistics: (u64, u64),
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
        let state = Box::new(RocksPluginKeyedState::open_for_owner(
            plugin_path,
            database_path,
            first_key_group,
            last_key_group,
            memory_limit,
            &scratch,
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
            spillable: None,
            spill_directory: std::env::temp_dir(),
            spill_statistics: (0, 0),
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
        if self.pending.is_some() || self.spillable.is_some() || self.drained {
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
        let existing = self.state.get_batch(&refs, &self.scratch)?;
        let _loaded_state_workspace =
            crate::state::reserve_decoded_values(&existing, &self.scratch)?;
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
            let snapshot = self.state.snapshot_key_group(key_group, &self.scratch)?;
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

    pub(crate) fn configure_spill_directory(
        &mut self,
        directory: std::path::PathBuf,
    ) -> Result<()> {
        if self.pending.is_some() || self.spillable.is_some() || self.drained {
            return Err(DataFusionError::Execution(
                "cannot change sort spill directory after output starts".to_string(),
            ));
        }
        self.spill_directory = directory;
        Ok(())
    }

    pub(crate) fn spill_statistics(&self) -> [u64; 2] {
        let (files, bytes) = self.spillable.as_ref().map_or(
            self.spill_statistics,
            spill::SpillableSort::spill_statistics,
        );
        [files, bytes]
    }

    pub(crate) fn finish(&mut self) -> Result<RecordBatch> {
        if !self.plan.physical_input_semantics {
            return self.finish_spillable();
        }
        if self.drained {
            return Ok(RecordBatch::new_empty(Arc::clone(&self.output_schema)));
        }
        if self.pending.is_none() {
            if let Err(error) = self.prepare_physical_output() {
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

    fn finish_spillable(&mut self) -> Result<RecordBatch> {
        if self.drained {
            return Ok(RecordBatch::new_empty(Arc::clone(&self.output_schema)));
        }
        if self.spillable.is_none() {
            self.scratch.resize(0)?;
            self.spillable = Some(spill::SpillableSort::prepare(
                self.state.as_ref(),
                self.first_key_group..=self.last_key_group,
                Arc::clone(&self.input_schema),
                &self.row_converter,
                &self.plan,
                &self.scratch,
                &self.spill_directory,
            )?);
        }
        let sort = self.spillable.as_mut().unwrap();
        let next = sort.next(Arc::clone(&self.output_schema), &mut self.scratch)?;
        self.spill_statistics = sort.spill_statistics();
        match next {
            Some(batch) => {
                let bytes = batch.get_array_memory_size();
                self.scratch.resize(bytes)?;
                self.scratch.transfer_to_arrow(bytes)?;
                self.emitted_rows = self.emitted_rows.saturating_add(batch.num_rows() as u64);
                Ok(batch)
            }
            None => {
                self.spillable = None;
                self.drained = true;
                self.scratch.resize(0)?;
                Ok(RecordBatch::new_empty(Arc::clone(&self.output_schema)))
            }
        }
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

    pub(crate) fn state_memory(&self) -> HostMemoryReservation {
        self.scratch.sibling("native state transfer")
    }

    pub(crate) fn snapshot_key_group(&self, key_group: u32) -> Result<crate::state::SnapshotBytes> {
        self.state.snapshot_key_group(key_group, &self.scratch)
    }

    pub(crate) fn restore_key_group(&mut self, key_group: u32, bytes: &[u8]) -> Result<()> {
        self.state
            .restore_key_group(key_group, bytes, &self.scratch)?;
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
    if !plan.physical_input_semantics {
        let arrow = arrow_schema(schema)?;
        for index in &plan.sort_key_indices {
            if super::top_n::compare::data_type_can_have_nan(
                arrow.field(*index as usize).data_type(),
            ) {
                return Err(DataFusionError::Plan("bounded sort floating-point ordering is not equivalent to Flink's NaN/signed-zero comparator".to_string()));
            }
        }
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
mod tests;
