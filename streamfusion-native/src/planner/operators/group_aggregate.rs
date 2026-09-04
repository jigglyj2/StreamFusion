// Copyright 2026 StreamFusion Authors
// Licensed under the Apache License, Version 2.0

use std::cmp::Ordering;
use std::collections::BTreeMap;
use std::sync::Arc;

use ahash::RandomState;
use arrow::array::{
    Array, ArrayRef, BinaryArray, BooleanArray, Date32Array, Decimal128Array, Float32Array,
    Float64Array, Int16Array, Int32Array, Int64Array, Int8Array, StringArray,
    Time32MillisecondArray, Time32SecondArray, Time64MicrosecondArray, Time64NanosecondArray,
    TimestampMicrosecondArray, TimestampMillisecondArray, TimestampNanosecondArray,
    TimestampSecondArray, UInt32Array,
};
use arrow::datatypes::{DataType, Field, Schema, SchemaRef, TimeUnit};
use arrow::record_batch::RecordBatch;
use arrow::row::{RowConverter, SortField};
use datafusion::error::{DataFusionError, Result};
use hashbrown::HashMap;

use crate::exchange::binary_row_hash;
use crate::exchange::{assign_key_group, encode_binary_row, KeyField};
use crate::memory_pool::HostMemoryReservation;
use crate::planner::expressions::null_literal;
use crate::planner::operators::select_distinct::{apply_count_change, CountChange};
use crate::state::{
    KeyedState, MemoryKeyedState, RocksPluginKeyedState, StateKey, StateKeyRef, StateMutation,
};
use crate::{decode_plan, proto};

const INSERT: i8 = 0;
const UPDATE_BEFORE: i8 = 1;
const UPDATE_AFTER: i8 = 2;
const DELETE: i8 = 3;
const STATE_MAGIC: &[u8; 4] = b"SFGA";
const STATE_VERSION: u8 = 5;

/// Persistent timer-free keyed aggregation handle shared by memory and RocksDB state.
pub(crate) struct GroupAggregateProcessor {
    plan: proto::GroupAggregate,
    calls: Vec<Call>,
    max_parallelism: u32,
    state: Box<dyn KeyedState>,
    input_schema: Option<SchemaRef>,
    key_fields: Vec<(usize, KeyField)>,
    preencoded_key_index: Option<usize>,
    input_kind_index: Option<usize>,
    visible_count: Option<usize>,
    scratch_reservation: HostMemoryReservation,
    bundle_reservation: HostMemoryReservation,
    grouping_converter: Option<RowConverter>,
    output_schema: Option<SchemaRef>,
    pending: HashMap<StateKey, PendingGroup, RandomState>,
    pending_order: Vec<StateKey>,
    pending_elements: usize,
    partial_input: bool,
    state_read_batches: u64,
    state_write_batches: u64,
}

#[derive(Clone)]
pub(super) struct Call {
    pub(super) function: proto::AggregateFunction,
    pub(super) input_index: Option<usize>,
    pub(super) filter_index: Option<usize>,
    pub(super) distinct: bool,
    pub(super) input_type: Option<DataType>,
    pub(super) output_type: DataType,
    pub(super) retractable: bool,
}

impl Call {
    fn average_accumulator_type(&self) -> DataType {
        match self
            .input_type
            .as_ref()
            .expect("AVG has a validated input type")
        {
            DataType::Int8 | DataType::Int16 | DataType::Int32 | DataType::Int64 => DataType::Int64,
            DataType::Float32 | DataType::Float64 => DataType::Float64,
            DataType::Decimal128(_, scale) => DataType::Decimal128(38, *scale),
            other => unreachable!("validated AVG input type {other}"),
        }
    }
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub(super) struct AccumulatorState {
    pub(super) row_count: i64,
    accumulators: Vec<Accumulator>,
}

#[derive(Clone, Debug, PartialEq, Eq)]
enum Accumulator {
    Count(i64),
    DistinctCount {
        count: i64,
        values: BTreeMap<AggregateValue, i64>,
    },
    Sum {
        value: Option<AggregateValue>,
        count: i64,
    },
    DistinctSum {
        value: Option<AggregateValue>,
        count: i64,
        values: BTreeMap<AggregateValue, i64>,
    },
    Average {
        value: Option<AggregateValue>,
        count: i64,
    },
    DistinctAverage {
        value: Option<AggregateValue>,
        count: i64,
        values: BTreeMap<AggregateValue, i64>,
    },
    AppendExtremum(Option<AggregateValue>),
    Extremum(BTreeMap<AggregateValue, i64>),
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub(super) enum AggregateValue {
    Boolean(bool),
    Int(i128),
    Float32(u32),
    Float64(u64),
    Bytes(Vec<u8>),
}

impl PartialOrd for AggregateValue {
    fn partial_cmp(&self, other: &Self) -> Option<Ordering> {
        Some(self.cmp(other))
    }
}

impl Ord for AggregateValue {
    fn cmp(&self, other: &Self) -> Ordering {
        match (self, other) {
            (Self::Boolean(left), Self::Boolean(right)) => left.cmp(right),
            (Self::Int(left), Self::Int(right)) => left.cmp(right),
            (Self::Float32(left), Self::Float32(right)) => flink_f32_cmp(*left, *right),
            (Self::Float64(left), Self::Float64(right)) => flink_f64_cmp(*left, *right),
            (Self::Bytes(left), Self::Bytes(right)) => left.cmp(right),
            _ => value_tag(self).cmp(&value_tag(other)),
        }
    }
}

struct OutputEvents {
    input_rows: Vec<u32>,
    row_kinds: Vec<i8>,
    values: Vec<Vec<Option<AggregateValue>>>,
}

struct PendingGroup {
    grouping_row: Vec<u8>,
    original: Option<AccumulatorState>,
    current: Option<AccumulatorState>,
}

struct BundleOutputEvents {
    grouping_rows: Vec<Vec<u8>>,
    row_kinds: Vec<i8>,
    values: Vec<Vec<Option<AggregateValue>>>,
}

impl BundleOutputEvents {
    fn new(calls: usize) -> Self {
        Self {
            grouping_rows: Vec::new(),
            row_kinds: Vec::new(),
            values: (0..calls).map(|_| Vec::new()).collect(),
        }
    }

    fn push(&mut self, grouping_row: Vec<u8>, row_kind: i8, values: Vec<Option<AggregateValue>>) {
        debug_assert_eq!(values.len(), self.values.len());
        self.grouping_rows.push(grouping_row);
        self.row_kinds.push(row_kind);
        for (column, value) in self.values.iter_mut().zip(values) {
            column.push(value);
        }
    }
}

impl OutputEvents {
    fn with_capacity(rows: usize, calls: usize) -> Self {
        Self {
            input_rows: Vec::with_capacity(rows),
            row_kinds: Vec::with_capacity(rows),
            values: (0..calls).map(|_| Vec::with_capacity(rows)).collect(),
        }
    }

    fn push(&mut self, input_row: u32, row_kind: i8, values: Vec<Option<AggregateValue>>) {
        debug_assert_eq!(values.len(), self.values.len());
        self.input_rows.push(input_row);
        self.row_kinds.push(row_kind);
        for (column, value) in self.values.iter_mut().zip(values) {
            column.push(value);
        }
    }
}

impl GroupAggregateProcessor {
    pub(crate) fn new(
        serialized_plan: &[u8],
        max_parallelism: u32,
        first_key_group: u32,
        last_key_group: u32,
        state_reservation: HostMemoryReservation,
    ) -> Result<Self> {
        let scratch_reservation =
            state_reservation.sibling("native group aggregate batch scratch and output");
        let state = Box::new(MemoryKeyedState::new(
            first_key_group,
            last_key_group,
            state_reservation,
        )?);
        Self::with_state(serialized_plan, max_parallelism, state, scratch_reservation)
    }

    pub(crate) fn new_rocksdb(
        serialized_plan: &[u8],
        max_parallelism: u32,
        first_key_group: u32,
        last_key_group: u32,
        plugin_path: &std::path::Path,
        database_path: &std::path::Path,
        memory_limit: usize,
        scratch_reservation: HostMemoryReservation,
    ) -> Result<Self> {
        let state = Box::new(RocksPluginKeyedState::open(
            plugin_path,
            database_path,
            first_key_group,
            last_key_group,
            memory_limit,
        )?);
        Self::with_state(serialized_plan, max_parallelism, state, scratch_reservation)
    }

    fn with_state(
        serialized_plan: &[u8],
        max_parallelism: u32,
        state: Box<dyn KeyedState>,
        scratch_reservation: HostMemoryReservation,
    ) -> Result<Self> {
        let native_plan = decode_plan(serialized_plan)?;
        let root = native_plan
            .root
            .ok_or_else(|| DataFusionError::Plan("group aggregate plan has no root".to_string()))?;
        let (plan, partial_input) = match root.operator {
            Some(proto::operator::Operator::GroupAggregate(plan)) => (*plan, false),
            Some(proto::operator::Operator::GlobalGroupAggregate(plan)) => {
                let plan = *plan;
                (
                    proto::GroupAggregate {
                        input: plan.input,
                        grouping_indices: plan.grouping_indices,
                        aggregate_calls: plan.aggregate_calls,
                        generate_update_before: plan.generate_update_before,
                        input_changelog: false,
                        mini_batch_size: plan.mini_batch_size,
                        input_schema: plan.input_schema,
                        output_schema: plan.output_schema,
                    },
                    true,
                )
            }
            _ => {
                return Err(DataFusionError::Plan(
                    "stateful group aggregate handle requires a GroupAggregate or GlobalGroupAggregate root"
                        .to_string(),
                ));
            }
        };
        validate_plan(&plan, max_parallelism)?;
        let calls = plan
            .aggregate_calls
            .iter()
            .map(lower_call)
            .collect::<Result<Vec<_>>>()?;
        if plan.input_changelog && calls.iter().any(|call| !call.retractable) {
            return Err(DataFusionError::Plan(
                "changelog group aggregate requires retractable aggregate calls".to_string(),
            ));
        }
        let bundle_reservation =
            scratch_reservation.sibling("native group aggregate pending mini-batch");
        let (grouping_converter, output_schema) = planned_group_output(&plan)?;
        Ok(Self {
            plan,
            calls,
            max_parallelism,
            state,
            input_schema: None,
            key_fields: Vec::new(),
            preencoded_key_index: None,
            input_kind_index: None,
            visible_count: None,
            scratch_reservation,
            bundle_reservation,
            grouping_converter,
            output_schema,
            pending: HashMap::with_hasher(RandomState::new()),
            pending_order: Vec::new(),
            pending_elements: 0,
            partial_input,
            state_read_batches: 0,
            state_write_batches: 0,
        })
    }

    pub(crate) fn process_arrow(&mut self, batch: RecordBatch) -> Result<RecordBatch> {
        // Input arrays are borrowed through Arrow C Data and remain charged to their Java
        // owner. Reserve only the native key/index/event structures created for this call.
        let base_reservation = batch
            .num_rows()
            .saturating_mul(192usize.saturating_add(self.calls.len().saturating_mul(64)));
        self.scratch_reservation.resize(base_reservation)?;
        let result = if self.partial_input {
            self.process_partial_mini_accounted(batch, base_reservation)
        } else if self.plan.mini_batch_size == 0 {
            self.process_arrow_accounted(batch, base_reservation)
        } else {
            self.process_mini_batch_accounted(batch, base_reservation)
        };
        match result {
            Ok(output) => {
                let output_bytes = output.get_array_memory_size();
                self.scratch_reservation
                    .resize(output_bytes.max(base_reservation))?;
                // The Java Arrow importer wraps these Rust-owned buffers without copying.
                // Move their existing host reservation to that owner instead of charging both.
                self.scratch_reservation.transfer_to_arrow(output_bytes)?;
                self.scratch_reservation.resize(0)?;
                Ok(output)
            }
            Err(error) => {
                self.scratch_reservation.resize(0)?;
                Err(error)
            }
        }
    }

    fn process_arrow_accounted(
        &mut self,
        batch: RecordBatch,
        base_reservation: usize,
    ) -> Result<RecordBatch> {
        self.prepare_schema(batch.schema(), batch.num_columns())?;
        let (unique_keys, row_key_indices) =
            if self.plan.grouping_indices.is_empty() && batch.num_rows() != 0 {
                // Flink forces a global aggregate through a singleton exchange. Its sole key is the
                // zero-field BinaryRow, so hashing and allocating the same key once per input row is
                // pure overhead. Retain the normal keyed-state path while materializing that key once.
                (vec![self.state_key(&batch, 0)?], vec![0; batch.num_rows()])
            } else {
                let mut unique_indices =
                    HashMap::<StateKey, usize, RandomState>::with_capacity_and_hasher(
                        batch.num_rows(),
                        RandomState::new(),
                    );
                let mut row_key_indices = Vec::with_capacity(batch.num_rows());
                for row in 0..batch.num_rows() {
                    let state_key = self.state_key(&batch, row)?;
                    let next_index = unique_indices.len();
                    let index = *unique_indices.entry(state_key).or_insert(next_index);
                    row_key_indices.push(index);
                }
                let mut ordered_keys = (0..unique_indices.len()).map(|_| None).collect::<Vec<_>>();
                for (key, index) in unique_indices.drain() {
                    ordered_keys[index] = Some(key);
                }
                let unique_keys = ordered_keys
                    .into_iter()
                    .map(|key| key.expect("every group aggregate key index is populated"))
                    .collect::<Vec<_>>();
                (unique_keys, row_key_indices)
            };
        let unique_key_refs = unique_keys
            .iter()
            .map(|key| StateKeyRef {
                key_group: key.key_group,
                key: &key.key,
            })
            .collect::<Vec<_>>();
        let existing = self.state.get_batch(&unique_key_refs)?;
        self.state_read_batches = self.state_read_batches.saturating_add(1);
        drop(unique_key_refs);
        let serialized_state_bytes = existing.iter().fold(0usize, |bytes, value| {
            bytes.saturating_add(value.as_deref().map_or(0, <[u8]>::len))
        });
        // Decoding retractable MIN/MAX expands packed pairs into BTree nodes. Four times the
        // canonical bytes is a conservative admission bound for that transient representation.
        self.scratch_reservation
            .resize(base_reservation.saturating_add(serialized_state_bytes.saturating_mul(4)))?;
        let mut staged_values = existing
            .iter()
            .map(|value| {
                value
                    .as_deref()
                    .map(|bytes| decode_state(bytes, &self.calls))
                    .transpose()
            })
            .collect::<Result<Vec<_>>>()?;
        drop(existing);
        let mut touched = vec![false; unique_keys.len()];
        let mut events = OutputEvents::with_capacity(batch.num_rows() * 2, self.calls.len());

        for row in 0..batch.num_rows() {
            let key_index = row_key_indices[row];
            let accumulate = self.accumulates(&batch, row)?;
            if self.calls.is_empty() {
                let previous_count = staged_values[key_index]
                    .as_ref()
                    .map(|state| state.row_count);
                let input_row = u32::try_from(row).map_err(|_| {
                    DataFusionError::Execution(
                        "select distinct batch exceeds UInt32 indexing".to_string(),
                    )
                })?;
                match apply_count_change(previous_count, accumulate) {
                    CountChange::Ignored => continue,
                    CountChange::Present(count, emit_insert) => {
                        staged_values[key_index] = Some(AccumulatorState {
                            row_count: count,
                            accumulators: Vec::new(),
                        });
                        if emit_insert {
                            events.push(input_row, INSERT, Vec::new());
                        }
                    }
                    CountChange::Removed => {
                        staged_values[key_index] = Some(AccumulatorState {
                            row_count: 0,
                            accumulators: Vec::new(),
                        });
                        events.push(input_row, DELETE, Vec::new());
                    }
                }
                touched[key_index] = true;
                continue;
            }
            let first_row = staged_values[key_index]
                .as_ref()
                .is_none_or(|state| state.row_count == 0);
            if first_row && !accumulate {
                continue;
            }
            let current =
                staged_values[key_index].get_or_insert_with(|| AccumulatorState::new(&self.calls));
            let previous_values = current.values(&self.calls);
            current.apply(&self.calls, &batch, row, accumulate)?;
            let current_values = current.values(&self.calls);
            let input_row = u32::try_from(row).map_err(|_| {
                DataFusionError::Execution(
                    "group aggregate batch exceeds UInt32 indexing".to_string(),
                )
            })?;

            if current.row_count == 0 {
                if !first_row {
                    events.push(input_row, DELETE, previous_values);
                }
            } else if first_row {
                events.push(input_row, INSERT, current_values);
            } else if previous_values != current_values {
                if self.plan.generate_update_before {
                    events.push(input_row, UPDATE_BEFORE, previous_values);
                }
                events.push(input_row, UPDATE_AFTER, current_values);
            }

            touched[key_index] = true;
        }

        let mutations = unique_keys
            .into_iter()
            .zip(staged_values.into_iter().zip(touched))
            .filter_map(|(key, (state, touched))| {
                touched.then(|| {
                    let state = state.expect("a touched group aggregate state exists");
                    StateMutation {
                        key,
                        value: (state.row_count != 0).then(|| encode_state(&state)),
                    }
                })
            })
            .collect();
        self.state.write_batch(mutations)?;
        self.state_write_batches = self.state_write_batches.saturating_add(1);
        self.output_batch(&batch, events)
    }

    fn process_mini_batch_accounted(
        &mut self,
        batch: RecordBatch,
        base_reservation: usize,
    ) -> Result<RecordBatch> {
        self.prepare_schema(batch.schema(), batch.num_columns())?;
        let admitted = self.estimated_pending_bytes().saturating_add(
            batch
                .num_rows()
                .saturating_mul(192usize.saturating_add(self.calls.len().saturating_mul(64))),
        );
        self.bundle_reservation.resize(admitted)?;
        let grouping_rows = self.encode_grouping_rows(&batch)?;
        let mut events = BundleOutputEvents::new(self.calls.len());
        let trigger = usize::try_from(self.plan.mini_batch_size).map_err(|_| {
            DataFusionError::Plan("group aggregate mini-batch size exceeds usize".to_string())
        })?;
        let mut offset = 0;
        while offset < batch.num_rows() {
            let remaining = trigger - self.pending_elements;
            let length = remaining.min(batch.num_rows() - offset);
            self.stage_mini_range(&batch, &grouping_rows, offset, length)?;
            self.pending_elements += length;
            offset += length;
            if self.pending_elements == trigger {
                self.finish_pending(&mut events)?;
            }
        }
        self.bundle_reservation
            .resize(self.estimated_pending_bytes())?;
        let output = self.bundle_output_batch(events)?;
        let output_bytes = output.get_array_memory_size();
        self.scratch_reservation
            .resize(output_bytes.max(base_reservation))?;
        Ok(output)
    }

    fn process_partial_mini_accounted(
        &mut self,
        batch: RecordBatch,
        base_reservation: usize,
    ) -> Result<RecordBatch> {
        self.prepare_schema(batch.schema(), batch.num_columns())?;
        let grouping_rows = self.encode_grouping_rows(&batch)?;
        let mut events = BundleOutputEvents::new(self.calls.len());
        let trigger = usize::try_from(self.plan.mini_batch_size).map_err(|_| {
            DataFusionError::Plan(
                "global group aggregate mini-batch size exceeds usize".to_string(),
            )
        })?;
        let mut offset = 0;
        while offset < batch.num_rows() {
            let remaining = trigger - self.pending_elements;
            let length = remaining.min(batch.num_rows() - offset);
            self.stage_partial_range(&batch, &grouping_rows, offset, length)?;
            self.pending_elements += length;
            offset += length;
            if self.pending_elements == trigger {
                self.finish_pending(&mut events)?;
            }
        }
        self.bundle_reservation
            .resize(self.estimated_pending_bytes())?;
        let output = self.bundle_output_batch(events)?;
        let output_bytes = output.get_array_memory_size();
        self.scratch_reservation
            .resize(output_bytes.max(base_reservation))?;
        Ok(output)
    }

    fn stage_partial_range(
        &mut self,
        batch: &RecordBatch,
        grouping_rows: &[Vec<u8>],
        offset: usize,
        length: usize,
    ) -> Result<()> {
        let mut unique = HashMap::<StateKey, usize, RandomState>::with_capacity_and_hasher(
            length,
            RandomState::new(),
        );
        let mut keys = Vec::new();
        let mut first_rows = Vec::new();
        let mut row_indices = Vec::with_capacity(length);
        for row in offset..offset + length {
            let key = self.state_key(batch, row)?;
            let next = keys.len();
            let index = match unique.entry(key.clone()) {
                hashbrown::hash_map::Entry::Occupied(entry) => *entry.get(),
                hashbrown::hash_map::Entry::Vacant(entry) => {
                    entry.insert(next);
                    keys.push(key);
                    first_rows.push(row);
                    next
                }
            };
            row_indices.push(index);
        }
        let missing = keys
            .iter()
            .enumerate()
            .filter_map(|(index, key)| (!self.pending.contains_key(key)).then_some(index))
            .collect::<Vec<_>>();
        let refs = missing
            .iter()
            .map(|&index| StateKeyRef {
                key_group: keys[index].key_group,
                key: &keys[index].key,
            })
            .collect::<Vec<_>>();
        let existing = if refs.is_empty() {
            Vec::new()
        } else {
            let existing = self.state.get_batch(&refs)?;
            self.state_read_batches = self.state_read_batches.saturating_add(1);
            existing
        };
        for (&index, value) in missing.iter().zip(existing) {
            let original = value
                .as_deref()
                .map(|bytes| decode_state(bytes, &self.calls))
                .transpose()?;
            self.pending_order.push(keys[index].clone());
            self.pending.insert(
                keys[index].clone(),
                PendingGroup {
                    grouping_row: grouping_rows[first_rows[index]].clone(),
                    current: Some(
                        original
                            .clone()
                            .unwrap_or_else(|| AccumulatorState::new(&self.calls)),
                    ),
                    original,
                },
            );
        }
        let accumulator_index = self
            .visible_count
            .expect("partial aggregate schema was prepared")
            - 1;
        let accumulators = batch
            .column(accumulator_index)
            .as_any()
            .downcast_ref::<BinaryArray>()
            .expect("partial accumulator type was validated");
        for (local_row, &key_index) in row_indices.iter().enumerate() {
            let row = offset + local_row;
            if accumulators.is_null(row) {
                return Err(DataFusionError::Execution(
                    "local aggregate accumulator cannot be null".to_string(),
                ));
            }
            let partial = decode_state(accumulators.value(row), &self.calls)?;
            self.pending
                .get_mut(&keys[key_index])
                .expect("every global mini-batch key was staged")
                .current
                .as_mut()
                .expect("global partial accumulation always has a current state")
                .merge(&self.calls, &partial)?;
        }
        Ok(())
    }

    fn stage_mini_range(
        &mut self,
        batch: &RecordBatch,
        grouping_rows: &[Vec<u8>],
        offset: usize,
        length: usize,
    ) -> Result<()> {
        let mut unique = HashMap::<StateKey, usize, RandomState>::with_capacity_and_hasher(
            length,
            RandomState::new(),
        );
        let mut keys = Vec::new();
        let mut first_rows = Vec::new();
        let mut row_indices = Vec::with_capacity(length);
        for row in offset..offset + length {
            let key = self.state_key(batch, row)?;
            let next = keys.len();
            let index = match unique.entry(key.clone()) {
                hashbrown::hash_map::Entry::Occupied(entry) => *entry.get(),
                hashbrown::hash_map::Entry::Vacant(entry) => {
                    entry.insert(next);
                    keys.push(key);
                    first_rows.push(row);
                    next
                }
            };
            row_indices.push(index);
        }

        let missing = keys
            .iter()
            .enumerate()
            .filter_map(|(index, key)| (!self.pending.contains_key(key)).then_some(index))
            .collect::<Vec<_>>();
        let refs = missing
            .iter()
            .map(|&index| StateKeyRef {
                key_group: keys[index].key_group,
                key: &keys[index].key,
            })
            .collect::<Vec<_>>();
        let existing = if refs.is_empty() {
            Vec::new()
        } else {
            let existing = self.state.get_batch(&refs)?;
            self.state_read_batches = self.state_read_batches.saturating_add(1);
            existing
        };
        for (&index, value) in missing.iter().zip(existing) {
            let original = value
                .as_deref()
                .map(|bytes| decode_state(bytes, &self.calls))
                .transpose()?;
            self.pending_order.push(keys[index].clone());
            self.pending.insert(
                keys[index].clone(),
                PendingGroup {
                    grouping_row: grouping_rows[first_rows[index]].clone(),
                    current: original.clone(),
                    original,
                },
            );
        }

        for (local_row, &key_index) in row_indices.iter().enumerate() {
            let row = offset + local_row;
            let accumulate = self.accumulates(batch, row)?;
            let entry = self
                .pending
                .get_mut(&keys[key_index])
                .expect("every mini-batch key was staged");
            if self.calls.is_empty() {
                let previous = entry.current.as_ref().map(|state| state.row_count);
                match apply_count_change(previous, accumulate) {
                    CountChange::Ignored => {}
                    CountChange::Present(count, _) => {
                        entry.current = Some(AccumulatorState {
                            row_count: count,
                            accumulators: Vec::new(),
                        });
                    }
                    CountChange::Removed => {
                        entry.current = Some(AccumulatorState {
                            row_count: 0,
                            accumulators: Vec::new(),
                        });
                    }
                }
                continue;
            }
            let empty = entry
                .current
                .as_ref()
                .is_none_or(|state| state.row_count == 0);
            if empty && !accumulate {
                continue;
            }
            entry
                .current
                .get_or_insert_with(|| AccumulatorState::new(&self.calls))
                .apply(&self.calls, batch, row, accumulate)?;
        }
        Ok(())
    }

    fn finish_pending(&mut self, events: &mut BundleOutputEvents) -> Result<()> {
        let mut order = std::mem::take(&mut self.pending_order);
        sort_flink_hashmap_keys(&mut order, |key| &key.key);
        let mut mutations = Vec::with_capacity(order.len());
        for key in order {
            let group = self
                .pending
                .remove(&key)
                .expect("pending mini-batch order and map remain synchronized");
            let first_row = group
                .original
                .as_ref()
                .is_none_or(|state| state.row_count == 0);
            let Some(current) = group.current else {
                continue;
            };
            let previous_values = group
                .original
                .as_ref()
                .map(|state| state.values(&self.calls))
                .unwrap_or_else(|| vec![None; self.calls.len()]);
            if current.row_count == 0 {
                if !first_row {
                    events.push(group.grouping_row, DELETE, previous_values);
                    mutations.push(StateMutation { key, value: None });
                }
                continue;
            }
            let current_values = current.values(&self.calls);
            mutations.push(StateMutation {
                key,
                value: Some(encode_state(&current)),
            });
            if first_row {
                events.push(group.grouping_row, INSERT, current_values);
            } else if previous_values != current_values {
                if self.plan.generate_update_before {
                    events.push(group.grouping_row.clone(), UPDATE_BEFORE, previous_values);
                }
                events.push(group.grouping_row, UPDATE_AFTER, current_values);
            }
        }
        if !mutations.is_empty() {
            self.state.write_batch(mutations)?;
            self.state_write_batches = self.state_write_batches.saturating_add(1);
        }
        self.pending_elements = 0;
        Ok(())
    }

    pub(crate) fn finish_bundle(&mut self) -> Result<RecordBatch> {
        let mut events = BundleOutputEvents::new(self.calls.len());
        self.finish_pending(&mut events)?;
        self.bundle_reservation.resize(0)?;
        let output = self.bundle_output_batch(events)?;
        let output_bytes = output.get_array_memory_size();
        self.scratch_reservation.resize(output_bytes)?;
        self.scratch_reservation.transfer_to_arrow(output_bytes)?;
        self.scratch_reservation.resize(0)?;
        Ok(output)
    }

    pub(crate) fn pending_element_count(&self) -> usize {
        self.pending_elements
    }

    pub(crate) fn pending_key_count(&self) -> usize {
        self.pending.len()
    }

    pub(crate) fn statistics(&self) -> [u64; 2] {
        [self.state_read_batches, self.state_write_batches]
    }

    fn estimated_pending_bytes(&self) -> usize {
        let map_storage = self
            .pending
            .capacity()
            .saturating_mul(std::mem::size_of::<(StateKey, PendingGroup)>().saturating_add(16));
        let order_storage = self
            .pending_order
            .capacity()
            .saturating_mul(std::mem::size_of::<StateKey>());
        self.pending.iter().fold(
            map_storage.saturating_add(order_storage),
            |bytes, (key, group)| {
                bytes
                    // The map key and the order vector deliberately own independent key bytes.
                    .saturating_add(key.key.capacity().saturating_mul(2))
                    .saturating_add(group.grouping_row.capacity())
                    .saturating_add(
                        group
                            .original
                            .as_ref()
                            .map_or(0, AccumulatorState::estimated_dynamic_bytes),
                    )
                    .saturating_add(
                        group
                            .current
                            .as_ref()
                            .map_or(0, AccumulatorState::estimated_dynamic_bytes),
                    )
            },
        )
    }

    fn encode_grouping_rows(&self, batch: &RecordBatch) -> Result<Vec<Vec<u8>>> {
        if self.plan.grouping_indices.is_empty() {
            return Ok((0..batch.num_rows()).map(|_| Vec::new()).collect());
        }
        let columns = self
            .plan
            .grouping_indices
            .iter()
            .map(|&index| Arc::clone(batch.column(index as usize)))
            .collect::<Vec<_>>();
        let rows = self
            .grouping_converter
            .as_ref()
            .expect("grouping converter was negotiated")
            .convert_columns(&columns)?;
        Ok((0..batch.num_rows())
            .map(|row| rows.row(row).as_ref().to_vec())
            .collect())
    }

    fn bundle_output_batch(&self, events: BundleOutputEvents) -> Result<RecordBatch> {
        let mut columns = if self.plan.grouping_indices.is_empty() {
            Vec::new()
        } else {
            let converter = self
                .grouping_converter
                .as_ref()
                .expect("grouping converter was negotiated");
            let parser = converter.parser();
            converter.convert_rows(events.grouping_rows.iter().map(|row| parser.parse(row)))?
        };
        for (call, values) in self.calls.iter().zip(events.values) {
            columns.push(aggregate_array(&values, &call.output_type)?);
        }
        columns.push(Arc::new(Int8Array::from(events.row_kinds)) as ArrayRef);
        Ok(RecordBatch::try_new(
            Arc::clone(self.output_schema.as_ref().ok_or_else(|| {
                DataFusionError::Execution(
                    "group aggregate mini-batch output schema is not negotiated".to_string(),
                )
            })?),
            columns,
        )?)
    }

    fn accumulates(&self, batch: &RecordBatch, row: usize) -> Result<bool> {
        if !self.plan.input_changelog {
            return Ok(true);
        }
        let kinds = batch
            .column(self.input_kind_index.expect("schema prepared"))
            .as_any()
            .downcast_ref::<Int8Array>()
            .ok_or_else(|| {
                DataFusionError::Execution(
                    "group aggregate input RowKind metadata is not Arrow Int8".to_string(),
                )
            })?;
        match kinds.value(row) {
            INSERT | UPDATE_AFTER => Ok(true),
            UPDATE_BEFORE | DELETE => Ok(false),
            other => Err(DataFusionError::Execution(format!(
                "unknown Flink RowKind byte {other}"
            ))),
        }
    }

    fn output_batch(&self, input: &RecordBatch, events: OutputEvents) -> Result<RecordBatch> {
        let indices = UInt32Array::from(events.input_rows);
        let mut columns =
            Vec::<ArrayRef>::with_capacity(self.plan.grouping_indices.len() + self.calls.len() + 1);
        let mut fields = Vec::with_capacity(columns.capacity());
        for &index in &self.plan.grouping_indices {
            let index = index as usize;
            columns.push(arrow::compute::take(
                input.column(index).as_ref(),
                &indices,
                None,
            )?);
            fields.push(input.schema().field(index).clone());
        }
        for (call_index, call) in self.calls.iter().enumerate() {
            columns.push(aggregate_array(
                &events.values[call_index],
                &call.output_type,
            )?);
            fields.push(Field::new(
                format!("aggregate_{call_index}"),
                call.output_type.clone(),
                !matches!(
                    call.function,
                    proto::AggregateFunction::CountStar | proto::AggregateFunction::Count
                ),
            ));
        }
        columns.push(Arc::new(Int8Array::from(events.row_kinds)));
        fields.push(Field::new("__streamfusion_row_kind", DataType::Int8, false));
        Ok(RecordBatch::try_new(
            Arc::new(Schema::new(fields)),
            columns,
        )?)
    }

    fn state_key(&self, batch: &RecordBatch, row: usize) -> Result<StateKey> {
        let key = match self.preencoded_key_index {
            Some(index) => batch
                .column(index)
                .as_any()
                .downcast_ref::<arrow::array::BinaryArray>()
                .ok_or_else(|| {
                    DataFusionError::Execution(
                        "group aggregate preencoded key column is not Arrow Binary".to_string(),
                    )
                })?
                .value(row)
                .to_vec(),
            None => encode_binary_row(batch, row, &self.key_fields)?,
        };
        Ok(StateKey {
            key_group: assign_key_group(&key, self.max_parallelism),
            key,
        })
    }

    fn prepare_schema(&mut self, schema: SchemaRef, column_count: usize) -> Result<()> {
        if let Some(expected) = &self.input_schema {
            if expected.as_ref() != schema.as_ref() {
                return Err(DataFusionError::Execution(
                    "group aggregate input schema changed while the operator was running"
                        .to_string(),
                ));
            }
            return Ok(());
        }
        self.preencoded_key_index = schema
            .fields()
            .iter()
            .position(|field| field.name() == "__streamfusion_key");
        self.input_kind_index = schema
            .fields()
            .iter()
            .position(|field| field.name() == "__streamfusion_input_row_kind");
        let visible_count = [
            self.preencoded_key_index,
            self.input_kind_index,
            Some(column_count),
        ]
        .into_iter()
        .flatten()
        .min()
        .ok_or_else(|| {
            DataFusionError::Execution("group aggregate input has no visible columns".to_string())
        })?;
        if self.plan.input_changelog && self.input_kind_index.is_none() {
            return Err(DataFusionError::Execution(
                "changelog group aggregate requires input RowKind metadata".to_string(),
            ));
        }
        if self.preencoded_key_index.is_none() {
            self.key_fields = self
                .plan
                .grouping_indices
                .iter()
                .map(|&index| {
                    let index = index as usize;
                    let field = schema
                        .fields()
                        .get(index)
                        .filter(|_| index < visible_count)
                        .ok_or_else(|| {
                            DataFusionError::Plan(format!(
                                "grouping index {index} is outside {visible_count} input fields"
                            ))
                        })?;
                    Ok((index, KeyField::from_arrow_type(field.data_type())?))
                })
                .collect::<std::result::Result<Vec<_>, arrow::error::ArrowError>>()?;
        }
        let grouping_fields = self
            .plan
            .grouping_indices
            .iter()
            .map(|&index| {
                let index = index as usize;
                schema
                    .fields()
                    .get(index)
                    .filter(|_| index < visible_count)
                    .cloned()
                    .ok_or_else(|| {
                        DataFusionError::Plan(format!(
                            "grouping index {index} is outside {visible_count} input fields"
                        ))
                    })
            })
            .collect::<Result<Vec<_>>>()?;
        if self.grouping_converter.is_none() {
            let sort_fields = grouping_fields
                .iter()
                .map(|field| SortField::new(field.data_type().clone()))
                .collect::<Vec<_>>();
            if !RowConverter::supports_fields(&sort_fields) {
                return Err(DataFusionError::Plan(
                    "group aggregate key type is not supported by Arrow row encoding".to_string(),
                ));
            }
            self.grouping_converter = Some(RowConverter::new(sort_fields)?);
        }
        if self.output_schema.is_none() {
            let mut fields = grouping_fields;
            fields.extend(self.calls.iter().enumerate().map(|(index, call)| {
                Arc::new(Field::new(
                    format!("aggregate_{index}"),
                    call.output_type.clone(),
                    !matches!(
                        call.function,
                        proto::AggregateFunction::CountStar | proto::AggregateFunction::Count
                    ),
                ))
            }));
            fields.push(Arc::new(Field::new(
                "__streamfusion_row_kind",
                DataType::Int8,
                false,
            )));
            self.output_schema = Some(Arc::new(Schema::new(fields)));
        }
        if self.partial_input {
            if visible_count != self.plan.grouping_indices.len() + 1
                || schema
                    .fields()
                    .get(visible_count - 1)
                    .is_none_or(|field| field.data_type() != &DataType::Binary)
            {
                return Err(DataFusionError::Plan(
                    "global group aggregate expects grouping fields followed by one BINARY accumulator"
                        .to_string(),
                ));
            }
        } else {
            for call in &self.calls {
                if let Some(index) = call.input_index {
                    let actual = schema
                        .fields()
                        .get(index)
                        .filter(|_| index < visible_count)
                        .ok_or_else(|| {
                            DataFusionError::Plan(format!(
                                "aggregate input index {index} is outside {visible_count} input fields"
                            ))
                        })?;
                    if Some(actual.data_type()) != call.input_type.as_ref() {
                        return Err(DataFusionError::Plan(format!(
                            "aggregate input {index} expected {:?}, got {}",
                            call.input_type,
                            actual.data_type()
                        )));
                    }
                }
            }
        }
        self.visible_count = Some(visible_count);
        self.input_schema = Some(schema);
        Ok(())
    }

    pub(crate) fn snapshot_key_group(&self, key_group: u32) -> Result<Vec<u8>> {
        self.state.snapshot_key_group(key_group)
    }

    pub(crate) fn checkpoint(&self, directory: &std::path::Path) -> Result<()> {
        self.state.checkpoint(directory)
    }

    pub(crate) fn restore_key_group(&mut self, key_group: u32, bytes: &[u8]) -> Result<()> {
        self.state.restore_key_group(key_group, bytes)
    }
}

pub(super) fn sort_flink_hashmap_keys<T>(keys: &mut [T], bytes: impl Fn(&T) -> &[u8]) {
    if keys.len() < 2 {
        return;
    }
    let mut capacity = 16usize;
    while keys.len() > capacity.saturating_mul(3) / 4 {
        capacity = capacity.saturating_mul(2);
    }
    keys.sort_by_key(|key| {
        let hash = binary_row_hash(bytes(key)) as u32;
        let spread = hash ^ (hash >> 16);
        spread as usize & (capacity - 1)
    });
}

impl AccumulatorState {
    pub(super) fn new(calls: &[Call]) -> Self {
        Self {
            row_count: 0,
            accumulators: calls
                .iter()
                .map(|call| match call.function {
                    proto::AggregateFunction::CountStar | proto::AggregateFunction::Count => {
                        if call.distinct {
                            Accumulator::DistinctCount {
                                count: 0,
                                values: BTreeMap::new(),
                            }
                        } else {
                            Accumulator::Count(0)
                        }
                    }
                    proto::AggregateFunction::Sum => {
                        if call.distinct {
                            Accumulator::DistinctSum {
                                value: None,
                                count: 0,
                                values: BTreeMap::new(),
                            }
                        } else {
                            Accumulator::Sum {
                                value: None,
                                count: 0,
                            }
                        }
                    }
                    proto::AggregateFunction::Avg => {
                        if call.distinct {
                            Accumulator::DistinctAverage {
                                value: Some(zero_value(&call.average_accumulator_type())),
                                count: 0,
                                values: BTreeMap::new(),
                            }
                        } else {
                            Accumulator::Average {
                                value: Some(zero_value(&call.average_accumulator_type())),
                                count: 0,
                            }
                        }
                    }
                    proto::AggregateFunction::Min | proto::AggregateFunction::Max => {
                        if call.retractable {
                            Accumulator::Extremum(BTreeMap::new())
                        } else {
                            Accumulator::AppendExtremum(None)
                        }
                    }
                    _ => unreachable!("validated aggregate function"),
                })
                .collect(),
        }
    }

    pub(super) fn estimated_dynamic_bytes(&self) -> usize {
        self.accumulators
            .capacity()
            .saturating_mul(std::mem::size_of::<Accumulator>())
            .saturating_add(self.accumulators.iter().fold(0usize, |bytes, accumulator| {
                bytes.saturating_add(match accumulator {
                    Accumulator::Count(_) => 0,
                    Accumulator::DistinctCount { values, .. } | Accumulator::Extremum(values) => {
                        estimated_value_map_bytes(values)
                    }
                    Accumulator::Sum { value, .. }
                    | Accumulator::Average { value, .. }
                    | Accumulator::AppendExtremum(value) => {
                        value.as_ref().map_or(0, AggregateValue::dynamic_bytes)
                    }
                    Accumulator::DistinctSum { value, values, .. }
                    | Accumulator::DistinctAverage { value, values, .. } => value
                        .as_ref()
                        .map_or(0, AggregateValue::dynamic_bytes)
                        .saturating_add(estimated_value_map_bytes(values)),
                })
            }))
    }

    /// Creates an accumulator seed from the result at the end of an already processed prefix.
    /// OVER aggregation uses this only to accumulate later rows; it never retracts from this
    /// compact seed. Keeping just the current extremum is therefore sufficient and avoids storing
    /// a complete accumulator alongside every ordered row.
    pub(super) fn from_prefix_values(
        calls: &[Call],
        values: &[Option<AggregateValue>],
    ) -> Result<Self> {
        if calls.len() != values.len() {
            return Err(DataFusionError::Internal(
                "aggregate prefix value count does not match its calls".to_string(),
            ));
        }
        let accumulators = calls
            .iter()
            .zip(values)
            .map(|(call, value)| match call.function {
                proto::AggregateFunction::CountStar | proto::AggregateFunction::Count => {
                    let count = match value {
                        Some(AggregateValue::Int(value)) => {
                            i64::try_from(*value).map_err(|_| {
                                DataFusionError::Execution(
                                    "OVER COUNT prefix does not fit i64".to_string(),
                                )
                            })?
                        }
                        None => 0,
                        Some(_) => {
                            return Err(DataFusionError::Internal(
                                "OVER COUNT prefix has a non-integer value".to_string(),
                            ));
                        }
                    };
                    Ok(Accumulator::Count(count))
                }
                proto::AggregateFunction::Sum => Ok(Accumulator::Sum {
                    value: value.clone(),
                    // Only zero versus non-zero affects accumulation output. This seed is never
                    // retracted, so the exact historical non-null count is not needed.
                    count: i64::from(value.is_some()),
                }),
                proto::AggregateFunction::Avg => Err(DataFusionError::Internal(
                    "OVER AVG cannot reconstruct its sum and count from a compact prefix"
                        .to_string(),
                )),
                proto::AggregateFunction::Min | proto::AggregateFunction::Max => {
                    if call.retractable {
                        let mut extrema = BTreeMap::new();
                        if let Some(value) = value.clone() {
                            extrema.insert(value, 1);
                        }
                        Ok(Accumulator::Extremum(extrema))
                    } else {
                        Ok(Accumulator::AppendExtremum(value.clone()))
                    }
                }
                _ => unreachable!("validated aggregate function"),
            })
            .collect::<Result<Vec<_>>>()?;
        Ok(Self {
            row_count: 1,
            accumulators,
        })
    }

    pub(super) fn apply(
        &mut self,
        calls: &[Call],
        batch: &RecordBatch,
        row: usize,
        accumulate: bool,
    ) -> Result<()> {
        let values = row_aggregate_values(calls, batch, row)?;
        let active = calls
            .iter()
            .map(|call| aggregate_filter(call, batch, row))
            .collect::<Result<Vec<_>>>()?;
        self.apply_values_with_activity(calls, &values, &active, accumulate)
    }

    pub(super) fn apply_values(
        &mut self,
        calls: &[Call],
        values: &[Option<AggregateValue>],
        accumulate: bool,
    ) -> Result<()> {
        let active = vec![true; calls.len()];
        self.apply_values_with_activity(calls, values, &active, accumulate)
    }

    fn apply_values_with_activity(
        &mut self,
        calls: &[Call],
        values: &[Option<AggregateValue>],
        active: &[bool],
        accumulate: bool,
    ) -> Result<()> {
        if values.len() != calls.len() {
            return Err(DataFusionError::Internal(format!(
                "aggregate contribution has {} values for {} calls",
                values.len(),
                calls.len()
            )));
        }
        let delta = if accumulate { 1 } else { -1 };
        self.row_count = self.row_count.wrapping_add(delta);
        for (((call, accumulator), value), active) in calls
            .iter()
            .zip(&mut self.accumulators)
            .zip(values)
            .zip(active)
        {
            if !active {
                continue;
            }
            match accumulator {
                Accumulator::Count(count) => {
                    let present = call.input_index.is_none() || value.is_some();
                    if present {
                        *count = count.wrapping_add(delta);
                    }
                }
                Accumulator::DistinctCount { count, values } => {
                    if let Some(value) = value.as_ref() {
                        let previous = values.get(value).copied().unwrap_or_default();
                        let current = previous.wrapping_add(delta);
                        if current == 0 {
                            values.remove(value);
                        } else {
                            values.insert(value.clone(), current);
                        }
                        if (accumulate && previous == 0) || (!accumulate && current == 0) {
                            *count = count.wrapping_add(delta);
                        }
                    }
                }
                Accumulator::Sum { value: sum, count } => {
                    if let Some(value) = value.as_ref() {
                        *sum = if let Some(current) = sum.as_ref() {
                            if accumulate {
                                aggregate_add(current, value, &call.output_type)?
                            } else {
                                aggregate_sub(current, value, &call.output_type)?
                            }
                        } else if accumulate {
                            Some(value.clone())
                        } else {
                            aggregate_sub(&zero_value(&call.output_type), value, &call.output_type)?
                        };
                        *count = count.wrapping_add(delta);
                    }
                }
                Accumulator::DistinctSum {
                    value: sum,
                    count,
                    values,
                } => {
                    if let Some(value) = value.as_ref() {
                        let previous = values.get(value).copied().unwrap_or_default();
                        let current = previous.wrapping_add(delta);
                        if current == 0 {
                            values.remove(value);
                        } else {
                            values.insert(value.clone(), current);
                        }
                        if (accumulate && previous == 0) || (!accumulate && current == 0) {
                            *sum = if let Some(current_sum) = sum.as_ref() {
                                if accumulate {
                                    aggregate_add(current_sum, value, &call.output_type)?
                                } else {
                                    aggregate_sub(current_sum, value, &call.output_type)?
                                }
                            } else if accumulate {
                                Some(value.clone())
                            } else {
                                aggregate_sub(
                                    &zero_value(&call.output_type),
                                    value,
                                    &call.output_type,
                                )?
                            };
                            *count = count.wrapping_add(delta);
                        }
                    }
                }
                Accumulator::Average { value: sum, count } => {
                    if let Some(value) = value.as_ref() {
                        let contribution = average_contribution(value, call)?;
                        *sum = if let Some(current) = sum.as_ref() {
                            if accumulate {
                                aggregate_add(
                                    current,
                                    &contribution,
                                    &call.average_accumulator_type(),
                                )?
                            } else {
                                aggregate_sub(
                                    current,
                                    &contribution,
                                    &call.average_accumulator_type(),
                                )?
                            }
                        } else {
                            None
                        };
                        *count = count.wrapping_add(delta);
                    }
                }
                Accumulator::DistinctAverage {
                    value: sum,
                    count,
                    values,
                } => {
                    if let Some(value) = value.as_ref() {
                        let previous = values.get(value).copied().unwrap_or_default();
                        let current = previous.wrapping_add(delta);
                        if current == 0 {
                            values.remove(value);
                        } else {
                            values.insert(value.clone(), current);
                        }
                        if (accumulate && previous == 0) || (!accumulate && current == 0) {
                            let contribution = average_contribution(value, call)?;
                            *sum = if let Some(current_sum) = sum.as_ref() {
                                if accumulate {
                                    aggregate_add(
                                        current_sum,
                                        &contribution,
                                        &call.average_accumulator_type(),
                                    )?
                                } else {
                                    aggregate_sub(
                                        current_sum,
                                        &contribution,
                                        &call.average_accumulator_type(),
                                    )?
                                }
                            } else {
                                None
                            };
                            *count = count.wrapping_add(delta);
                        }
                    }
                }
                Accumulator::AppendExtremum(extremum) => {
                    if let Some(value) = value.as_ref() {
                        *extremum = Some(match (extremum.as_ref(), call.function) {
                            (Some(current), proto::AggregateFunction::Min) => {
                                flink_append_extremum(current, value, true)
                            }
                            (Some(current), proto::AggregateFunction::Max) => {
                                flink_append_extremum(current, value, false)
                            }
                            (None, _) => value.clone(),
                            _ => unreachable!("validated extremum function"),
                        });
                    }
                }
                Accumulator::Extremum(values) => {
                    if let Some(value) = value.as_ref() {
                        let count = values.entry(value.clone()).or_default();
                        *count = count.wrapping_add(delta);
                        if *count == 0 {
                            values.remove(&value);
                        }
                    }
                }
            }
        }
        Ok(())
    }

    /// Merge another accumulator namespace into this one using the same arithmetic and
    /// extremum semantics as row-by-row accumulation. Session windows use this when Flink's
    /// merging-window set coalesces namespaces; replaying every historical input row here is
    /// both unnecessary and quadratic for long-lived sessions.
    pub(super) fn merge(&mut self, calls: &[Call], other: &Self) -> Result<()> {
        if self.accumulators.len() != calls.len() || other.accumulators.len() != calls.len() {
            return Err(DataFusionError::Internal(
                "aggregate accumulator shape does not match its calls".to_string(),
            ));
        }
        self.row_count = self.row_count.wrapping_add(other.row_count);
        for ((call, accumulator), other) in calls
            .iter()
            .zip(&mut self.accumulators)
            .zip(&other.accumulators)
        {
            match (accumulator, other) {
                (Accumulator::Count(value), Accumulator::Count(other)) => {
                    *value = value.wrapping_add(*other);
                }
                (
                    Accumulator::DistinctCount { count, values },
                    Accumulator::DistinctCount {
                        values: other_values,
                        ..
                    },
                ) => {
                    for (value, delta) in other_values {
                        let previous = values.get(value).copied().unwrap_or_default();
                        let current = previous.wrapping_add(*delta);
                        if current == 0 {
                            values.remove(value);
                        } else {
                            values.insert(value.clone(), current);
                        }
                        *count =
                            count.wrapping_add(i64::from(current > 0) - i64::from(previous > 0));
                    }
                }
                (
                    Accumulator::DistinctSum {
                        value,
                        count,
                        values,
                    },
                    Accumulator::DistinctSum {
                        values: other_values,
                        ..
                    },
                ) => {
                    for (other_value, delta) in other_values {
                        let previous = values.get(other_value).copied().unwrap_or_default();
                        let current = previous.wrapping_add(*delta);
                        if current == 0 {
                            values.remove(other_value);
                        } else {
                            values.insert(other_value.clone(), current);
                        }
                        match (previous > 0, current > 0) {
                            (false, true) => {
                                *value = match value.as_ref() {
                                    Some(sum) => {
                                        aggregate_add(sum, other_value, &call.output_type)?
                                    }
                                    None if *count == 0 => Some(other_value.clone()),
                                    None => None,
                                };
                                *count = count.wrapping_add(1);
                            }
                            (true, false) => {
                                *value = match value.as_ref() {
                                    Some(sum) => {
                                        aggregate_sub(sum, other_value, &call.output_type)?
                                    }
                                    None => None,
                                };
                                *count = count.wrapping_sub(1);
                            }
                            _ => {}
                        }
                    }
                }
                (
                    Accumulator::DistinctAverage {
                        value,
                        count,
                        values,
                    },
                    Accumulator::DistinctAverage {
                        values: other_values,
                        ..
                    },
                ) => {
                    for (other_value, delta) in other_values {
                        let previous = values.get(other_value).copied().unwrap_or_default();
                        let current = previous.wrapping_add(*delta);
                        if current == 0 {
                            values.remove(other_value);
                        } else {
                            values.insert(other_value.clone(), current);
                        }
                        let contribution = average_contribution(other_value, call)?;
                        match (previous > 0, current > 0) {
                            (false, true) => {
                                *value = if let Some(sum) = value.as_ref() {
                                    aggregate_add(
                                        sum,
                                        &contribution,
                                        &call.average_accumulator_type(),
                                    )?
                                } else {
                                    None
                                };
                                *count = count.wrapping_add(1);
                            }
                            (true, false) => {
                                *value = if let Some(sum) = value.as_ref() {
                                    aggregate_sub(
                                        sum,
                                        &contribution,
                                        &call.average_accumulator_type(),
                                    )?
                                } else {
                                    None
                                };
                                *count = count.wrapping_sub(1);
                            }
                            _ => {}
                        }
                    }
                }
                (
                    Accumulator::Sum { value, count },
                    Accumulator::Sum {
                        value: other_value,
                        count: other_count,
                    },
                ) => {
                    if *other_count != 0 {
                        *value = match (value.as_ref(), other_value.as_ref()) {
                            (Some(left), Some(right)) => {
                                aggregate_add(left, right, &call.output_type)?
                            }
                            (None, Some(right)) if *count == 0 => Some(right.clone()),
                            // A populated SUM with no value represents an overflowed decimal.
                            // Preserve that state rather than incorrectly resurrecting a value.
                            _ => None,
                        };
                        *count = count.wrapping_add(*other_count);
                    }
                }
                (
                    Accumulator::Average { value, count },
                    Accumulator::Average {
                        value: other_value,
                        count: other_count,
                    },
                ) => {
                    if *other_count != 0 {
                        *value = match (value.as_ref(), other_value.as_ref()) {
                            (Some(left), Some(right)) => {
                                aggregate_add(left, right, &call.average_accumulator_type())?
                            }
                            _ => None,
                        };
                        *count = count.wrapping_add(*other_count);
                    }
                }
                (Accumulator::AppendExtremum(value), Accumulator::AppendExtremum(other_value)) => {
                    if let Some(other_value) = other_value {
                        *value = Some(match (value.as_ref(), call.function) {
                            (Some(current), proto::AggregateFunction::Min) => {
                                flink_append_extremum(current, other_value, true)
                            }
                            (Some(current), proto::AggregateFunction::Max) => {
                                flink_append_extremum(current, other_value, false)
                            }
                            (None, _) => other_value.clone(),
                            _ => unreachable!("validated extremum function"),
                        });
                    }
                }
                (Accumulator::Extremum(values), Accumulator::Extremum(other_values)) => {
                    for (value, other_count) in other_values {
                        let count = values.entry(value.clone()).or_default();
                        *count = count.wrapping_add(*other_count);
                        if *count == 0 {
                            values.remove(value);
                        }
                    }
                }
                _ => {
                    return Err(DataFusionError::Internal(
                        "aggregate accumulator variants do not match their calls".to_string(),
                    ));
                }
            }
        }
        Ok(())
    }

    pub(super) fn values(&self, calls: &[Call]) -> Vec<Option<AggregateValue>> {
        calls
            .iter()
            .zip(&self.accumulators)
            .map(|(call, accumulator)| match accumulator {
                Accumulator::Count(value) => Some(AggregateValue::Int(*value as i128)),
                Accumulator::DistinctCount { count, .. } => {
                    Some(AggregateValue::Int(*count as i128))
                }
                Accumulator::Sum { value, count } => {
                    if *count == 0 {
                        None
                    } else {
                        value.clone()
                    }
                }
                Accumulator::DistinctSum { value, count, .. } => {
                    if *count == 0 {
                        None
                    } else {
                        value.clone()
                    }
                }
                Accumulator::Average { value, count }
                | Accumulator::DistinctAverage { value, count, .. } => {
                    average_value(value.as_ref(), *count, call)
                }
                Accumulator::AppendExtremum(value) => value.clone(),
                Accumulator::Extremum(values) => match call.function {
                    proto::AggregateFunction::Min => values
                        .iter()
                        .find_map(|(value, count)| (*count > 0).then(|| value.clone())),
                    proto::AggregateFunction::Max => values
                        .iter()
                        .rev()
                        .find_map(|(value, count)| (*count > 0).then(|| value.clone())),
                    _ => unreachable!("validated extremum function"),
                },
            })
            .collect()
    }
}

fn estimated_value_map_bytes(values: &BTreeMap<AggregateValue, i64>) -> usize {
    values.iter().fold(0usize, |bytes, (value, _)| {
        bytes
            // Include the key/value pair plus conservative B-tree node links and occupancy slack.
            .saturating_add(std::mem::size_of::<(AggregateValue, i64)>())
            .saturating_add(64)
            .saturating_add(value.dynamic_bytes())
    })
}

impl AggregateValue {
    fn dynamic_bytes(&self) -> usize {
        match self {
            Self::Bytes(value) => value.capacity(),
            _ => 0,
        }
    }
}

pub(super) fn row_aggregate_values(
    calls: &[Call],
    batch: &RecordBatch,
    row: usize,
) -> Result<Vec<Option<AggregateValue>>> {
    calls
        .iter()
        .map(|call| match call.input_index {
            Some(index) if call.function == proto::AggregateFunction::Count && !call.distinct => {
                Ok((!batch.column(index).is_null(row)).then_some(AggregateValue::Boolean(true)))
            }
            Some(index) => aggregate_value(batch.column(index).as_ref(), row),
            None => Ok(None),
        })
        .collect()
}

fn aggregate_filter(call: &Call, batch: &RecordBatch, row: usize) -> Result<bool> {
    let Some(index) = call.filter_index else {
        return Ok(true);
    };
    let filter = batch
        .column(index)
        .as_any()
        .downcast_ref::<BooleanArray>()
        .ok_or_else(|| {
            DataFusionError::Execution(format!(
                "aggregate FILTER input {index} is not Arrow Boolean"
            ))
        })?;
    Ok(!filter.is_null(row) && filter.value(row))
}

fn validate_plan(_plan: &proto::GroupAggregate, max_parallelism: u32) -> Result<()> {
    if max_parallelism == 0 {
        return Err(DataFusionError::Plan(
            "group aggregate max parallelism must be positive".to_string(),
        ));
    }
    if _plan.mini_batch_size > usize::MAX as u64 {
        return Err(DataFusionError::Plan(
            "group aggregate mini-batch size exceeds usize".to_string(),
        ));
    }
    if _plan.mini_batch_size > 0 && (_plan.input_schema.is_none() || _plan.output_schema.is_none())
    {
        return Err(DataFusionError::Plan(
            "mini-batch group aggregate requires input and output schemas".to_string(),
        ));
    }
    Ok(())
}

fn planned_group_output(
    plan: &proto::GroupAggregate,
) -> Result<(Option<RowConverter>, Option<SchemaRef>)> {
    let (Some(input), Some(output)) = (&plan.input_schema, &plan.output_schema) else {
        return Ok((None, None));
    };
    let input = crate::planner::arrow_schema(input)?;
    let output = crate::planner::arrow_schema(output)?;
    if output.fields().len() != plan.grouping_indices.len() + plan.aggregate_calls.len() {
        return Err(DataFusionError::Plan(
            "group aggregate output schema does not match keys and calls".to_string(),
        ));
    }
    let sort_fields = plan
        .grouping_indices
        .iter()
        .map(|&index| {
            input
                .fields()
                .get(index as usize)
                .map(|field| SortField::new(field.data_type().clone()))
                .ok_or_else(|| {
                    DataFusionError::Plan(format!(
                        "group aggregate grouping index {index} is outside the planned input"
                    ))
                })
        })
        .collect::<Result<Vec<_>>>()?;
    if !RowConverter::supports_fields(&sort_fields) {
        return Err(DataFusionError::Plan(
            "group aggregate key type is not supported by Arrow row encoding".to_string(),
        ));
    }
    let converter = RowConverter::new(sort_fields)?;
    let mut fields = output.fields().iter().cloned().collect::<Vec<_>>();
    fields.push(Arc::new(Field::new(
        "__streamfusion_row_kind",
        DataType::Int8,
        false,
    )));
    Ok((Some(converter), Some(Arc::new(Schema::new(fields)))))
}

pub(super) fn lower_call(call: &proto::AggregateCall) -> Result<Call> {
    let function = proto::AggregateFunction::try_from(call.function).map_err(|_| {
        DataFusionError::Plan(format!("unknown aggregate function {}", call.function))
    })?;
    if function == proto::AggregateFunction::Unspecified {
        return Err(DataFusionError::Plan(
            "aggregate function is unspecified".to_string(),
        ));
    }
    let input_index = call.input_index.map(|index| index as usize);
    let filter_index = call.filter_index.map(|index| index as usize);
    if function == proto::AggregateFunction::CountStar && input_index.is_some() {
        return Err(DataFusionError::Plan(
            "COUNT(*) must not name an input field".to_string(),
        ));
    }
    if function == proto::AggregateFunction::CountStar && call.distinct {
        return Err(DataFusionError::Plan(
            "COUNT(DISTINCT *) is not a valid aggregate call".to_string(),
        ));
    }
    if function != proto::AggregateFunction::CountStar && input_index.is_none() {
        return Err(DataFusionError::Plan(
            "aggregate call requires an input field".to_string(),
        ));
    }
    let input_type = call
        .input_type
        .as_ref()
        .map(null_literal::data_type)
        .transpose()?;
    let output_type =
        null_literal::data_type(call.output_type.as_ref().ok_or_else(|| {
            DataFusionError::Plan("aggregate output type is missing".to_string())
        })?)?;
    let accumulator_type = match call.accumulator_type.as_ref() {
        Some(data_type) => null_literal::data_type(data_type)?,
        None => output_type.clone(),
    };
    match function {
        proto::AggregateFunction::CountStar | proto::AggregateFunction::Count => {
            if output_type != DataType::Int64 {
                return Err(DataFusionError::Plan(format!(
                    "COUNT output must be BIGINT, got {output_type}"
                )));
            }
        }
        proto::AggregateFunction::Sum => {
            ensure_sum_type(&output_type)?;
            ensure_sum_type(input_type.as_ref().expect("SUM input was validated"))?;
        }
        proto::AggregateFunction::Avg => {
            let input_type = input_type.as_ref().expect("AVG input was validated");
            ensure_average_types(input_type, &accumulator_type, &output_type)?;
        }
        proto::AggregateFunction::Min | proto::AggregateFunction::Max => {
            ensure_extremum_type(&output_type)?;
            let input_type = input_type.as_ref().expect("extremum input was validated");
            ensure_extremum_type(input_type)?;
            if input_type != &output_type {
                return Err(DataFusionError::Plan(format!(
                    "MIN/MAX input {input_type} does not match output {output_type}"
                )));
            }
        }
        proto::AggregateFunction::Unspecified => unreachable!("validated aggregate function"),
    }
    Ok(Call {
        function,
        input_index,
        filter_index,
        distinct: call.distinct,
        input_type,
        output_type,
        retractable: call.retractable,
    })
}

fn ensure_average_types(input: &DataType, accumulator: &DataType, output: &DataType) -> Result<()> {
    let valid = match input {
        DataType::Int8 => accumulator == &DataType::Int64 && output == &DataType::Int8,
        DataType::Int16 => accumulator == &DataType::Int64 && output == &DataType::Int16,
        DataType::Int32 => accumulator == &DataType::Int64 && output == &DataType::Int32,
        DataType::Int64 => accumulator == &DataType::Int64 && output == &DataType::Int64,
        DataType::Float32 => accumulator == &DataType::Float64 && output == &DataType::Float32,
        DataType::Float64 => accumulator == &DataType::Float64 && output == &DataType::Float64,
        DataType::Decimal128(_, input_scale) => {
            matches!(accumulator, DataType::Decimal128(38, sum_scale) if sum_scale == input_scale)
                && matches!(output, DataType::Decimal128(38, output_scale) if output_scale >= input_scale)
        }
        _ => false,
    };
    if valid {
        Ok(())
    } else {
        Err(DataFusionError::Plan(format!(
            "group aggregate AVG requires a Flink-compatible input/buffer/output triple, got {input}/{accumulator}/{output}"
        )))
    }
}

fn ensure_sum_type(data_type: &DataType) -> Result<()> {
    if matches!(
        data_type,
        DataType::Int8
            | DataType::Int16
            | DataType::Int32
            | DataType::Int64
            | DataType::Float32
            | DataType::Float64
            | DataType::Decimal128(_, _)
    ) {
        Ok(())
    } else {
        Err(DataFusionError::Plan(format!(
            "group aggregate SUM type {data_type} is not supported"
        )))
    }
}

fn ensure_extremum_type(data_type: &DataType) -> Result<()> {
    if matches!(
        data_type,
        DataType::Int8
            | DataType::Int16
            | DataType::Int32
            | DataType::Int64
            | DataType::Float32
            | DataType::Float64
            | DataType::Decimal128(_, _)
            | DataType::Boolean
            | DataType::Utf8
            | DataType::Date32
            | DataType::Time32(TimeUnit::Second | TimeUnit::Millisecond)
            | DataType::Time64(TimeUnit::Microsecond | TimeUnit::Nanosecond)
            | DataType::Timestamp(
                TimeUnit::Second
                    | TimeUnit::Millisecond
                    | TimeUnit::Microsecond
                    | TimeUnit::Nanosecond,
                _
            )
    ) {
        Ok(())
    } else {
        Err(DataFusionError::Plan(format!(
            "group aggregate MIN/MAX type {data_type} is not supported"
        )))
    }
}

fn aggregate_value(array: &dyn Array, row: usize) -> Result<Option<AggregateValue>> {
    if array.is_null(row) {
        return Ok(None);
    }
    let value = match array.data_type() {
        DataType::Int8 => array
            .as_any()
            .downcast_ref::<Int8Array>()
            .unwrap()
            .value(row) as i128,
        DataType::Int16 => array
            .as_any()
            .downcast_ref::<Int16Array>()
            .unwrap()
            .value(row) as i128,
        DataType::Int32 => array
            .as_any()
            .downcast_ref::<Int32Array>()
            .unwrap()
            .value(row) as i128,
        DataType::Int64 => array
            .as_any()
            .downcast_ref::<Int64Array>()
            .unwrap()
            .value(row) as i128,
        DataType::Decimal128(_, _) => array
            .as_any()
            .downcast_ref::<Decimal128Array>()
            .unwrap()
            .value(row),
        DataType::Float32 => {
            return Ok(Some(float32_value(
                array
                    .as_any()
                    .downcast_ref::<Float32Array>()
                    .unwrap()
                    .value(row),
            )))
        }
        DataType::Float64 => {
            return Ok(Some(float64_value(
                array
                    .as_any()
                    .downcast_ref::<Float64Array>()
                    .unwrap()
                    .value(row),
            )))
        }
        DataType::Boolean => {
            return Ok(Some(AggregateValue::Boolean(
                array
                    .as_any()
                    .downcast_ref::<BooleanArray>()
                    .unwrap()
                    .value(row),
            )))
        }
        DataType::Utf8 => {
            return Ok(Some(AggregateValue::Bytes(
                array
                    .as_any()
                    .downcast_ref::<StringArray>()
                    .unwrap()
                    .value(row)
                    .as_bytes()
                    .to_vec(),
            )))
        }
        DataType::Date32 => array
            .as_any()
            .downcast_ref::<Date32Array>()
            .unwrap()
            .value(row) as i128,
        DataType::Time32(TimeUnit::Second) => array
            .as_any()
            .downcast_ref::<Time32SecondArray>()
            .unwrap()
            .value(row) as i128,
        DataType::Time32(TimeUnit::Millisecond) => array
            .as_any()
            .downcast_ref::<Time32MillisecondArray>()
            .unwrap()
            .value(row) as i128,
        DataType::Time64(TimeUnit::Microsecond) => array
            .as_any()
            .downcast_ref::<Time64MicrosecondArray>()
            .unwrap()
            .value(row) as i128,
        DataType::Time64(TimeUnit::Nanosecond) => array
            .as_any()
            .downcast_ref::<Time64NanosecondArray>()
            .unwrap()
            .value(row) as i128,
        DataType::Timestamp(TimeUnit::Second, _) => array
            .as_any()
            .downcast_ref::<TimestampSecondArray>()
            .unwrap()
            .value(row) as i128,
        DataType::Timestamp(TimeUnit::Millisecond, _) => array
            .as_any()
            .downcast_ref::<TimestampMillisecondArray>()
            .unwrap()
            .value(row) as i128,
        DataType::Timestamp(TimeUnit::Microsecond, _) => array
            .as_any()
            .downcast_ref::<TimestampMicrosecondArray>()
            .unwrap()
            .value(row) as i128,
        DataType::Timestamp(TimeUnit::Nanosecond, _) => array
            .as_any()
            .downcast_ref::<TimestampNanosecondArray>()
            .unwrap()
            .value(row) as i128,
        other => {
            return Err(DataFusionError::Execution(format!(
                "group aggregate cannot read input type {other}"
            )));
        }
    };
    Ok(Some(AggregateValue::Int(value)))
}

pub(super) fn aggregate_array(
    values: &[Option<AggregateValue>],
    data_type: &DataType,
) -> Result<ArrayRef> {
    Ok(match data_type {
        DataType::Int8 => Arc::new(Int8Array::from_iter(
            values
                .iter()
                .map(|value| value.as_ref().map(int_value).transpose())
                .collect::<Result<Vec<_>>>()?
                .into_iter()
                .map(|value| value.map(|value| value as i8)),
        )),
        DataType::Int16 => Arc::new(Int16Array::from_iter(
            values
                .iter()
                .map(|value| value.as_ref().map(int_value).transpose())
                .collect::<Result<Vec<_>>>()?
                .into_iter()
                .map(|value| value.map(|value| value as i16)),
        )),
        DataType::Int32 => Arc::new(Int32Array::from_iter(
            values
                .iter()
                .map(|value| value.as_ref().map(int_value).transpose())
                .collect::<Result<Vec<_>>>()?
                .into_iter()
                .map(|value| value.map(|value| value as i32)),
        )),
        DataType::Int64 => Arc::new(Int64Array::from_iter(
            values
                .iter()
                .map(|value| value.as_ref().map(int_value).transpose())
                .collect::<Result<Vec<_>>>()?
                .into_iter()
                .map(|value| value.map(|value| value as i64)),
        )),
        DataType::Decimal128(precision, scale) => Arc::new(
            Decimal128Array::from_iter(
                values
                    .iter()
                    .map(|value| value.as_ref().map(int_value).transpose())
                    .collect::<Result<Vec<_>>>()?,
            )
            .with_precision_and_scale(*precision, *scale)?,
        ),
        DataType::Float32 => Arc::new(Float32Array::from_iter(float32_options(values)?)),
        DataType::Float64 => Arc::new(Float64Array::from_iter(float64_options(values)?)),
        DataType::Boolean => Arc::new(BooleanArray::from_iter(boolean_options(values)?)),
        DataType::Utf8 => {
            let strings = values
                .iter()
                .map(|value| match value {
                    None => Ok(None),
                    Some(AggregateValue::Bytes(value)) => std::str::from_utf8(value)
                        .map(Some)
                        .map_err(|error| DataFusionError::External(Box::new(error))),
                    Some(other) => Err(value_type_error("Utf8", other)),
                })
                .collect::<Result<Vec<_>>>()?;
            Arc::new(StringArray::from(strings))
        }
        DataType::Date32 => Arc::new(Date32Array::from_iter(int32_options(values)?)),
        DataType::Time32(TimeUnit::Second) => {
            Arc::new(Time32SecondArray::from_iter(int32_options(values)?))
        }
        DataType::Time32(TimeUnit::Millisecond) => {
            Arc::new(Time32MillisecondArray::from_iter(int32_options(values)?))
        }
        DataType::Time64(TimeUnit::Microsecond) => {
            Arc::new(Time64MicrosecondArray::from_iter(int64_options(values)?))
        }
        DataType::Time64(TimeUnit::Nanosecond) => {
            Arc::new(Time64NanosecondArray::from_iter(int64_options(values)?))
        }
        DataType::Timestamp(TimeUnit::Second, _) => {
            Arc::new(TimestampSecondArray::from_iter(int64_options(values)?))
        }
        DataType::Timestamp(TimeUnit::Millisecond, _) => {
            Arc::new(TimestampMillisecondArray::from_iter(int64_options(values)?))
        }
        DataType::Timestamp(TimeUnit::Microsecond, _) => {
            Arc::new(TimestampMicrosecondArray::from_iter(int64_options(values)?))
        }
        DataType::Timestamp(TimeUnit::Nanosecond, _) => {
            Arc::new(TimestampNanosecondArray::from_iter(int64_options(values)?))
        }
        other => {
            return Err(DataFusionError::Execution(format!(
                "group aggregate cannot emit type {other}"
            )));
        }
    })
}

fn zero_value(data_type: &DataType) -> AggregateValue {
    match data_type {
        DataType::Float32 => float32_value(0.0),
        DataType::Float64 => float64_value(0.0),
        _ => AggregateValue::Int(0),
    }
}

fn average_contribution(value: &AggregateValue, call: &Call) -> Result<AggregateValue> {
    let accumulator_type = call.average_accumulator_type();
    match (value, &call.input_type, &accumulator_type) {
        (AggregateValue::Int(value), Some(input), DataType::Int64)
            if matches!(
                input,
                DataType::Int8 | DataType::Int16 | DataType::Int32 | DataType::Int64
            ) =>
        {
            Ok(AggregateValue::Int(*value))
        }
        (AggregateValue::Float32(value), Some(DataType::Float32), DataType::Float64) => {
            Ok(float64_value(f32::from_bits(*value) as f64))
        }
        (AggregateValue::Float64(value), Some(DataType::Float64), DataType::Float64) => {
            Ok(AggregateValue::Float64(*value))
        }
        (
            AggregateValue::Int(value),
            Some(DataType::Decimal128(_, input_scale)),
            DataType::Decimal128(38, sum_scale),
        ) if input_scale == sum_scale => Ok(AggregateValue::Int(*value)),
        _ => Err(DataFusionError::Internal(format!(
            "AVG cannot convert {value:?} from {:?} into {}",
            call.input_type, accumulator_type
        ))),
    }
}

fn average_value(
    value: Option<&AggregateValue>,
    count: i64,
    call: &Call,
) -> Option<AggregateValue> {
    if count == 0 {
        return None;
    }
    let accumulator_type = call.average_accumulator_type();
    match (value?, &accumulator_type, &call.output_type) {
        (AggregateValue::Int(sum), DataType::Int64, DataType::Int8) => Some(AggregateValue::Int(
            (*sum as i64).wrapping_div(count) as i8 as i128,
        )),
        (AggregateValue::Int(sum), DataType::Int64, DataType::Int16) => Some(AggregateValue::Int(
            (*sum as i64).wrapping_div(count) as i16 as i128,
        )),
        (AggregateValue::Int(sum), DataType::Int64, DataType::Int32) => Some(AggregateValue::Int(
            (*sum as i64).wrapping_div(count) as i32 as i128,
        )),
        (AggregateValue::Int(sum), DataType::Int64, DataType::Int64) => Some(AggregateValue::Int(
            (*sum as i64).wrapping_div(count) as i128,
        )),
        (AggregateValue::Float64(sum), DataType::Float64, DataType::Float32) => {
            Some(float32_value((f64::from_bits(*sum) / count as f64) as f32))
        }
        (AggregateValue::Float64(sum), DataType::Float64, DataType::Float64) => {
            Some(float64_value(f64::from_bits(*sum) / count as f64))
        }
        (
            AggregateValue::Int(sum),
            DataType::Decimal128(_, sum_scale),
            DataType::Decimal128(precision, output_scale),
        ) => crate::planner::expressions::decimal::flink_divide_nonzero(
            *sum,
            *sum_scale,
            count as i128,
            0,
            *precision,
            *output_scale,
        )
        .map(AggregateValue::Int),
        _ => None,
    }
}

fn aggregate_add(
    left: &AggregateValue,
    right: &AggregateValue,
    data_type: &DataType,
) -> Result<Option<AggregateValue>> {
    Ok(Some(match (left, right, data_type) {
        (AggregateValue::Int(left), AggregateValue::Int(right), DataType::Int8) => {
            AggregateValue::Int((*left as i8).wrapping_add(*right as i8) as i128)
        }
        (AggregateValue::Int(left), AggregateValue::Int(right), DataType::Int16) => {
            AggregateValue::Int((*left as i16).wrapping_add(*right as i16) as i128)
        }
        (AggregateValue::Int(left), AggregateValue::Int(right), DataType::Int32) => {
            AggregateValue::Int((*left as i32).wrapping_add(*right as i32) as i128)
        }
        (AggregateValue::Int(left), AggregateValue::Int(right), DataType::Int64) => {
            AggregateValue::Int((*left as i64).wrapping_add(*right as i64) as i128)
        }
        (AggregateValue::Int(left), AggregateValue::Int(right), DataType::Decimal128(_, _)) => {
            return Ok(decimal_operation(
                *left,
                *right,
                data_type,
                i128::checked_add,
            ));
        }
        (AggregateValue::Float32(left), AggregateValue::Float32(right), DataType::Float32) => {
            float32_value(f32::from_bits(*left) + f32::from_bits(*right))
        }
        (AggregateValue::Float64(left), AggregateValue::Float64(right), DataType::Float64) => {
            float64_value(f64::from_bits(*left) + f64::from_bits(*right))
        }
        _ => return Err(value_operation_error("add", left, right, data_type)),
    }))
}

fn aggregate_sub(
    left: &AggregateValue,
    right: &AggregateValue,
    data_type: &DataType,
) -> Result<Option<AggregateValue>> {
    Ok(Some(match (left, right, data_type) {
        (AggregateValue::Int(left), AggregateValue::Int(right), DataType::Int8) => {
            AggregateValue::Int((*left as i8).wrapping_sub(*right as i8) as i128)
        }
        (AggregateValue::Int(left), AggregateValue::Int(right), DataType::Int16) => {
            AggregateValue::Int((*left as i16).wrapping_sub(*right as i16) as i128)
        }
        (AggregateValue::Int(left), AggregateValue::Int(right), DataType::Int32) => {
            AggregateValue::Int((*left as i32).wrapping_sub(*right as i32) as i128)
        }
        (AggregateValue::Int(left), AggregateValue::Int(right), DataType::Int64) => {
            AggregateValue::Int((*left as i64).wrapping_sub(*right as i64) as i128)
        }
        (AggregateValue::Int(left), AggregateValue::Int(right), DataType::Decimal128(_, _)) => {
            return Ok(decimal_operation(
                *left,
                *right,
                data_type,
                i128::checked_sub,
            ));
        }
        (AggregateValue::Float32(left), AggregateValue::Float32(right), DataType::Float32) => {
            float32_value(f32::from_bits(*left) - f32::from_bits(*right))
        }
        (AggregateValue::Float64(left), AggregateValue::Float64(right), DataType::Float64) => {
            float64_value(f64::from_bits(*left) - f64::from_bits(*right))
        }
        _ => return Err(value_operation_error("subtract", left, right, data_type)),
    }))
}

fn decimal_operation(
    left: i128,
    right: i128,
    data_type: &DataType,
    operation: fn(i128, i128) -> Option<i128>,
) -> Option<AggregateValue> {
    let DataType::Decimal128(precision, _) = data_type else {
        unreachable!("decimal operation has a decimal output type")
    };
    let maximum = 10_i128.pow(*precision as u32) - 1;
    operation(left, right)
        .filter(|value| *value >= -maximum && *value <= maximum)
        .map(AggregateValue::Int)
}

fn flink_append_extremum(
    current: &AggregateValue,
    candidate: &AggregateValue,
    minimum: bool,
) -> AggregateValue {
    let replace = match (current, candidate) {
        (AggregateValue::Float32(current), AggregateValue::Float32(candidate)) => {
            let current = f32::from_bits(*current);
            let candidate = f32::from_bits(*candidate);
            if minimum {
                candidate < current
            } else {
                candidate > current
            }
        }
        (AggregateValue::Float64(current), AggregateValue::Float64(candidate)) => {
            let current = f64::from_bits(*current);
            let candidate = f64::from_bits(*candidate);
            if minimum {
                candidate < current
            } else {
                candidate > current
            }
        }
        _ => {
            if minimum {
                candidate < current
            } else {
                candidate > current
            }
        }
    };
    if replace {
        candidate.clone()
    } else {
        current.clone()
    }
}

fn float32_value(value: f32) -> AggregateValue {
    AggregateValue::Float32(if value.is_nan() {
        f32::NAN.to_bits()
    } else {
        value.to_bits()
    })
}

fn float64_value(value: f64) -> AggregateValue {
    AggregateValue::Float64(if value.is_nan() {
        f64::NAN.to_bits()
    } else {
        value.to_bits()
    })
}

fn flink_f32_cmp(left: u32, right: u32) -> Ordering {
    let left_value = f32::from_bits(left);
    let right_value = f32::from_bits(right);
    if left_value < right_value {
        Ordering::Less
    } else if left_value > right_value {
        Ordering::Greater
    } else {
        (left as i32).cmp(&(right as i32))
    }
}

fn flink_f64_cmp(left: u64, right: u64) -> Ordering {
    let left_value = f64::from_bits(left);
    let right_value = f64::from_bits(right);
    if left_value < right_value {
        Ordering::Less
    } else if left_value > right_value {
        Ordering::Greater
    } else {
        (left as i64).cmp(&(right as i64))
    }
}

fn value_tag(value: &AggregateValue) -> u8 {
    match value {
        AggregateValue::Boolean(_) => 1,
        AggregateValue::Int(_) => 2,
        AggregateValue::Float32(_) => 3,
        AggregateValue::Float64(_) => 4,
        AggregateValue::Bytes(_) => 5,
    }
}

fn int_value(value: &AggregateValue) -> Result<i128> {
    match value {
        AggregateValue::Int(value) => Ok(*value),
        other => Err(value_type_error("integer", other)),
    }
}

fn float32_options(values: &[Option<AggregateValue>]) -> Result<Vec<Option<f32>>> {
    values
        .iter()
        .map(|value| match value {
            Some(AggregateValue::Float32(value)) => Ok(Some(f32::from_bits(*value))),
            None => Ok(None),
            Some(other) => Err(value_type_error("Float32", other)),
        })
        .collect()
}

fn float64_options(values: &[Option<AggregateValue>]) -> Result<Vec<Option<f64>>> {
    values
        .iter()
        .map(|value| match value {
            Some(AggregateValue::Float64(value)) => Ok(Some(f64::from_bits(*value))),
            None => Ok(None),
            Some(other) => Err(value_type_error("Float64", other)),
        })
        .collect()
}

fn boolean_options(values: &[Option<AggregateValue>]) -> Result<Vec<Option<bool>>> {
    values
        .iter()
        .map(|value| match value {
            Some(AggregateValue::Boolean(value)) => Ok(Some(*value)),
            None => Ok(None),
            Some(other) => Err(value_type_error("Boolean", other)),
        })
        .collect()
}

fn int32_options(values: &[Option<AggregateValue>]) -> Result<Vec<Option<i32>>> {
    values
        .iter()
        .map(|value| {
            value
                .as_ref()
                .map(int_value)
                .transpose()
                .map(|value| value.map(|value| value as i32))
        })
        .collect()
}

fn int64_options(values: &[Option<AggregateValue>]) -> Result<Vec<Option<i64>>> {
    values
        .iter()
        .map(|value| {
            value
                .as_ref()
                .map(int_value)
                .transpose()
                .map(|value| value.map(|value| value as i64))
        })
        .collect()
}

fn value_type_error(expected: &str, actual: &AggregateValue) -> DataFusionError {
    DataFusionError::Internal(format!(
        "group aggregate expected {expected} accumulator value, got {actual:?}"
    ))
}

fn value_operation_error(
    operation: &str,
    left: &AggregateValue,
    right: &AggregateValue,
    data_type: &DataType,
) -> DataFusionError {
    DataFusionError::Internal(format!(
        "group aggregate cannot {operation} {left:?} and {right:?} as {data_type}"
    ))
}

pub(super) fn encode_state(state: &AccumulatorState) -> Vec<u8> {
    let mut bytes = Vec::with_capacity(32 + state.accumulators.len() * 24);
    bytes.extend_from_slice(STATE_MAGIC);
    bytes.push(STATE_VERSION);
    bytes.extend_from_slice(&state.row_count.to_le_bytes());
    bytes.extend_from_slice(&(state.accumulators.len() as u32).to_le_bytes());
    for accumulator in &state.accumulators {
        match accumulator {
            Accumulator::Count(value) => {
                bytes.push(1);
                bytes.extend_from_slice(&value.to_le_bytes());
            }
            Accumulator::DistinctCount { count, values } => {
                bytes.push(5);
                bytes.extend_from_slice(&count.to_le_bytes());
                encode_counted_values(values, &mut bytes);
            }
            Accumulator::Sum { value, count } => {
                bytes.push(2);
                bytes.push(value.is_some() as u8);
                if let Some(value) = value {
                    encode_value(value, &mut bytes);
                }
                bytes.extend_from_slice(&count.to_le_bytes());
            }
            Accumulator::DistinctSum {
                value,
                count,
                values,
            } => {
                bytes.push(6);
                bytes.push(value.is_some() as u8);
                if let Some(value) = value {
                    encode_value(value, &mut bytes);
                }
                bytes.extend_from_slice(&count.to_le_bytes());
                encode_counted_values(values, &mut bytes);
            }
            Accumulator::Average { value, count } => {
                bytes.push(7);
                bytes.push(value.is_some() as u8);
                if let Some(value) = value {
                    encode_value(value, &mut bytes);
                }
                bytes.extend_from_slice(&count.to_le_bytes());
            }
            Accumulator::DistinctAverage {
                value,
                count,
                values,
            } => {
                bytes.push(8);
                bytes.push(value.is_some() as u8);
                if let Some(value) = value {
                    encode_value(value, &mut bytes);
                }
                bytes.extend_from_slice(&count.to_le_bytes());
                encode_counted_values(values, &mut bytes);
            }
            Accumulator::AppendExtremum(value) => {
                bytes.push(4);
                bytes.push(value.is_some() as u8);
                if let Some(value) = value {
                    encode_value(value, &mut bytes);
                }
            }
            Accumulator::Extremum(values) => {
                bytes.push(3);
                bytes.extend_from_slice(&(values.len() as u32).to_le_bytes());
                for (value, count) in values {
                    encode_value(value, &mut bytes);
                    bytes.extend_from_slice(&count.to_le_bytes());
                }
            }
        }
    }
    bytes
}

fn encode_counted_values(values: &BTreeMap<AggregateValue, i64>, bytes: &mut Vec<u8>) {
    bytes.extend_from_slice(&(values.len() as u32).to_le_bytes());
    for (value, count) in values {
        encode_value(value, bytes);
        bytes.extend_from_slice(&count.to_le_bytes());
    }
}

pub(super) fn decode_state(bytes: &[u8], calls: &[Call]) -> Result<AccumulatorState> {
    let mut cursor = Cursor::new(bytes);
    if cursor.read_exact(4)? != STATE_MAGIC {
        return Err(DataFusionError::Execution(
            "group aggregate state has invalid magic".to_string(),
        ));
    }
    let version = cursor.read_u8()?;
    if version < 1 || version > STATE_VERSION {
        return Err(DataFusionError::Execution(format!(
            "group aggregate state version {version} is unsupported"
        )));
    }
    let row_count = cursor.read_i64()?;
    let count = cursor.read_u32()? as usize;
    if count != calls.len() {
        return Err(DataFusionError::Execution(format!(
            "group aggregate state has {count} accumulators, expected {}",
            calls.len()
        )));
    }
    let mut accumulators = Vec::with_capacity(count);
    for call in calls {
        let tag = cursor.read_u8()?;
        let accumulator = match tag {
            1 => Accumulator::Count(cursor.read_i64()?),
            2 => Accumulator::Sum {
                value: if version == 1 {
                    Some(AggregateValue::Int(cursor.read_i128()?))
                } else if version == 2 {
                    Some(decode_value(&mut cursor)?)
                } else {
                    match cursor.read_u8()? {
                        0 => None,
                        1 => Some(decode_value(&mut cursor)?),
                        other => {
                            return Err(DataFusionError::Execution(format!(
                                "group aggregate sum presence {other} is invalid"
                            )))
                        }
                    }
                },
                count: cursor.read_i64()?,
            },
            3 => {
                let entries = cursor.read_u32()? as usize;
                let mut values = BTreeMap::new();
                for _ in 0..entries {
                    let value = if version == 1 {
                        AggregateValue::Int(cursor.read_i128()?)
                    } else {
                        decode_value(&mut cursor)?
                    };
                    values.insert(value, cursor.read_i64()?);
                }
                Accumulator::Extremum(values)
            }
            4 => {
                let value = match cursor.read_u8()? {
                    0 => None,
                    1 => Some(if version == 1 {
                        AggregateValue::Int(cursor.read_i128()?)
                    } else {
                        decode_value(&mut cursor)?
                    }),
                    other => {
                        return Err(DataFusionError::Execution(format!(
                            "group aggregate append extremum presence {other} is invalid"
                        )));
                    }
                };
                Accumulator::AppendExtremum(value)
            }
            5 if version >= 4 => Accumulator::DistinctCount {
                count: cursor.read_i64()?,
                values: decode_counted_values(&mut cursor)?,
            },
            6 if version >= 4 => {
                let value = match cursor.read_u8()? {
                    0 => None,
                    1 => Some(decode_value(&mut cursor)?),
                    other => {
                        return Err(DataFusionError::Execution(format!(
                            "group aggregate distinct sum presence {other} is invalid"
                        )))
                    }
                };
                Accumulator::DistinctSum {
                    value,
                    count: cursor.read_i64()?,
                    values: decode_counted_values(&mut cursor)?,
                }
            }
            7 if version >= 5 => Accumulator::Average {
                value: decode_optional_value(&mut cursor, "average")?,
                count: cursor.read_i64()?,
            },
            8 if version >= 5 => Accumulator::DistinctAverage {
                value: decode_optional_value(&mut cursor, "distinct average")?,
                count: cursor.read_i64()?,
                values: decode_counted_values(&mut cursor)?,
            },
            other => {
                return Err(DataFusionError::Execution(format!(
                    "group aggregate state accumulator tag {other} is invalid"
                )));
            }
        };
        let expected = match call.function {
            proto::AggregateFunction::CountStar | proto::AggregateFunction::Count => {
                if call.distinct {
                    5
                } else {
                    1
                }
            }
            proto::AggregateFunction::Sum => {
                if call.distinct {
                    6
                } else {
                    2
                }
            }
            proto::AggregateFunction::Avg => {
                if call.distinct {
                    8
                } else {
                    7
                }
            }
            proto::AggregateFunction::Min | proto::AggregateFunction::Max => {
                if call.retractable {
                    3
                } else {
                    4
                }
            }
            _ => unreachable!("validated aggregate function"),
        };
        if tag != expected {
            return Err(DataFusionError::Execution(
                "group aggregate state does not match its plan".to_string(),
            ));
        }
        validate_accumulator_values(&accumulator, call)?;
        accumulators.push(accumulator);
    }
    if !cursor.is_empty() {
        return Err(DataFusionError::Execution(
            "group aggregate state has trailing bytes".to_string(),
        ));
    }
    Ok(AccumulatorState {
        row_count,
        accumulators,
    })
}

fn decode_optional_value(
    cursor: &mut Cursor<'_>,
    description: &str,
) -> Result<Option<AggregateValue>> {
    match cursor.read_u8()? {
        0 => Ok(None),
        1 => decode_value(cursor).map(Some),
        other => Err(DataFusionError::Execution(format!(
            "group aggregate {description} presence {other} is invalid"
        ))),
    }
}

fn decode_counted_values(cursor: &mut Cursor<'_>) -> Result<BTreeMap<AggregateValue, i64>> {
    let entries = cursor.read_u32()? as usize;
    let mut values = BTreeMap::new();
    for _ in 0..entries {
        let value = decode_value(cursor)?;
        values.insert(value, cursor.read_i64()?);
    }
    Ok(values)
}

fn validate_accumulator_values(accumulator: &Accumulator, call: &Call) -> Result<()> {
    let values: Box<dyn Iterator<Item = &AggregateValue> + '_> = match accumulator {
        Accumulator::Count(_) => return Ok(()),
        Accumulator::DistinctCount { values, .. } => {
            for value in values.keys() {
                if !aggregate_value_matches_type(
                    value,
                    call.input_type
                        .as_ref()
                        .expect("DISTINCT COUNT has input type"),
                ) {
                    return Err(DataFusionError::Execution(format!(
                        "group aggregate distinct value {value:?} does not match its input type"
                    )));
                }
            }
            return Ok(());
        }
        Accumulator::Sum { value, .. } => Box::new(value.iter()),
        Accumulator::DistinctSum { value, values, .. } => {
            for distinct in values.keys() {
                if !aggregate_value_matches_type(
                    distinct,
                    call.input_type
                        .as_ref()
                        .expect("DISTINCT SUM has input type"),
                ) {
                    return Err(DataFusionError::Execution(format!(
                        "group aggregate distinct value {distinct:?} does not match its input type"
                    )));
                }
            }
            Box::new(value.iter())
        }
        Accumulator::Average { value, .. } => Box::new(value.iter()),
        Accumulator::DistinctAverage { value, values, .. } => {
            for distinct in values.keys() {
                if !aggregate_value_matches_type(
                    distinct,
                    call.input_type
                        .as_ref()
                        .expect("DISTINCT AVG has input type"),
                ) {
                    return Err(DataFusionError::Execution(format!(
                        "group aggregate distinct value {distinct:?} does not match its input type"
                    )));
                }
            }
            Box::new(value.iter())
        }
        Accumulator::AppendExtremum(value) => Box::new(value.iter()),
        Accumulator::Extremum(values) => Box::new(values.keys()),
    };
    for value in values {
        let state_type = if call.function == proto::AggregateFunction::Avg {
            call.average_accumulator_type()
        } else {
            call.output_type.clone()
        };
        if !aggregate_value_matches_type(value, &state_type) {
            return Err(DataFusionError::Execution(format!(
                "group aggregate state value {value:?} does not match {state_type}",
            )));
        }
    }
    Ok(())
}

fn aggregate_value_matches_type(value: &AggregateValue, data_type: &DataType) -> bool {
    match value {
        AggregateValue::Boolean(_) => data_type == &DataType::Boolean,
        AggregateValue::Float32(_) => data_type == &DataType::Float32,
        AggregateValue::Float64(_) => data_type == &DataType::Float64,
        AggregateValue::Bytes(_) => data_type == &DataType::Utf8,
        AggregateValue::Int(_) => matches!(
            data_type,
            DataType::Int8
                | DataType::Int16
                | DataType::Int32
                | DataType::Int64
                | DataType::Decimal128(_, _)
                | DataType::Date32
                | DataType::Time32(_)
                | DataType::Time64(_)
                | DataType::Timestamp(_, _)
        ),
    }
}

fn encode_value(value: &AggregateValue, bytes: &mut Vec<u8>) {
    bytes.push(value_tag(value));
    match value {
        AggregateValue::Boolean(value) => bytes.push(*value as u8),
        AggregateValue::Int(value) => bytes.extend_from_slice(&value.to_le_bytes()),
        AggregateValue::Float32(value) => bytes.extend_from_slice(&value.to_le_bytes()),
        AggregateValue::Float64(value) => bytes.extend_from_slice(&value.to_le_bytes()),
        AggregateValue::Bytes(value) => {
            bytes.extend_from_slice(&(value.len() as u32).to_le_bytes());
            bytes.extend_from_slice(value);
        }
    }
}

fn decode_value(cursor: &mut Cursor<'_>) -> Result<AggregateValue> {
    match cursor.read_u8()? {
        1 => match cursor.read_u8()? {
            0 => Ok(AggregateValue::Boolean(false)),
            1 => Ok(AggregateValue::Boolean(true)),
            other => Err(DataFusionError::Execution(format!(
                "group aggregate Boolean state byte {other} is invalid"
            ))),
        },
        2 => Ok(AggregateValue::Int(cursor.read_i128()?)),
        3 => Ok(AggregateValue::Float32(cursor.read_u32()?)),
        4 => Ok(AggregateValue::Float64(cursor.read_u64()?)),
        5 => {
            let length = cursor.read_u32()? as usize;
            Ok(AggregateValue::Bytes(cursor.read_exact(length)?.to_vec()))
        }
        other => Err(DataFusionError::Execution(format!(
            "group aggregate value state tag {other} is invalid"
        ))),
    }
}

struct Cursor<'a> {
    bytes: &'a [u8],
    offset: usize,
}

impl<'a> Cursor<'a> {
    fn new(bytes: &'a [u8]) -> Self {
        Self { bytes, offset: 0 }
    }

    fn read_exact(&mut self, length: usize) -> Result<&'a [u8]> {
        let end = self.offset.checked_add(length).ok_or_else(|| {
            DataFusionError::Execution("group aggregate state length overflow".to_string())
        })?;
        let value = self.bytes.get(self.offset..end).ok_or_else(|| {
            DataFusionError::Execution("group aggregate state is truncated".to_string())
        })?;
        self.offset = end;
        Ok(value)
    }

    fn read_u8(&mut self) -> Result<u8> {
        Ok(self.read_exact(1)?[0])
    }

    fn read_u32(&mut self) -> Result<u32> {
        Ok(u32::from_le_bytes(self.read_exact(4)?.try_into().unwrap()))
    }

    fn read_i64(&mut self) -> Result<i64> {
        Ok(i64::from_le_bytes(self.read_exact(8)?.try_into().unwrap()))
    }

    fn read_u64(&mut self) -> Result<u64> {
        Ok(u64::from_le_bytes(self.read_exact(8)?.try_into().unwrap()))
    }

    fn read_i128(&mut self) -> Result<i128> {
        Ok(i128::from_le_bytes(
            self.read_exact(16)?.try_into().unwrap(),
        ))
    }

    fn is_empty(&self) -> bool {
        self.offset == self.bytes.len()
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::memory_pool::{tests_support::TestBroker, HostMemoryReservation};
    use crate::state::KeyedState;
    use arrow::array::{BooleanArray, Float32Array, Int64Array, StringArray};
    use prost::Message;
    use std::borrow::Cow;
    use std::sync::atomic::{AtomicUsize, Ordering};

    fn logical_bigint(nullable: bool) -> proto::LogicalType {
        proto::LogicalType {
            nullable,
            r#type: Some(proto::logical_type::Type::Bigint(proto::EmptyType {})),
        }
    }

    fn logical_varchar(nullable: bool) -> proto::LogicalType {
        proto::LogicalType {
            nullable,
            r#type: Some(proto::logical_type::Type::Varchar(proto::EmptyType {})),
        }
    }

    fn group_schema() -> proto::Schema {
        proto::Schema {
            fields: vec![
                proto::Field {
                    name: "bidder".to_string(),
                    r#type: Some(logical_bigint(false)),
                },
                proto::Field {
                    name: "price".to_string(),
                    r#type: Some(logical_bigint(true)),
                },
            ],
        }
    }

    fn group_output_schema() -> proto::Schema {
        proto::Schema {
            fields: vec![
                proto::Field {
                    name: "bidder".to_string(),
                    r#type: Some(logical_bigint(false)),
                },
                proto::Field {
                    name: "count".to_string(),
                    r#type: Some(logical_bigint(false)),
                },
                proto::Field {
                    name: "sum".to_string(),
                    r#type: Some(logical_bigint(true)),
                },
                proto::Field {
                    name: "min".to_string(),
                    r#type: Some(logical_bigint(true)),
                },
                proto::Field {
                    name: "max".to_string(),
                    r#type: Some(logical_bigint(true)),
                },
            ],
        }
    }

    fn plan(input_changelog: bool, update_before: bool) -> Vec<u8> {
        plan_with_grouping(input_changelog, update_before, vec![0])
    }

    fn plan_with_grouping(
        input_changelog: bool,
        update_before: bool,
        grouping_indices: Vec<u32>,
    ) -> Vec<u8> {
        let call =
            |function: proto::AggregateFunction, input_index: Option<u32>| proto::AggregateCall {
                function: function as i32,
                input_index,
                input_type: input_index.map(|_| logical_bigint(true)),
                output_type: Some(logical_bigint(
                    function != proto::AggregateFunction::CountStar,
                )),
                retractable: input_changelog,
                filter_index: None,
                distinct: false,
                accumulator_type: None,
            };
        proto::NativePlan {
            protocol_version: crate::PLAN_PROTOCOL_VERSION,
            root: Some(proto::Operator {
                operator: Some(proto::operator::Operator::GroupAggregate(Box::new(
                    proto::GroupAggregate {
                        input: Some(Box::new(proto::Operator {
                            operator: Some(proto::operator::Operator::Input(proto::Input {
                                schema: None,
                                input_index: 0,
                            })),
                        })),
                        grouping_indices,
                        aggregate_calls: vec![
                            call(proto::AggregateFunction::CountStar, None),
                            call(proto::AggregateFunction::Sum, Some(1)),
                            call(proto::AggregateFunction::Min, Some(1)),
                            call(proto::AggregateFunction::Max, Some(1)),
                        ],
                        generate_update_before: update_before,
                        input_changelog,
                        ..Default::default()
                    },
                ))),
            }),
        }
        .encode_to_vec()
    }

    fn distinct_plan() -> Vec<u8> {
        proto::NativePlan {
            protocol_version: crate::PLAN_PROTOCOL_VERSION,
            root: Some(proto::Operator {
                operator: Some(proto::operator::Operator::GroupAggregate(Box::new(
                    proto::GroupAggregate {
                        input: Some(Box::new(proto::Operator {
                            operator: Some(proto::operator::Operator::Input(proto::Input {
                                schema: None,
                                input_index: 0,
                            })),
                        })),
                        grouping_indices: vec![0],
                        aggregate_calls: Vec::new(),
                        generate_update_before: false,
                        input_changelog: true,
                        ..Default::default()
                    },
                ))),
            }),
        }
        .encode_to_vec()
    }

    fn aggregate_distinct_plan() -> Vec<u8> {
        let call = |function: proto::AggregateFunction| proto::AggregateCall {
            function: function as i32,
            input_index: Some(1),
            input_type: Some(logical_bigint(true)),
            output_type: Some(logical_bigint(function != proto::AggregateFunction::Count)),
            retractable: true,
            filter_index: None,
            distinct: true,
            accumulator_type: None,
        };
        proto::NativePlan {
            protocol_version: crate::PLAN_PROTOCOL_VERSION,
            root: Some(proto::Operator {
                operator: Some(proto::operator::Operator::GroupAggregate(Box::new(
                    proto::GroupAggregate {
                        input: None,
                        grouping_indices: vec![0],
                        aggregate_calls: vec![
                            call(proto::AggregateFunction::Count),
                            call(proto::AggregateFunction::Sum),
                        ],
                        generate_update_before: true,
                        input_changelog: true,
                        ..Default::default()
                    },
                ))),
            }),
        }
        .encode_to_vec()
    }

    fn distinct_processor() -> GroupAggregateProcessor {
        GroupAggregateProcessor::new(
            &distinct_plan(),
            128,
            0,
            127,
            HostMemoryReservation::new(
                Arc::new(TestBroker::new(1 << 30)),
                "test select distinct state",
            ),
        )
        .unwrap()
    }

    fn processor(input_changelog: bool, update_before: bool) -> GroupAggregateProcessor {
        processor_range(input_changelog, update_before, 0, 127)
    }

    fn mini_processor(size: u64, input_changelog: bool) -> GroupAggregateProcessor {
        let native = proto::NativePlan::decode(plan(input_changelog, true).as_slice()).unwrap();
        let mut aggregate = match native.root.unwrap().operator.unwrap() {
            proto::operator::Operator::GroupAggregate(aggregate) => *aggregate,
            _ => unreachable!(),
        };
        aggregate.mini_batch_size = size;
        aggregate.input_schema = Some(group_schema());
        aggregate.output_schema = Some(group_output_schema());
        let plan = proto::NativePlan {
            protocol_version: crate::PLAN_PROTOCOL_VERSION,
            root: Some(proto::Operator {
                operator: Some(proto::operator::Operator::GroupAggregate(Box::new(
                    aggregate,
                ))),
            }),
        }
        .encode_to_vec();
        GroupAggregateProcessor::new(
            &plan,
            128,
            0,
            127,
            HostMemoryReservation::new(
                Arc::new(TestBroker::new(1 << 30)),
                "test mini-batch group aggregate state",
            ),
        )
        .unwrap()
    }

    fn processor_range(
        input_changelog: bool,
        update_before: bool,
        first_key_group: u32,
        last_key_group: u32,
    ) -> GroupAggregateProcessor {
        GroupAggregateProcessor::new(
            &plan(input_changelog, update_before),
            128,
            first_key_group,
            last_key_group,
            HostMemoryReservation::new(
                Arc::new(TestBroker::new(1 << 30)),
                "test group aggregate state",
            ),
        )
        .unwrap()
    }

    fn batch(keys: Vec<i64>, values: Vec<Option<i64>>, kinds: Option<Vec<i8>>) -> RecordBatch {
        let mut fields = vec![
            Field::new("bidder", DataType::Int64, false),
            Field::new("price", DataType::Int64, true),
        ];
        let mut columns: Vec<ArrayRef> = vec![
            Arc::new(Int64Array::from(keys)),
            Arc::new(Int64Array::from(values)),
        ];
        if let Some(kinds) = kinds {
            fields.push(Field::new(
                "__streamfusion_input_row_kind",
                DataType::Int8,
                false,
            ));
            columns.push(Arc::new(Int8Array::from(kinds)));
        }
        RecordBatch::try_new(Arc::new(Schema::new(fields)), columns).unwrap()
    }

    #[test]
    fn mini_batch_uses_exact_count_boundaries_across_arrow_batches() {
        let mut processor = mini_processor(3, false);
        let first = processor
            .process_arrow(batch(vec![7, 7], vec![Some(10), Some(20)], None))
            .unwrap();
        assert_eq!(first.num_rows(), 0);
        assert_eq!(processor.pending_element_count(), 2);
        assert_eq!(processor.pending_key_count(), 1);

        let second = processor
            .process_arrow(batch(
                vec![8, 7, 8, 9],
                vec![Some(5), Some(30), Some(7), Some(1)],
                None,
            ))
            .unwrap();
        assert_eq!(second.num_rows(), 7);
        assert_eq!(processor.pending_element_count(), 0);
        assert_eq!(
            second
                .column(5)
                .as_any()
                .downcast_ref::<Int8Array>()
                .unwrap()
                .values(),
            &[
                INSERT,
                INSERT,
                UPDATE_BEFORE,
                UPDATE_AFTER,
                INSERT,
                UPDATE_BEFORE,
                UPDATE_AFTER
            ]
        );
    }

    #[test]
    fn mini_batch_flushes_partial_bundle_and_suppresses_equal_result() {
        let mut processor = mini_processor(10, true);
        let initial = processor
            .process_arrow(batch(vec![7], vec![Some(10)], Some(vec![INSERT])))
            .unwrap();
        assert_eq!(initial.num_rows(), 0);
        assert_eq!(processor.finish_bundle().unwrap().num_rows(), 1);

        let unchanged = processor
            .process_arrow(batch(
                vec![7, 7],
                vec![Some(10), Some(10)],
                Some(vec![DELETE, INSERT]),
            ))
            .unwrap();
        assert_eq!(unchanged.num_rows(), 0);
        assert_eq!(processor.finish_bundle().unwrap().num_rows(), 0);
    }

    #[test]
    fn emits_flink_per_record_changelog_and_suppresses_equal_results() {
        let output = processor(false, true)
            .process_arrow(batch(
                vec![7, 7, 7, 8],
                vec![Some(10), Some(20), None, Some(5)],
                None,
            ))
            .unwrap();
        assert_eq!(output.num_rows(), 6);
        assert_eq!(
            output
                .column(1)
                .as_any()
                .downcast_ref::<Int64Array>()
                .unwrap()
                .values(),
            &[1, 1, 2, 2, 3, 1]
        );
        assert_eq!(
            output
                .column(output.num_columns() - 1)
                .as_any()
                .downcast_ref::<Int8Array>()
                .unwrap()
                .values(),
            &[
                INSERT,
                UPDATE_BEFORE,
                UPDATE_AFTER,
                UPDATE_BEFORE,
                UPDATE_AFTER,
                INSERT
            ]
        );
    }

    #[test]
    fn global_aggregate_uses_one_empty_binary_row_key() {
        let mut processor = GroupAggregateProcessor::new(
            &plan_with_grouping(false, true, Vec::new()),
            128,
            0,
            127,
            HostMemoryReservation::new(
                Arc::new(TestBroker::new(1 << 30)),
                "test global aggregate state",
            ),
        )
        .unwrap();
        let output = processor
            .process_arrow(batch(vec![7, 8, 9], vec![Some(10), None, Some(20)], None))
            .unwrap();

        assert_eq!(output.num_columns(), 5);
        assert_eq!(
            output
                .column(0)
                .as_any()
                .downcast_ref::<Int64Array>()
                .unwrap()
                .values(),
            &[1, 1, 2, 2, 3]
        );
        assert_eq!(
            output
                .column(output.num_columns() - 1)
                .as_any()
                .downcast_ref::<Int8Array>()
                .unwrap()
                .values(),
            &[
                INSERT,
                UPDATE_BEFORE,
                UPDATE_AFTER,
                UPDATE_BEFORE,
                UPDATE_AFTER
            ]
        );
    }

    #[test]
    fn retracts_sum_and_extrema_and_deletes_the_last_record() {
        let mut processor = processor(true, true);
        processor
            .process_arrow(batch(
                vec![7, 7],
                vec![Some(10), Some(20)],
                Some(vec![INSERT, INSERT]),
            ))
            .unwrap();
        let output = processor
            .process_arrow(batch(
                vec![7, 7],
                vec![Some(20), Some(10)],
                Some(vec![DELETE, DELETE]),
            ))
            .unwrap();
        assert_eq!(output.num_rows(), 3);
        assert_eq!(
            output
                .column(output.num_columns() - 1)
                .as_any()
                .downcast_ref::<Int8Array>()
                .unwrap()
                .values(),
            &[UPDATE_BEFORE, UPDATE_AFTER, DELETE]
        );
        assert_eq!(
            output
                .column(2)
                .as_any()
                .downcast_ref::<Int64Array>()
                .unwrap()
                .iter()
                .collect::<Vec<_>>(),
            vec![Some(30), Some(10), Some(10)]
        );
    }

    #[test]
    fn select_distinct_count_restores_and_emits_the_final_delete() {
        let mut source = distinct_processor();
        let inserted = source
            .process_arrow(batch(
                vec![7, 7],
                vec![Some(10), Some(20)],
                Some(vec![INSERT, UPDATE_AFTER]),
            ))
            .unwrap();
        assert_eq!(inserted.num_rows(), 1);
        let key = encode_binary_row(
            &batch(vec![7], vec![Some(10)], None),
            0,
            &[(0, KeyField::BigInt)],
        )
        .unwrap();
        let key_group = assign_key_group(&key, 128);
        let snapshot = source.snapshot_key_group(key_group).unwrap();

        let mut restored = distinct_processor();
        restored.restore_key_group(key_group, &snapshot).unwrap();
        let output = restored
            .process_arrow(batch(
                vec![7, 7],
                vec![Some(20), Some(10)],
                Some(vec![UPDATE_BEFORE, DELETE]),
            ))
            .unwrap();
        assert_eq!(output.num_rows(), 1);
        assert_eq!(
            output
                .column(output.num_columns() - 1)
                .as_any()
                .downcast_ref::<Int8Array>()
                .unwrap()
                .value(0),
            DELETE
        );
    }

    #[test]
    fn aggregate_distinct_restores_counted_values_and_retracts_membership_boundaries() {
        let plan = aggregate_distinct_plan();
        let reservation = || {
            HostMemoryReservation::new(
                Arc::new(TestBroker::new(1 << 30)),
                "test aggregate distinct state",
            )
        };
        let mut source = GroupAggregateProcessor::new(&plan, 128, 0, 127, reservation()).unwrap();
        let inserted = source
            .process_arrow(batch(
                vec![7, 7, 7],
                vec![Some(10), Some(10), Some(20)],
                Some(vec![INSERT, INSERT, INSERT]),
            ))
            .unwrap();
        assert_eq!(inserted.num_rows(), 3);
        let key = encode_binary_row(
            &batch(vec![7], vec![Some(10)], None),
            0,
            &[(0, KeyField::BigInt)],
        )
        .unwrap();
        let key_group = assign_key_group(&key, 128);
        let snapshot = source.snapshot_key_group(key_group).unwrap();

        let mut restored = GroupAggregateProcessor::new(&plan, 128, 0, 127, reservation()).unwrap();
        restored.restore_key_group(key_group, &snapshot).unwrap();
        let retracted = restored
            .process_arrow(batch(
                vec![7, 7],
                vec![Some(10), Some(10)],
                Some(vec![DELETE, DELETE]),
            ))
            .unwrap();
        assert_eq!(retracted.num_rows(), 2);
        assert_eq!(
            retracted
                .column(1)
                .as_any()
                .downcast_ref::<Int64Array>()
                .unwrap()
                .values(),
            &[2, 1]
        );
        assert_eq!(
            retracted
                .column(2)
                .as_any()
                .downcast_ref::<Int64Array>()
                .unwrap()
                .values(),
            &[30, 20]
        );
    }

    #[test]
    fn delete_then_reinsert_in_one_batch_starts_a_new_group() {
        let mut processor = processor(true, true);
        processor
            .process_arrow(batch(vec![7], vec![Some(10)], Some(vec![INSERT])))
            .unwrap();
        let output = processor
            .process_arrow(batch(
                vec![7, 7],
                vec![Some(10), Some(20)],
                Some(vec![DELETE, INSERT]),
            ))
            .unwrap();
        assert_eq!(output.num_rows(), 2);
        assert_eq!(
            output
                .column(output.num_columns() - 1)
                .as_any()
                .downcast_ref::<Int8Array>()
                .unwrap()
                .values(),
            &[DELETE, INSERT]
        );
    }

    #[test]
    fn count_accepts_nonnumeric_inputs_and_ignores_nulls() {
        let aggregate = proto::GroupAggregate {
            input: Some(Box::new(proto::Operator {
                operator: Some(proto::operator::Operator::Input(proto::Input {
                    schema: None,
                    input_index: 0,
                })),
            })),
            grouping_indices: vec![0],
            aggregate_calls: vec![proto::AggregateCall {
                function: proto::AggregateFunction::Count as i32,
                input_index: Some(1),
                input_type: Some(logical_varchar(true)),
                output_type: Some(logical_bigint(false)),
                retractable: false,
                filter_index: None,
                distinct: false,
                accumulator_type: None,
            }],
            generate_update_before: false,
            input_changelog: false,
            ..Default::default()
        };
        let bytes = proto::NativePlan {
            protocol_version: crate::PLAN_PROTOCOL_VERSION,
            root: Some(proto::Operator {
                operator: Some(proto::operator::Operator::GroupAggregate(Box::new(
                    aggregate,
                ))),
            }),
        }
        .encode_to_vec();
        let mut processor = GroupAggregateProcessor::new(
            &bytes,
            128,
            0,
            127,
            HostMemoryReservation::new(
                Arc::new(TestBroker::new(1 << 30)),
                "test count string state",
            ),
        )
        .unwrap();
        let input = RecordBatch::try_new(
            Arc::new(Schema::new(vec![
                Field::new("key", DataType::Int64, false),
                Field::new("value", DataType::Utf8, true),
            ])),
            vec![
                Arc::new(Int64Array::from(vec![1, 1])),
                Arc::new(StringArray::from(vec![Some("x"), None])),
            ],
        )
        .unwrap();
        let output = processor.process_arrow(input).unwrap();
        assert_eq!(output.num_rows(), 1);
        assert_eq!(
            output
                .column(1)
                .as_any()
                .downcast_ref::<Int64Array>()
                .unwrap()
                .value(0),
            1
        );
    }

    #[test]
    fn performs_one_distinct_multi_get_and_one_write_batch() {
        let reads = Arc::new(AtomicUsize::new(0));
        let read_keys = Arc::new(AtomicUsize::new(0));
        let writes = Arc::new(AtomicUsize::new(0));
        let written_keys = Arc::new(AtomicUsize::new(0));
        let state = CountingState {
            reads: reads.clone(),
            read_keys: read_keys.clone(),
            writes: writes.clone(),
            written_keys: written_keys.clone(),
        };
        let broker = Arc::new(TestBroker::new(64 << 20));
        let mut processor = GroupAggregateProcessor::with_state(
            &plan(false, false),
            128,
            Box::new(state),
            HostMemoryReservation::new(broker, "test group aggregate scratch"),
        )
        .unwrap();

        processor
            .process_arrow(batch(
                vec![7, 7, 8, 7],
                vec![Some(1), Some(2), Some(3), Some(4)],
                None,
            ))
            .unwrap();

        assert_eq!(reads.load(Ordering::Relaxed), 1);
        assert_eq!(read_keys.load(Ordering::Relaxed), 2);
        assert_eq!(writes.load(Ordering::Relaxed), 1);
        assert_eq!(written_keys.load(Ordering::Relaxed), 2);
        assert_eq!(processor.statistics(), [1, 1]);
    }

    #[test]
    fn accounts_state_batch_scratch_and_exported_output_in_host_memory() {
        let broker = Arc::new(TestBroker::new(64 << 20));
        let mut processor = GroupAggregateProcessor::new(
            &plan(false, false),
            128,
            0,
            127,
            HostMemoryReservation::new(broker.clone(), "test group aggregate state"),
        )
        .unwrap();
        let state_only = broker.reserved();

        let output = processor
            .process_arrow(batch(vec![7, 8], vec![Some(10), Some(20)], None))
            .unwrap();

        assert!(broker.reserved() > state_only);
        drop(output);
        drop(processor);
        assert_eq!(broker.reserved(), 0);
    }

    #[test]
    fn canonical_key_group_snapshots_rescale_without_changing_results() {
        let keys = (0..64).collect::<Vec<_>>();
        let values = keys.iter().map(|key| Some(key * 10)).collect::<Vec<_>>();
        let mut source = processor(false, false);
        source
            .process_arrow(batch(keys.clone(), values.clone(), None))
            .unwrap();
        let snapshots = (0..128)
            .map(|key_group| source.snapshot_key_group(key_group).unwrap())
            .collect::<Vec<_>>();

        let mut baseline = processor(false, false);
        for (key_group, snapshot) in snapshots.iter().enumerate() {
            baseline
                .restore_key_group(key_group as u32, snapshot)
                .unwrap();
        }
        let update = batch(keys.clone(), vec![Some(1); keys.len()], None);
        let expected = output_rows(baseline.process_arrow(update.clone()).unwrap());

        let mut low_keys = Vec::new();
        let mut low_values = Vec::new();
        let mut high_keys = Vec::new();
        let mut high_values = Vec::new();
        for row in 0..update.num_rows() {
            let key_group = source.state_key(&update, row).unwrap().key_group;
            if key_group <= 63 {
                low_keys.push(keys[row]);
                low_values.push(Some(1));
            } else {
                high_keys.push(keys[row]);
                high_values.push(Some(1));
            }
        }
        let mut low = processor_range(false, false, 0, 63);
        let mut high = processor_range(false, false, 64, 127);
        for key_group in 0..64 {
            low.restore_key_group(key_group, &snapshots[key_group as usize])
                .unwrap();
        }
        for key_group in 64..128 {
            high.restore_key_group(key_group, &snapshots[key_group as usize])
                .unwrap();
        }
        let mut actual = output_rows(
            low.process_arrow(batch(low_keys, low_values, None))
                .unwrap(),
        );
        actual.extend(output_rows(
            high.process_arrow(batch(high_keys, high_values, None))
                .unwrap(),
        ));
        actual.sort_unstable();

        assert_eq!(actual, expected);
    }

    #[test]
    fn canonical_group_state_moves_from_memory_to_rocksdb() {
        let Ok(plugin_path) = std::env::var("STREAMFUSION_TEST_ROCKSDB_PLUGIN") else {
            return;
        };
        let mut memory = processor(false, false);
        memory
            .process_arrow(batch(
                vec![7, 7, 8],
                vec![Some(10), Some(20), Some(5)],
                None,
            ))
            .unwrap();
        let snapshots = (0..128)
            .map(|key_group| memory.snapshot_key_group(key_group).unwrap())
            .collect::<Vec<_>>();
        let directory = tempfile::tempdir().unwrap();
        let broker = Arc::new(TestBroker::new(256 << 20));
        let mut rocks = GroupAggregateProcessor::new_rocksdb(
            &plan(false, false),
            128,
            0,
            127,
            std::path::Path::new(&plugin_path),
            directory.path(),
            64 << 20,
            HostMemoryReservation::new(broker, "test RocksDB group aggregate scratch"),
        )
        .unwrap();
        for (key_group, snapshot) in snapshots.iter().enumerate() {
            rocks.restore_key_group(key_group as u32, snapshot).unwrap();
            assert_eq!(
                rocks.snapshot_key_group(key_group as u32).unwrap(),
                *snapshot
            );
        }

        let update = batch(vec![7, 8], vec![Some(1), Some(2)], None);
        assert_eq!(
            output_rows(rocks.process_arrow(update.clone()).unwrap()),
            output_rows(memory.process_arrow(update).unwrap())
        );
    }

    #[test]
    fn canonical_distinct_state_moves_from_memory_to_rocksdb() {
        let Ok(plugin_path) = std::env::var("STREAMFUSION_TEST_ROCKSDB_PLUGIN") else {
            return;
        };
        let plan = aggregate_distinct_plan();
        let mut memory = GroupAggregateProcessor::new(
            &plan,
            128,
            0,
            127,
            HostMemoryReservation::new(
                Arc::new(TestBroker::new(1 << 30)),
                "test distinct memory state",
            ),
        )
        .unwrap();
        memory
            .process_arrow(batch(
                vec![7, 7, 7],
                vec![Some(10), Some(10), Some(20)],
                Some(vec![INSERT, INSERT, INSERT]),
            ))
            .unwrap();
        let snapshots = (0..128)
            .map(|key_group| memory.snapshot_key_group(key_group).unwrap())
            .collect::<Vec<_>>();
        let directory = tempfile::tempdir().unwrap();
        let mut rocks = GroupAggregateProcessor::new_rocksdb(
            &plan,
            128,
            0,
            127,
            std::path::Path::new(&plugin_path),
            directory.path(),
            64 << 20,
            HostMemoryReservation::new(
                Arc::new(TestBroker::new(256 << 20)),
                "test distinct RocksDB scratch",
            ),
        )
        .unwrap();
        for (key_group, snapshot) in snapshots.iter().enumerate() {
            rocks.restore_key_group(key_group as u32, snapshot).unwrap();
        }

        let update = batch(
            vec![7, 7],
            vec![Some(10), Some(10)],
            Some(vec![DELETE, DELETE]),
        );
        assert_eq!(
            rocks.process_arrow(update.clone()).unwrap(),
            memory.process_arrow(update).unwrap()
        );
    }

    #[test]
    fn state_codec_round_trips_every_accumulator_value_family() {
        let state = AccumulatorState {
            row_count: 3,
            accumulators: vec![
                Accumulator::Count(3),
                Accumulator::Sum {
                    value: Some(float32_value(12.5)),
                    count: 2,
                },
                Accumulator::Extremum(BTreeMap::from([
                    (AggregateValue::Boolean(false), 2),
                    (AggregateValue::Boolean(true), 1),
                ])),
                Accumulator::AppendExtremum(Some(AggregateValue::Bytes(b"flink".to_vec()))),
                Accumulator::Extremum(BTreeMap::from([
                    (AggregateValue::Int(100), 2),
                    (AggregateValue::Int(200), -1),
                ])),
                Accumulator::DistinctCount {
                    count: 2,
                    values: BTreeMap::from([
                        (AggregateValue::Int(10), 2),
                        (AggregateValue::Int(20), 1),
                    ]),
                },
                Accumulator::DistinctSum {
                    value: Some(AggregateValue::Int(30)),
                    count: 2,
                    values: BTreeMap::from([
                        (AggregateValue::Int(10), 2),
                        (AggregateValue::Int(20), 1),
                    ]),
                },
                Accumulator::Average {
                    value: Some(float64_value(17.5)),
                    count: 3,
                },
                Accumulator::DistinctAverage {
                    value: Some(AggregateValue::Int(3000)),
                    count: 2,
                    values: BTreeMap::from([
                        (AggregateValue::Int(1000), 2),
                        (AggregateValue::Int(2000), 1),
                    ]),
                },
            ],
        };
        let calls = vec![
            Call {
                function: proto::AggregateFunction::CountStar,
                input_index: None,
                filter_index: None,
                distinct: false,
                input_type: None,
                output_type: DataType::Int64,
                retractable: true,
            },
            Call {
                function: proto::AggregateFunction::Sum,
                input_index: Some(1),
                filter_index: None,
                distinct: false,
                input_type: Some(DataType::Float32),
                output_type: DataType::Float32,
                retractable: true,
            },
            Call {
                function: proto::AggregateFunction::Min,
                input_index: Some(1),
                filter_index: None,
                distinct: false,
                input_type: Some(DataType::Boolean),
                output_type: DataType::Boolean,
                retractable: true,
            },
            Call {
                function: proto::AggregateFunction::Max,
                input_index: Some(1),
                filter_index: None,
                distinct: false,
                input_type: Some(DataType::Utf8),
                output_type: DataType::Utf8,
                retractable: false,
            },
            Call {
                function: proto::AggregateFunction::Min,
                input_index: Some(1),
                filter_index: None,
                distinct: false,
                input_type: Some(DataType::Date32),
                output_type: DataType::Date32,
                retractable: true,
            },
            Call {
                function: proto::AggregateFunction::Count,
                input_index: Some(1),
                filter_index: None,
                distinct: true,
                input_type: Some(DataType::Int64),
                output_type: DataType::Int64,
                retractable: true,
            },
            Call {
                function: proto::AggregateFunction::Sum,
                input_index: Some(1),
                filter_index: None,
                distinct: true,
                input_type: Some(DataType::Int64),
                output_type: DataType::Int64,
                retractable: true,
            },
            Call {
                function: proto::AggregateFunction::Avg,
                input_index: Some(1),
                filter_index: None,
                distinct: false,
                input_type: Some(DataType::Float32),
                output_type: DataType::Float32,
                retractable: true,
            },
            Call {
                function: proto::AggregateFunction::Avg,
                input_index: Some(1),
                filter_index: None,
                distinct: true,
                input_type: Some(DataType::Decimal128(10, 2)),
                output_type: DataType::Decimal128(38, 6),
                retractable: true,
            },
        ];
        assert_eq!(decode_state(&encode_state(&state), &calls).unwrap(), state);
    }

    #[test]
    fn average_accumulates_all_numeric_families_and_distinct_retractions() {
        let calls = vec![
            Call {
                function: proto::AggregateFunction::Avg,
                input_index: Some(0),
                filter_index: None,
                distinct: false,
                input_type: Some(DataType::Int8),
                output_type: DataType::Int8,
                retractable: true,
            },
            Call {
                function: proto::AggregateFunction::Avg,
                input_index: Some(1),
                filter_index: None,
                distinct: false,
                input_type: Some(DataType::Float32),
                output_type: DataType::Float32,
                retractable: true,
            },
            Call {
                function: proto::AggregateFunction::Avg,
                input_index: Some(2),
                filter_index: None,
                distinct: false,
                input_type: Some(DataType::Float64),
                output_type: DataType::Float64,
                retractable: true,
            },
            Call {
                function: proto::AggregateFunction::Avg,
                input_index: Some(3),
                filter_index: None,
                distinct: false,
                input_type: Some(DataType::Decimal128(10, 2)),
                output_type: DataType::Decimal128(38, 6),
                retractable: true,
            },
            Call {
                function: proto::AggregateFunction::Avg,
                input_index: Some(4),
                filter_index: None,
                distinct: true,
                input_type: Some(DataType::Int64),
                output_type: DataType::Int64,
                retractable: true,
            },
        ];
        let first = vec![
            Some(AggregateValue::Int(1)),
            Some(float32_value(1.5)),
            Some(float64_value(2.0)),
            Some(AggregateValue::Int(100)),
            Some(AggregateValue::Int(10)),
        ];
        let second = vec![
            Some(AggregateValue::Int(2)),
            Some(float32_value(2.5)),
            Some(float64_value(4.0)),
            Some(AggregateValue::Int(200)),
            Some(AggregateValue::Int(10)),
        ];
        let mut state = AccumulatorState::new(&calls);
        state.apply_values(&calls, &first, true).unwrap();
        state.apply_values(&calls, &second, true).unwrap();
        assert_eq!(
            state.values(&calls),
            vec![
                Some(AggregateValue::Int(1)),
                Some(float32_value(2.0)),
                Some(float64_value(3.0)),
                Some(AggregateValue::Int(1_500_000)),
                Some(AggregateValue::Int(10)),
            ]
        );

        state.apply_values(&calls, &second, false).unwrap();
        assert_eq!(
            state.values(&calls),
            vec![
                Some(AggregateValue::Int(1)),
                Some(float32_value(1.5)),
                Some(float64_value(2.0)),
                Some(AggregateValue::Int(1_000_000)),
                Some(AggregateValue::Int(10)),
            ]
        );
    }

    #[test]
    fn state_codec_restores_version_one_integer_savepoints() {
        let mut bytes = Vec::new();
        bytes.extend_from_slice(STATE_MAGIC);
        bytes.push(1);
        bytes.extend_from_slice(&2_i64.to_le_bytes());
        bytes.extend_from_slice(&1_u32.to_le_bytes());
        bytes.push(2);
        bytes.extend_from_slice(&123_i128.to_le_bytes());
        bytes.extend_from_slice(&2_i64.to_le_bytes());
        let calls = vec![Call {
            function: proto::AggregateFunction::Sum,
            input_index: Some(1),
            filter_index: None,
            distinct: false,
            input_type: Some(DataType::Int64),
            output_type: DataType::Int64,
            retractable: true,
        }];
        assert_eq!(
            decode_state(&bytes, &calls).unwrap(),
            AccumulatorState {
                row_count: 2,
                accumulators: vec![Accumulator::Sum {
                    value: Some(AggregateValue::Int(123)),
                    count: 2,
                }],
            }
        );

        let mut version_two = Vec::new();
        version_two.extend_from_slice(STATE_MAGIC);
        version_two.push(2);
        version_two.extend_from_slice(&1_i64.to_le_bytes());
        version_two.extend_from_slice(&1_u32.to_le_bytes());
        version_two.push(2);
        encode_value(&float32_value(1.5), &mut version_two);
        version_two.extend_from_slice(&1_i64.to_le_bytes());
        let float_calls = vec![Call {
            function: proto::AggregateFunction::Sum,
            input_index: Some(1),
            filter_index: None,
            distinct: false,
            input_type: Some(DataType::Float32),
            output_type: DataType::Float32,
            retractable: true,
        }];
        assert_eq!(
            decode_state(&version_two, &float_calls).unwrap(),
            AccumulatorState {
                row_count: 1,
                accumulators: vec![Accumulator::Sum {
                    value: Some(float32_value(1.5)),
                    count: 1,
                }],
            }
        );
    }

    #[test]
    fn float_extrema_use_flink_nan_and_signed_zero_ordering() {
        let negative_zero = float32_value(-0.0);
        let positive_zero = float32_value(0.0);
        let nan = float32_value(f32::NAN);
        assert!(negative_zero < positive_zero);
        assert!(nan > positive_zero);

        let output = aggregate_array(
            &[Some(negative_zero), Some(positive_zero), Some(nan)],
            &DataType::Float32,
        )
        .unwrap();
        let values = output.as_any().downcast_ref::<Float32Array>().unwrap();
        assert_eq!(values.value(0).to_bits(), (-0.0_f32).to_bits());
        assert_eq!(values.value(1).to_bits(), 0.0_f32.to_bits());
        assert!(values.value(2).is_nan());

        let booleans = aggregate_array(
            &[
                Some(AggregateValue::Boolean(false)),
                None,
                Some(AggregateValue::Boolean(true)),
            ],
            &DataType::Boolean,
        )
        .unwrap();
        assert_eq!(
            booleans
                .as_any()
                .downcast_ref::<BooleanArray>()
                .unwrap()
                .iter()
                .collect::<Vec<_>>(),
            vec![Some(false), None, Some(true)]
        );
    }

    #[test]
    fn sum_uses_flink_integer_wrap_and_decimal_null_on_overflow() {
        assert_eq!(
            aggregate_add(
                &AggregateValue::Int(i64::MAX as i128),
                &AggregateValue::Int(1),
                &DataType::Int64,
            )
            .unwrap(),
            Some(AggregateValue::Int(i64::MIN as i128))
        );
        assert_eq!(
            aggregate_add(
                &AggregateValue::Int(10_i128.pow(38) - 1),
                &AggregateValue::Int(1),
                &DataType::Decimal128(38, 0),
            )
            .unwrap(),
            None
        );
        assert_eq!(
            aggregate_sub(
                &AggregateValue::Int(-(10_i128.pow(38) - 1)),
                &AggregateValue::Int(1),
                &DataType::Decimal128(38, 0),
            )
            .unwrap(),
            None
        );
    }

    fn output_rows(
        batch: RecordBatch,
    ) -> Vec<(i64, i64, Option<i64>, Option<i64>, Option<i64>, i8)> {
        let column = |index: usize| {
            batch
                .column(index)
                .as_any()
                .downcast_ref::<Int64Array>()
                .unwrap()
        };
        let keys = column(0);
        let counts = column(1);
        let sums = column(2);
        let minimums = column(3);
        let maximums = column(4);
        let kinds = batch
            .column(5)
            .as_any()
            .downcast_ref::<Int8Array>()
            .unwrap();
        let mut rows = (0..batch.num_rows())
            .map(|row| {
                (
                    keys.value(row),
                    counts.value(row),
                    (!sums.is_null(row)).then(|| sums.value(row)),
                    (!minimums.is_null(row)).then(|| minimums.value(row)),
                    (!maximums.is_null(row)).then(|| maximums.value(row)),
                    kinds.value(row),
                )
            })
            .collect::<Vec<_>>();
        rows.sort_unstable();
        rows
    }

    struct CountingState {
        reads: Arc<AtomicUsize>,
        read_keys: Arc<AtomicUsize>,
        writes: Arc<AtomicUsize>,
        written_keys: Arc<AtomicUsize>,
    }

    impl KeyedState for CountingState {
        fn get_batch<'a>(&'a self, keys: &[StateKeyRef<'_>]) -> Result<Vec<Option<Cow<'a, [u8]>>>> {
            self.reads.fetch_add(1, Ordering::Relaxed);
            self.read_keys.fetch_add(keys.len(), Ordering::Relaxed);
            Ok(vec![None; keys.len()])
        }

        fn write_batch(&mut self, mutations: Vec<StateMutation>) -> Result<()> {
            self.writes.fetch_add(1, Ordering::Relaxed);
            self.written_keys
                .fetch_add(mutations.len(), Ordering::Relaxed);
            Ok(())
        }

        fn snapshot_key_group(&self, _key_group: u32) -> Result<Vec<u8>> {
            unreachable!("snapshot is not used by the batch-call test")
        }

        fn restore_key_group(&mut self, _key_group: u32, _bytes: &[u8]) -> Result<()> {
            unreachable!("restore is not used by the batch-call test")
        }
    }
}
