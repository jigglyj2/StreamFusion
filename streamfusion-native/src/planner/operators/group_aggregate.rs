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
use crate::planner::persistent::unary::InvocationState;
use crate::state::{
    decode_key_group_snapshot, KeyedState, MemoryKeyedState, RocksPluginKeyedState, StateKey,
    StateKeyRef, StateMutation,
};
use crate::{decode_plan, proto};

mod accumulator;
mod control;
pub(crate) mod execution_plan;
mod mini_batch;
mod output_admission;
mod partial_batch;
mod planning;
mod state_codec;
mod values;
pub(super) use planning::lower_call;
use planning::*;
use values::*;
pub(super) use values::{aggregate_array, row_aggregate_values, value_tag};

use state_codec::accumulator_is_neutral;
#[cfg(test)]
use state_codec::encode_value;
use state_codec::{decode_bounded_state, encode_bounded_state};
pub(super) use state_codec::{decode_state, encode_state};

const INSERT: i8 = 0;
const UPDATE_BEFORE: i8 = 1;
const UPDATE_AFTER: i8 = 2;
const DELETE: i8 = 3;
const STATE_MAGIC: &[u8; 4] = b"SFGA";
const STATE_VERSION: u8 = 6;
const BOUNDED_STATE_MAGIC: &[u8; 4] = b"SFBG";
const BOUNDED_STATE_VERSION: u8 = 1;
const BOUNDED_OUTPUT_MAX_ROWS: usize = 16_384;

/// Persistent timer-free keyed aggregation handle shared by memory and RocksDB state.
pub(crate) struct GroupAggregateProcessor {
    plan: proto::GroupAggregate,
    calls: Vec<Call>,
    max_parallelism: u32,
    last_key_group: u32,
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
    bounded_output_finished: bool,
    bounded_output_key_group: Option<u32>,
    bounded_output_entries: Vec<(Vec<u8>, Vec<u8>)>,
    bounded_output_entry_index: usize,
    bounded_output_had_rows: bool,
    invocation: InvocationState,
    native_envelope: bool,
    native_owned_envelope: bool,
    control_flushing: bool,
    // Plan, codecs and schemas must remain admitted independently of batch scratch.
    control_reservation: HostMemoryReservation,
    _planned_schema_reservation: HostMemoryReservation,
    schema_reservation: Option<HostMemoryReservation>,
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
    pub(super) fn average_accumulator_type(&self) -> DataType {
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
    pub(super) accumulators: Vec<Accumulator>,
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub(super) enum Accumulator {
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

    fn estimated_dynamic_bytes(&self) -> usize {
        let grouping = self
            .grouping_rows
            .capacity()
            .saturating_mul(std::mem::size_of::<Vec<u8>>())
            .saturating_add(
                self.grouping_rows
                    .iter()
                    .map(|row| row.capacity())
                    .sum::<usize>(),
            );
        let values = self.values.iter().fold(
            self.values
                .capacity()
                .saturating_mul(std::mem::size_of::<Vec<Option<AggregateValue>>>()),
            |bytes, column| {
                bytes
                    .saturating_add(
                        column
                            .capacity()
                            .saturating_mul(std::mem::size_of::<Option<AggregateValue>>()),
                    )
                    .saturating_add(column.iter().fold(0usize, |value_bytes, value| {
                        value_bytes.saturating_add(match value {
                            Some(AggregateValue::Bytes(bytes)) => bytes.capacity(),
                            _ => 0,
                        })
                    }))
            },
        );
        grouping
            .saturating_add(self.row_kinds.capacity())
            .saturating_add(values)
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
        Self::with_state(
            serialized_plan,
            max_parallelism,
            first_key_group,
            last_key_group,
            state,
            scratch_reservation,
        )
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
        let state = Box::new(RocksPluginKeyedState::open_for_owner(
            plugin_path,
            database_path,
            first_key_group,
            last_key_group,
            memory_limit,
            &scratch_reservation,
        )?);
        Self::with_state(
            serialized_plan,
            max_parallelism,
            first_key_group,
            last_key_group,
            state,
            scratch_reservation,
        )
    }

    fn with_state(
        serialized_plan: &[u8],
        max_parallelism: u32,
        first_key_group: u32,
        last_key_group: u32,
        state: Box<dyn KeyedState>,
        scratch_reservation: HostMemoryReservation,
    ) -> Result<Self> {
        let mut control_reservation =
            scratch_reservation.sibling("native group aggregate plan and codecs");
        control_reservation.resize(
            crate::execution_context::wire_memory::PlanMemory::scan(serialized_plan)?
                .decoded()?
                .saturating_add(4096),
        )?;
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
                        bounded_final_output: plan.bounded_final_output,
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
        if partial_input && !plan.bounded_final_output && plan.mini_batch_size == 0 {
            return Err(DataFusionError::Plan(
                "global aggregate mini-batch size must be positive".into(),
            ));
        }
        // Decoded protobuf size is not a bound on Arrow fields, recursive row codecs, or
        // their temporary null arrays. Admit those separately before lowering any call/type.
        let mut planned_schema_reservation =
            scratch_reservation.sibling("group aggregate planned Arrow schemas and codecs");
        planned_schema_reservation.resize(schema_admission::planned_workspace(&plan)?)?;
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
        let retained_schema_bytes = schema_admission::retained_workspace(
            &calls,
            grouping_converter.as_ref(),
            output_schema.as_ref(),
        )?;
        if retained_schema_bytes > planned_schema_reservation.size() {
            return Err(DataFusionError::ResourcesExhausted(
                "group aggregate retained codecs exceeded admitted construction workspace".into(),
            ));
        }
        planned_schema_reservation.resize(retained_schema_bytes)?;
        Ok(Self {
            plan,
            calls,
            max_parallelism,
            last_key_group,
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
            bounded_output_finished: false,
            bounded_output_key_group: Some(first_key_group),
            bounded_output_entries: Vec::new(),
            bounded_output_entry_index: 0,
            bounded_output_had_rows: false,
            invocation: InvocationState::default(),
            native_envelope: false,
            native_owned_envelope: false,
            control_flushing: false,
            control_reservation,
            _planned_schema_reservation: planned_schema_reservation,
            schema_reservation: None,
        })
    }

    pub(crate) fn process_arrow(&mut self, batch: RecordBatch) -> Result<RecordBatch> {
        self.invocation.require_idle("group aggregate")?;
        // Input arrays are borrowed through Arrow C Data and remain charged to their Java
        // owner. Reserve only the native key/index/event structures created for this call.
        let base_reservation = batch
            .num_rows()
            .saturating_mul(192usize.saturating_add(self.calls.len().saturating_mul(64)));
        self.scratch_reservation.resize(base_reservation)?;
        let result = if self.partial_input && self.plan.bounded_final_output {
            self.process_bounded_partial_accounted(batch, base_reservation)
        } else if self.partial_input {
            self.process_partial_mini_accounted(batch, base_reservation)
        } else if self.plan.bounded_final_output {
            self.process_bounded_accounted(batch, base_reservation)
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
        let base_reservation = base_reservation
            .checked_add(self.input_admission(&batch)?)
            .ok_or_else(|| {
                DataFusionError::ResourcesExhausted(
                    "group aggregate input admission overflow".into(),
                )
            })?;
        self.scratch_reservation.resize(base_reservation)?;
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
        let base_reservation = base_reservation
            .checked_add(self.accumulator_admission(unique_keys.len(), batch.num_rows())?)
            .ok_or_else(|| {
                DataFusionError::ResourcesExhausted(
                    "group aggregate accumulator admission overflow".into(),
                )
            })?;
        self.scratch_reservation.resize(base_reservation)?;
        let unique_key_refs = unique_keys
            .iter()
            .map(|key| StateKeyRef {
                key_group: key.key_group,
                key: &key.key,
            })
            .collect::<Vec<_>>();
        let existing = self
            .state
            .get_batch(&unique_key_refs, &self.scratch_reservation)?;
        let _loaded_state_workspace =
            crate::state::reserve_decoded_values(&existing, &self.scratch_reservation)?;
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
        let mut event_memory = self
            .scratch_reservation
            .sibling("group aggregate historical output events");
        event_memory.resize(self.event_admission(&batch, &staged_values, &row_key_indices)?)?;

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
        // Admit gather/builder buffers while historical values and dirty mutations are still
        // alive. Checking the finished batch would allocate before asking Flink for memory.
        let output_allowance = self.output_admission(&batch, &events)?;
        self.scratch_reservation.try_grow(output_allowance)?;
        let output = self.output_batch(&batch, events)?;
        if output.get_array_memory_size() > output_allowance {
            return Err(DataFusionError::Internal(
                "group aggregate output exceeded its pre-admitted capacity".into(),
            ));
        }
        self.state.write_batch(mutations)?;
        self.state_write_batches = self.state_write_batches.saturating_add(1);
        Ok(output)
    }

    fn process_bounded_accounted(
        &mut self,
        batch: RecordBatch,
        base_reservation: usize,
    ) -> Result<RecordBatch> {
        if self.bounded_output_finished {
            return Err(DataFusionError::Execution(
                "bounded group aggregate received input after terminal output".to_string(),
            ));
        }
        self.prepare_schema(batch.schema(), batch.num_columns())?;
        let grouping_rows = self.encode_grouping_rows(&batch)?;
        let mut unique = HashMap::<StateKey, usize, RandomState>::with_capacity_and_hasher(
            batch.num_rows().max(1),
            RandomState::new(),
        );
        let mut keys = Vec::new();
        let mut first_rows = Vec::new();
        let mut row_key_indices = Vec::with_capacity(batch.num_rows());
        for row in 0..batch.num_rows() {
            let key = self.state_key(&batch, row)?;
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
            row_key_indices.push(index);
        }
        if keys.is_empty() {
            return self.bundle_output_batch(BundleOutputEvents::new(self.calls.len()));
        }
        let refs = keys
            .iter()
            .map(|key| StateKeyRef {
                key_group: key.key_group,
                key: &key.key,
            })
            .collect::<Vec<_>>();
        let existing = self.state.get_batch(&refs, &self.scratch_reservation)?;
        let _loaded_state_workspace =
            crate::state::reserve_decoded_values(&existing, &self.scratch_reservation)?;
        self.state_read_batches = self.state_read_batches.saturating_add(1);
        drop(refs);
        let serialized_bytes = existing.iter().fold(0usize, |bytes, value| {
            bytes.saturating_add(value.as_deref().map_or(0, <[u8]>::len))
        });
        self.scratch_reservation
            .resize(base_reservation.saturating_add(serialized_bytes.saturating_mul(4)))?;
        let mut staged = existing
            .iter()
            .enumerate()
            .map(|(index, value)| {
                value
                    .as_deref()
                    .map(|bytes| decode_bounded_state(bytes, &self.calls))
                    .transpose()
                    .map(|decoded| {
                        decoded.unwrap_or_else(|| {
                            (
                                grouping_rows[first_rows[index]].clone(),
                                AccumulatorState::new(&self.calls),
                            )
                        })
                    })
            })
            .collect::<Result<Vec<_>>>()?;
        drop(existing);
        for row in 0..batch.num_rows() {
            let index = row_key_indices[row];
            let accumulate = self.accumulates(&batch, row)?;
            if staged[index].1.row_count == 0 && !accumulate {
                continue;
            }
            staged[index]
                .1
                .apply(&self.calls, &batch, row, accumulate)?;
        }
        let mutations = keys
            .into_iter()
            .zip(staged)
            .map(|(key, (grouping_row, state))| -> Result<StateMutation> {
                Ok(StateMutation {
                    key,
                    value: (state.row_count != 0)
                        .then(|| encode_bounded_state(&grouping_row, &state))
                        .transpose()?,
                })
            })
            .collect::<Result<Vec<_>>>()?;
        self.state.write_batch(mutations)?;
        self.state_write_batches = self.state_write_batches.saturating_add(1);
        self.bundle_output_batch(BundleOutputEvents::new(self.calls.len()))
    }

    fn process_bounded_partial_accounted(
        &mut self,
        batch: RecordBatch,
        base_reservation: usize,
    ) -> Result<RecordBatch> {
        if self.bounded_output_finished {
            return Err(DataFusionError::Execution(
                "bounded global group aggregate received input after terminal output".to_string(),
            ));
        }
        self.prepare_schema(batch.schema(), batch.num_columns())?;
        let grouping_rows = self.encode_grouping_rows(&batch)?;
        let mut unique = HashMap::<StateKey, usize, RandomState>::with_capacity_and_hasher(
            batch.num_rows().max(1),
            RandomState::new(),
        );
        let mut keys = Vec::new();
        let mut first_rows = Vec::new();
        let mut row_key_indices = Vec::with_capacity(batch.num_rows());
        for row in 0..batch.num_rows() {
            let key = self.state_key(&batch, row)?;
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
            row_key_indices.push(index);
        }
        if keys.is_empty() {
            return self.bundle_output_batch(BundleOutputEvents::new(self.calls.len()));
        }
        let refs = keys
            .iter()
            .map(|key| StateKeyRef {
                key_group: key.key_group,
                key: &key.key,
            })
            .collect::<Vec<_>>();
        let existing = self.state.get_batch(&refs, &self.scratch_reservation)?;
        let _loaded_state_workspace =
            crate::state::reserve_decoded_values(&existing, &self.scratch_reservation)?;
        self.state_read_batches = self.state_read_batches.saturating_add(1);
        drop(refs);
        let serialized_bytes = existing.iter().fold(0usize, |bytes, value| {
            bytes.saturating_add(value.as_deref().map_or(0, <[u8]>::len))
        });
        self.scratch_reservation
            .resize(base_reservation.saturating_add(serialized_bytes.saturating_mul(4)))?;
        let mut staged = existing
            .iter()
            .enumerate()
            .map(|(index, value)| {
                value
                    .as_deref()
                    .map(|bytes| decode_bounded_state(bytes, &self.calls))
                    .transpose()
                    .map(|decoded| {
                        decoded.unwrap_or_else(|| {
                            (
                                grouping_rows[first_rows[index]].clone(),
                                AccumulatorState::new(&self.calls),
                            )
                        })
                    })
            })
            .collect::<Result<Vec<_>>>()?;
        drop(existing);
        let accumulator_index = self
            .visible_count
            .expect("bounded partial schema was prepared")
            - 1;
        let accumulators = batch
            .column(accumulator_index)
            .as_any()
            .downcast_ref::<BinaryArray>()
            .expect("bounded partial accumulator type was validated");
        for row in 0..batch.num_rows() {
            if accumulators.is_null(row) {
                return Err(DataFusionError::Execution(
                    "local aggregate accumulator cannot be null".to_string(),
                ));
            }
            let partial = decode_state(accumulators.value(row), &self.calls)?;
            staged[row_key_indices[row]]
                .1
                .merge(&self.calls, &partial)?;
        }
        let mutations = keys
            .into_iter()
            .zip(staged)
            .map(|(key, (grouping_row, state))| -> Result<StateMutation> {
                Ok(StateMutation {
                    key,
                    value: (state.row_count != 0)
                        .then(|| encode_bounded_state(&grouping_row, &state))
                        .transpose()?,
                })
            })
            .collect::<Result<Vec<_>>>()?;
        self.state.write_batch(mutations)?;
        self.state_write_batches = self.state_write_batches.saturating_add(1);
        self.bundle_output_batch(BundleOutputEvents::new(self.calls.len()))
    }

    fn process_mini_batch_accounted(
        &mut self,
        batch: RecordBatch,
        base_reservation: usize,
    ) -> Result<RecordBatch> {
        self.process_raw_mini_batch(batch, base_reservation)
    }

    fn process_partial_mini_accounted(
        &mut self,
        batch: RecordBatch,
        base_reservation: usize,
    ) -> Result<RecordBatch> {
        self.process_partial_batch(batch, base_reservation)
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
        self.invocation.require_idle("group aggregate")?;
        if self.plan.bounded_final_output {
            return self.finish_bounded_output();
        }
        let mut events = BundleOutputEvents::new(self.calls.len());
        self.finish_pending(&mut events)?;
        self.bundle_reservation
            .resize(self.estimated_pending_bytes())?;
        let output = self.bundle_output_batch(events)?;
        let output_bytes = output.get_array_memory_size();
        self.scratch_reservation.resize(output_bytes)?;
        self.scratch_reservation.transfer_to_arrow(output_bytes)?;
        self.scratch_reservation.resize(0)?;
        Ok(output)
    }

    fn finish_bounded_output(&mut self) -> Result<RecordBatch> {
        if self.bounded_output_finished {
            return self.bundle_output_batch(BundleOutputEvents::new(self.calls.len()));
        }
        let mut events = BundleOutputEvents::new(self.calls.len());
        self.scratch_reservation
            .resize(self.bounded_output_retained_bytes())?;
        while events.grouping_rows.len() < BOUNDED_OUTPUT_MAX_ROWS {
            if self.bounded_output_entry_index < self.bounded_output_entries.len() {
                let value = &self.bounded_output_entries[self.bounded_output_entry_index].1;
                self.bounded_output_entry_index += 1;
                let (grouping_row, state) = decode_bounded_state(value, &self.calls)?;
                if state.row_count != 0 {
                    events.push(grouping_row, INSERT, state.values(&self.calls));
                    self.bounded_output_had_rows = true;
                }
                continue;
            }
            self.bounded_output_entries.clear();
            self.bounded_output_entry_index = 0;
            let Some(key_group) = self.bounded_output_key_group else {
                break;
            };
            let snapshot = self
                .state
                .snapshot_key_group(key_group, &self.scratch_reservation)?;
            self.scratch_reservation.resize(
                self.bounded_output_retained_bytes()
                    .saturating_add(snapshot.len().saturating_mul(2)),
            )?;
            self.bounded_output_entries = decode_key_group_snapshot(key_group, &snapshot)?;
            self.bounded_output_key_group = if key_group < self.last_key_group {
                Some(key_group + 1)
            } else {
                None
            };
            if self.bounded_output_entries.is_empty() {
                continue;
            }
        }
        let exhausted = self.bounded_output_key_group.is_none()
            && self.bounded_output_entry_index == self.bounded_output_entries.len();
        if events.grouping_rows.is_empty()
            && exhausted
            && !self.bounded_output_had_rows
            && self.plan.grouping_indices.is_empty()
        {
            let state = AccumulatorState::new(&self.calls);
            events.push(Vec::new(), INSERT, state.values(&self.calls));
            self.bounded_output_had_rows = true;
        } else if events.grouping_rows.is_empty() && exhausted {
            self.bounded_output_finished = true;
            self.bounded_output_entries.clear();
            self.scratch_reservation.resize(0)?;
            return self.bundle_output_batch(events);
        }
        let retained_bytes = self.bounded_output_retained_bytes();
        let event_bytes = events.estimated_dynamic_bytes();
        let anticipated_output = event_bytes
            .saturating_mul(2)
            .saturating_add(events.grouping_rows.len().saturating_mul(64))
            .saturating_add(4096);
        self.scratch_reservation.resize(
            retained_bytes
                .saturating_add(event_bytes)
                .saturating_add(anticipated_output),
        )?;
        let output = self.bundle_output_batch(events)?;
        let output_bytes = output.get_array_memory_size();
        self.scratch_reservation
            .resize(retained_bytes.saturating_add(output_bytes))?;
        self.scratch_reservation.transfer_to_arrow(output_bytes)?;
        self.scratch_reservation.resize(retained_bytes)?;
        Ok(output)
    }

    fn bounded_output_retained_bytes(&self) -> usize {
        self.bounded_output_entries
            .capacity()
            .saturating_mul(std::mem::size_of::<(Vec<u8>, Vec<u8>)>())
            .saturating_add(self.bounded_output_entries.iter().fold(
                0usize,
                |bytes, (key, value)| {
                    bytes
                        .saturating_add(key.capacity())
                        .saturating_add(value.capacity())
                },
            ))
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
        let map_storage = self.pending.allocation_size();
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
        let rows = events.row_kinds.len();
        if self.native_owned_envelope {
            columns.push(Arc::new(Int64Array::new_null(rows)) as ArrayRef);
        }
        columns.push(Arc::new(Int8Array::from(events.row_kinds)) as ArrayRef);
        if self.native_envelope {
            columns.push(Arc::new(Int32Array::from(vec![-1; rows])) as ArrayRef);
        }
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
        if self.native_owned_envelope {
            columns.push(arrow::compute::take(
                input.column(input.num_columns() - 3).as_ref(),
                &indices,
                None,
            )?);
        }
        columns.push(Arc::new(Int8Array::from(events.row_kinds)));
        fields.push(Field::new("__streamfusion_row_kind", DataType::Int8, false));
        if self.native_envelope {
            columns.push(arrow::compute::take(
                input.column(input.num_columns() - 1).as_ref(),
                &indices,
                None,
            )?);
            return Ok(RecordBatch::try_new(
                self.output_schema
                    .clone()
                    .expect("native output schema prepared"),
                columns,
            )?);
        }
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
        let mut schema_reservation = self
            .control_reservation
            .sibling("group aggregate retained schema");
        schema_reservation.resize(schema.fields().iter().fold(4096usize, |bytes, field| {
            bytes.saturating_add(field.size().saturating_mul(16))
        }))?;
        let preencoded_key_index = schema
            .fields()
            .iter()
            .position(|field| field.name() == "__streamfusion_key");
        let input_kind_index = schema.fields().iter().position(|field| {
            matches!(
                field.name().as_str(),
                "__streamfusion_input_row_kind" | "__streamfusion_row_kind"
            )
        });
        let visible_count = [
            preencoded_key_index,
            input_kind_index,
            schema.fields().iter().position(|field| {
                field.name() == crate::planner::operators::envelope::OWNED_TIMESTAMP_V1
            }),
            schema
                .fields()
                .iter()
                .position(|field| field.name() == "__streamfusion_input_row"),
            Some(column_count),
        ]
        .into_iter()
        .flatten()
        .min()
        .ok_or_else(|| {
            DataFusionError::Execution("group aggregate input has no visible columns".to_string())
        })?;
        if self.plan.input_changelog && input_kind_index.is_none() {
            return Err(DataFusionError::Execution(
                "changelog group aggregate requires input RowKind metadata".to_string(),
            ));
        }
        let key_fields = if preencoded_key_index.is_none() {
            self.plan
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
                .collect::<std::result::Result<Vec<_>, arrow::error::ArrowError>>()?
        } else {
            Vec::new()
        };
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
        let grouping_converter = if self.grouping_converter.is_none() {
            let sort_fields = grouping_fields
                .iter()
                .map(|field| SortField::new(field.data_type().clone()))
                .collect::<Vec<_>>();
            if !RowConverter::supports_fields(&sort_fields) {
                return Err(DataFusionError::Plan(
                    "group aggregate key type is not supported by Arrow row encoding".to_string(),
                ));
            }
            Some(RowConverter::new(sort_fields)?)
        } else {
            None
        };
        let output_schema = if self.output_schema.is_none() {
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
            Some(Arc::new(Schema::new(fields)))
        } else {
            None
        };
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
                if let Some(index) = call.filter_index {
                    if schema
                        .fields()
                        .get(index)
                        .filter(|_| index < visible_count)
                        .is_none_or(|field| field.data_type() != &DataType::Boolean)
                    {
                        return Err(DataFusionError::Plan(
                            "aggregate FILTER must reference a BOOLEAN payload field".into(),
                        ));
                    }
                }
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
        // Publish codec/schema state only after all validation succeeds; failed setup may retry.
        self.preencoded_key_index = preencoded_key_index;
        self.input_kind_index = input_kind_index;
        self.key_fields = key_fields;
        if grouping_converter.is_some() {
            self.grouping_converter = grouping_converter;
        }
        if output_schema.is_some() {
            self.output_schema = output_schema;
        }
        self.visible_count = Some(visible_count);
        self.input_schema = Some(schema);
        self.schema_reservation = Some(schema_reservation);
        Ok(())
    }

    pub(crate) fn state_memory(&self) -> HostMemoryReservation {
        self.scratch_reservation.sibling("native state transfer")
    }

    pub(crate) fn snapshot_key_group(&self, key_group: u32) -> Result<crate::state::SnapshotBytes> {
        self.invocation.require_idle("group aggregate")?;
        self.state
            .snapshot_key_group(key_group, &self.scratch_reservation)
    }

    pub(crate) fn checkpoint(&self, directory: &std::path::Path) -> Result<()> {
        self.invocation.require_idle("group aggregate")?;
        self.state.checkpoint(directory)
    }

    pub(crate) fn restore_key_group(&mut self, key_group: u32, bytes: &[u8]) -> Result<()> {
        self.invocation.require_idle("group aggregate")?;
        self.state
            .restore_key_group(key_group, bytes, &self.scratch_reservation)
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

#[cfg(test)]
mod allocation_tests;
pub(super) mod schema_admission;
#[cfg(test)]
mod tests;
