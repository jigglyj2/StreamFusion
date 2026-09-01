// Copyright 2026 StreamFusion Authors
// Licensed under the Apache License, Version 2.0

use std::collections::BTreeMap;
use std::sync::Arc;

use ahash::RandomState;
use arrow::array::{
    Array, ArrayRef, Decimal128Array, Int16Array, Int32Array, Int64Array, Int8Array, UInt32Array,
};
use arrow::datatypes::{DataType, Field, Schema, SchemaRef};
use arrow::record_batch::RecordBatch;
use datafusion::error::{DataFusionError, Result};
use hashbrown::HashMap;

use crate::exchange::{assign_key_group, encode_binary_row, KeyField};
use crate::memory_pool::HostMemoryReservation;
use crate::planner::expressions::null_literal;
use crate::state::{
    KeyedState, MemoryKeyedState, RocksPluginKeyedState, StateKey, StateKeyRef, StateMutation,
};
use crate::{decode_plan, proto};

const INSERT: i8 = 0;
const UPDATE_BEFORE: i8 = 1;
const UPDATE_AFTER: i8 = 2;
const DELETE: i8 = 3;
const STATE_MAGIC: &[u8; 4] = b"SFGA";
const STATE_VERSION: u8 = 1;

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
}

#[derive(Clone)]
struct Call {
    function: proto::AggregateFunction,
    input_index: Option<usize>,
    input_type: Option<DataType>,
    output_type: DataType,
    retractable: bool,
}

#[derive(Clone, Debug, PartialEq, Eq)]
struct AccumulatorState {
    row_count: i64,
    accumulators: Vec<Accumulator>,
}

#[derive(Clone, Debug, PartialEq, Eq)]
enum Accumulator {
    Count(i64),
    Sum { value: i128, count: i64 },
    AppendExtremum(Option<i128>),
    Extremum(BTreeMap<i128, i64>),
}

struct OutputEvents {
    input_rows: Vec<u32>,
    row_kinds: Vec<i8>,
    values: Vec<Vec<Option<i128>>>,
}

impl OutputEvents {
    fn with_capacity(rows: usize, calls: usize) -> Self {
        Self {
            input_rows: Vec::with_capacity(rows),
            row_kinds: Vec::with_capacity(rows),
            values: (0..calls).map(|_| Vec::with_capacity(rows)).collect(),
        }
    }

    fn push(&mut self, input_row: u32, row_kind: i8, values: Vec<Option<i128>>) {
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
        let plan = match root.operator {
            Some(proto::operator::Operator::GroupAggregate(plan)) => *plan,
            _ => {
                return Err(DataFusionError::Plan(
                    "stateful group aggregate handle requires a GroupAggregate root".to_string(),
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
        })
    }

    pub(crate) fn process_arrow(&mut self, batch: RecordBatch) -> Result<RecordBatch> {
        // Input arrays are borrowed through Arrow C Data and remain charged to their Java
        // owner. Reserve only the native key/index/event structures created for this call.
        let base_reservation = batch
            .num_rows()
            .saturating_mul(192usize.saturating_add(self.calls.len().saturating_mul(64)));
        self.scratch_reservation.resize(base_reservation)?;
        match self.process_arrow_accounted(batch, base_reservation) {
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
        let mut unique_indices = HashMap::<StateKey, usize, RandomState>::with_capacity_and_hasher(
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
        let unique_key_refs = unique_keys
            .iter()
            .map(|key| StateKeyRef {
                key_group: key.key_group,
                key: &key.key,
            })
            .collect::<Vec<_>>();
        let existing = self.state.get_batch(&unique_key_refs)?;
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
        self.output_batch(&batch, events)
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
            columns.push(numeric_array(
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

impl AccumulatorState {
    fn new(calls: &[Call]) -> Self {
        Self {
            row_count: 0,
            accumulators: calls
                .iter()
                .map(|call| match call.function {
                    proto::AggregateFunction::CountStar | proto::AggregateFunction::Count => {
                        Accumulator::Count(0)
                    }
                    proto::AggregateFunction::Sum => Accumulator::Sum { value: 0, count: 0 },
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

    fn apply(
        &mut self,
        calls: &[Call],
        batch: &RecordBatch,
        row: usize,
        accumulate: bool,
    ) -> Result<()> {
        let delta = if accumulate { 1 } else { -1 };
        self.row_count = self.row_count.wrapping_add(delta);
        for (call, accumulator) in calls.iter().zip(&mut self.accumulators) {
            match accumulator {
                Accumulator::Count(count) => {
                    let present = call
                        .input_index
                        .is_none_or(|index| !batch.column(index).is_null(row));
                    if present {
                        *count = count.wrapping_add(delta);
                    }
                }
                Accumulator::Sum { value: sum, count } => {
                    let value = numeric_value(
                        batch
                            .column(call.input_index.expect("SUM has an input"))
                            .as_ref(),
                        row,
                    )?;
                    if let Some(value) = value {
                        *sum = if accumulate {
                            wrapping_numeric_add(*sum, value, &call.output_type)
                        } else {
                            wrapping_numeric_sub(*sum, value, &call.output_type)
                        };
                        *count = count.wrapping_add(delta);
                    }
                }
                Accumulator::AppendExtremum(extremum) => {
                    let value = numeric_value(
                        batch
                            .column(call.input_index.expect("MIN/MAX has an input"))
                            .as_ref(),
                        row,
                    )?;
                    if let Some(value) = value {
                        *extremum = Some(match (*extremum, call.function) {
                            (Some(current), proto::AggregateFunction::Min) => current.min(value),
                            (Some(current), proto::AggregateFunction::Max) => current.max(value),
                            (None, _) => value,
                            _ => unreachable!("validated extremum function"),
                        });
                    }
                }
                Accumulator::Extremum(values) => {
                    let value = numeric_value(
                        batch
                            .column(call.input_index.expect("MIN/MAX has an input"))
                            .as_ref(),
                        row,
                    )?;
                    if let Some(value) = value {
                        let count = values.entry(value).or_default();
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

    fn values(&self, calls: &[Call]) -> Vec<Option<i128>> {
        calls
            .iter()
            .zip(&self.accumulators)
            .map(|(call, accumulator)| match accumulator {
                Accumulator::Count(value) => Some(*value as i128),
                Accumulator::Sum { value, count } => (*count != 0).then_some(*value),
                Accumulator::AppendExtremum(value) => *value,
                Accumulator::Extremum(values) => match call.function {
                    proto::AggregateFunction::Min => values
                        .iter()
                        .find_map(|(value, count)| (*count > 0).then_some(*value)),
                    proto::AggregateFunction::Max => values
                        .iter()
                        .rev()
                        .find_map(|(value, count)| (*count > 0).then_some(*value)),
                    _ => unreachable!("validated extremum function"),
                },
            })
            .collect()
    }
}

fn validate_plan(plan: &proto::GroupAggregate, max_parallelism: u32) -> Result<()> {
    if max_parallelism == 0 {
        return Err(DataFusionError::Plan(
            "group aggregate max parallelism must be positive".to_string(),
        ));
    }
    if plan.grouping_indices.is_empty() {
        return Err(DataFusionError::Plan(
            "group aggregate requires at least one grouping field".to_string(),
        ));
    }
    if plan.aggregate_calls.is_empty() {
        return Err(DataFusionError::Plan(
            "group aggregate requires at least one aggregate call".to_string(),
        ));
    }
    Ok(())
}

fn lower_call(call: &proto::AggregateCall) -> Result<Call> {
    let function = proto::AggregateFunction::try_from(call.function).map_err(|_| {
        DataFusionError::Plan(format!("unknown aggregate function {}", call.function))
    })?;
    if function == proto::AggregateFunction::Unspecified {
        return Err(DataFusionError::Plan(
            "aggregate function is unspecified".to_string(),
        ));
    }
    let input_index = call.input_index.map(|index| index as usize);
    if function == proto::AggregateFunction::CountStar && input_index.is_some() {
        return Err(DataFusionError::Plan(
            "COUNT(*) must not name an input field".to_string(),
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
    ensure_numeric(&output_type)?;
    if function != proto::AggregateFunction::Count {
        if let Some(input_type) = &input_type {
            ensure_numeric(input_type)?;
        }
    }
    Ok(Call {
        function,
        input_index,
        input_type,
        output_type,
        retractable: call.retractable,
    })
}

fn ensure_numeric(data_type: &DataType) -> Result<()> {
    if matches!(
        data_type,
        DataType::Int8
            | DataType::Int16
            | DataType::Int32
            | DataType::Int64
            | DataType::Decimal128(_, _)
    ) {
        Ok(())
    } else {
        Err(DataFusionError::Plan(format!(
            "group aggregate numeric type {data_type} is not supported"
        )))
    }
}

fn numeric_value(array: &dyn Array, row: usize) -> Result<Option<i128>> {
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
        other => {
            return Err(DataFusionError::Execution(format!(
                "group aggregate expected numeric input, got {other}"
            )));
        }
    };
    Ok(Some(value))
}

fn numeric_array(values: &[Option<i128>], data_type: &DataType) -> Result<ArrayRef> {
    Ok(match data_type {
        DataType::Int8 => Arc::new(Int8Array::from_iter(
            values.iter().map(|value| value.map(|value| value as i8)),
        )),
        DataType::Int16 => Arc::new(Int16Array::from_iter(
            values.iter().map(|value| value.map(|value| value as i16)),
        )),
        DataType::Int32 => Arc::new(Int32Array::from_iter(
            values.iter().map(|value| value.map(|value| value as i32)),
        )),
        DataType::Int64 => Arc::new(Int64Array::from_iter(
            values.iter().map(|value| value.map(|value| value as i64)),
        )),
        DataType::Decimal128(precision, scale) => Arc::new(
            Decimal128Array::from_iter(values.iter().copied())
                .with_precision_and_scale(*precision, *scale)?,
        ),
        other => {
            return Err(DataFusionError::Execution(format!(
                "group aggregate cannot emit numeric type {other}"
            )));
        }
    })
}

fn wrapping_numeric_add(left: i128, right: i128, data_type: &DataType) -> i128 {
    match data_type {
        DataType::Int8 => (left as i8).wrapping_add(right as i8) as i128,
        DataType::Int16 => (left as i16).wrapping_add(right as i16) as i128,
        DataType::Int32 => (left as i32).wrapping_add(right as i32) as i128,
        DataType::Int64 => (left as i64).wrapping_add(right as i64) as i128,
        DataType::Decimal128(_, _) => left.wrapping_add(right),
        _ => unreachable!("validated numeric output"),
    }
}

fn wrapping_numeric_sub(left: i128, right: i128, data_type: &DataType) -> i128 {
    match data_type {
        DataType::Int8 => (left as i8).wrapping_sub(right as i8) as i128,
        DataType::Int16 => (left as i16).wrapping_sub(right as i16) as i128,
        DataType::Int32 => (left as i32).wrapping_sub(right as i32) as i128,
        DataType::Int64 => (left as i64).wrapping_sub(right as i64) as i128,
        DataType::Decimal128(_, _) => left.wrapping_sub(right),
        _ => unreachable!("validated numeric output"),
    }
}

fn encode_state(state: &AccumulatorState) -> Vec<u8> {
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
            Accumulator::Sum { value, count } => {
                bytes.push(2);
                bytes.extend_from_slice(&value.to_le_bytes());
                bytes.extend_from_slice(&count.to_le_bytes());
            }
            Accumulator::AppendExtremum(value) => {
                bytes.push(4);
                bytes.push(value.is_some() as u8);
                if let Some(value) = value {
                    bytes.extend_from_slice(&value.to_le_bytes());
                }
            }
            Accumulator::Extremum(values) => {
                bytes.push(3);
                bytes.extend_from_slice(&(values.len() as u32).to_le_bytes());
                for (value, count) in values {
                    bytes.extend_from_slice(&value.to_le_bytes());
                    bytes.extend_from_slice(&count.to_le_bytes());
                }
            }
        }
    }
    bytes
}

fn decode_state(bytes: &[u8], calls: &[Call]) -> Result<AccumulatorState> {
    let mut cursor = Cursor::new(bytes);
    if cursor.read_exact(4)? != STATE_MAGIC {
        return Err(DataFusionError::Execution(
            "group aggregate state has invalid magic".to_string(),
        ));
    }
    let version = cursor.read_u8()?;
    if version != STATE_VERSION {
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
                value: cursor.read_i128()?,
                count: cursor.read_i64()?,
            },
            3 => {
                let entries = cursor.read_u32()? as usize;
                let mut values = BTreeMap::new();
                for _ in 0..entries {
                    values.insert(cursor.read_i128()?, cursor.read_i64()?);
                }
                Accumulator::Extremum(values)
            }
            4 => {
                let value = match cursor.read_u8()? {
                    0 => None,
                    1 => Some(cursor.read_i128()?),
                    other => {
                        return Err(DataFusionError::Execution(format!(
                            "group aggregate append extremum presence {other} is invalid"
                        )));
                    }
                };
                Accumulator::AppendExtremum(value)
            }
            other => {
                return Err(DataFusionError::Execution(format!(
                    "group aggregate state accumulator tag {other} is invalid"
                )));
            }
        };
        let expected = match call.function {
            proto::AggregateFunction::CountStar | proto::AggregateFunction::Count => 1,
            proto::AggregateFunction::Sum => 2,
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
    use arrow::array::{Decimal128Array, Int64Array, StringArray};
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

    fn plan(input_changelog: bool, update_before: bool) -> Vec<u8> {
        let call =
            |function: proto::AggregateFunction, input_index: Option<u32>| proto::AggregateCall {
                function: function as i32,
                input_index,
                input_type: input_index.map(|_| logical_bigint(true)),
                output_type: Some(logical_bigint(
                    function != proto::AggregateFunction::CountStar,
                )),
                retractable: input_changelog,
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
                        grouping_indices: vec![0],
                        aggregate_calls: vec![
                            call(proto::AggregateFunction::CountStar, None),
                            call(proto::AggregateFunction::Sum, Some(1)),
                            call(proto::AggregateFunction::Min, Some(1)),
                            call(proto::AggregateFunction::Max, Some(1)),
                        ],
                        generate_update_before: update_before,
                        input_changelog,
                    },
                ))),
            }),
        }
        .encode_to_vec()
    }

    fn processor(input_changelog: bool, update_before: bool) -> GroupAggregateProcessor {
        processor_range(input_changelog, update_before, 0, 127)
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
            }],
            generate_update_before: false,
            input_changelog: false,
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
    fn state_codec_round_trips_decimal_extrema_counts() {
        let state = AccumulatorState {
            row_count: 3,
            accumulators: vec![
                Accumulator::Count(3),
                Accumulator::Sum {
                    value: 12_345,
                    count: 2,
                },
                Accumulator::Extremum(BTreeMap::from([(100, 2), (200, -1)])),
            ],
        };
        let calls = vec![
            Call {
                function: proto::AggregateFunction::CountStar,
                input_index: None,
                input_type: None,
                output_type: DataType::Int64,
                retractable: true,
            },
            Call {
                function: proto::AggregateFunction::Sum,
                input_index: Some(1),
                input_type: Some(DataType::Decimal128(10, 2)),
                output_type: DataType::Decimal128(20, 2),
                retractable: true,
            },
            Call {
                function: proto::AggregateFunction::Min,
                input_index: Some(1),
                input_type: Some(DataType::Decimal128(10, 2)),
                output_type: DataType::Decimal128(10, 2),
                retractable: true,
            },
        ];
        assert_eq!(decode_state(&encode_state(&state), &calls).unwrap(), state);
        let decimal = Decimal128Array::from(vec![Some(12_345)])
            .with_precision_and_scale(20, 2)
            .unwrap();
        assert_eq!(decimal.value(0), 12_345);
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
