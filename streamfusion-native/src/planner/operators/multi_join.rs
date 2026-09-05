// Copyright 2026 StreamFusion Authors
// Licensed under the Apache License, Version 2.0

use std::sync::Arc;

use ahash::RandomState;
use arrow::array::{new_null_array, Array, BinaryArray, Int32Array, Int8Array};
use arrow::compute::SortOptions;
use arrow::datatypes::{DataType, Field, Schema, SchemaRef};
use arrow::record_batch::RecordBatch;
use arrow_row::{RowConverter, Rows, SortField};
use datafusion::error::{DataFusionError, Result};
use hashbrown::HashMap;

use crate::exchange::{assign_key_group, encode_binary_row, KeyField};
use crate::memory_pool::HostMemoryReservation;
use crate::planner::arrow_schema;
use crate::state::{
    KeyedState, MemoryKeyedState, RocksPluginKeyedState, StateKey, StateKeyRef, StateMutation,
};
use crate::{decode_plan, proto};

const INSERT: i8 = 0;
const UPDATE_BEFORE: i8 = 1;
const UPDATE_AFTER: i8 = 2;
const DELETE: i8 = 3;
const STATE_MAGIC: &[u8; 4] = b"SFMJ";
const STATE_VERSION: u8 = 1;

#[derive(Clone, Debug, PartialEq, Eq)]
struct StoredRow {
    row: Vec<u8>,
    condition_values: Vec<Option<Vec<u8>>>,
}

#[derive(Debug, PartialEq, Eq)]
struct MultiJoinState {
    inputs: Vec<Vec<StoredRow>>,
}

impl MultiJoinState {
    fn empty(input_count: usize) -> Self {
        Self {
            inputs: (0..input_count).map(|_| Vec::new()).collect(),
        }
    }

    fn is_empty(&self) -> bool {
        self.inputs.iter().all(Vec::is_empty)
    }
}

struct StagedState {
    key: StateKey,
    value: MultiJoinState,
    touched: bool,
}

struct OutputRow {
    inputs: Vec<Vec<u8>>,
    kind: i8,
    input_ordinal: i32,
}

/// Persistent N-input join state. One native invocation performs one backend multi-get and one
/// write batch, regardless of the number of rows in the incoming Arrow batch.
pub(crate) struct MultiJoinProcessor {
    plan: proto::MultiJoin,
    max_parallelism: u32,
    state: Box<dyn KeyedState>,
    visible_schemas: Vec<SchemaRef>,
    output_schema: SchemaRef,
    row_converters: Vec<RowConverter>,
    null_rows: Vec<Vec<u8>>,
    input_schemas: Vec<Option<SchemaRef>>,
    key_fields: Vec<Vec<(usize, KeyField)>>,
    preencoded_key_indices: Vec<Option<usize>>,
    input_kind_indices: Vec<Option<usize>>,
    condition_indices: Vec<HashMap<usize, usize, RandomState>>,
    scratch_reservation: HostMemoryReservation,
    state_read_batches: u64,
    state_write_batches: u64,
}

impl MultiJoinProcessor {
    pub(crate) fn new(
        serialized_plan: &[u8],
        max_parallelism: u32,
        first_key_group: u32,
        last_key_group: u32,
        reservation: HostMemoryReservation,
    ) -> Result<Self> {
        let scratch = reservation.sibling("native multi-join batch scratch and output");
        let state = Box::new(MemoryKeyedState::new(
            first_key_group,
            last_key_group,
            reservation,
        )?);
        Self::with_state(serialized_plan, max_parallelism, state, scratch)
    }

    pub(crate) fn new_rocksdb(
        serialized_plan: &[u8],
        max_parallelism: u32,
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
        Self::with_state(serialized_plan, max_parallelism, state, scratch)
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
            .ok_or_else(|| DataFusionError::Plan("multi-join plan has no root".to_string()))?;
        let plan = match root.operator {
            Some(proto::operator::Operator::MultiJoin(plan)) => plan,
            _ => {
                return Err(DataFusionError::Plan(
                    "multi-join handle requires a MultiJoin root".to_string(),
                ));
            }
        };
        validate_plan(&plan, max_parallelism)?;
        let visible_schemas = plan
            .inputs
            .iter()
            .map(|input| arrow_schema(input.schema.as_ref().expect("validated schema")))
            .collect::<Result<Vec<_>>>()?;
        let row_converters = visible_schemas
            .iter()
            .map(row_converter)
            .collect::<Result<Vec<_>>>()?;
        let null_rows = visible_schemas
            .iter()
            .zip(&row_converters)
            .map(|(schema, converter)| encode_null_row(schema, converter))
            .collect::<Result<Vec<_>>>()?;
        let mut output_fields = Vec::new();
        for (input, schema) in visible_schemas.iter().enumerate() {
            let nullable =
                input > 0 && plan.join_types[input] == proto::RegularJoinType::Left as i32;
            for (index, field) in schema.fields().iter().enumerate() {
                output_fields.push(Arc::new(Field::new(
                    format!("__streamfusion_multi_join_{input}_{index}"),
                    field.data_type().clone(),
                    field.is_nullable() || nullable,
                )));
            }
        }
        output_fields.push(Arc::new(Field::new(
            "__streamfusion_row_kind",
            DataType::Int8,
            false,
        )));
        output_fields.push(Arc::new(Field::new(
            "__streamfusion_input_row",
            DataType::Int32,
            false,
        )));
        let input_count = plan.inputs.len();
        Ok(Self {
            plan,
            max_parallelism,
            state,
            visible_schemas,
            output_schema: Arc::new(Schema::new(output_fields)),
            row_converters,
            null_rows,
            input_schemas: (0..input_count).map(|_| None).collect(),
            key_fields: (0..input_count).map(|_| Vec::new()).collect(),
            preencoded_key_indices: vec![None; input_count],
            input_kind_indices: vec![None; input_count],
            condition_indices: (0..input_count)
                .map(|_| HashMap::with_hasher(RandomState::new()))
                .collect(),
            scratch_reservation,
            state_read_batches: 0,
            state_write_batches: 0,
        })
    }

    pub(crate) fn process_arrow(
        &mut self,
        input: usize,
        batch: RecordBatch,
    ) -> Result<RecordBatch> {
        if input >= self.plan.inputs.len() {
            return Err(DataFusionError::Execution(format!(
                "multi-join input {input} is outside {} inputs",
                self.plan.inputs.len()
            )));
        }
        self.prepare_schema(input, batch.schema())?;
        let visible_count = self.visible_schemas[input].fields().len();
        let input_bytes = batch.columns()[..visible_count]
            .iter()
            .map(|column| column.get_array_memory_size())
            .sum::<usize>();
        let base = input_bytes.saturating_add(batch.num_rows().saturating_mul(256));
        self.scratch_reservation.resize(base)?;
        let encoded = self.row_converters[input].convert_columns(&batch.columns()[..visible_count]);
        let result = match encoded {
            Ok(encoded) => self.process_accounted(input, &batch, &encoded),
            Err(error) => Err(error.into()),
        };
        match result {
            Ok(output) => self.finish_output(output, base),
            Err(error) => {
                self.scratch_reservation.resize(0)?;
                Err(error)
            }
        }
    }

    fn process_accounted(
        &mut self,
        input: usize,
        batch: &RecordBatch,
        encoded: &Rows,
    ) -> Result<RecordBatch> {
        let kinds = batch
            .column(self.input_kind_indices[input].expect("schema prepared"))
            .as_any()
            .downcast_ref::<Int8Array>()
            .ok_or_else(|| {
                DataFusionError::Execution("multi-join RowKinds are not Int8".to_string())
            })?;
        let mut unique = HashMap::<StateKey, usize, RandomState>::with_capacity_and_hasher(
            batch.num_rows(),
            RandomState::new(),
        );
        let mut row_state_indices = Vec::with_capacity(batch.num_rows());
        for row in 0..batch.num_rows() {
            let key = self.group_key(input, batch, row)?;
            let state_key = StateKey {
                key_group: assign_key_group(&key, self.max_parallelism),
                key,
            };
            let next = unique.len();
            row_state_indices.push(*unique.entry(state_key).or_insert(next));
        }
        if unique.is_empty() {
            return self.empty_output();
        }
        let mut ordered_keys = (0..unique.len()).map(|_| None).collect::<Vec<_>>();
        for (key, index) in unique {
            ordered_keys[index] = Some(key);
        }
        let keys = ordered_keys
            .into_iter()
            .map(|key| key.expect("multi-join state index is populated"))
            .collect::<Vec<_>>();
        let refs = keys
            .iter()
            .map(|key| StateKeyRef {
                key_group: key.key_group,
                key: &key.key,
            })
            .collect::<Vec<_>>();
        let existing = self.state.get_batch(&refs)?;
        self.state_read_batches = self.state_read_batches.saturating_add(1);
        let input_count = self.plan.inputs.len();
        let mut staged = keys
            .into_iter()
            .zip(existing)
            .map(|(key, bytes)| {
                Ok(StagedState {
                    key,
                    value: bytes
                        .map(|bytes| decode_state(bytes.as_ref(), input_count))
                        .transpose()?
                        .unwrap_or_else(|| MultiJoinState::empty(input_count)),
                    touched: false,
                })
            })
            .collect::<Result<Vec<_>>>()?;
        let mut output = Vec::new();
        for row in 0..batch.num_rows() {
            let ordinal = i32::try_from(row).map_err(|_| {
                DataFusionError::Execution("multi-join batch exceeds Int32 indexing".to_string())
            })?;
            let kind = kinds.value(row);
            let accumulate = match kind {
                INSERT | UPDATE_AFTER => true,
                UPDATE_BEFORE | DELETE => false,
                other => {
                    return Err(DataFusionError::Execution(format!(
                        "unknown Flink RowKind byte {other}"
                    )))
                }
            };
            let stored = self.stored_row(input, batch, row, encoded.row(row).data().to_vec())?;
            let state = &mut staged[row_state_indices[row]];
            if accumulate {
                enumerate_join(
                    &self.plan,
                    &state.value,
                    &self.null_rows,
                    input,
                    &stored,
                    kind,
                    ordinal,
                    &mut output,
                );
                state.value.inputs[input].push(stored);
            } else if let Some(position) = state.value.inputs[input]
                .iter()
                .position(|candidate| candidate.row == stored.row)
            {
                // A retract only produces joined retracts when Flink's multiset state contains
                // the row. Use the stored condition sidecars so a malformed/changed retract
                // payload cannot influence matches before the exact state row is removed.
                let existing = state.value.inputs[input][position].clone();
                enumerate_join(
                    &self.plan,
                    &state.value,
                    &self.null_rows,
                    input,
                    &existing,
                    kind,
                    ordinal,
                    &mut output,
                );
                state.value.inputs[input].remove(position);
            }
            state.touched = true;
        }
        let mutations = staged
            .into_iter()
            .filter(|entry| entry.touched)
            .map(|entry| StateMutation {
                key: entry.key,
                value: (!entry.value.is_empty()).then(|| encode_state(&entry.value)),
            })
            .collect::<Vec<_>>();
        if !mutations.is_empty() {
            self.state.write_batch(mutations)?;
            self.state_write_batches = self.state_write_batches.saturating_add(1);
        }
        self.output_batch(output)
    }

    pub(crate) fn statistics(&self) -> [u64; 2] {
        [self.state_read_batches, self.state_write_batches]
    }

    pub(crate) fn snapshot_key_group(&self, key_group: u32) -> Result<Vec<u8>> {
        self.state.snapshot_key_group(key_group)
    }

    pub(crate) fn restore_key_group(&mut self, key_group: u32, bytes: &[u8]) -> Result<()> {
        self.state.restore_key_group(key_group, bytes)
    }

    pub(crate) fn checkpoint(&self, directory: &std::path::Path) -> Result<()> {
        self.state.checkpoint(directory)
    }

    fn group_key(&self, input: usize, batch: &RecordBatch, row: usize) -> Result<Vec<u8>> {
        match self.preencoded_key_indices[input] {
            Some(index) => Ok(batch
                .column(index)
                .as_any()
                .downcast_ref::<BinaryArray>()
                .ok_or_else(|| {
                    DataFusionError::Execution(
                        "multi-join preencoded keys are not Binary".to_string(),
                    )
                })?
                .value(row)
                .to_vec()),
            None if self.key_fields[input].is_empty() => Ok(Vec::new()),
            None => Ok(encode_binary_row(batch, row, &self.key_fields[input])?),
        }
    }

    fn stored_row(
        &self,
        input: usize,
        batch: &RecordBatch,
        row: usize,
        encoded: Vec<u8>,
    ) -> Result<StoredRow> {
        let mut values = vec![None; self.visible_schemas[input].fields().len()];
        for (&field, &column) in &self.condition_indices[input] {
            let array = batch
                .column(column)
                .as_any()
                .downcast_ref::<BinaryArray>()
                .ok_or_else(|| {
                    DataFusionError::Execution(format!(
                    "multi-join condition sidecar for input {input} field {field} is not Binary"
                ))
                })?;
            if !array.is_null(row) {
                values[field] = Some(array.value(row).to_vec());
            }
        }
        Ok(StoredRow {
            row: encoded,
            condition_values: values,
        })
    }

    fn prepare_schema(&mut self, input: usize, schema: SchemaRef) -> Result<()> {
        if let Some(expected) = &self.input_schemas[input] {
            if expected.as_ref() != schema.as_ref() {
                return Err(DataFusionError::Execution(format!(
                    "multi-join input {input} schema changed while running"
                )));
            }
            return Ok(());
        }
        self.preencoded_key_indices[input] = metadata_index(&schema, "__streamfusion_key");
        self.input_kind_indices[input] = metadata_index(&schema, "__streamfusion_input_row_kind")
            .or_else(|| metadata_index(&schema, "__streamfusion_row_kind"));
        if self.input_kind_indices[input].is_none() {
            return Err(DataFusionError::Execution(
                "multi-join requires RowKind metadata".to_string(),
            ));
        }
        for field in condition_fields(&self.plan, input) {
            let name = format!("__streamfusion_condition_{field}");
            let index = metadata_index(&schema, &name).ok_or_else(|| {
                DataFusionError::Execution(format!(
                    "multi-join input {input} is missing condition sidecar {name}"
                ))
            })?;
            self.condition_indices[input].insert(field, index);
        }
        let visible_count = self.visible_schemas[input].fields().len();
        if schema.fields().len() <= visible_count
            || schema.fields()[..visible_count]
                .iter()
                .zip(self.visible_schemas[input].fields())
                .any(|(actual, planned)| actual.data_type() != planned.data_type())
        {
            return Err(DataFusionError::Execution(format!(
                "multi-join input {input} Arrow schema does not match its protobuf schema"
            )));
        }
        if self.preencoded_key_indices[input].is_none() {
            self.key_fields[input] = self.plan.inputs[input]
                .common_key_indices
                .iter()
                .map(|&index| {
                    let field = schema.fields().get(index as usize).ok_or_else(|| {
                        arrow::error::ArrowError::SchemaError(format!(
                            "multi-join input {input} key {index} is outside the visible row"
                        ))
                    })?;
                    Ok((
                        index as usize,
                        KeyField::from_arrow_type(field.data_type())?,
                    ))
                })
                .collect::<std::result::Result<Vec<_>, arrow::error::ArrowError>>()?;
        }
        self.input_schemas[input] = Some(schema);
        Ok(())
    }

    fn output_batch(&self, rows: Vec<OutputRow>) -> Result<RecordBatch> {
        let count = rows.len();
        let mut encoded = (0..self.plan.inputs.len())
            .map(|_| Vec::with_capacity(count))
            .collect::<Vec<_>>();
        let mut kinds = Vec::with_capacity(count);
        let mut ordinals = Vec::with_capacity(count);
        for row in rows {
            for (input, value) in row.inputs.into_iter().enumerate() {
                encoded[input].push(value);
            }
            kinds.push(row.kind);
            ordinals.push(row.input_ordinal);
        }
        let mut columns = Vec::new();
        for (input, values) in encoded.iter().enumerate() {
            let parser = self.row_converters[input].parser();
            let decoded = self.row_converters[input]
                .convert_rows(values.iter().map(|row| parser.parse(row)))?;
            columns.extend(decoded);
        }
        columns.push(Arc::new(Int8Array::from(kinds)));
        columns.push(Arc::new(Int32Array::from(ordinals)));
        Ok(RecordBatch::try_new(self.output_schema.clone(), columns)?)
    }

    fn empty_output(&self) -> Result<RecordBatch> {
        Ok(RecordBatch::new_empty(self.output_schema.clone()))
    }

    fn finish_output(&mut self, output: RecordBatch, base: usize) -> Result<RecordBatch> {
        let output_bytes = output.get_array_memory_size();
        self.scratch_reservation.resize(output_bytes.max(base))?;
        self.scratch_reservation.transfer_to_arrow(output_bytes)?;
        self.scratch_reservation.resize(0)?;
        Ok(output)
    }
}

fn enumerate_join(
    plan: &proto::MultiJoin,
    state: &MultiJoinState,
    null_rows: &[Vec<u8>],
    active_input: usize,
    active: &StoredRow,
    kind: i8,
    ordinal: i32,
    output: &mut Vec<OutputRow>,
) {
    fn recurse<'a>(
        plan: &proto::MultiJoin,
        state: &'a MultiJoinState,
        null_rows: &[Vec<u8>],
        active_input: usize,
        active: &'a StoredRow,
        kind: i8,
        ordinal: i32,
        depth: usize,
        joined: &mut Vec<Option<&'a StoredRow>>,
        is_active: bool,
        output: &mut Vec<OutputRow>,
    ) {
        if depth == state.inputs.len() {
            if is_active {
                output.push(OutputRow {
                    inputs: joined
                        .iter()
                        .enumerate()
                        .map(|(input, row)| {
                            row.as_ref()
                                .map(|row| row.row.clone())
                                .unwrap_or_else(|| null_rows[input].clone())
                        })
                        .collect(),
                    kind,
                    input_ordinal: ordinal,
                });
            }
            return;
        }

        let is_left = depth > 0 && plan.join_types[depth] == proto::RegularJoinType::Left as i32;
        let accumulate = matches!(kind, INSERT | UPDATE_AFTER);
        let mut any_match = false;
        let mut associations = 0_i32;

        // Flink only scans the active level's existing state for a LEFT join, where the
        // association count determines null-padded transitions. INNER joins can activate the
        // incoming row directly.
        if is_left || depth != active_input {
            for candidate in &state.inputs[depth] {
                if !conditions_match(plan, depth, joined, candidate) {
                    continue;
                }
                any_match = true;
                if is_left {
                    associations += if !is_active || accumulate { 1 } else { -1 };
                    if depth == active_input
                        && ((accumulate && associations > 0) || (!accumulate && associations > 1))
                    {
                        break;
                    }
                }
                if !is_active && depth == active_input {
                    continue;
                }
                joined.push(Some(candidate));
                recurse(
                    plan,
                    state,
                    null_rows,
                    active_input,
                    active,
                    kind,
                    ordinal,
                    depth + 1,
                    joined,
                    is_active,
                    output,
                );
                joined.pop();
            }
        }

        if depth == active_input {
            if conditions_match(plan, depth, joined, active) {
                if is_left {
                    associations += if accumulate { 1 } else { -1 };
                }
                if accumulate && is_left && !any_match {
                    joined.push(None);
                    recurse(
                        plan,
                        state,
                        null_rows,
                        active_input,
                        active,
                        DELETE,
                        ordinal,
                        depth + 1,
                        joined,
                        true,
                        output,
                    );
                    joined.pop();
                }
                joined.push(Some(active));
                recurse(
                    plan,
                    state,
                    null_rows,
                    active_input,
                    active,
                    kind,
                    ordinal,
                    depth + 1,
                    joined,
                    true,
                    output,
                );
                joined.pop();
                if !accumulate && is_left && associations == 0 {
                    joined.push(None);
                    recurse(
                        plan,
                        state,
                        null_rows,
                        active_input,
                        active,
                        INSERT,
                        ordinal,
                        depth + 1,
                        joined,
                        true,
                        output,
                    );
                    joined.pop();
                }
            }
        } else if is_left && !any_match && associations == 0 {
            joined.push(None);
            recurse(
                plan,
                state,
                null_rows,
                active_input,
                active,
                kind,
                ordinal,
                depth + 1,
                joined,
                is_active,
                output,
            );
            joined.pop();
        }
    }

    recurse(
        plan,
        state,
        null_rows,
        active_input,
        active,
        kind,
        ordinal,
        0,
        &mut Vec::with_capacity(state.inputs.len()),
        false,
        output,
    );
}

fn conditions_match(
    plan: &proto::MultiJoin,
    depth: usize,
    joined: &[Option<&StoredRow>],
    candidate: &StoredRow,
) -> bool {
    plan.equi_conditions
        .iter()
        .filter(|condition| condition.depth as usize == depth)
        .all(|condition| {
            let Some(Some(left)) = joined.get(condition.left_input_index as usize) else {
                return false;
            };
            let left = left
                .condition_values
                .get(condition.left_field_index as usize)
                .and_then(Option::as_ref);
            let right = candidate
                .condition_values
                .get(condition.right_field_index as usize)
                .and_then(Option::as_ref);
            matches!((left, right), (Some(left), Some(right)) if left == right)
        })
}

fn encode_null_row(schema: &SchemaRef, converter: &RowConverter) -> Result<Vec<u8>> {
    let columns = schema
        .fields()
        .iter()
        .map(|field| new_null_array(field.data_type(), 1))
        .collect::<Vec<_>>();
    let rows = converter.convert_columns(&columns)?;
    Ok(rows.row(0).data().to_vec())
}

fn condition_fields(plan: &proto::MultiJoin, input: usize) -> Vec<usize> {
    let mut fields = plan
        .equi_conditions
        .iter()
        .filter_map(|condition| {
            if condition.left_input_index as usize == input {
                Some(condition.left_field_index as usize)
            } else if condition.depth as usize == input {
                Some(condition.right_field_index as usize)
            } else {
                None
            }
        })
        .collect::<Vec<_>>();
    fields.sort_unstable();
    fields.dedup();
    fields
}

fn validate_plan(plan: &proto::MultiJoin, max_parallelism: u32) -> Result<()> {
    let input_count = plan.inputs.len();
    let valid_types = plan.join_types.iter().enumerate().all(|(input, value)| {
        matches!(
            proto::RegularJoinType::try_from(*value).ok(),
            Some(proto::RegularJoinType::Inner)
        ) || (input > 0
            && matches!(
                proto::RegularJoinType::try_from(*value).ok(),
                Some(proto::RegularJoinType::Left)
            ))
    });
    let valid_conditions = plan.equi_conditions.iter().all(|condition| {
        let depth = condition.depth as usize;
        let left = condition.left_input_index as usize;
        depth > 0
            && depth < input_count
            && left < depth
            && condition.right_field_index
                < plan.inputs[depth]
                    .schema
                    .as_ref()
                    .map(|schema| schema.fields.len() as u32)
                    .unwrap_or(0)
            && condition.left_field_index
                < plan.inputs[left]
                    .schema
                    .as_ref()
                    .map(|schema| schema.fields.len() as u32)
                    .unwrap_or(0)
    });
    if max_parallelism == 0
        || input_count < 2
        || plan.join_types.len() != input_count
        || !valid_types
        || !valid_conditions
        || plan.inputs.iter().any(|input| {
            input.schema.is_none()
                || input.state_retention_millis != 0
                || input.common_key_indices.iter().any(|index| {
                    *index
                        >= input
                            .schema
                            .as_ref()
                            .map(|schema| schema.fields.len() as u32)
                            .unwrap_or(0)
                })
        })
    {
        return Err(DataFusionError::Plan(
            "multi-join input/schema/type/condition/TTL contract is invalid".to_string(),
        ));
    }
    Ok(())
}

fn metadata_index(schema: &SchemaRef, name: &str) -> Option<usize> {
    schema
        .fields()
        .iter()
        .position(|field| field.name() == name)
}

fn row_converter(schema: &SchemaRef) -> Result<RowConverter> {
    Ok(RowConverter::new(
        schema
            .fields()
            .iter()
            .map(|field| {
                SortField::new_with_options(
                    field.data_type().clone(),
                    SortOptions {
                        descending: false,
                        nulls_first: true,
                    },
                )
            })
            .collect(),
    )?)
}

fn encode_state(state: &MultiJoinState) -> Vec<u8> {
    let mut bytes = Vec::new();
    bytes.extend_from_slice(STATE_MAGIC);
    bytes.push(STATE_VERSION);
    bytes.extend_from_slice(&(state.inputs.len() as u32).to_le_bytes());
    for rows in &state.inputs {
        bytes.extend_from_slice(&(rows.len() as u32).to_le_bytes());
        for row in rows {
            bytes.extend_from_slice(&(row.row.len() as u32).to_le_bytes());
            bytes.extend_from_slice(&row.row);
            bytes.extend_from_slice(&(row.condition_values.len() as u32).to_le_bytes());
            for value in &row.condition_values {
                match value {
                    Some(value) => {
                        bytes.extend_from_slice(&(value.len() as u32).to_le_bytes());
                        bytes.extend_from_slice(value);
                    }
                    None => bytes.extend_from_slice(&u32::MAX.to_le_bytes()),
                }
            }
        }
    }
    bytes
}

fn decode_state(bytes: &[u8], expected_inputs: usize) -> Result<MultiJoinState> {
    if bytes.len() < 9 || &bytes[..4] != STATE_MAGIC || bytes[4] != STATE_VERSION {
        return Err(DataFusionError::Execution(
            "invalid native multi-join state".to_string(),
        ));
    }
    let mut offset = 5;
    let input_count = read_u32(bytes, &mut offset)? as usize;
    if input_count != expected_inputs {
        return Err(DataFusionError::Execution(format!(
            "multi-join state has {input_count} inputs, expected {expected_inputs}"
        )));
    }
    let mut inputs = Vec::with_capacity(input_count);
    for _ in 0..input_count {
        let count = read_u32(bytes, &mut offset)? as usize;
        let mut rows = Vec::with_capacity(count);
        for _ in 0..count {
            let row = read_bytes(bytes, &mut offset)?;
            let condition_count = read_u32(bytes, &mut offset)? as usize;
            let mut condition_values = Vec::with_capacity(condition_count);
            for _ in 0..condition_count {
                let length = read_u32(bytes, &mut offset)?;
                condition_values.push(if length == u32::MAX {
                    None
                } else {
                    Some(read_bytes_with_length(bytes, &mut offset, length as usize)?)
                });
            }
            rows.push(StoredRow {
                row,
                condition_values,
            });
        }
        inputs.push(rows);
    }
    if offset != bytes.len() {
        return Err(DataFusionError::Execution(
            "multi-join state has trailing bytes".to_string(),
        ));
    }
    Ok(MultiJoinState { inputs })
}

fn read_bytes(bytes: &[u8], offset: &mut usize) -> Result<Vec<u8>> {
    let length = read_u32(bytes, offset)? as usize;
    read_bytes_with_length(bytes, offset, length)
}

fn read_bytes_with_length(bytes: &[u8], offset: &mut usize, length: usize) -> Result<Vec<u8>> {
    let end = offset.checked_add(length).ok_or_else(truncated)?;
    let value = bytes.get(*offset..end).ok_or_else(truncated)?.to_vec();
    *offset = end;
    Ok(value)
}

fn read_u32(bytes: &[u8], offset: &mut usize) -> Result<u32> {
    let end = offset.checked_add(4).ok_or_else(truncated)?;
    let value = bytes.get(*offset..end).ok_or_else(truncated)?;
    *offset = end;
    Ok(u32::from_le_bytes(value.try_into().unwrap()))
}

fn truncated() -> DataFusionError {
    DataFusionError::Execution("truncated native multi-join state".to_string())
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::memory_pool::{tests_support::TestBroker, HostMemoryReservation};
    use arrow::array::{ArrayRef, Int64Array, StringArray};
    use prost::Message;

    #[test]
    fn three_input_inner_join_preserves_row_kinds_and_duplicates() {
        let broker = Arc::new(TestBroker::new(64 << 20));
        let mut processor = MultiJoinProcessor::new(
            &plan(),
            128,
            0,
            127,
            HostMemoryReservation::new(broker.clone(), "multi-join test"),
        )
        .unwrap();
        assert_eq!(
            processor
                .process_arrow(0, batch(&[1], &["a"], &[INSERT]))
                .unwrap()
                .num_rows(),
            0
        );
        assert_eq!(
            processor
                .process_arrow(1, batch(&[1, 1], &["b", "b2"], &[INSERT, INSERT]))
                .unwrap()
                .num_rows(),
            0
        );
        let joined = processor
            .process_arrow(2, batch(&[1], &["c"], &[UPDATE_AFTER]))
            .unwrap();
        assert_eq!(joined.num_rows(), 2);
        assert_eq!(kinds(&joined), vec![UPDATE_AFTER, UPDATE_AFTER]);
        let retract = processor
            .process_arrow(1, batch(&[1], &["b"], &[UPDATE_BEFORE]))
            .unwrap();
        assert_eq!(retract.num_rows(), 1);
        assert_eq!(kinds(&retract), vec![UPDATE_BEFORE]);
        assert_eq!(processor.statistics(), [4, 4]);
        drop(joined);
        drop(retract);
        drop(processor);
        assert_eq!(broker.reserved(), 0);
    }

    #[test]
    fn absent_retraction_does_not_emit_a_phantom_join() {
        let broker = Arc::new(TestBroker::new(64 << 20));
        let mut processor = MultiJoinProcessor::new(
            &plan(),
            128,
            0,
            127,
            HostMemoryReservation::new(broker, "multi-join absent retract"),
        )
        .unwrap();
        processor
            .process_arrow(0, batch(&[1], &["a"], &[INSERT]))
            .unwrap();
        processor
            .process_arrow(1, batch(&[1], &["b"], &[INSERT]))
            .unwrap();
        assert_eq!(
            processor
                .process_arrow(2, batch(&[1], &["missing"], &[DELETE]))
                .unwrap()
                .num_rows(),
            0
        );
    }

    #[test]
    fn chained_left_joins_retract_and_restore_null_padding() {
        let broker = Arc::new(TestBroker::new(64 << 20));
        let mut processor = MultiJoinProcessor::new(
            &plan_with_types([
                proto::RegularJoinType::Inner,
                proto::RegularJoinType::Left,
                proto::RegularJoinType::Left,
            ]),
            128,
            0,
            127,
            HostMemoryReservation::new(broker, "multi-join left transitions"),
        )
        .unwrap();
        assert_eq!(
            kinds(
                &processor
                    .process_arrow(0, batch(&[1], &["a"], &[INSERT]))
                    .unwrap()
            ),
            vec![INSERT]
        );
        assert_eq!(
            kinds(
                &processor
                    .process_arrow(1, batch(&[1], &["b"], &[INSERT]))
                    .unwrap()
            ),
            vec![DELETE, INSERT]
        );
        assert_eq!(
            kinds(
                &processor
                    .process_arrow(2, batch(&[1], &["c"], &[INSERT]))
                    .unwrap()
            ),
            vec![DELETE, INSERT]
        );
        assert_eq!(
            kinds(
                &processor
                    .process_arrow(2, batch(&[1], &["c"], &[DELETE]))
                    .unwrap()
            ),
            vec![DELETE, INSERT]
        );
        assert_eq!(
            kinds(
                &processor
                    .process_arrow(1, batch(&[1], &["b"], &[DELETE]))
                    .unwrap()
            ),
            vec![DELETE, INSERT]
        );
        assert_eq!(
            kinds(
                &processor
                    .process_arrow(0, batch(&[1], &["a"], &[DELETE]))
                    .unwrap()
            ),
            vec![DELETE]
        );
    }

    #[test]
    fn null_condition_values_do_not_match() {
        let broker = Arc::new(TestBroker::new(64 << 20));
        let mut processor = MultiJoinProcessor::new(
            &plan(),
            128,
            0,
            127,
            HostMemoryReservation::new(broker, "multi-join null condition"),
        )
        .unwrap();
        processor
            .process_arrow(0, nullable_batch(&[1], &["a"], &[INSERT], &[None]))
            .unwrap();
        processor
            .process_arrow(1, batch(&[1], &["b"], &[INSERT]))
            .unwrap();
        assert_eq!(
            processor
                .process_arrow(2, batch(&[1], &["c"], &[INSERT]))
                .unwrap()
                .num_rows(),
            0
        );
    }

    #[test]
    fn rejects_unadmitted_batch_memory_and_releases_the_reservation() {
        let broker = Arc::new(TestBroker::new(8_192));
        let mut processor = MultiJoinProcessor::new(
            &plan(),
            128,
            0,
            127,
            HostMemoryReservation::new(broker.clone(), "multi-join constrained memory"),
        )
        .unwrap();
        let failure = processor
            .process_arrow(0, batch(&[1], &["a"], &[INSERT]))
            .unwrap_err();
        assert!(failure.to_string().contains("Flink denied"));
        drop(processor);
        assert_eq!(broker.reserved(), 0);
    }

    #[test]
    fn canonical_state_restores_after_rescaling() {
        let broker = Arc::new(TestBroker::new(1 << 30));
        let mut source = processor(broker.clone(), 0, 127);
        source
            .process_arrow(0, batch(&[1, 2], &["a", "x"], &[INSERT, INSERT]))
            .unwrap();
        source
            .process_arrow(1, batch(&[1, 2], &["b", "y"], &[INSERT, INSERT]))
            .unwrap();
        let snapshots = (0..128)
            .map(|group| source.snapshot_key_group(group).unwrap())
            .collect::<Vec<_>>();
        let mut low = processor(broker.clone(), 0, 63);
        let mut high = processor(broker, 64, 127);
        for (group, snapshot) in snapshots.iter().enumerate() {
            if group < 64 {
                low.restore_key_group(group as u32, snapshot).unwrap();
            } else {
                high.restore_key_group(group as u32, snapshot).unwrap();
            }
        }
        for key in [1, 2] {
            let input = batch(&[key], &[if key == 1 { "c" } else { "z" }], &[INSERT]);
            let encoded = source.group_key(2, &input, 0).unwrap();
            let result = if assign_key_group(&encoded, 128) < 64 {
                low.process_arrow(2, input).unwrap()
            } else {
                high.process_arrow(2, input).unwrap()
            };
            assert_eq!(kinds(&result), vec![INSERT]);
        }
    }

    #[test]
    fn canonical_state_moves_from_memory_to_rocksdb_with_batched_io() {
        let Ok(plugin_path) = std::env::var("STREAMFUSION_TEST_ROCKSDB_PLUGIN") else {
            return;
        };
        let broker = Arc::new(TestBroker::new(1 << 30));
        let mut memory = processor(broker.clone(), 0, 127);
        memory
            .process_arrow(0, batch(&[1, 2], &["a", "x"], &[INSERT, INSERT]))
            .unwrap();
        memory
            .process_arrow(1, batch(&[1, 2], &["b", "y"], &[INSERT, INSERT]))
            .unwrap();
        assert_eq!(memory.statistics(), [2, 2]);
        let snapshots = (0..128)
            .map(|group| memory.snapshot_key_group(group).unwrap())
            .collect::<Vec<_>>();

        let directory = tempfile::tempdir().unwrap();
        let mut rocks = MultiJoinProcessor::new_rocksdb(
            &plan(),
            128,
            0,
            127,
            std::path::Path::new(&plugin_path),
            directory.path(),
            64 << 20,
            HostMemoryReservation::new(broker, "multi-join RocksDB scratch"),
        )
        .unwrap();
        for (group, snapshot) in snapshots.iter().enumerate() {
            rocks.restore_key_group(group as u32, snapshot).unwrap();
            assert_eq!(rocks.snapshot_key_group(group as u32).unwrap(), *snapshot);
        }
        let output = rocks
            .process_arrow(2, batch(&[1, 2], &["c", "z"], &[INSERT, INSERT]))
            .unwrap();
        assert_eq!(kinds(&output), vec![INSERT, INSERT]);
        assert_eq!(rocks.statistics(), [1, 1]);
    }

    fn processor(broker: Arc<TestBroker>, first: u32, last: u32) -> MultiJoinProcessor {
        MultiJoinProcessor::new(
            &plan(),
            128,
            first,
            last,
            HostMemoryReservation::new(broker, "multi-join rescale"),
        )
        .unwrap()
    }

    fn plan() -> Vec<u8> {
        plan_with_types([
            proto::RegularJoinType::Inner,
            proto::RegularJoinType::Inner,
            proto::RegularJoinType::Inner,
        ])
    }

    fn plan_with_types(join_types: [proto::RegularJoinType; 3]) -> Vec<u8> {
        let input = || proto::MultiJoinInput {
            schema: Some(schema()),
            common_key_indices: vec![0],
            state_retention_millis: 0,
        };
        proto::NativePlan {
            protocol_version: crate::PLAN_PROTOCOL_VERSION,
            root: Some(proto::Operator {
                plan_node_id: 0,
                operator: Some(proto::operator::Operator::MultiJoin(proto::MultiJoin {
                    inputs: vec![input(), input(), input()],
                    join_types: join_types.into_iter().map(|kind| kind as i32).collect(),
                    equi_conditions: vec![
                        proto::MultiJoinEquiCondition {
                            depth: 1,
                            left_input_index: 0,
                            left_field_index: 0,
                            right_field_index: 0,
                        },
                        proto::MultiJoinEquiCondition {
                            depth: 2,
                            left_input_index: 1,
                            left_field_index: 0,
                            right_field_index: 0,
                        },
                    ],
                })),
            }),
        }
        .encode_to_vec()
    }

    fn schema() -> proto::Schema {
        proto::Schema {
            fields: vec![
                field(
                    "key",
                    proto::logical_type::Type::Bigint(proto::EmptyType::default()),
                ),
                field(
                    "value",
                    proto::logical_type::Type::Varchar(proto::EmptyType::default()),
                ),
            ],
        }
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

    fn batch(keys: &[i64], values: &[&str], row_kinds: &[i8]) -> RecordBatch {
        let conditions = keys
            .iter()
            .map(|key| Some(key.to_le_bytes().to_vec()))
            .collect::<Vec<_>>();
        nullable_batch(keys, values, row_kinds, &conditions)
    }

    fn nullable_batch(
        keys: &[i64],
        values: &[&str],
        row_kinds: &[i8],
        conditions: &[Option<Vec<u8>>],
    ) -> RecordBatch {
        let condition_refs = conditions
            .iter()
            .map(|value| value.as_deref())
            .collect::<Vec<_>>();
        RecordBatch::try_from_iter(vec![
            ("key", Arc::new(Int64Array::from(keys.to_vec())) as ArrayRef),
            (
                "value",
                Arc::new(StringArray::from(values.to_vec())) as ArrayRef,
            ),
            (
                "__streamfusion_input_row_kind",
                Arc::new(Int8Array::from(row_kinds.to_vec())) as ArrayRef,
            ),
            (
                "__streamfusion_condition_0",
                Arc::new(BinaryArray::from(condition_refs)) as ArrayRef,
            ),
        ])
        .unwrap()
    }

    fn kinds(batch: &RecordBatch) -> Vec<i8> {
        batch
            .column(batch.num_columns() - 2)
            .as_any()
            .downcast_ref::<Int8Array>()
            .unwrap()
            .values()
            .to_vec()
    }
}
