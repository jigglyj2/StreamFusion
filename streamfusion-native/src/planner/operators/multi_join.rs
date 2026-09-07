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
const STATE_VERSION: u8 = 2;
mod codec;
mod pages;
use codec::{decode_state, encode_state, read_u32, truncated};
mod cursor;
mod work;

#[derive(Clone, Debug, PartialEq, Eq)]
struct StoredRow {
    slot: u64,
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
    directory: pages::Directory,
    dirty: std::collections::BTreeSet<(usize, u64)>,
}

struct OutputRow {
    inputs: Vec<Vec<u8>>,
    kind: i8,
    input_ordinal: i32,
}

/// Persistent N-input join state. Each incoming batch loads directories/pages with batched
/// state reads, drains bounded Arrow output, and flushes only dirty pages in one write batch.
pub(crate) struct MultiJoinProcessor {
    pending: Option<work::Work>,
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
    failed: bool,
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
        let state = Box::new(RocksPluginKeyedState::open_for_owner(
            plugin_path,
            database_path,
            first_key_group,
            last_key_group,
            memory_limit,
            &scratch,
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
            pending: None,
            failed: false,
        })
    }

    /// Compatibility for old single-batch harnesses. Large fan-out must use the native pull
    /// interface; never concatenate or retain the complete Cartesian output at this edge.
    pub(crate) fn process_arrow(
        &mut self,
        input: usize,
        batch: RecordBatch,
    ) -> Result<RecordBatch> {
        self.start_arrow_stream(input, batch)?;
        let Some(mut output) = self.next_output()? else {
            return self.empty_output();
        };
        if self.next_output()?.is_some() {
            self.abort_unpublished()?;
            return Err(DataFusionError::ResourcesExhausted(
                "multi-join fan-out requires a bounded C Stream consumer".into(),
            ));
        }
        output.memory.transfer_to_arrow(output.memory.size())?;
        Ok(output.batch)
    }

    pub(crate) fn statistics(&self) -> [u64; 2] {
        [self.state_read_batches, self.state_write_batches]
    }

    pub(crate) fn state_memory(&self) -> HostMemoryReservation {
        self.scratch_reservation.sibling("native state transfer")
    }

    fn require_idle(&self) -> Result<()> {
        if self.pending.is_some() || self.failed {
            return Err(DataFusionError::Execution(
                "multi-join checkpoint/restore requires a fully drained healthy stream".into(),
            ));
        }
        Ok(())
    }

    pub(crate) fn snapshot_key_group(&self, key_group: u32) -> Result<crate::state::SnapshotBytes> {
        self.require_idle()?;
        self.state
            .snapshot_key_group(key_group, &self.scratch_reservation)
    }

    pub(crate) fn restore_key_group(&mut self, key_group: u32, bytes: &[u8]) -> Result<()> {
        self.require_idle()?;
        self.state
            .restore_key_group(key_group, bytes, &self.scratch_reservation)
    }

    pub(crate) fn checkpoint(&self, directory: &std::path::Path) -> Result<()> {
        self.require_idle()?;
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
            slot: 0,
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

#[cfg(test)]
mod tests;
