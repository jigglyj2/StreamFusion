// Copyright 2026 StreamFusion Authors
// Licensed under the Apache License, Version 2.0

use std::sync::Arc;

use ahash::RandomState;
use arrow::array::{Array, BinaryArray, Int32Array, Int8Array, UInt32Array};
use arrow::compute::{take, SortOptions};
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
const STATE_MAGIC: &[u8; 4] = b"SFRJ";
const STATE_VERSION: u8 = 1;

#[derive(Clone, Debug, PartialEq, Eq)]
struct StoredRow {
    row: Vec<u8>,
    // Flink's OuterJoinRecordStateView stores this as a Java int. Preserve its
    // wrapping arithmetic as well as its width in canonical state.
    associations: i32,
}

#[derive(Default, Debug, PartialEq, Eq)]
struct JoinState {
    left: Vec<StoredRow>,
    right: Vec<StoredRow>,
}

struct StagedState {
    key: StateKey,
    value: JoinState,
    touched: bool,
}

struct OutputRow {
    left: Option<Vec<u8>>,
    right: Option<Vec<u8>>,
    kind: i8,
    input_ordinal: i32,
}

/// Persistent Arrow-native implementation of Flink's synchronous regular streaming join.
pub(crate) struct RegularJoinProcessor {
    plan: proto::RegularJoin,
    join_type: proto::RegularJoinType,
    max_parallelism: u32,
    state: Box<dyn KeyedState>,
    visible_schemas: [SchemaRef; 2],
    output_schema: SchemaRef,
    row_converters: [RowConverter; 2],
    input_schemas: [Option<SchemaRef>; 2],
    key_fields: [Vec<(usize, KeyField)>; 2],
    preencoded_key_indices: [Option<usize>; 2],
    input_kind_indices: [Option<usize>; 2],
    scratch_reservation: HostMemoryReservation,
    state_read_batches: u64,
    state_write_batches: u64,
}

impl RegularJoinProcessor {
    pub(crate) fn new(
        serialized_plan: &[u8],
        max_parallelism: u32,
        first_key_group: u32,
        last_key_group: u32,
        reservation: HostMemoryReservation,
    ) -> Result<Self> {
        let scratch = reservation.sibling("native regular join batch scratch and output");
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
            .ok_or_else(|| DataFusionError::Plan("regular join plan has no root".to_string()))?;
        let plan = match root.operator {
            Some(proto::operator::Operator::RegularJoin(plan)) => plan,
            _ => {
                return Err(DataFusionError::Plan(
                    "regular join handle requires a RegularJoin root".to_string(),
                ));
            }
        };
        validate_plan(&plan, max_parallelism)?;
        let join_type = proto::RegularJoinType::try_from(plan.join_type).map_err(|_| {
            DataFusionError::Plan(format!("unknown regular join type {}", plan.join_type))
        })?;
        let visible_schemas = [
            arrow_schema(plan.left_schema.as_ref().expect("validated left schema"))?,
            arrow_schema(plan.right_schema.as_ref().expect("validated right schema"))?,
        ];
        let row_converters = [
            row_converter(&visible_schemas[0])?,
            row_converter(&visible_schemas[1])?,
        ];
        let mut output_fields = Vec::new();
        for (index, field) in visible_schemas[0].fields().iter().enumerate() {
            output_fields.push(Arc::new(Field::new(
                format!("__streamfusion_join_left_{index}"),
                field.data_type().clone(),
                field.is_nullable()
                    || matches!(
                        join_type,
                        proto::RegularJoinType::Right | proto::RegularJoinType::Full
                    ),
            )));
        }
        if !matches!(
            join_type,
            proto::RegularJoinType::Semi | proto::RegularJoinType::Anti
        ) {
            for (index, field) in visible_schemas[1].fields().iter().enumerate() {
                output_fields.push(Arc::new(Field::new(
                    format!("__streamfusion_join_right_{index}"),
                    field.data_type().clone(),
                    field.is_nullable()
                        || matches!(
                            join_type,
                            proto::RegularJoinType::Left | proto::RegularJoinType::Full
                        ),
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
        Ok(Self {
            plan,
            join_type,
            max_parallelism,
            state,
            visible_schemas,
            output_schema: Arc::new(Schema::new(output_fields)),
            row_converters,
            input_schemas: [None, None],
            key_fields: [Vec::new(), Vec::new()],
            preencoded_key_indices: [None, None],
            input_kind_indices: [None, None],
            scratch_reservation,
            state_read_batches: 0,
            state_write_batches: 0,
        })
    }

    pub(crate) fn process_arrow(&mut self, side: usize, batch: RecordBatch) -> Result<RecordBatch> {
        if side > 1 {
            return Err(DataFusionError::Execution(
                "regular join side must be zero or one".to_string(),
            ));
        }
        self.prepare_schema(side, batch.schema())?;
        let visible_count = self.visible_schemas[side].fields().len();
        let input_bytes = batch.columns()[..visible_count]
            .iter()
            .map(|column| column.get_array_memory_size())
            .sum::<usize>();
        let base = input_bytes.saturating_add(batch.num_rows().saturating_mul(256));
        self.scratch_reservation.resize(base)?;
        let encoded = self.row_converters[side].convert_columns(&batch.columns()[..visible_count]);
        let result = match encoded {
            Ok(encoded) => self.process_accounted(side, &batch, &encoded),
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
        side: usize,
        batch: &RecordBatch,
        encoded: &Rows,
    ) -> Result<RecordBatch> {
        let kinds = batch
            .column(self.input_kind_indices[side].expect("schema prepared"))
            .as_any()
            .downcast_ref::<Int8Array>()
            .ok_or_else(|| {
                DataFusionError::Execution("regular join RowKinds are not Int8".to_string())
            })?;
        let mut unique = HashMap::<StateKey, usize, RandomState>::with_capacity_and_hasher(
            batch.num_rows(),
            RandomState::new(),
        );
        let mut row_state_indices = Vec::with_capacity(batch.num_rows());
        for row in 0..batch.num_rows() {
            let key = self.group_key(side, batch, row)?;
            let key_group = assign_key_group(&key, self.max_parallelism);
            let state_key = StateKey { key_group, key };
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
            .map(|key| key.expect("regular join state index is populated"))
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
        let mut staged = keys
            .into_iter()
            .zip(existing)
            .map(|(key, bytes)| {
                Ok(StagedState {
                    key,
                    value: bytes
                        .map(|bytes| decode_state(bytes.as_ref()))
                        .transpose()?
                        .unwrap_or_default(),
                    touched: false,
                })
            })
            .collect::<Result<Vec<_>>>()?;
        let mut output = Vec::new();
        for row in 0..batch.num_rows() {
            let ordinal = i32::try_from(row).map_err(|_| {
                DataFusionError::Execution("regular join batch exceeds Int32 indexing".to_string())
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
            let state = &mut staged[row_state_indices[row]];
            let row_bytes = encoded.row(row).data().to_vec();
            let matchable = self.row_is_matchable(side, batch, row);
            process_change(
                self.join_type,
                side,
                kind,
                accumulate,
                matchable,
                row_bytes,
                ordinal,
                &mut state.value,
                &mut output,
            )?;
            state.touched = true;
        }
        let mutations = staged
            .into_iter()
            .filter(|entry| entry.touched)
            .map(|entry| StateMutation {
                key: entry.key,
                value: (!(entry.value.left.is_empty() && entry.value.right.is_empty()))
                    .then(|| encode_state(&entry.value)),
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

    fn group_key(&self, side: usize, batch: &RecordBatch, row: usize) -> Result<Vec<u8>> {
        match self.preencoded_key_indices[side] {
            Some(index) => Ok(batch
                .column(index)
                .as_any()
                .downcast_ref::<BinaryArray>()
                .ok_or_else(|| {
                    DataFusionError::Execution(
                        "regular join preencoded keys are not Binary".to_string(),
                    )
                })?
                .value(row)
                .to_vec()),
            None if self.key_fields[side].is_empty() => Ok(Vec::new()),
            None => Ok(encode_binary_row(batch, row, &self.key_fields[side])?),
        }
    }

    fn row_is_matchable(&self, side: usize, batch: &RecordBatch, row: usize) -> bool {
        self.plan
            .filter_nulls
            .iter()
            .zip(self.key_indices(side))
            .all(|(&filter, &index)| !filter || !batch.column(index as usize).is_null(row))
    }

    fn prepare_schema(&mut self, side: usize, schema: SchemaRef) -> Result<()> {
        if let Some(expected) = &self.input_schemas[side] {
            if expected.as_ref() != schema.as_ref() {
                return Err(DataFusionError::Execution(format!(
                    "regular join input {side} schema changed while running"
                )));
            }
            return Ok(());
        }
        self.preencoded_key_indices[side] = metadata_index(&schema, "__streamfusion_key");
        self.input_kind_indices[side] = metadata_index(&schema, "__streamfusion_input_row_kind")
            .or_else(|| metadata_index(&schema, "__streamfusion_row_kind"));
        if self.input_kind_indices[side].is_none() {
            return Err(DataFusionError::Execution(
                "regular join requires RowKind metadata".to_string(),
            ));
        }
        let visible_count = [
            self.preencoded_key_indices[side],
            self.input_kind_indices[side],
            Some(schema.fields().len()),
        ]
        .into_iter()
        .flatten()
        .min()
        .unwrap();
        if visible_count != self.visible_schemas[side].fields().len()
            || schema.fields()[..visible_count]
                .iter()
                .zip(self.visible_schemas[side].fields())
                .any(|(actual, planned)| actual.data_type() != planned.data_type())
        {
            return Err(DataFusionError::Execution(format!(
                "regular join input {side} Arrow schema does not match its protobuf schema"
            )));
        }
        if self.preencoded_key_indices[side].is_none() {
            self.key_fields[side] = self
                .key_indices(side)
                .iter()
                .map(|&index| {
                    let field = schema.fields().get(index as usize).ok_or_else(|| {
                        arrow::error::ArrowError::SchemaError(format!(
                            "regular join input {side} key {index} is outside the visible row"
                        ))
                    })?;
                    Ok((
                        index as usize,
                        KeyField::from_arrow_type(field.data_type())?,
                    ))
                })
                .collect::<std::result::Result<Vec<_>, arrow::error::ArrowError>>()?;
        }
        self.input_schemas[side] = Some(schema);
        Ok(())
    }

    fn key_indices(&self, side: usize) -> &[u32] {
        if side == 0 {
            &self.plan.left_key_indices
        } else {
            &self.plan.right_key_indices
        }
    }

    fn output_batch(&self, rows: Vec<OutputRow>) -> Result<RecordBatch> {
        let mut encoded = [Vec::new(), Vec::new()];
        let mut selections = [
            Vec::with_capacity(rows.len()),
            Vec::with_capacity(rows.len()),
        ];
        let mut kinds = Vec::with_capacity(rows.len());
        let mut ordinals = Vec::with_capacity(rows.len());
        for row in rows {
            for (side, value) in [row.left, row.right].into_iter().enumerate() {
                if let Some(value) = value {
                    let index = u32::try_from(encoded[side].len()).map_err(|_| {
                        DataFusionError::Execution(
                            "regular join output exceeds u32 rows".to_string(),
                        )
                    })?;
                    encoded[side].push(value);
                    selections[side].push(Some(index));
                } else {
                    selections[side].push(None);
                }
            }
            kinds.push(row.kind);
            ordinals.push(row.input_ordinal);
        }
        let left_only = matches!(
            self.join_type,
            proto::RegularJoinType::Semi | proto::RegularJoinType::Anti
        );
        let mut columns = Vec::new();
        for side in 0..if left_only { 1 } else { 2 } {
            let parser = self.row_converters[side].parser();
            let decoded = self.row_converters[side]
                .convert_rows(encoded[side].iter().map(|row| parser.parse(row)))?;
            let selection = UInt32Array::from(selections[side].clone());
            for column in decoded {
                columns.push(take(column.as_ref(), &selection, None)?);
            }
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

#[allow(clippy::too_many_arguments)]
fn process_change(
    join_type: proto::RegularJoinType,
    side: usize,
    input_kind: i8,
    accumulate: bool,
    matchable: bool,
    input: Vec<u8>,
    ordinal: i32,
    state: &mut JoinState,
    output: &mut Vec<OutputRow>,
) -> Result<()> {
    if matches!(
        join_type,
        proto::RegularJoinType::Semi | proto::RegularJoinType::Anti
    ) {
        return process_semi_anti(
            join_type, side, input_kind, accumulate, matchable, input, ordinal, state, output,
        );
    }
    let input_outer = is_outer(join_type, side);
    let other_outer = is_outer(join_type, 1 - side);
    let (input_rows, other) = if side == 0 {
        (&mut state.left, &mut state.right)
    } else {
        (&mut state.right, &mut state.left)
    };
    let matches = if matchable { other.len() } else { 0 };
    if accumulate {
        if matches == 0 {
            if input_outer {
                push_pair(output, side, Some(input.clone()), None, INSERT, ordinal);
            }
        } else {
            for candidate in other.iter_mut() {
                if other_outer {
                    if candidate.associations == 0 {
                        push_pair(
                            output,
                            1 - side,
                            Some(candidate.row.clone()),
                            None,
                            DELETE,
                            ordinal,
                        );
                    }
                    candidate.associations = candidate.associations.wrapping_add(1);
                }
                let kind = if input_outer || other_outer {
                    INSERT
                } else {
                    input_kind
                };
                push_pair(
                    output,
                    side,
                    Some(input.clone()),
                    Some(candidate.row.clone()),
                    kind,
                    ordinal,
                );
            }
        }
        let stored = StoredRow {
            row: input,
            associations: if input_outer { matches as i32 } else { 0 },
        };
        input_rows.push(stored);
    } else {
        // Flink's no-unique-key state view ignores a missing record (for
        // example after TTL expiry) but still executes the join transition.
        if let Some(position) = input_rows
            .iter()
            .position(|candidate| candidate.row == input)
        {
            input_rows.remove(position);
        }
        if matches == 0 {
            if input_outer {
                push_pair(output, side, Some(input), None, DELETE, ordinal);
            }
        } else {
            for candidate in other.iter_mut() {
                let kind = if input_outer { DELETE } else { input_kind };
                push_pair(
                    output,
                    side,
                    Some(input.clone()),
                    Some(candidate.row.clone()),
                    kind,
                    ordinal,
                );
                if other_outer {
                    if candidate.associations == 1 {
                        push_pair(
                            output,
                            1 - side,
                            Some(candidate.row.clone()),
                            None,
                            INSERT,
                            ordinal,
                        );
                    }
                    candidate.associations = candidate.associations.wrapping_sub(1);
                }
            }
        }
    }
    Ok(())
}

#[allow(clippy::too_many_arguments)]
fn process_semi_anti(
    join_type: proto::RegularJoinType,
    side: usize,
    input_kind: i8,
    accumulate: bool,
    matchable: bool,
    input: Vec<u8>,
    ordinal: i32,
    state: &mut JoinState,
    output: &mut Vec<OutputRow>,
) -> Result<()> {
    let anti = matches!(join_type, proto::RegularJoinType::Anti);
    if side == 0 {
        let matches = if matchable { state.right.len() } else { 0 };
        if (anti && matches == 0) || (!anti && matches > 0) {
            output.push(OutputRow {
                left: Some(input.clone()),
                right: None,
                kind: input_kind,
                input_ordinal: ordinal,
            });
        }
        if accumulate {
            state.left.push(StoredRow {
                row: input,
                associations: matches as i32,
            });
        } else {
            if let Some(position) = state
                .left
                .iter()
                .position(|candidate| candidate.row == input)
            {
                state.left.remove(position);
            }
        }
        return Ok(());
    }
    if accumulate {
        state.right.push(StoredRow {
            row: input,
            associations: 0,
        });
        if matchable {
            for left in &mut state.left {
                if left.associations == 0 {
                    output.push(OutputRow {
                        left: Some(left.row.clone()),
                        right: None,
                        kind: if anti { DELETE } else { input_kind },
                        input_ordinal: ordinal,
                    });
                }
                left.associations = left.associations.wrapping_add(1);
            }
        }
    } else {
        if let Some(position) = state
            .right
            .iter()
            .position(|candidate| candidate.row == input)
        {
            state.right.remove(position);
        }
        if matchable {
            for left in &mut state.left {
                if left.associations == 1 {
                    output.push(OutputRow {
                        left: Some(left.row.clone()),
                        right: None,
                        kind: if anti { INSERT } else { input_kind },
                        input_ordinal: ordinal,
                    });
                }
                left.associations = left.associations.wrapping_sub(1);
            }
        }
    }
    Ok(())
}

fn is_outer(join_type: proto::RegularJoinType, side: usize) -> bool {
    matches!(join_type, proto::RegularJoinType::Full)
        || (side == 0 && matches!(join_type, proto::RegularJoinType::Left))
        || (side == 1 && matches!(join_type, proto::RegularJoinType::Right))
}

fn push_pair(
    output: &mut Vec<OutputRow>,
    input_side: usize,
    input: Option<Vec<u8>>,
    other: Option<Vec<u8>>,
    kind: i8,
    ordinal: i32,
) {
    let (left, right) = if input_side == 0 {
        (input, other)
    } else {
        (other, input)
    };
    output.push(OutputRow {
        left,
        right,
        kind,
        input_ordinal: ordinal,
    });
}

fn validate_plan(plan: &proto::RegularJoin, max_parallelism: u32) -> Result<()> {
    let join_type = proto::RegularJoinType::try_from(plan.join_type).ok();
    if max_parallelism == 0
        || plan.left_key_indices.len() != plan.right_key_indices.len()
        || plan.left_key_indices.len() != plan.filter_nulls.len()
        || plan.left_schema.is_none()
        || plan.right_schema.is_none()
        || join_type.is_none()
        || matches!(join_type, Some(proto::RegularJoinType::Unspecified))
        || plan.left_state_ttl_millis != 0
        || plan.right_state_ttl_millis != 0
    {
        return Err(DataFusionError::Plan(
            "regular join key/schema/type/TTL contract is invalid".to_string(),
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

fn encode_state(state: &JoinState) -> Vec<u8> {
    let mut bytes = Vec::new();
    bytes.extend_from_slice(STATE_MAGIC);
    bytes.push(STATE_VERSION);
    for rows in [&state.left, &state.right] {
        bytes.extend_from_slice(&(rows.len() as u32).to_le_bytes());
        for row in rows {
            bytes.extend_from_slice(&row.associations.to_le_bytes());
            bytes.extend_from_slice(&(row.row.len() as u32).to_le_bytes());
            bytes.extend_from_slice(&row.row);
        }
    }
    bytes
}

fn decode_state(bytes: &[u8]) -> Result<JoinState> {
    if bytes.len() < 13 || &bytes[..4] != STATE_MAGIC || bytes[4] != STATE_VERSION {
        return Err(DataFusionError::Execution(
            "invalid native regular join state".to_string(),
        ));
    }
    let mut offset = 5;
    let left = decode_rows(bytes, &mut offset)?;
    let right = decode_rows(bytes, &mut offset)?;
    if offset != bytes.len() {
        return Err(DataFusionError::Execution(
            "regular join state has trailing bytes".to_string(),
        ));
    }
    Ok(JoinState { left, right })
}

fn decode_rows(bytes: &[u8], offset: &mut usize) -> Result<Vec<StoredRow>> {
    let count = read_u32(bytes, offset)? as usize;
    let mut rows = Vec::with_capacity(count);
    for _ in 0..count {
        let associations = read_i32(bytes, offset)?;
        let length = read_u32(bytes, offset)? as usize;
        let end = offset.checked_add(length).ok_or_else(truncated)?;
        rows.push(StoredRow {
            associations,
            row: bytes.get(*offset..end).ok_or_else(truncated)?.to_vec(),
        });
        *offset = end;
    }
    Ok(rows)
}

fn read_u32(bytes: &[u8], offset: &mut usize) -> Result<u32> {
    let end = offset.checked_add(4).ok_or_else(truncated)?;
    let value = bytes.get(*offset..end).ok_or_else(truncated)?;
    *offset = end;
    Ok(u32::from_le_bytes(value.try_into().unwrap()))
}

fn read_i32(bytes: &[u8], offset: &mut usize) -> Result<i32> {
    let end = offset.checked_add(4).ok_or_else(truncated)?;
    let value = bytes.get(*offset..end).ok_or_else(truncated)?;
    *offset = end;
    Ok(i32::from_le_bytes(value.try_into().unwrap()))
}

fn truncated() -> DataFusionError {
    DataFusionError::Execution("truncated native regular join state".to_string())
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::memory_pool::{tests_support::TestBroker, HostMemoryReservation};
    use arrow::array::{ArrayRef, Int64Array, StringArray};
    use prost::Message;

    #[test]
    fn full_join_handles_duplicates_and_retractions() {
        let broker = Arc::new(TestBroker::new(64 << 20));
        let mut processor = RegularJoinProcessor::new(
            &plan(proto::RegularJoinType::Full),
            128,
            0,
            127,
            HostMemoryReservation::new(broker.clone(), "regular join test"),
        )
        .unwrap();
        let left = processor
            .process_arrow(0, batch(&[1], &["left"], &[INSERT]))
            .unwrap();
        assert_eq!(kinds(&left), vec![INSERT]);
        let right = processor
            .process_arrow(1, batch(&[1, 1], &["r1", "r2"], &[INSERT, INSERT]))
            .unwrap();
        assert_eq!(kinds(&right), vec![DELETE, INSERT, INSERT]);
        let retract = processor
            .process_arrow(1, batch(&[1, 1], &["r1", "r2"], &[DELETE, DELETE]))
            .unwrap();
        assert_eq!(kinds(&retract), vec![DELETE, DELETE, INSERT]);
        drop(left);
        drop(right);
        drop(retract);
        drop(processor);
        assert_eq!(broker.reserved(), 0);
    }

    #[test]
    fn semi_and_anti_transition_on_first_and_last_match() {
        for (join_type, expected) in [
            (proto::RegularJoinType::Semi, vec![INSERT, DELETE]),
            (proto::RegularJoinType::Anti, vec![DELETE, INSERT]),
        ] {
            let broker = Arc::new(TestBroker::new(64 << 20));
            let mut processor = RegularJoinProcessor::new(
                &plan(join_type),
                128,
                0,
                127,
                HostMemoryReservation::new(broker, "semi anti test"),
            )
            .unwrap();
            let initial = processor
                .process_arrow(0, batch(&[1], &["left"], &[INSERT]))
                .unwrap();
            assert_eq!(
                kinds(&initial),
                if join_type == proto::RegularJoinType::Anti {
                    vec![INSERT]
                } else {
                    vec![]
                }
            );
            let add = processor
                .process_arrow(1, batch(&[1], &["right"], &[INSERT]))
                .unwrap();
            let remove = processor
                .process_arrow(1, batch(&[1], &["right"], &[DELETE]))
                .unwrap();
            assert_eq!([kinds(&add), kinds(&remove)].concat(), expected);
        }
    }

    #[test]
    fn regular_join_modes_match_flink_transition_contract() {
        for (join_type, first, add, remove) in [
            (
                proto::RegularJoinType::Inner,
                vec![],
                vec![INSERT],
                vec![DELETE],
            ),
            (
                proto::RegularJoinType::Left,
                vec![INSERT],
                vec![DELETE, INSERT],
                vec![DELETE, INSERT],
            ),
            (
                proto::RegularJoinType::Right,
                vec![],
                vec![INSERT],
                vec![DELETE],
            ),
            (
                proto::RegularJoinType::Full,
                vec![INSERT],
                vec![DELETE, INSERT],
                vec![DELETE, INSERT],
            ),
        ] {
            let broker = Arc::new(TestBroker::new(64 << 20));
            let mut processor = RegularJoinProcessor::new(
                &plan(join_type),
                128,
                0,
                127,
                HostMemoryReservation::new(broker, "regular join modes"),
            )
            .unwrap();
            assert_eq!(
                kinds(
                    &processor
                        .process_arrow(0, batch(&[1], &["left"], &[INSERT]))
                        .unwrap()
                ),
                first
            );
            assert_eq!(
                kinds(
                    &processor
                        .process_arrow(1, batch(&[1], &["right"], &[INSERT]))
                        .unwrap()
                ),
                add
            );
            assert_eq!(
                kinds(
                    &processor
                        .process_arrow(1, batch(&[1], &["right"], &[DELETE]))
                        .unwrap()
                ),
                remove
            );
        }
    }

    #[test]
    fn null_filtered_keys_never_match_and_missing_retractions_are_tolerated() {
        let broker = Arc::new(TestBroker::new(64 << 20));
        let mut processor = RegularJoinProcessor::new(
            &plan(proto::RegularJoinType::Full),
            128,
            0,
            127,
            HostMemoryReservation::new(broker, "regular join null keys"),
        )
        .unwrap();
        let left = nullable_batch(&[None], &["left"], &[INSERT]);
        let right = nullable_batch(&[None], &["right"], &[INSERT]);
        assert_eq!(
            kinds(&processor.process_arrow(0, left).unwrap()),
            vec![INSERT]
        );
        assert_eq!(
            kinds(&processor.process_arrow(1, right).unwrap()),
            vec![INSERT]
        );

        // Flink's MapState-based no-unique-key view ignores the absent state
        // record and continues evaluating the transition.
        let missing = processor
            .process_arrow(0, nullable_batch(&[None], &["missing"], &[DELETE]))
            .unwrap();
        assert_eq!(kinds(&missing), vec![DELETE]);
    }

    #[test]
    fn null_safe_keys_match_for_intersect_and_except_join_shapes() {
        let broker = Arc::new(TestBroker::new(64 << 20));
        for (join_type, expected_before, expected_match) in [
            (proto::RegularJoinType::Semi, Vec::new(), vec![INSERT]),
            (proto::RegularJoinType::Anti, vec![INSERT], vec![DELETE]),
        ] {
            let mut processor = RegularJoinProcessor::new(
                &plan_with_filter(join_type, false),
                128,
                0,
                127,
                HostMemoryReservation::new(broker.clone(), "regular join null-safe keys"),
            )
            .unwrap();
            assert_eq!(
                kinds(
                    &processor
                        .process_arrow(0, nullable_batch(&[None], &["left"], &[INSERT]))
                        .unwrap()
                ),
                expected_before
            );
            assert_eq!(
                kinds(
                    &processor
                        .process_arrow(1, nullable_batch(&[None], &["right"], &[INSERT]))
                        .unwrap()
                ),
                expected_match
            );
        }
    }

    #[test]
    fn canonical_state_restores_after_rescaling() {
        let broker = Arc::new(TestBroker::new(1 << 30));
        let mut source = processor(broker.clone(), 0, 127);
        source
            .process_arrow(0, batch(&[1, 2], &["a", "b"], &[INSERT, INSERT]))
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
            let input = batch(&[key], &["r"], &[INSERT]);
            let encoded = source.group_key(1, &input, 0).unwrap();
            let result = if assign_key_group(&encoded, 128) < 64 {
                low.process_arrow(1, input).unwrap()
            } else {
                high.process_arrow(1, input).unwrap()
            };
            assert_eq!(kinds(&result), vec![DELETE, INSERT]);
        }
    }

    #[test]
    fn canonical_state_moves_from_memory_to_rocksdb_and_batches_io() {
        let Ok(plugin_path) = std::env::var("STREAMFUSION_TEST_ROCKSDB_PLUGIN") else {
            return;
        };
        let broker = Arc::new(TestBroker::new(1 << 30));
        let mut memory = RegularJoinProcessor::new(
            &plan(proto::RegularJoinType::Full),
            128,
            0,
            127,
            HostMemoryReservation::new(broker.clone(), "regular join memory source"),
        )
        .unwrap();
        memory
            .process_arrow(0, batch(&[1, 2], &["a", "b"], &[INSERT, INSERT]))
            .unwrap();
        assert_eq!(memory.statistics(), [1, 1]);
        let snapshots = (0..128)
            .map(|group| memory.snapshot_key_group(group).unwrap())
            .collect::<Vec<_>>();

        let directory = tempfile::tempdir().unwrap();
        let mut rocks = RegularJoinProcessor::new_rocksdb(
            &plan(proto::RegularJoinType::Full),
            128,
            0,
            127,
            std::path::Path::new(&plugin_path),
            directory.path(),
            64 << 20,
            HostMemoryReservation::new(broker, "regular join RocksDB scratch"),
        )
        .unwrap();
        for (group, snapshot) in snapshots.iter().enumerate() {
            rocks.restore_key_group(group as u32, snapshot).unwrap();
            assert_eq!(rocks.snapshot_key_group(group as u32).unwrap(), *snapshot);
        }
        let output = rocks
            .process_arrow(1, batch(&[1, 2], &["r1", "r2"], &[INSERT, INSERT]))
            .unwrap();
        assert_eq!(kinds(&output), vec![DELETE, INSERT, DELETE, INSERT]);
        assert_eq!(rocks.statistics(), [1, 1]);
    }

    fn processor(broker: Arc<TestBroker>, first: u32, last: u32) -> RegularJoinProcessor {
        RegularJoinProcessor::new(
            &plan(proto::RegularJoinType::Left),
            128,
            first,
            last,
            HostMemoryReservation::new(broker, "regular join rescale"),
        )
        .unwrap()
    }

    fn plan(join_type: proto::RegularJoinType) -> Vec<u8> {
        plan_with_filter(join_type, true)
    }

    fn plan_with_filter(join_type: proto::RegularJoinType, filter_nulls: bool) -> Vec<u8> {
        proto::NativePlan {
            protocol_version: crate::PLAN_PROTOCOL_VERSION,
            root: Some(proto::Operator {
                operator: Some(proto::operator::Operator::RegularJoin(proto::RegularJoin {
                    left_key_indices: vec![0],
                    right_key_indices: vec![0],
                    filter_nulls: vec![filter_nulls],
                    left_schema: Some(schema()),
                    right_schema: Some(schema()),
                    join_type: join_type as i32,
                    left_state_ttl_millis: 0,
                    right_state_ttl_millis: 0,
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
        ])
        .unwrap()
    }

    fn nullable_batch(keys: &[Option<i64>], values: &[&str], row_kinds: &[i8]) -> RecordBatch {
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
