// Copyright 2026 StreamFusion Authors
// Licensed under the Apache License, Version 2.0

use std::sync::Arc;

use ahash::RandomState;
use arrow::array::{Array, BinaryArray, BooleanArray, Int32Array, Int8Array, UInt32Array};
use arrow::compute::{take, SortOptions};
use arrow::datatypes::{DataType, Field, Schema, SchemaRef};
use arrow::record_batch::RecordBatch;
use arrow_row::{RowConverter, Rows, SortField};
use datafusion::error::{DataFusionError, Result};
use datafusion::physical_expr::PhysicalExpr;
use hashbrown::HashMap;

use crate::exchange::{assign_key_group, encode_binary_row, KeyField};
use crate::memory_pool::HostMemoryReservation;
use crate::planner::arrow_schema;
use crate::state::{
    decode_key_group_snapshot, KeyedState, MemoryKeyedState, RocksPluginKeyedState, StateKey,
    StateKeyRef, StateMutation,
};
use crate::{decode_plan, proto};

use super::calc;
use super::fused_calc::FusedCalcPipeline;

const INSERT: i8 = 0;
const UPDATE_BEFORE: i8 = 1;
const UPDATE_AFTER: i8 = 2;
const DELETE: i8 = 3;
const STATE_MAGIC: &[u8; 4] = b"SFRJ";
const LEGACY_STATE_VERSION: u8 = 1;
const STATE_VERSION: u8 = 2;
const BOUNDED_EDGE_OUTPUT_MAX_ROWS: usize = 4_096;
const BOUNDED_FUSED_OUTPUT_MAX_ROWS: usize = 16_384;

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
    // Bounded equality keys have one null-filter result per side and key. Persist it once rather
    // than decoding every stored Arrow row again during terminal output. `None` preserves
    // compatibility with streaming and version-one canonical state.
    left_matchable: Option<bool>,
    right_matchable: Option<bool>,
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

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
enum BoundedOutputPhase {
    Pairs,
    LeftRemainder,
    RightRemainder,
    Done,
}

struct BoundedJoinCursor {
    state: JoinState,
    left_matchable: Vec<bool>,
    right_matchable: Vec<bool>,
    left_matched: Vec<bool>,
    right_matched: Vec<bool>,
    pair_index: usize,
    remainder_index: usize,
    phase: BoundedOutputPhase,
}

impl BoundedJoinCursor {
    fn estimated_dynamic_bytes(&self) -> usize {
        self.state
            .left
            .iter()
            .chain(&self.state.right)
            .fold(0usize, |bytes, row| {
                bytes.saturating_add(row.row.capacity())
            })
            .saturating_add(
                self.state
                    .left
                    .capacity()
                    .saturating_add(self.state.right.capacity())
                    .saturating_mul(std::mem::size_of::<StoredRow>()),
            )
            .saturating_add(self.left_matchable.capacity())
            .saturating_add(self.right_matchable.capacity())
            .saturating_add(self.left_matched.capacity())
            .saturating_add(self.right_matched.capacity())
    }
}

/// Persistent Arrow-native implementation of Flink's synchronous regular streaming join.
pub(crate) struct RegularJoinProcessor {
    plan: proto::RegularJoin,
    join_type: proto::RegularJoinType,
    max_parallelism: u32,
    state: Box<dyn KeyedState>,
    visible_schemas: [SchemaRef; 2],
    condition_schema: SchemaRef,
    residual_condition: Option<Arc<dyn PhysicalExpr>>,
    output_schema: SchemaRef,
    fused_output_calcs: Option<FusedCalcPipeline>,
    row_converters: [RowConverter; 2],
    input_schemas: [Option<SchemaRef>; 2],
    key_fields: [Vec<(usize, KeyField)>; 2],
    preencoded_key_indices: [Option<usize>; 2],
    input_kind_indices: [Option<usize>; 2],
    scratch_reservation: HostMemoryReservation,
    exchange_frame_plans: [Option<(Vec<u8>, SchemaRef, bool)>; 2],
    exchange_frame_reservation: HostMemoryReservation,
    state_read_batches: u64,
    state_write_batches: u64,
    fused_calc_batches: u64,
    first_key_group: u32,
    last_key_group: u32,
    bounded_output_key_group: Option<u32>,
    bounded_output_entries: Vec<(Vec<u8>, Vec<u8>)>,
    bounded_output_entry_index: usize,
    bounded_cursor: Option<BoundedJoinCursor>,
    bounded_output_finished: bool,
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
        Self::with_state(
            serialized_plan,
            max_parallelism,
            first_key_group,
            last_key_group,
            state,
            scratch,
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
            max_parallelism,
            first_key_group,
            last_key_group,
            state,
            scratch,
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
        let native_plan = decode_plan(serialized_plan)?;
        let root = native_plan
            .root
            .ok_or_else(|| DataFusionError::Plan("regular join plan has no root".to_string()))?;
        let (plan, output_calcs) = split_regular_join_and_calc_tail(root)?;
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
        let mut condition_fields = Vec::new();
        for (side, schema) in visible_schemas.iter().enumerate() {
            for (index, field) in schema.fields().iter().enumerate() {
                condition_fields.push(Arc::new(Field::new(
                    format!("__streamfusion_join_condition_{side}_{index}"),
                    field.data_type().clone(),
                    field.is_nullable(),
                )));
            }
        }
        let condition_schema = Arc::new(Schema::new(condition_fields));
        let residual_condition = plan
            .residual_condition
            .as_ref()
            .map(|condition| calc::create_expression(condition, condition_schema.as_ref()))
            .transpose()?;
        if let Some(condition) = &residual_condition {
            let data_type = condition.data_type(condition_schema.as_ref())?;
            if data_type != DataType::Boolean {
                return Err(DataFusionError::Plan(format!(
                    "regular join residual condition must return BOOLEAN, got {data_type}"
                )));
            }
        }
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
        let output_schema = Arc::new(Schema::new(output_fields));
        let exchange_frame_reservation =
            scratch_reservation.sibling("native regular join exchange frame decode");
        let fused_output_calcs = if output_calcs.is_empty() {
            None
        } else {
            Some(FusedCalcPipeline::new(
                Arc::clone(&output_schema),
                output_calcs,
                scratch_reservation.sibling("native regular join fused Calc plan"),
            )?)
        };
        Ok(Self {
            plan,
            join_type,
            max_parallelism,
            state,
            visible_schemas,
            condition_schema,
            residual_condition,
            output_schema,
            fused_output_calcs,
            row_converters,
            input_schemas: [None, None],
            key_fields: [Vec::new(), Vec::new()],
            preencoded_key_indices: [None, None],
            input_kind_indices: [None, None],
            scratch_reservation,
            exchange_frame_plans: [None, None],
            exchange_frame_reservation,
            state_read_batches: 0,
            state_write_batches: 0,
            fused_calc_batches: 0,
            first_key_group,
            last_key_group,
            bounded_output_key_group: Some(first_key_group),
            bounded_output_entries: Vec::new(),
            bounded_output_entry_index: 0,
            bounded_cursor: None,
            bounded_output_finished: false,
        })
    }

    pub(crate) fn process_arrow(&mut self, side: usize, batch: RecordBatch) -> Result<RecordBatch> {
        if side > 1 {
            return Err(DataFusionError::Execution(
                "regular join side must be zero or one".to_string(),
            ));
        }
        if self.plan.bounded_final_output && self.bounded_output_finished {
            return Err(DataFusionError::Execution(
                "bounded regular join received input after terminal output".to_string(),
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
            Ok(encoded) if self.plan.bounded_final_output => {
                self.process_bounded_accounted(side, &batch, &encoded, None)
            }
            Ok(encoded) => self.process_accounted(side, &batch, &encoded, base),
            Err(error) => Err(error.into()),
        };
        match result {
            Ok(output) => {
                let output = if !self.plan.bounded_final_output {
                    if let Some(calcs) = &self.fused_output_calcs {
                        self.fused_calc_batches = self
                            .fused_calc_batches
                            .saturating_add(calcs.stage_count() as u64);
                        match calcs.execute(output) {
                            Ok(output) => output,
                            Err(error) => {
                                self.scratch_reservation.resize(0)?;
                                return Err(error);
                            }
                        }
                    } else {
                        output
                    }
                } else {
                    output
                };
                self.finish_output(output, base)
            }
            Err(error) => {
                self.scratch_reservation.resize(0)?;
                Err(error)
            }
        }
    }

    pub(crate) fn process_bounded_exchange_frame(
        &mut self,
        side: usize,
        key_group: u32,
        exchange_plan: &[u8],
        payload: Vec<u8>,
        metadata_length: usize,
    ) -> Result<usize> {
        if !self.plan.bounded_final_output {
            return Err(DataFusionError::Execution(
                "only a bounded regular join can consume an exchange frame directly".to_string(),
            ));
        }
        if side > 1 {
            return Err(DataFusionError::Execution(
                "regular join side must be zero or one".to_string(),
            ));
        }
        if key_group < self.first_key_group || key_group > self.last_key_group {
            return Err(DataFusionError::Execution(format!(
                "bounded regular join received key group {key_group} outside its owned range {}..={}",
                self.first_key_group, self.last_key_group
            )));
        }
        let (schema, preserve_key_groups) = match &self.exchange_frame_plans[side] {
            Some((expected, schema, preserve)) if expected.as_slice() == exchange_plan => {
                (Arc::clone(schema), *preserve)
            }
            Some(_) => {
                return Err(DataFusionError::Execution(format!(
                    "regular join exchange plan for side {side} changed while running"
                )));
            }
            None => {
                let plan = crate::exchange::decode_exchange_plan(exchange_plan)?;
                let mut schema = arrow_schema(plan.schema.as_ref().ok_or_else(|| {
                    DataFusionError::Plan("regular join exchange schema is required".to_string())
                })?)?;
                if plan.transport_routing_key {
                    let mut fields = schema
                        .fields()
                        .iter()
                        .map(|field| field.as_ref().clone())
                        .collect::<Vec<_>>();
                    fields.push(Field::new("__streamfusion_key", DataType::Binary, false));
                    schema = Arc::new(Schema::new(fields));
                }
                self.exchange_frame_plans[side] = Some((
                    exchange_plan.to_vec(),
                    Arc::clone(&schema),
                    plan.preserve_key_groups,
                ));
                (schema, plan.preserve_key_groups)
            }
        };
        let retained = self.exchange_frame_retained_bytes();
        self.exchange_frame_reservation.resize(
            retained
                .saturating_add(payload.capacity())
                .saturating_add(schema.fields().len().saturating_mul(1024)),
        )?;
        let result = (|| {
            let batch = crate::exchange::IpcBatchFrame::decode_contiguous(
                payload,
                metadata_length,
                schema,
            )?;
            let rows = batch.num_rows();
            self.process_bounded_arrow_without_output(
                side,
                preserve_key_groups.then_some(key_group),
                batch,
            )?;
            Ok(rows)
        })();
        self.exchange_frame_reservation.resize(retained)?;
        result
    }

    /// Ingests a bounded input batch without exporting the deliberately empty result through
    /// Arrow C Data. The ordinary `process_arrow` edge transfers that empty batch's allocations
    /// to Arrow Java; the direct exchange path has no Java Arrow consumer and must release them
    /// locally instead.
    fn process_bounded_arrow_without_output(
        &mut self,
        side: usize,
        key_group: Option<u32>,
        batch: RecordBatch,
    ) -> Result<()> {
        if self.bounded_output_finished {
            return Err(DataFusionError::Execution(
                "bounded regular join received input after terminal output".to_string(),
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
        let result = self.row_converters[side]
            .convert_columns(&batch.columns()[..visible_count])
            .map_err(DataFusionError::from)
            .and_then(|encoded| self.process_bounded_accounted(side, &batch, &encoded, key_group));
        match result {
            Ok(output) => {
                if output.num_rows() != 0 {
                    self.scratch_reservation.resize(0)?;
                    return Err(DataFusionError::Internal(
                        "bounded regular join emitted output while ingesting an exchange frame"
                            .to_string(),
                    ));
                }
                drop(output);
                self.scratch_reservation.resize(0)
            }
            Err(error) => {
                self.scratch_reservation.resize(0)?;
                Err(error)
            }
        }
    }

    fn exchange_frame_retained_bytes(&self) -> usize {
        self.exchange_frame_plans
            .iter()
            .flatten()
            .fold(0usize, |bytes, (plan, schema, _)| {
                bytes
                    .saturating_add(plan.capacity())
                    .saturating_add(schema.fields().len().saturating_mul(1024))
                    .saturating_add(4096)
            })
    }

    fn process_accounted(
        &mut self,
        side: usize,
        batch: &RecordBatch,
        encoded: &Rows,
        base: usize,
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
        // Every row in this call belongs to the same input side, so its opposite-side candidate
        // multiset is stable throughout the batch. Evaluate the generated residual once over all
        // candidate pairs instead of constructing and evaluating one tiny Arrow batch per input
        // row/key. State transitions below remain in Flink input order.
        let batch_candidate_matches =
            self.condition_matches_batch(side, batch, encoded, &staged, &row_state_indices, base)?;
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
            process_change(
                self.join_type,
                side,
                kind,
                accumulate,
                &batch_candidate_matches[row],
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

    fn process_bounded_accounted(
        &mut self,
        side: usize,
        batch: &RecordBatch,
        encoded: &Rows,
        key_group_override: Option<u32>,
    ) -> Result<RecordBatch> {
        let kinds = batch
            .column(self.input_kind_indices[side].expect("schema prepared"))
            .as_any()
            .downcast_ref::<Int8Array>()
            .ok_or_else(|| {
                DataFusionError::Execution("regular join RowKinds are not Int8".to_string())
            })?;
        let mut unique = HashMap::<StateKey, usize, RandomState>::with_capacity_and_hasher(
            batch.num_rows().max(1),
            RandomState::new(),
        );
        let mut row_state_indices = Vec::with_capacity(batch.num_rows());
        for row in 0..batch.num_rows() {
            let key = self.group_key(side, batch, row)?;
            let state_key = StateKey {
                key_group: key_group_override
                    .unwrap_or_else(|| assign_key_group(&key, self.max_parallelism)),
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
            .map(|key| key.expect("bounded regular join state index is populated"))
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
        for row in 0..batch.num_rows() {
            let matchable = self.row_is_matchable(side, batch, row);
            let state = &mut staged[row_state_indices[row]];
            let cached_matchable = if side == 0 {
                &mut state.value.left_matchable
            } else {
                &mut state.value.right_matchable
            };
            match cached_matchable {
                Some(cached) if *cached != matchable => {
                    return Err(DataFusionError::Internal(
                        "bounded regular join equality key changed its null-filter result"
                            .to_string(),
                    ));
                }
                None => *cached_matchable = Some(matchable),
                Some(_) => {}
            }
            let rows = if side == 0 {
                &mut state.value.left
            } else {
                &mut state.value.right
            };
            let row_bytes = encoded.row(row).data();
            match kinds.value(row) {
                INSERT | UPDATE_AFTER => rows.push(StoredRow {
                    row: row_bytes.to_vec(),
                    associations: 0,
                }),
                UPDATE_BEFORE | DELETE => {
                    if let Some(position) =
                        rows.iter().position(|candidate| candidate.row == row_bytes)
                    {
                        rows.remove(position);
                    }
                }
                other => {
                    return Err(DataFusionError::Execution(format!(
                        "unknown Flink RowKind byte {other}"
                    )))
                }
            }
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
        self.empty_output()
    }

    fn condition_matches_batch(
        &mut self,
        input_side: usize,
        batch: &RecordBatch,
        input_rows: &Rows,
        staged: &[StagedState],
        row_state_indices: &[usize],
        base: usize,
    ) -> Result<Vec<Vec<bool>>> {
        let mut result = Vec::with_capacity(batch.num_rows());
        let mut pair_locations = Vec::new();
        let mut encoded_pairs = [Vec::<&[u8]>::new(), Vec::<&[u8]>::new()];
        let mut condition_bytes = 0usize;
        for row in 0..batch.num_rows() {
            let candidates = if input_side == 0 {
                &staged[row_state_indices[row]].value.right
            } else {
                &staged[row_state_indices[row]].value.left
            };
            result.push(vec![false; candidates.len()]);
            if !self.row_is_matchable(input_side, batch, row) {
                continue;
            }
            if self.residual_condition.is_none() {
                result[row].fill(true);
                continue;
            }
            let input = input_rows.row(row).data();
            for (candidate_index, candidate) in candidates.iter().enumerate() {
                encoded_pairs[input_side].push(input);
                encoded_pairs[1 - input_side].push(&candidate.row);
                pair_locations.push((row, candidate_index));
                condition_bytes = condition_bytes
                    .saturating_add(input.len())
                    .saturating_add(candidate.row.len())
                    .saturating_add(128);
            }
        }
        let Some(condition) = &self.residual_condition else {
            return Ok(result);
        };
        if pair_locations.is_empty() {
            return Ok(result);
        }
        self.scratch_reservation
            .resize(base.saturating_add(condition_bytes))?;

        let mut columns = Vec::new();
        for side in 0..2 {
            let parser = self.row_converters[side].parser();
            let decoded = self.row_converters[side]
                .convert_rows(encoded_pairs[side].iter().map(|row| parser.parse(row)))?;
            columns.extend(decoded);
        }
        let pairs = RecordBatch::try_new(self.condition_schema.clone(), columns)?;
        let values = condition
            .evaluate(&pairs)?
            .into_array(pair_locations.len())?;
        let values = values
            .as_any()
            .downcast_ref::<BooleanArray>()
            .ok_or_else(|| {
                DataFusionError::Execution(
                    "regular join residual condition did not evaluate to BooleanArray".to_string(),
                )
            })?;
        for (index, (row, candidate)) in pair_locations.into_iter().enumerate() {
            result[row][candidate] = !values.is_null(index) && values.value(index);
        }
        Ok(result)
    }

    pub(crate) fn finish_bounded_output(&mut self) -> Result<RecordBatch> {
        if !self.plan.bounded_final_output {
            return Err(DataFusionError::Execution(
                "streaming regular join has no terminal output".to_string(),
            ));
        }
        loop {
            let raw = self.next_bounded_output_batch()?;
            if raw.num_rows() == 0 {
                return self.empty_output();
            }
            let output = if let Some(calcs) = &self.fused_output_calcs {
                self.fused_calc_batches = self
                    .fused_calc_batches
                    .saturating_add(calcs.stage_count() as u64);
                calcs.execute(raw)?
            } else {
                raw
            };
            // A filter in the fused tail may remove an entire raw chunk. That is not the native
            // end-of-input marker: keep advancing the join cursor until data survives or the
            // state is genuinely exhausted.
            if output.num_rows() == 0 && !self.bounded_output_finished {
                continue;
            }
            if self.bounded_output_finished {
                self.bounded_output_entries.clear();
                self.bounded_cursor = None;
            }
            let retained = self.bounded_retained_bytes();
            let output_bytes = output.get_array_memory_size();
            self.scratch_reservation
                .resize(retained.saturating_add(output_bytes))?;
            self.scratch_reservation.transfer_to_arrow(output_bytes)?;
            self.scratch_reservation.resize(retained)?;
            return Ok(output);
        }
    }

    fn next_bounded_output_batch(&mut self) -> Result<RecordBatch> {
        if self.bounded_output_finished {
            self.bounded_output_entries.clear();
            self.bounded_cursor = None;
            self.scratch_reservation.resize(0)?;
            return Ok(RecordBatch::new_empty(Arc::clone(&self.output_schema)));
        }
        let mut output = Vec::new();
        // A fused tail shares this operator's host-memory broker, so it can safely consume a
        // larger batch. A bare join retains the conservative edge size needed by arbitrary
        // downstream Flink operators.
        let max_rows = if self.fused_output_calcs.is_some() {
            BOUNDED_FUSED_OUTPUT_MAX_ROWS
        } else {
            BOUNDED_EDGE_OUTPUT_MAX_ROWS
        };
        // Account the Arrow array headers and selection vectors even when the encoded payloads
        // themselves are tiny. Payload bytes are added before every row clone below.
        let mut output_estimate = 4096usize;
        while output.len() < max_rows {
            if self.bounded_cursor.is_none()
                && !self.load_next_bounded_cursor(output_estimate.saturating_mul(2))?
            {
                self.bounded_output_finished = true;
                break;
            }
            let mut cursor = self
                .bounded_cursor
                .take()
                .expect("bounded regular join cursor was loaded");
            self.drain_bounded_cursor(&mut cursor, &mut output, &mut output_estimate, max_rows)?;
            if cursor.phase != BoundedOutputPhase::Done {
                self.bounded_cursor = Some(cursor);
                break;
            }
        }
        if output.is_empty() && self.bounded_output_finished {
            self.bounded_output_entries.clear();
            self.scratch_reservation.resize(0)?;
            return Ok(RecordBatch::new_empty(Arc::clone(&self.output_schema)));
        }
        let retained = self.bounded_retained_bytes();
        self.scratch_reservation.resize(
            retained
                .saturating_add(output_estimate)
                .saturating_add(output_estimate),
        )?;
        let batch = self.output_batch(output)?;
        let output_bytes = batch.get_array_memory_size();
        self.scratch_reservation
            .resize(retained.saturating_add(output_bytes))?;
        Ok(batch)
    }

    fn load_next_bounded_cursor(&mut self, pending_output_bytes: usize) -> Result<bool> {
        loop {
            if self.bounded_output_entry_index < self.bounded_output_entries.len() {
                let value = &self.bounded_output_entries[self.bounded_output_entry_index].1;
                self.bounded_output_entry_index += 1;
                self.scratch_reservation.resize(
                    self.bounded_retained_bytes()
                        .saturating_add(value.len().saturating_mul(2))
                        .saturating_add(pending_output_bytes),
                )?;
                let state = decode_state(value)?;
                let left_matchable =
                    self.bounded_matchable(0, &state.left, state.left_matchable)?;
                let right_matchable =
                    self.bounded_matchable(1, &state.right, state.right_matchable)?;
                self.bounded_cursor = Some(BoundedJoinCursor {
                    left_matched: vec![false; state.left.len()],
                    right_matched: vec![false; state.right.len()],
                    left_matchable,
                    right_matchable,
                    state,
                    pair_index: 0,
                    remainder_index: 0,
                    phase: BoundedOutputPhase::Pairs,
                });
                self.scratch_reservation.resize(
                    self.bounded_retained_bytes()
                        .saturating_add(pending_output_bytes),
                )?;
                return Ok(true);
            }
            self.bounded_output_entries.clear();
            self.bounded_output_entry_index = 0;
            let Some(key_group) = self.bounded_output_key_group else {
                return Ok(false);
            };
            let snapshot = self.state.snapshot_key_group(key_group)?;
            self.scratch_reservation.resize(
                self.bounded_retained_bytes()
                    .saturating_add(snapshot.len().saturating_mul(2))
                    .saturating_add(pending_output_bytes),
            )?;
            self.bounded_output_entries = decode_key_group_snapshot(key_group, &snapshot)?;
            self.bounded_output_key_group = if key_group < self.last_key_group {
                Some(key_group + 1)
            } else {
                None
            };
        }
    }

    fn bounded_matchable(
        &self,
        side: usize,
        rows: &[StoredRow],
        cached: Option<bool>,
    ) -> Result<Vec<bool>> {
        if rows.is_empty() || !self.plan.filter_nulls.iter().any(|filter| *filter) {
            return Ok(vec![true; rows.len()]);
        }
        if let Some(cached) = cached {
            return Ok(vec![cached; rows.len()]);
        }
        let parser = self.row_converters[side].parser();
        let columns = self.row_converters[side]
            .convert_rows(rows.iter().map(|row| parser.parse(&row.row)))?;
        Ok((0..rows.len())
            .map(|row| {
                self.plan
                    .filter_nulls
                    .iter()
                    .zip(self.key_indices(side))
                    .all(|(&filter, &index)| !filter || !columns[index as usize].is_null(row))
            })
            .collect())
    }

    fn drain_bounded_cursor(
        &mut self,
        cursor: &mut BoundedJoinCursor,
        output: &mut Vec<OutputRow>,
        output_estimate: &mut usize,
        max_rows: usize,
    ) -> Result<()> {
        while output.len() < max_rows && cursor.phase != BoundedOutputPhase::Done {
            match cursor.phase {
                BoundedOutputPhase::Pairs => {
                    let total = cursor
                        .state
                        .left
                        .len()
                        .checked_mul(cursor.state.right.len())
                        .ok_or_else(|| {
                            DataFusionError::ResourcesExhausted(
                                "bounded regular join pair count overflowed usize".to_string(),
                            )
                        })?;
                    if cursor.pair_index == total {
                        cursor.phase = BoundedOutputPhase::LeftRemainder;
                        cursor.remainder_index = 0;
                        continue;
                    }
                    let mut candidates = (max_rows - output.len())
                        .max(1)
                        .min(total - cursor.pair_index)
                        .min(max_rows);
                    let start = cursor.pair_index;
                    let matches = loop {
                        let end = start + candidates;
                        match self.bounded_pair_matches(
                            cursor,
                            start,
                            end,
                            output_estimate.saturating_mul(2),
                        ) {
                            Err(DataFusionError::ResourcesExhausted(_)) if candidates > 1 => {
                                candidates = (candidates / 2).max(1);
                            }
                            result => break result?,
                        }
                    };
                    for (offset, matched) in matches.into_iter().enumerate() {
                        if !matched {
                            cursor.pair_index = start + offset + 1;
                            continue;
                        }
                        let pair = start + offset;
                        let left = pair / cursor.state.right.len();
                        let right = pair % cursor.state.right.len();
                        if !matches!(
                            self.join_type,
                            proto::RegularJoinType::Semi | proto::RegularJoinType::Anti
                        ) {
                            let Some(next_estimate) = self.prepare_bounded_output_row(
                                cursor,
                                *output_estimate,
                                !output.is_empty(),
                                Some(&cursor.state.left[left]),
                                Some(&cursor.state.right[right]),
                            )?
                            else {
                                cursor.pair_index = pair;
                                return Ok(());
                            };
                            *output_estimate = next_estimate;
                            output.push(OutputRow {
                                left: Some(cursor.state.left[left].row.clone()),
                                right: Some(cursor.state.right[right].row.clone()),
                                kind: INSERT,
                                input_ordinal: bounded_ordinal(output.len())?,
                            });
                        }
                        cursor.left_matched[left] = true;
                        cursor.right_matched[right] = true;
                        cursor.pair_index = pair + 1;
                    }
                }
                BoundedOutputPhase::LeftRemainder => {
                    while cursor.remainder_index < cursor.state.left.len()
                        && output.len() < max_rows
                    {
                        let index = cursor.remainder_index;
                        let emit = match self.join_type {
                            proto::RegularJoinType::Semi => cursor.left_matched[index],
                            proto::RegularJoinType::Anti => !cursor.left_matched[index],
                            _ => is_outer(self.join_type, 0) && !cursor.left_matched[index],
                        };
                        if emit {
                            let Some(next_estimate) = self.prepare_bounded_output_row(
                                cursor,
                                *output_estimate,
                                !output.is_empty(),
                                Some(&cursor.state.left[index]),
                                None,
                            )?
                            else {
                                return Ok(());
                            };
                            *output_estimate = next_estimate;
                            output.push(OutputRow {
                                left: Some(cursor.state.left[index].row.clone()),
                                right: None,
                                kind: INSERT,
                                input_ordinal: bounded_ordinal(output.len())?,
                            });
                        }
                        cursor.remainder_index += 1;
                    }
                    if cursor.remainder_index == cursor.state.left.len() {
                        cursor.phase = BoundedOutputPhase::RightRemainder;
                        cursor.remainder_index = 0;
                    }
                }
                BoundedOutputPhase::RightRemainder => {
                    while cursor.remainder_index < cursor.state.right.len()
                        && output.len() < max_rows
                    {
                        let index = cursor.remainder_index;
                        if !matches!(
                            self.join_type,
                            proto::RegularJoinType::Semi | proto::RegularJoinType::Anti
                        ) && is_outer(self.join_type, 1)
                            && !cursor.right_matched[index]
                        {
                            let Some(next_estimate) = self.prepare_bounded_output_row(
                                cursor,
                                *output_estimate,
                                !output.is_empty(),
                                None,
                                Some(&cursor.state.right[index]),
                            )?
                            else {
                                return Ok(());
                            };
                            *output_estimate = next_estimate;
                            output.push(OutputRow {
                                left: None,
                                right: Some(cursor.state.right[index].row.clone()),
                                kind: INSERT,
                                input_ordinal: bounded_ordinal(output.len())?,
                            });
                        }
                        cursor.remainder_index += 1;
                    }
                    if cursor.remainder_index == cursor.state.right.len() {
                        cursor.phase = BoundedOutputPhase::Done;
                    }
                }
                BoundedOutputPhase::Done => break,
            }
        }
        Ok(())
    }

    fn prepare_bounded_output_row(
        &mut self,
        cursor: &BoundedJoinCursor,
        output_estimate: usize,
        has_output: bool,
        left: Option<&StoredRow>,
        right: Option<&StoredRow>,
    ) -> Result<Option<usize>> {
        let next_estimate = output_estimate
            .saturating_add(left.map_or(0, |row| row.row.len()))
            .saturating_add(right.map_or(0, |row| row.row.len()))
            .saturating_add(64);
        let required = self
            .bounded_retained_bytes()
            .saturating_add(cursor.estimated_dynamic_bytes())
            .saturating_add(next_estimate.saturating_mul(2));
        if !self.ensure_bounded_scratch_capacity(required, has_output)? {
            return Ok(None);
        }
        Ok(Some(next_estimate))
    }

    fn ensure_bounded_scratch_capacity(
        &mut self,
        required: usize,
        can_split: bool,
    ) -> Result<bool> {
        let current = self.scratch_reservation.size();
        if required <= current {
            return Ok(true);
        }
        // Host admission is a JNI call in production. Grow geometrically so ordinary rows reuse
        // native-side capacity; if the speculative headroom is denied, retry the exact size so a
        // batch is never split merely because the growth quantum was optimistic.
        let target = required.max(current.max(64 * 1024).saturating_mul(2));
        match self.scratch_reservation.resize(target) {
            Ok(()) => Ok(true),
            Err(DataFusionError::ResourcesExhausted(_)) if target != required => {
                match self.scratch_reservation.resize(required) {
                    Ok(()) => Ok(true),
                    Err(DataFusionError::ResourcesExhausted(_)) if can_split => Ok(false),
                    Err(error) => Err(error),
                }
            }
            Err(DataFusionError::ResourcesExhausted(_)) if can_split => Ok(false),
            Err(error) => Err(error),
        }
    }

    fn bounded_pair_matches(
        &mut self,
        cursor: &BoundedJoinCursor,
        start: usize,
        end: usize,
        pending_output_bytes: usize,
    ) -> Result<Vec<bool>> {
        let right_count = cursor.state.right.len();
        let mut result = Vec::with_capacity(end - start);
        let mut pair_rows = [Vec::<&[u8]>::new(), Vec::<&[u8]>::new()];
        let mut locations = Vec::new();
        for pair in start..end {
            let left = pair / right_count;
            let right = pair % right_count;
            let matchable = cursor.left_matchable[left] && cursor.right_matchable[right];
            result.push(matchable && self.residual_condition.is_none());
            if matchable && self.residual_condition.is_some() {
                pair_rows[0].push(&cursor.state.left[left].row);
                pair_rows[1].push(&cursor.state.right[right].row);
                locations.push(pair - start);
            }
        }
        let Some(condition) = &self.residual_condition else {
            return Ok(result);
        };
        if locations.is_empty() {
            return Ok(result);
        }
        let condition_bytes = pair_rows
            .iter()
            .flatten()
            .map(|row| row.len().saturating_add(64))
            .sum::<usize>();
        self.scratch_reservation.resize(
            self.bounded_retained_bytes()
                .saturating_add(cursor.estimated_dynamic_bytes())
                .saturating_add(pending_output_bytes)
                .saturating_add(condition_bytes.saturating_mul(2)),
        )?;
        let mut columns = Vec::new();
        for side in 0..2 {
            let parser = self.row_converters[side].parser();
            columns.extend(
                self.row_converters[side]
                    .convert_rows(pair_rows[side].iter().map(|row| parser.parse(row)))?,
            );
        }
        let pairs = RecordBatch::try_new(self.condition_schema.clone(), columns)?;
        let values = condition.evaluate(&pairs)?.into_array(locations.len())?;
        let values = values
            .as_any()
            .downcast_ref::<BooleanArray>()
            .ok_or_else(|| {
                DataFusionError::Execution(
                    "regular join residual condition did not evaluate to BooleanArray".to_string(),
                )
            })?;
        for (condition_row, output_row) in locations.into_iter().enumerate() {
            result[output_row] = !values.is_null(condition_row) && values.value(condition_row);
        }
        Ok(result)
    }

    fn bounded_retained_bytes(&self) -> usize {
        let entries = self
            .bounded_output_entries
            .capacity()
            .saturating_mul(std::mem::size_of::<(Vec<u8>, Vec<u8>)>())
            .saturating_add(self.bounded_output_entries.iter().fold(
                0usize,
                |bytes, (key, value)| {
                    bytes
                        .saturating_add(key.capacity())
                        .saturating_add(value.capacity())
                },
            ));
        entries.saturating_add(
            self.bounded_cursor
                .as_ref()
                .map_or(0, BoundedJoinCursor::estimated_dynamic_bytes),
        )
    }

    pub(crate) fn statistics(&self) -> [u64; 3] {
        [
            self.state_read_batches,
            self.state_write_batches,
            self.fused_calc_batches,
        ]
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
            if selections[side]
                .iter()
                .enumerate()
                .all(|(index, selected)| *selected == Some(index as u32))
            {
                columns.extend(decoded);
            } else {
                let selection = UInt32Array::from(selections[side].clone());
                for column in decoded {
                    columns.push(take(column.as_ref(), &selection, None)?);
                }
            }
        }
        columns.push(Arc::new(Int8Array::from(kinds)));
        columns.push(Arc::new(Int32Array::from(ordinals)));
        Ok(RecordBatch::try_new(self.output_schema.clone(), columns)?)
    }

    fn empty_output(&self) -> Result<RecordBatch> {
        Ok(self.fused_output_calcs.as_ref().map_or_else(
            || RecordBatch::new_empty(self.output_schema.clone()),
            FusedCalcPipeline::empty_output,
        ))
    }

    fn finish_output(&mut self, output: RecordBatch, base: usize) -> Result<RecordBatch> {
        let output_bytes = output.get_array_memory_size();
        self.scratch_reservation.resize(output_bytes.max(base))?;
        self.scratch_reservation.transfer_to_arrow(output_bytes)?;
        self.scratch_reservation.resize(0)?;
        Ok(output)
    }
}

fn split_regular_join_and_calc_tail(
    mut root: proto::Operator,
) -> Result<(proto::RegularJoin, Vec<proto::Calc>)> {
    let mut outer_to_inner = Vec::new();
    loop {
        match root.operator.take() {
            Some(proto::operator::Operator::Calc(mut calc)) => {
                let input = calc.input.take().ok_or_else(|| {
                    DataFusionError::Plan("fused regular-join Calc has no input".to_string())
                })?;
                outer_to_inner.push(*calc);
                root = *input;
            }
            Some(proto::operator::Operator::RegularJoin(plan)) => {
                outer_to_inner.reverse();
                return Ok((plan, outer_to_inner));
            }
            _ => {
                return Err(DataFusionError::Plan(
                    "regular join handle requires a RegularJoin root with only an optional Calc tail"
                        .to_string(),
                ));
            }
        }
    }
}

#[allow(clippy::too_many_arguments)]
fn process_change(
    join_type: proto::RegularJoinType,
    side: usize,
    input_kind: i8,
    accumulate: bool,
    candidate_matches: &[bool],
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
            join_type,
            side,
            input_kind,
            accumulate,
            candidate_matches,
            input,
            ordinal,
            state,
            output,
        );
    }
    let input_outer = is_outer(join_type, side);
    let other_outer = is_outer(join_type, 1 - side);
    let (input_rows, other) = if side == 0 {
        (&mut state.left, &mut state.right)
    } else {
        (&mut state.right, &mut state.left)
    };
    debug_assert_eq!(candidate_matches.len(), other.len());
    let matches = candidate_matches.iter().filter(|&&matched| matched).count();
    if accumulate {
        if matches == 0 {
            if input_outer {
                push_pair(output, side, Some(input.clone()), None, INSERT, ordinal);
            }
        } else {
            for (candidate, _) in other
                .iter_mut()
                .zip(candidate_matches)
                .filter(|(_, matched)| **matched)
            {
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
            for (candidate, _) in other
                .iter_mut()
                .zip(candidate_matches)
                .filter(|(_, matched)| **matched)
            {
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
    candidate_matches: &[bool],
    input: Vec<u8>,
    ordinal: i32,
    state: &mut JoinState,
    output: &mut Vec<OutputRow>,
) -> Result<()> {
    let anti = matches!(join_type, proto::RegularJoinType::Anti);
    if side == 0 {
        debug_assert_eq!(candidate_matches.len(), state.right.len());
        let matches = candidate_matches.iter().filter(|&&matched| matched).count();
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
        for (left, _) in state
            .left
            .iter_mut()
            .zip(candidate_matches)
            .filter(|(_, matched)| **matched)
        {
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
    } else {
        if let Some(position) = state
            .right
            .iter()
            .position(|candidate| candidate.row == input)
        {
            state.right.remove(position);
        }
        for (left, _) in state
            .left
            .iter_mut()
            .zip(candidate_matches)
            .filter(|(_, matched)| **matched)
        {
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

fn bounded_ordinal(rows_already_emitted: usize) -> Result<i32> {
    i32::try_from(rows_already_emitted).map_err(|_| {
        DataFusionError::Execution("bounded regular join output ordinal exceeds i32".to_string())
    })
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
    bytes.push(encode_matchable(state.left_matchable));
    bytes.push(encode_matchable(state.right_matchable));
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
    if bytes.len() < 13
        || &bytes[..4] != STATE_MAGIC
        || !matches!(bytes[4], LEGACY_STATE_VERSION | STATE_VERSION)
    {
        return Err(DataFusionError::Execution(
            "invalid native regular join state".to_string(),
        ));
    }
    let version = bytes[4];
    let (left_matchable, right_matchable, mut offset) = if version == STATE_VERSION {
        if bytes.len() < 15 {
            return Err(truncated());
        }
        (decode_matchable(bytes[5])?, decode_matchable(bytes[6])?, 7)
    } else {
        (None, None, 5)
    };
    let left = decode_rows(bytes, &mut offset)?;
    let right = decode_rows(bytes, &mut offset)?;
    if offset != bytes.len() {
        return Err(DataFusionError::Execution(
            "regular join state has trailing bytes".to_string(),
        ));
    }
    Ok(JoinState {
        left,
        right,
        left_matchable,
        right_matchable,
    })
}

fn encode_matchable(value: Option<bool>) -> u8 {
    match value {
        None => 0,
        Some(false) => 1,
        Some(true) => 2,
    }
}

fn decode_matchable(value: u8) -> Result<Option<bool>> {
    match value {
        0 => Ok(None),
        1 => Ok(Some(false)),
        2 => Ok(Some(true)),
        other => Err(DataFusionError::Execution(format!(
            "invalid bounded regular join matchability byte {other}"
        ))),
    }
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
    use arrow::array::{ArrayRef, Int64Array, Int8Array, ListArray, StringArray};
    use arrow::datatypes::Int32Type;
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
    fn residual_condition_controls_outer_association_transitions() {
        let broker = Arc::new(TestBroker::new(64 << 20));
        let mut processor = RegularJoinProcessor::new(
            &plan_contract(
                proto::RegularJoinType::Left,
                true,
                Some(not_equal_value_condition()),
            ),
            128,
            0,
            127,
            HostMemoryReservation::new(broker.clone(), "regular join residual condition"),
        )
        .unwrap();

        let left = processor
            .process_arrow(0, batch(&[1], &["same"], &[INSERT]))
            .unwrap();
        assert_eq!(kinds(&left), vec![INSERT]);
        let rejected = processor
            .process_arrow(1, batch(&[1], &["same"], &[INSERT]))
            .unwrap();
        assert_eq!(rejected.num_rows(), 0);
        let accepted = processor
            .process_arrow(1, batch(&[1], &["different"], &[INSERT]))
            .unwrap();
        assert_eq!(kinds(&accepted), vec![DELETE, INSERT]);
        let rejected_retract = processor
            .process_arrow(1, batch(&[1], &["same"], &[DELETE]))
            .unwrap();
        assert_eq!(rejected_retract.num_rows(), 0);
        let accepted_retract = processor
            .process_arrow(1, batch(&[1], &["different"], &[DELETE]))
            .unwrap();
        assert_eq!(kinds(&accepted_retract), vec![DELETE, INSERT]);

        drop(left);
        drop(rejected);
        drop(accepted);
        drop(rejected_retract);
        drop(accepted_retract);
        drop(processor);
        assert_eq!(broker.reserved(), 0);
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
    fn residual_join_state_restores_after_rescaling() {
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
    fn residual_join_state_moves_from_memory_to_rocksdb_and_batches_io() {
        let Ok(plugin_path) = std::env::var("STREAMFUSION_TEST_ROCKSDB_PLUGIN") else {
            return;
        };
        let broker = Arc::new(TestBroker::new(1 << 30));
        let mut memory = RegularJoinProcessor::new(
            &plan_contract(
                proto::RegularJoinType::Full,
                true,
                Some(not_equal_value_condition()),
            ),
            128,
            0,
            127,
            HostMemoryReservation::new(broker.clone(), "regular join memory source"),
        )
        .unwrap();
        memory
            .process_arrow(0, batch(&[1, 2], &["a", "b"], &[INSERT, INSERT]))
            .unwrap();
        assert_eq!(memory.statistics(), [1, 1, 0]);
        let snapshots = (0..128)
            .map(|group| memory.snapshot_key_group(group).unwrap())
            .collect::<Vec<_>>();

        let directory = tempfile::tempdir().unwrap();
        let mut rocks = RegularJoinProcessor::new_rocksdb(
            &plan_contract(
                proto::RegularJoinType::Full,
                true,
                Some(not_equal_value_condition()),
            ),
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
            .process_arrow(1, batch(&[1, 2], &["a", "r2"], &[INSERT, INSERT]))
            .unwrap();
        assert_eq!(kinds(&output), vec![INSERT, DELETE, INSERT]);
        assert_eq!(rocks.statistics(), [1, 1, 0]);
    }

    #[test]
    fn bounded_join_emits_final_insert_only_results_for_every_join_type() {
        for (join_type, expected_rows) in [
            (proto::RegularJoinType::Inner, 1),
            (proto::RegularJoinType::Left, 2),
            (proto::RegularJoinType::Right, 2),
            (proto::RegularJoinType::Full, 3),
            (proto::RegularJoinType::Semi, 1),
            (proto::RegularJoinType::Anti, 1),
        ] {
            let broker = Arc::new(TestBroker::new(64 << 20));
            let mut processor = RegularJoinProcessor::new(
                &bounded_plan(join_type, true, None),
                128,
                0,
                127,
                HostMemoryReservation::new(broker.clone(), "bounded regular join modes"),
            )
            .unwrap();
            assert_eq!(
                processor
                    .process_arrow(0, batch(&[1, 2], &["l1", "l2"], &[INSERT, INSERT]))
                    .unwrap()
                    .num_rows(),
                0
            );
            assert_eq!(
                processor
                    .process_arrow(1, batch(&[1, 3], &["r1", "r3"], &[INSERT, INSERT]))
                    .unwrap()
                    .num_rows(),
                0
            );
            let output = processor.finish_bounded_output().unwrap();
            assert_eq!(output.num_rows(), expected_rows, "{join_type:?}");
            assert_eq!(kinds(&output), vec![INSERT; expected_rows], "{join_type:?}");
            assert_eq!(processor.finish_bounded_output().unwrap().num_rows(), 0);
            drop(output);
            drop(processor);
            assert_eq!(broker.reserved(), 0);
        }
    }

    #[test]
    fn bounded_join_fuses_the_adjacent_calc_tail_in_one_native_plan() {
        let broker = Arc::new(TestBroker::new(64 << 20));
        let mut processor = RegularJoinProcessor::new(
            &bounded_plan_with_identity_calc(proto::RegularJoinType::Inner),
            128,
            0,
            127,
            HostMemoryReservation::new(broker.clone(), "bounded fused Calc join"),
        )
        .unwrap();
        processor
            .process_arrow(0, batch(&[1], &["left"], &[INSERT]))
            .unwrap();
        processor
            .process_arrow(1, batch(&[1], &["right"], &[INSERT]))
            .unwrap();

        let output = processor.finish_bounded_output().unwrap();
        assert_eq!(output.num_rows(), 1);
        assert_eq!(output.num_columns(), 6);
        assert_eq!(kinds(&output), vec![INSERT]);
        assert_eq!(processor.statistics()[2], 1);
        drop(output);
        drop(processor);
        assert_eq!(broker.reserved(), 0);
    }

    #[test]
    fn streaming_join_fuses_the_adjacent_calc_tail_in_one_native_plan() {
        let broker = Arc::new(TestBroker::new(64 << 20));
        let mut processor = RegularJoinProcessor::new(
            &streaming_plan_with_identity_calc(proto::RegularJoinType::Inner),
            128,
            0,
            127,
            HostMemoryReservation::new(broker.clone(), "streaming fused Calc join"),
        )
        .unwrap();
        assert_eq!(
            processor
                .process_arrow(0, batch(&[1], &["left"], &[INSERT]))
                .unwrap()
                .num_rows(),
            0
        );
        let output = processor
            .process_arrow(1, batch(&[1], &["right"], &[INSERT]))
            .unwrap();
        assert_eq!(output.num_rows(), 1);
        assert_eq!(output.num_columns(), 6);
        assert_eq!(kinds(&output), vec![INSERT]);
        assert_eq!(processor.statistics()[2], 2);
        drop(output);
        drop(processor);
        assert_eq!(broker.reserved(), 0);
    }

    #[test]
    fn bounded_join_ingests_exchange_frames_without_exporting_empty_arrow_batches() {
        let broker = Arc::new(TestBroker::new(64 << 20));
        let mut processor = RegularJoinProcessor::new(
            &bounded_plan(proto::RegularJoinType::Inner, true, None),
            128,
            0,
            127,
            HostMemoryReservation::new(broker.clone(), "bounded direct exchange join"),
        )
        .unwrap();
        let exchange_plan = exchange_plan();
        for (side, value) in [(0, "left"), (1, "right")] {
            let frame = crate::exchange::IpcBatchFrame::encode(&exchange_batch(1, value)).unwrap();
            let metadata_length = frame.metadata.len();
            let mut payload = frame.metadata;
            payload.extend_from_slice(&frame.body);
            let key_group = assign_key_group(
                &encode_binary_row(&exchange_batch(1, value), 0, &[(0, KeyField::BigInt)]).unwrap(),
                128,
            );
            assert_eq!(
                processor
                    .process_bounded_exchange_frame(
                        side,
                        key_group,
                        &exchange_plan,
                        payload,
                        metadata_length,
                    )
                    .unwrap(),
                1
            );
        }

        let output = processor.finish_bounded_output().unwrap();
        assert_eq!(output.num_rows(), 1);
        assert_eq!(kinds(&output), vec![INSERT]);
        drop(output);
        drop(processor);
        assert_eq!(broker.reserved(), 0);
    }

    #[test]
    fn bounded_join_direct_exchange_transports_opaque_complex_keys() {
        let broker = Arc::new(TestBroker::new(64 << 20));
        let mut processor = RegularJoinProcessor::new(
            &bounded_array_key_plan(),
            128,
            0,
            127,
            HostMemoryReservation::new(broker.clone(), "bounded complex direct exchange join"),
        )
        .unwrap();
        let exchange_plan = array_key_exchange_plan();
        for (side, keys, values) in [
            (0, vec![vec![1, 2]], vec!["left"]),
            (1, vec![vec![9], vec![1, 2]], vec!["miss", "right"]),
        ] {
            let frame =
                crate::exchange::IpcBatchFrame::encode(&array_key_exchange_batch(&keys, &values))
                    .unwrap();
            let metadata_length = frame.metadata.len();
            let mut payload = frame.metadata;
            payload.extend_from_slice(&frame.body);
            assert_eq!(
                processor
                    .process_bounded_exchange_frame(
                        side,
                        37,
                        &exchange_plan,
                        payload,
                        metadata_length,
                    )
                    .unwrap(),
                keys.len()
            );
        }

        let output = processor.finish_bounded_output().unwrap();
        assert_eq!(output.num_rows(), 1);
        assert_eq!(kinds(&output), vec![INSERT]);
        drop(output);
        drop(processor);
        assert_eq!(broker.reserved(), 0);
    }

    #[test]
    fn bounded_join_applies_retractions_null_filters_and_residual_conditions() {
        let broker = Arc::new(TestBroker::new(64 << 20));
        let mut processor = RegularJoinProcessor::new(
            &bounded_plan(
                proto::RegularJoinType::Full,
                true,
                Some(not_equal_value_condition()),
            ),
            128,
            0,
            127,
            HostMemoryReservation::new(broker, "bounded join contract"),
        )
        .unwrap();
        processor
            .process_arrow(
                0,
                nullable_batch(
                    &[Some(1), Some(1), None],
                    &["removed", "kept", "null-left"],
                    &[INSERT, INSERT, INSERT],
                ),
            )
            .unwrap();
        processor
            .process_arrow(
                0,
                nullable_batch(
                    &[Some(1), None],
                    &["removed", "absent-null"],
                    &[DELETE, DELETE],
                ),
            )
            .unwrap();
        processor
            .process_arrow(
                1,
                nullable_batch(
                    &[Some(1), None],
                    &["right", "null-right"],
                    &[INSERT, INSERT],
                ),
            )
            .unwrap();
        let output = processor.finish_bounded_output().unwrap();
        // kept/right passes the residual predicate; null keys remain two unmatched full-join rows.
        assert_eq!(output.num_rows(), 3);
        assert_eq!(kinds(&output), vec![INSERT; 3]);
    }

    #[test]
    fn state_codec_restores_version_one_rows_without_cached_matchability() {
        let state = JoinState {
            left: vec![StoredRow {
                row: vec![1, 2, 3],
                associations: 4,
            }],
            right: vec![StoredRow {
                row: vec![5, 6],
                associations: -2,
            }],
            left_matchable: Some(true),
            right_matchable: Some(false),
        };
        let mut legacy = encode_state(&state);
        legacy[4] = LEGACY_STATE_VERSION;
        legacy.drain(5..7);

        let restored = decode_state(&legacy).unwrap();
        assert_eq!(restored.left, state.left);
        assert_eq!(restored.right, state.right);
        assert_eq!(restored.left_matchable, None);
        assert_eq!(restored.right_matchable, None);
    }

    #[test]
    fn bounded_join_chunks_hot_key_output_and_retains_accounted_cursor_memory() {
        let broker = Arc::new(TestBroker::new(256 << 20));
        let mut processor = RegularJoinProcessor::new(
            &bounded_plan(proto::RegularJoinType::Inner, true, None),
            128,
            0,
            127,
            HostMemoryReservation::new(broker.clone(), "bounded join terminal chunks"),
        )
        .unwrap();
        let left_keys = vec![7; 200];
        let left_values = vec!["left"; 200];
        let left_kinds = vec![INSERT; 200];
        let right_keys = vec![7; 100];
        let right_values = vec!["right"; 100];
        let right_kinds = vec![INSERT; 100];
        processor
            .process_arrow(0, batch(&left_keys, &left_values, &left_kinds))
            .unwrap();
        processor
            .process_arrow(1, batch(&right_keys, &right_values, &right_kinds))
            .unwrap();

        let first = processor.finish_bounded_output().unwrap();
        assert_eq!(first.num_rows(), BOUNDED_EDGE_OUTPUT_MAX_ROWS);
        assert!(processor.scratch_reservation.size() > 0);
        let mut rows = first.num_rows();
        loop {
            let next = processor.finish_bounded_output().unwrap();
            assert!(next.num_rows() <= BOUNDED_EDGE_OUTPUT_MAX_ROWS);
            if next.num_rows() == 0 {
                break;
            }
            rows += next.num_rows();
        }
        assert_eq!(rows, 20_000);
        assert_eq!(processor.scratch_reservation.size(), 0);
        drop(first);
        drop(processor);
        assert_eq!(broker.reserved(), 0);
    }

    #[test]
    fn bounded_join_splits_wide_terminal_output_to_fit_the_host_budget() {
        let broker = Arc::new(TestBroker::new(8 << 20));
        let mut processor = RegularJoinProcessor::new(
            &bounded_plan(proto::RegularJoinType::Inner, true, None),
            128,
            0,
            127,
            HostMemoryReservation::new(broker.clone(), "bounded wide output budget"),
        )
        .unwrap();
        let wide = "x".repeat(1024);
        let keys = vec![7; 128];
        let values = vec![wide.as_str(); 128];
        let kinds = vec![INSERT; 128];
        processor
            .process_arrow(0, batch(&keys, &values, &kinds))
            .unwrap();
        processor
            .process_arrow(1, batch(&keys, &values, &kinds))
            .unwrap();

        let mut rows = 0;
        let mut batches = 0;
        loop {
            let output = processor.finish_bounded_output().unwrap();
            if output.num_rows() == 0 {
                break;
            }
            rows += output.num_rows();
            batches += 1;
        }
        assert_eq!(rows, 128 * 128);
        assert!(
            batches > 4,
            "the wide output must be byte-bounded, not only row-bounded"
        );
        drop(processor);
        assert_eq!(broker.reserved(), 0);
    }

    #[test]
    fn bounded_join_restores_key_groups_after_rescaling() {
        let broker = Arc::new(TestBroker::new(1 << 30));
        let mut source = RegularJoinProcessor::new(
            &bounded_plan(proto::RegularJoinType::Inner, true, None),
            128,
            0,
            127,
            HostMemoryReservation::new(broker.clone(), "bounded join rescale source"),
        )
        .unwrap();
        source
            .process_arrow(0, batch(&[1, 2], &["left-1", "left-2"], &[INSERT, INSERT]))
            .unwrap();
        let snapshots = (0..128)
            .map(|group| source.snapshot_key_group(group).unwrap())
            .collect::<Vec<_>>();
        let mut low = RegularJoinProcessor::new(
            &bounded_plan(proto::RegularJoinType::Inner, true, None),
            128,
            0,
            63,
            HostMemoryReservation::new(broker.clone(), "bounded join rescale low"),
        )
        .unwrap();
        let mut high = RegularJoinProcessor::new(
            &bounded_plan(proto::RegularJoinType::Inner, true, None),
            128,
            64,
            127,
            HostMemoryReservation::new(broker, "bounded join rescale high"),
        )
        .unwrap();
        for (group, snapshot) in snapshots.iter().enumerate() {
            if group < 64 {
                low.restore_key_group(group as u32, snapshot).unwrap();
            } else {
                high.restore_key_group(group as u32, snapshot).unwrap();
            }
        }
        for key in [1, 2] {
            let input = batch(&[key], &["right"], &[INSERT]);
            let encoded = source.group_key(1, &input, 0).unwrap();
            if assign_key_group(&encoded, 128) < 64 {
                low.process_arrow(1, input).unwrap();
            } else {
                high.process_arrow(1, input).unwrap();
            }
        }
        assert_eq!(
            low.finish_bounded_output().unwrap().num_rows()
                + high.finish_bounded_output().unwrap().num_rows(),
            2
        );
    }

    fn processor(broker: Arc<TestBroker>, first: u32, last: u32) -> RegularJoinProcessor {
        RegularJoinProcessor::new(
            &plan_contract(
                proto::RegularJoinType::Left,
                true,
                Some(not_equal_value_condition()),
            ),
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
        plan_contract(join_type, filter_nulls, None)
    }

    fn plan_contract(
        join_type: proto::RegularJoinType,
        filter_nulls: bool,
        residual_condition: Option<proto::Expression>,
    ) -> Vec<u8> {
        proto::NativePlan {
            protocol_version: crate::PLAN_PROTOCOL_VERSION,
            root: Some(proto::Operator {
                plan_node_id: 0,
                operator: Some(proto::operator::Operator::RegularJoin(proto::RegularJoin {
                    left_key_indices: vec![0],
                    right_key_indices: vec![0],
                    filter_nulls: vec![filter_nulls],
                    left_schema: Some(schema()),
                    right_schema: Some(schema()),
                    join_type: join_type as i32,
                    left_state_ttl_millis: 0,
                    right_state_ttl_millis: 0,
                    residual_condition,
                    bounded_final_output: false,
                })),
            }),
        }
        .encode_to_vec()
    }

    fn bounded_plan(
        join_type: proto::RegularJoinType,
        filter_nulls: bool,
        residual_condition: Option<proto::Expression>,
    ) -> Vec<u8> {
        let mut native = proto::NativePlan::decode(
            plan_contract(join_type, filter_nulls, residual_condition).as_slice(),
        )
        .unwrap();
        let Some(proto::operator::Operator::RegularJoin(join)) =
            native.root.as_mut().unwrap().operator.as_mut()
        else {
            unreachable!()
        };
        join.bounded_final_output = true;
        native.encode_to_vec()
    }

    fn bounded_plan_with_identity_calc(join_type: proto::RegularJoinType) -> Vec<u8> {
        let mut native =
            proto::NativePlan::decode(bounded_plan(join_type, true, None).as_slice()).unwrap();
        let join = native.root.take().unwrap();
        native.root = Some(proto::Operator {
            plan_node_id: 2,
            operator: Some(proto::operator::Operator::Calc(Box::new(proto::Calc {
                input: Some(Box::new(join)),
                projections: (0..6).map(input_reference).collect(),
                condition: None,
            }))),
        });
        native.encode_to_vec()
    }

    fn streaming_plan_with_identity_calc(join_type: proto::RegularJoinType) -> Vec<u8> {
        let mut native = proto::NativePlan::decode(plan(join_type).as_slice()).unwrap();
        let join = native.root.take().unwrap();
        native.root = Some(proto::Operator {
            plan_node_id: 2,
            operator: Some(proto::operator::Operator::Calc(Box::new(proto::Calc {
                input: Some(Box::new(join)),
                projections: (0..6).map(input_reference).collect(),
                condition: None,
            }))),
        });
        native.encode_to_vec()
    }

    fn exchange_plan() -> Vec<u8> {
        let mut transport_schema = schema();
        transport_schema.fields.push(proto::Field {
            name: "__streamfusion_row_kind".to_string(),
            r#type: Some(proto::LogicalType {
                nullable: false,
                r#type: Some(proto::logical_type::Type::Tinyint(
                    proto::EmptyType::default(),
                )),
            }),
        });
        proto::NativeExchangePlan {
            protocol_version: crate::PLAN_PROTOCOL_VERSION,
            schema: Some(transport_schema),
            distribution: proto::ExchangeDistribution::Hash as i32,
            key_indices: vec![0],
            max_parallelism: 128,
            transport: proto::ExchangeTransport::ArrowIpcStream as i32,
            metadata_columns: Some(proto::ExchangeMetadataColumns {
                row_kind_index: 2,
                stream_record_timestamp_index: None,
                routing_key_index: None,
            }),
            parallelism: 1,
            preserve_key_groups: true,
            transport_routing_key: false,
        }
        .encode_to_vec()
    }

    fn bounded_array_key_plan() -> Vec<u8> {
        let mut native = proto::NativePlan::decode(
            bounded_plan(proto::RegularJoinType::Inner, true, None).as_slice(),
        )
        .unwrap();
        let Some(proto::operator::Operator::RegularJoin(join)) =
            native.root.as_mut().unwrap().operator.as_mut()
        else {
            unreachable!()
        };
        join.left_schema = Some(array_key_schema());
        join.right_schema = Some(array_key_schema());
        native.encode_to_vec()
    }

    fn array_key_exchange_plan() -> Vec<u8> {
        let mut transport_schema = array_key_schema();
        transport_schema.fields.push(proto::Field {
            name: "__streamfusion_row_kind".to_string(),
            r#type: Some(proto::LogicalType {
                nullable: false,
                r#type: Some(proto::logical_type::Type::Tinyint(
                    proto::EmptyType::default(),
                )),
            }),
        });
        let routing_key_index = transport_schema.fields.len() as u32;
        proto::NativeExchangePlan {
            protocol_version: crate::PLAN_PROTOCOL_VERSION,
            schema: Some(transport_schema),
            distribution: proto::ExchangeDistribution::Hash as i32,
            key_indices: vec![0],
            max_parallelism: 128,
            transport: proto::ExchangeTransport::ArrowIpcStream as i32,
            metadata_columns: Some(proto::ExchangeMetadataColumns {
                row_kind_index: 2,
                stream_record_timestamp_index: None,
                routing_key_index: Some(routing_key_index),
            }),
            parallelism: 1,
            preserve_key_groups: false,
            transport_routing_key: true,
        }
        .encode_to_vec()
    }

    fn array_key_schema() -> proto::Schema {
        proto::Schema {
            fields: vec![
                field(
                    "key",
                    proto::logical_type::Type::Array(Box::new(proto::CollectionType {
                        element_type: Some(Box::new(proto::LogicalType {
                            nullable: true,
                            r#type: Some(proto::logical_type::Type::Integer(
                                proto::EmptyType::default(),
                            )),
                        })),
                    })),
                ),
                field(
                    "value",
                    proto::logical_type::Type::Varchar(proto::EmptyType::default()),
                ),
            ],
        }
    }

    fn input_reference(index: u32) -> proto::Expression {
        proto::Expression {
            expression: Some(proto::expression::Expression::InputReference(
                proto::InputReference {
                    index,
                    r#type: None,
                },
            )),
        }
    }

    fn not_equal_value_condition() -> proto::Expression {
        proto::Expression {
            expression: Some(proto::expression::Expression::Comparison(Box::new(
                proto::Comparison {
                    left: Some(Box::new(input_reference(1))),
                    right: Some(Box::new(input_reference(3))),
                    operator: proto::ComparisonOperator::NotEqual as i32,
                },
            ))),
        }
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

    fn exchange_batch(key: i64, value: &str) -> RecordBatch {
        RecordBatch::try_from_iter(vec![
            ("key", Arc::new(Int64Array::from(vec![key])) as ArrayRef),
            (
                "value",
                Arc::new(StringArray::from(vec![value])) as ArrayRef,
            ),
            (
                "__streamfusion_row_kind",
                Arc::new(Int8Array::from(vec![INSERT])) as ArrayRef,
            ),
        ])
        .unwrap()
    }

    fn array_key_exchange_batch(keys: &[Vec<i32>], values: &[&str]) -> RecordBatch {
        let keys = ListArray::from_iter_primitive::<Int32Type, _, _>(
            keys.iter()
                .map(|values| Some(values.iter().copied().map(Some).collect::<Vec<_>>())),
        );
        RecordBatch::try_from_iter(vec![
            ("key", Arc::new(keys) as ArrayRef),
            (
                "value",
                Arc::new(StringArray::from(values.to_vec())) as ArrayRef,
            ),
            (
                "__streamfusion_row_kind",
                Arc::new(Int8Array::from(vec![INSERT; values.len()])) as ArrayRef,
            ),
            (
                "__streamfusion_routing_key",
                Arc::new(BinaryArray::from_iter_values(values.iter().map(|value| {
                    if *value == "miss" {
                        b"\x09\0\0\0\0\0\0\0".as_slice()
                    } else {
                        b"\x01\x02\0\0\0\0\0\0".as_slice()
                    }
                }))) as ArrayRef,
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
