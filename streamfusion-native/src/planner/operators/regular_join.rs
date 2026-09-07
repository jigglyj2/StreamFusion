// Copyright 2026 StreamFusion Authors
// Licensed under the Apache License, Version 2.0

use std::sync::Arc;

mod bounded_datafusion;
mod candidates;
mod change_cursor;
#[cfg(test)]
mod change_cursor_tests;
pub(crate) mod execution_plan;
mod native_output;
mod paged_codec;
mod paged_state;
#[cfg(test)]
mod paged_state_tests;
pub(crate) mod region;
mod region_input;
mod state_codec;
mod streaming;
mod transitions;
use candidates::CandidateMatches;
use state_codec::{decode_state, encode_state};
use transitions::is_outer;
#[cfg(test)]
use transitions::process_change;

use ahash::RandomState;
use arrow::array::{
    Array, ArrayRef, BinaryArray, BooleanArray, Int32Array, Int8Array, UInt32Array,
};
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
    id: u64,
    row: Arc<[u8]>,
    // Flink's OuterJoinRecordStateView stores this as a Java int. Preserve its
    // wrapping arithmetic as well as its width in canonical state.
    associations: i32,
}

#[derive(Clone, Default, Debug, PartialEq, Eq)]
struct JoinState {
    next_row_id: [u64; 2],
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
    original: JoinState,
    touched: bool,
}

#[derive(Debug, PartialEq, Eq)]
struct OutputRow {
    left: Option<Arc<[u8]>>,
    right: Option<Arc<[u8]>>,
    kind: i8,
    input_ordinal: i32,
}

struct BoundedJoinCursor {
    state: JoinState,
    native: bounded_datafusion::JoinStream,
    done: bool,
    estimated_dynamic_bytes: usize,
}

impl BoundedJoinCursor {
    fn estimated_dynamic_bytes(&self) -> usize {
        self.estimated_dynamic_bytes
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
    fused_output_projection: Option<(Vec<usize>, SchemaRef)>,
    fused_calc_stage_count: usize,
    bounded_row_required: [bool; 2],
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
    bounded_output_entries_bytes: usize,
    bounded_output_entry_index: usize,
    bounded_cursor: Option<BoundedJoinCursor>,
    bounded_runtime: Option<Arc<bounded_datafusion::JoinRuntime>>,
    bounded_output_finished: bool,
    streaming_cursor: Option<streaming::StreamingCursor>,
    streaming_failed: bool,
    streaming_region_active: bool,
    streaming_invocation_active: bool,
    _plan_reservation: HostMemoryReservation,
    _schema_reservation: HostMemoryReservation,
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
        let mut plan_reservation = scratch_reservation.sibling("native regular join decoded plan");
        plan_reservation.resize(
            crate::execution_context::wire_memory::PlanMemory::scan(serialized_plan)?.decoded()?,
        )?;
        let native_plan = decode_plan(serialized_plan)?;
        let root = native_plan
            .root
            .ok_or_else(|| DataFusionError::Plan("regular join plan has no root".to_string()))?;
        let (plan, output_calcs) = split_regular_join_and_calc_tail(root)?;
        validate_plan(&plan, max_parallelism)?;
        let join_type = proto::RegularJoinType::try_from(plan.join_type).map_err(|_| {
            DataFusionError::Plan(format!("unknown regular join type {}", plan.join_type))
        })?;
        let mut schema_reservation =
            scratch_reservation.sibling("native regular join planned schemas and codecs");
        schema_reservation.resize(crate::planner::schema_memory::planned_schemas(
            plan.left_schema.as_ref(),
            plan.right_schema.as_ref(),
            &[],
        )?)?;
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
        let fused_calc_stage_count = output_calcs.len();
        let fused_output_projection = plan
            .bounded_final_output
            .then(|| pure_projection(&output_calcs, &output_schema))
            .flatten();
        let fused_output_calcs = if output_calcs.is_empty() || fused_output_projection.is_some() {
            None
        } else {
            Some(FusedCalcPipeline::new(
                Arc::clone(&output_schema),
                output_calcs,
                scratch_reservation.sibling("native regular join fused Calc plan"),
            )?)
        };
        let bounded_row_required = bounded_row_requirements(
            &plan,
            join_type,
            &visible_schemas,
            fused_output_projection.as_ref(),
            residual_condition.is_some(),
        );
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
            fused_output_projection,
            fused_calc_stage_count,
            bounded_row_required,
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
            bounded_output_entries_bytes: 0,
            bounded_output_entry_index: 0,
            bounded_cursor: None,
            bounded_runtime: None,
            bounded_output_finished: false,
            streaming_cursor: None,
            streaming_failed: false,
            streaming_region_active: false,
            streaming_invocation_active: false,
            _plan_reservation: plan_reservation,
            _schema_reservation: schema_reservation,
        })
    }

    #[cfg(test)]
    pub(crate) fn process_arrow(&mut self, side: usize, batch: RecordBatch) -> Result<RecordBatch> {
        self.require_idle_stream()?;
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
        let result = if self.plan.bounded_final_output && !self.bounded_row_required[side] {
            self.process_bounded_accounted(side, &batch, None, None)
        } else {
            match self.row_converters[side].convert_columns(&batch.columns()[..visible_count]) {
                Ok(encoded) if self.plan.bounded_final_output => {
                    self.process_bounded_accounted(side, &batch, Some(&encoded), None)
                }
                Ok(encoded) => self.process_accounted(side, &batch, &encoded),
                Err(error) => Err(error.into()),
            }
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
        let result = if self.bounded_row_required[side] {
            self.row_converters[side]
                .convert_columns(&batch.columns()[..visible_count])
                .map_err(DataFusionError::from)
                .and_then(|encoded| {
                    self.process_bounded_accounted(side, &batch, Some(&encoded), key_group)
                })
        } else {
            self.process_bounded_accounted(side, &batch, None, key_group)
        };
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

    #[cfg(test)]
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
        let (mut staged, reads) =
            paged_state::load(self.state.as_ref(), keys, &mut self.scratch_reservation)?;
        self.state_read_batches = self.state_read_batches.saturating_add(reads);
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
            let state = &staged[row_state_indices[row]].value;
            let candidates = if side == 0 { &state.right } else { &state.left };
            let candidate_matches =
                self.condition_matches_row(side, batch, row, encoded.row(row).data(), candidates)?;
            // The legacy single-output API must reject oversized fan-out before growing
            // descriptors or decoding repeated Arrow payloads. A bounded cursor replaces this
            // API separately; sharing encoded rows alone is not sufficient admission.
            let mut output_bytes = encoded.row(row).data().len().saturating_add(256);
            for (candidate, matched) in candidates.iter().zip(candidate_matches.iter()) {
                if matched {
                    output_bytes = output_bytes
                        .saturating_add(encoded.row(row).data().len())
                        .saturating_add(candidate.row.len().saturating_mul(2))
                        .saturating_add(512);
                }
            }
            self.scratch_reservation
                .try_grow(output_bytes.saturating_mul(4))?;
            let state = &mut staged[row_state_indices[row]];
            let row_bytes = Arc::from(encoded.row(row).data());
            process_change(
                self.join_type,
                side,
                kind,
                accumulate,
                &candidate_matches,
                row_bytes,
                ordinal,
                &mut state.value,
                &mut output,
            )?;
            state.touched = true;
        }
        let mutations = paged_state::batch_mutations(&staged, &mut self.scratch_reservation)?;
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
        encoded: Option<&Rows>,
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
        let (mut staged, reads) =
            paged_state::load(self.state.as_ref(), keys, &mut self.scratch_reservation)?;
        self.state_read_batches = self.state_read_batches.saturating_add(reads);
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
            let row_bytes = encoded.map_or(&[][..], |rows| rows.row(row).data());
            match kinds.value(row) {
                INSERT | UPDATE_AFTER => {
                    let id = state.value.next_row_id[side];
                    state.value.next_row_id[side] = id.checked_add(1).ok_or_else(|| {
                        DataFusionError::Execution("regular join row identity exhausted".into())
                    })?;
                    rows.push(StoredRow {
                        id,
                        row: Arc::from(row_bytes),
                        associations: 0,
                    });
                }
                UPDATE_BEFORE | DELETE => {
                    if let Some(position) = rows
                        .iter()
                        .position(|candidate| candidate.row.as_ref() == row_bytes)
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
        let mutations = paged_state::batch_mutations(&staged, &mut self.scratch_reservation)?;
        if !mutations.is_empty() {
            self.state.write_batch(mutations)?;
            self.state_write_batches = self.state_write_batches.saturating_add(1);
        }
        self.empty_output()
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
            let output = if self.fused_output_projection.is_some() {
                self.fused_calc_batches = self
                    .fused_calc_batches
                    .saturating_add(self.fused_calc_stage_count as u64);
                raw
            } else if let Some(calcs) = &self.fused_output_calcs {
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
                self.bounded_output_entries_bytes = 0;
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
            self.bounded_output_entries_bytes = 0;
            self.bounded_cursor = None;
            self.scratch_reservation.resize(0)?;
            return Ok(RecordBatch::new_empty(Arc::clone(&self.output_schema)));
        }
        let mut output = Vec::new();
        // A fused tail shares this operator's host-memory broker, so it can safely consume a
        // larger batch. A bare join retains the conservative edge size needed by arbitrary
        // downstream Flink operators.
        let max_rows =
            if self.fused_output_calcs.is_some() || self.fused_output_projection.is_some() {
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
            if !cursor.done {
                self.bounded_cursor = Some(cursor);
                break;
            }
        }
        if output.is_empty() && self.bounded_output_finished {
            self.bounded_output_entries.clear();
            self.bounded_output_entries_bytes = 0;
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
                let estimated_dynamic_bytes = state
                    .left
                    .iter()
                    .chain(&state.right)
                    .map(|row| row.row.len() + size_of::<StoredRow>())
                    .sum::<usize>();
                self.scratch_reservation.resize(
                    self.bounded_retained_bytes()
                        .saturating_add(estimated_dynamic_bytes)
                        .saturating_add(pending_output_bytes),
                )?;
                let runtime = match &self.bounded_runtime {
                    Some(runtime) => runtime.clone(),
                    None => {
                        let runtime = Arc::new(bounded_datafusion::JoinRuntime::new(
                            &self.scratch_reservation,
                        )?);
                        self.bounded_runtime = Some(runtime.clone());
                        runtime
                    }
                };
                let native = bounded_datafusion::JoinStream::new(
                    self,
                    &state,
                    &left_matchable,
                    &right_matchable,
                    runtime,
                )?;
                self.bounded_cursor = Some(BoundedJoinCursor {
                    state,
                    native,
                    done: false,
                    estimated_dynamic_bytes,
                });
                self.scratch_reservation.resize(
                    self.bounded_retained_bytes()
                        .saturating_add(pending_output_bytes),
                )?;
                return Ok(true);
            }
            self.bounded_output_entries.clear();
            self.bounded_output_entries_bytes = 0;
            self.bounded_output_entry_index = 0;
            let Some(key_group) = self.bounded_output_key_group else {
                return Ok(false);
            };
            let snapshot = self
                .state
                .snapshot_key_group(key_group, &self.scratch_reservation)?;
            self.scratch_reservation.resize(
                self.bounded_retained_bytes()
                    .saturating_add(snapshot.len().saturating_mul(16))
                    .saturating_add(pending_output_bytes),
            )?;
            let entries = decode_key_group_snapshot(key_group, &snapshot)?;
            self.bounded_output_entries = paged_state::decode_entries(key_group, &entries)?
                .into_iter()
                .map(|(key, state)| (key, encode_state(&state)))
                .collect();
            self.bounded_output_entries_bytes = self
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
        while output.len() < max_rows {
            let Some((left, right)) = cursor.native.peek()? else {
                cursor.done = true;
                break;
            };
            let left = left.map(|i| &cursor.state.left[i]);
            let right = right.map(|i| &cursor.state.right[i]);
            let Some(estimate) = self.prepare_bounded_output_row(
                cursor,
                *output_estimate,
                !output.is_empty(),
                left,
                right,
            )?
            else {
                break;
            };
            *output_estimate = estimate;
            output.push(OutputRow {
                left: left.map(|r| r.row.clone()),
                right: right.map(|r| r.row.clone()),
                kind: INSERT,
                input_ordinal: bounded_ordinal(output.len())?,
            });
            cursor.native.advance();
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

    fn bounded_retained_bytes(&self) -> usize {
        self.bounded_output_entries_bytes.saturating_add(
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

    pub(crate) fn state_memory(&self) -> HostMemoryReservation {
        self.scratch_reservation.sibling("native state transfer")
    }

    pub(crate) fn snapshot_key_group(&self, key_group: u32) -> Result<crate::state::SnapshotBytes> {
        self.require_idle_stream()?;
        self.state
            .snapshot_key_group(key_group, &self.scratch_reservation)
    }

    pub(crate) fn restore_key_group(&mut self, key_group: u32, bytes: &[u8]) -> Result<()> {
        self.require_idle_stream()?;
        paged_state::restore(
            self.state.as_mut(),
            key_group,
            bytes,
            &self.scratch_reservation,
        )
    }

    pub(crate) fn checkpoint(&self, directory: &std::path::Path) -> Result<()> {
        self.require_idle_stream()?;
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
            if schema
                .fields()
                .iter()
                .any(|field| field.name().starts_with("__streamfusion_owned_timestamp_"))
            {
                Some(super::envelope::Envelope::from_schema(&schema)?.payload_width)
            } else {
                None
            },
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
        let raw_field_count = self.output_schema.fields().len();
        let projected_indices = self
            .fused_output_projection
            .as_ref()
            .map(|(indices, _)| indices.as_slice());
        let needs_index =
            |index: usize| projected_indices.map_or(true, |indices| indices.contains(&index));
        let mut columns = vec![None::<ArrayRef>; raw_field_count];
        let mut field_offset = 0usize;
        for side in 0..if left_only { 1 } else { 2 } {
            let side_field_count = self.visible_schemas[side].fields().len();
            if !(field_offset..field_offset + side_field_count).any(&needs_index) {
                field_offset += side_field_count;
                continue;
            }
            let parser = self.row_converters[side].parser();
            let decoded = self.row_converters[side]
                .convert_rows(encoded[side].iter().map(|row| parser.parse(row)))?;
            let decoded = if selections[side]
                .iter()
                .enumerate()
                .all(|(index, selected)| *selected == Some(index as u32))
            {
                decoded
            } else {
                let selection = UInt32Array::from(selections[side].clone());
                decoded
                    .into_iter()
                    .map(|column| take(column.as_ref(), &selection, None))
                    .collect::<std::result::Result<Vec<_>, _>>()?
            };
            for (index, column) in decoded.into_iter().enumerate() {
                if needs_index(field_offset + index) {
                    columns[field_offset + index] = Some(column);
                }
            }
            field_offset += side_field_count;
        }
        let kind_index = raw_field_count - 2;
        let ordinal_index = raw_field_count - 1;
        if needs_index(kind_index) {
            columns[kind_index] = Some(Arc::new(Int8Array::from(kinds)));
        }
        if needs_index(ordinal_index) {
            columns[ordinal_index] = Some(Arc::new(Int32Array::from(ordinals)));
        }
        let (schema, columns) = if let Some((indices, schema)) = &self.fused_output_projection {
            let projected = indices
                .iter()
                .map(|&index| {
                    columns[index].clone().ok_or_else(|| {
                        DataFusionError::Internal(format!(
                            "bounded regular join projection column {index} was not decoded"
                        ))
                    })
                })
                .collect::<Result<Vec<_>>>()?;
            (Arc::clone(schema), projected)
        } else {
            let decoded = columns
                .into_iter()
                .enumerate()
                .map(|(index, column)| {
                    column.ok_or_else(|| {
                        DataFusionError::Internal(format!(
                            "regular join output column {index} was not decoded"
                        ))
                    })
                })
                .collect::<Result<Vec<_>>>()?;
            (Arc::clone(&self.output_schema), decoded)
        };
        Ok(RecordBatch::try_new(schema, columns)?)
    }

    fn empty_output(&self) -> Result<RecordBatch> {
        Ok(if let Some((_, schema)) = &self.fused_output_projection {
            RecordBatch::new_empty(Arc::clone(schema))
        } else {
            self.fused_output_calcs.as_ref().map_or_else(
                || RecordBatch::new_empty(self.output_schema.clone()),
                FusedCalcPipeline::empty_output,
            )
        })
    }

    #[cfg(test)]
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
                return Ok((*plan, outer_to_inner));
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

/// Recognizes the common bounded-join tail that only selects/reorders input columns. Applying
/// that projection while decoding the stored Arrow rows avoids materializing an unused join side
/// and is equivalent to DataFusion's ProjectionExec for this exact expression subset.
fn pure_projection(
    stages: &[proto::Calc],
    input_schema: &SchemaRef,
) -> Option<(Vec<usize>, SchemaRef)> {
    let [calc] = stages else {
        return None;
    };
    if calc.condition.is_some() {
        return None;
    }
    let indices = calc
        .projections
        .iter()
        .map(|expression| {
            let proto::expression::Expression::InputReference(reference) =
                expression.expression.as_ref()?
            else {
                return None;
            };
            let index = reference.index as usize;
            (index < input_schema.fields().len()).then_some(index)
        })
        .collect::<Option<Vec<_>>>()?;
    let fields = indices
        .iter()
        .enumerate()
        .map(|(output_index, &input_index)| {
            let input = input_schema.field(input_index);
            let name = if output_index + 1 == indices.len()
                && input_index + 1 == input_schema.fields().len()
                && input.name() == "__streamfusion_input_row"
            {
                "__streamfusion_input_row".to_string()
            } else {
                format!("projection_{output_index}")
            };
            Field::new(name, input.data_type().clone(), input.is_nullable())
        })
        .collect::<Vec<_>>();
    Some((indices, Arc::new(Schema::new(fields))))
}

fn bounded_row_requirements(
    plan: &proto::RegularJoin,
    join_type: proto::RegularJoinType,
    visible_schemas: &[SchemaRef; 2],
    projection: Option<&(Vec<usize>, SchemaRef)>,
    has_residual: bool,
) -> [bool; 2] {
    if !plan.bounded_final_output || has_residual {
        return [true, true];
    }
    let left_fields = visible_schemas[0].fields().len();
    let right_fields = visible_schemas[1].fields().len();
    let left_only = matches!(
        join_type,
        proto::RegularJoinType::Semi | proto::RegularJoinType::Anti
    );
    [0usize, 1usize].map(|side| {
        let (start, count, raw_output_uses_side) = if side == 0 {
            (0, left_fields, true)
        } else {
            (left_fields, right_fields, !left_only)
        };
        let output_uses_side = projection.map_or(raw_output_uses_side, |(indices, _)| {
            indices
                .iter()
                .any(|&index| index >= start && index < start + count)
        });
        let keys = if side == 0 {
            &plan.left_key_indices
        } else {
            &plan.right_key_indices
        };
        let row_identity_is_the_key = (0..count).all(|index| keys.contains(&(index as u32)));
        output_uses_side || !row_identity_is_the_key
    })
}

#[allow(clippy::too_many_arguments)]
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

#[cfg(test)]
mod tests;
