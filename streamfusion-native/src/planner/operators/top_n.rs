// Copyright 2026 StreamFusion Authors
// Licensed under the Apache License, Version 2.0

mod batch;
mod bounded;
pub(crate) mod compare;
mod datafusion_append;
mod datafusion_top_one;
pub(crate) mod execution_plan;
mod output_admission;
mod planning;
mod selection;
mod state;
mod state_write;

use planning::*;
use selection::*;

use std::cmp::Ordering;
use std::sync::Arc;

use ahash::RandomState;
use arrow::array::{
    Array, ArrayRef, BinaryArray, Int16Array, Int32Array, Int64Array, Int8Array, UInt32Array,
};
use arrow::compute::{interleave_record_batch, take};
use arrow::datatypes::{DataType, Field, Schema, SchemaRef};
use arrow::record_batch::RecordBatch;
use arrow_row::{RowConverter, Rows};
use datafusion::error::{DataFusionError, Result};
use datafusion::functions_window::rank::Rank;
use datafusion::logical_expr::WindowUDFImpl;
use datafusion::scalar::ScalarValue;
use hashbrown::HashMap;

use self::compare::{compare_rows, equal_rows};
#[cfg(test)]
use self::state::{decode_state, encode_state, StoredState};
use self::state::{decode_state_rows, encode_state_rows_with_kinds, row_converter};
use crate::exchange::{assign_key_group, encode_binary_row, KeyField};
use crate::memory_pool::HostMemoryReservation;
use crate::planner::arrow_schema;
use crate::state::{
    KeyedState, OrderedMemoryKeyedState, RocksPluginKeyedState, StateKey, StateKeyRef,
    StateMutation,
};
use crate::{decode_plan, proto};

const INSERT: i8 = 0;
const UPDATE_BEFORE: i8 = 1;
const UPDATE_AFTER: i8 = 2;
const DELETE: i8 = 3;
const STATE_KEY_PREFIX: u8 = 2;
const INPUT_KIND_COLUMN: &str = "__streamfusion_input_row_kind";
const PREENCODED_KEY_COLUMN: &str = "__streamfusion_key";
const OUTPUT_KIND_COLUMN: &str = "__streamfusion_row_kind";

/// Persistent Arrow-native non-window Top-N shared by memory and direct RocksDB state.
pub(crate) struct TopNProcessor {
    plan: proto::TopN,
    input_schema: SchemaRef,
    output_schema: SchemaRef,
    max_parallelism: u32,
    first_key_group: u32,
    last_key_group: u32,
    state: Box<dyn KeyedState>,
    scratch_reservation: HostMemoryReservation,
    prepared_input_schema: Option<SchemaRef>,
    key_fields: Vec<(usize, KeyField)>,
    preencoded_key_index: Option<usize>,
    input_kind_index: Option<usize>,
    row_converter: Option<RowConverter>,
    sort_keys: Option<super::sortable_state::SortKeys>,
    state_read_batches: u64,
    state_write_batches: u64,
    groups_read: u64,
    groups_written: u64,
    expired_groups: u64,
    comparator_calls: u64,
    invalid_retractions: u64,
    invalid_top_sizes: u64,
    saturated_append_limit: bool,
    bounded_output: Option<BoundedOutput>,
    bounded_drained: bool,
    invocation: crate::planner::persistent::unary::InvocationState,
    native_schema: Option<SchemaRef>,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
struct CandidateRef {
    source: usize,
    row: usize,
    sequence: u64,
    kind: i8,
}

#[cfg(test)]
mod indexed_tests;

struct CandidateSources {
    batches: Vec<Arc<RecordBatch>>,
    orders: Option<Vec<Rows>>,
}
impl std::ops::Deref for CandidateSources {
    type Target = [Arc<RecordBatch>];
    fn deref(&self) -> &Self::Target {
        &self.batches
    }
}

struct GroupWork {
    state_key: StateKey,
    next_sequence: u64,
    rank_end: Option<i64>,
    candidates: Vec<CandidateRef>,
}

struct DecodedGroup {
    next_sequence: u64,
    rank_end: Option<i64>,
    sequences: Vec<u64>,
    row_kinds: Vec<i8>,
    restored_row_offset: usize,
}

struct BoundedOutput {
    rows: RecordBatch,
    indices: Vec<u32>,
    ranks: Vec<i64>,
    kinds: Vec<i8>,
    position: usize,
}

impl BoundedOutput {
    fn retained_bytes(&self) -> usize {
        self.rows
            .get_array_memory_size()
            .saturating_add(
                self.indices
                    .capacity()
                    .saturating_mul(std::mem::size_of::<u32>()),
            )
            .saturating_add(
                self.ranks
                    .capacity()
                    .saturating_mul(std::mem::size_of::<i64>()),
            )
            .saturating_add(
                self.kinds
                    .capacity()
                    .saturating_mul(std::mem::size_of::<i8>()),
            )
    }
}

#[derive(Clone, Copy)]
struct OutputEvent {
    candidate: CandidateRef,
    rank: i64,
    kind: i8,
}

impl TopNProcessor {
    pub(crate) fn new(
        serialized_plan: &[u8],
        max_parallelism: u32,
        first_key_group: u32,
        last_key_group: u32,
        reservation: HostMemoryReservation,
    ) -> Result<Self> {
        let scratch = reservation.sibling("native top-n batch scratch and output");
        let state = Box::new(OrderedMemoryKeyedState::new(
            first_key_group,
            last_key_group,
            reservation,
        )?);
        Self::with_state_with_range(
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
        reservation: HostMemoryReservation,
    ) -> Result<Self> {
        let state = Box::new(RocksPluginKeyedState::open_for_owner(
            plugin_path,
            database_path,
            first_key_group,
            last_key_group,
            memory_limit,
            &reservation,
        )?);
        Self::with_state_with_range(
            serialized_plan,
            max_parallelism,
            first_key_group,
            last_key_group,
            state,
            reservation,
        )
    }

    pub(crate) fn with_state_with_range(
        serialized_plan: &[u8],
        max_parallelism: u32,
        first_key_group: u32,
        last_key_group: u32,
        state: Box<dyn KeyedState>,
        scratch_reservation: HostMemoryReservation,
    ) -> Result<Self> {
        let root = decode_plan(serialized_plan)?
            .root
            .ok_or_else(|| DataFusionError::Plan("top-n plan has no root".to_string()))?;
        let plan = match root.operator {
            Some(proto::operator::Operator::TopN(plan)) => *plan,
            _ => {
                return Err(DataFusionError::Plan(
                    "top-n handle requires a TopN root".to_string(),
                ));
            }
        };
        validate_plan(&plan, max_parallelism)?;
        let input_schema = arrow_schema(plan.input_schema.as_ref().expect("validated"))?;
        let output_schema = arrow_schema(plan.output_schema.as_ref().expect("validated"))?;
        validate_output_schema(&plan, &input_schema, &output_schema)?;
        let sort_keys = super::sortable_state::SortKeys::new(
            &input_schema,
            &plan.sort_key_indices,
            &plan.sort_ascending,
            &plan.sort_nulls_last,
        )?;
        let initial_row_converter = row_converter(&input_schema)?;
        Ok(Self {
            plan,
            input_schema,
            output_schema,
            max_parallelism,
            first_key_group,
            last_key_group,
            state,
            scratch_reservation,
            prepared_input_schema: None,
            key_fields: Vec::new(),
            preencoded_key_index: None,
            input_kind_index: None,
            row_converter: Some(initial_row_converter),
            sort_keys,
            state_read_batches: 0,
            state_write_batches: 0,
            groups_read: 0,
            groups_written: 0,
            expired_groups: 0,
            comparator_calls: 0,
            invalid_retractions: 0,
            invalid_top_sizes: 0,
            saturated_append_limit: false,
            bounded_output: None,
            bounded_drained: false,
            invocation: Default::default(),
            native_schema: None,
        })
    }

    pub(crate) fn process_arrow(
        &mut self,
        batch: RecordBatch,
        now_millis: i64,
    ) -> Result<RecordBatch> {
        if self.bounded_output.is_some() || self.bounded_drained {
            return Err(DataFusionError::Execution(
                "bounded Top-N received input after terminal output started".to_string(),
            ));
        }
        self.prepare_schema(batch.schema())?;
        if self.saturated_append_limit {
            let kinds = batch
                .column(self.input_kind_index.expect("input schema prepared"))
                .as_any()
                .downcast_ref::<Int8Array>()
                .ok_or_else(|| {
                    DataFusionError::Execution(
                        "top-n RowKind metadata is not Arrow Int8".to_string(),
                    )
                })?;
            for row in 0..batch.num_rows() {
                require_insert(kinds.value(row), "append-fast")?;
            }
            return self.finish_output(
                output_batch(&self.plan, &self.output_schema, &[], Vec::new())?,
                0,
            );
        }
        // The input buffers are owned and accounted by the upstream Arrow operator. Top-N can
        // simultaneously allocate a group-key copy, gathered canonical state, Arrow IPC bytes,
        // and an output gather of the same payload. Admit that peak before creating any of them;
        // the persistent backend has its own independent reservation.
        let base = batch
            .get_array_memory_size()
            .saturating_mul(3)
            .saturating_add(batch.num_rows().saturating_mul(512));
        self.scratch_reservation.resize(base)?;
        match self.process_arrow_accounted(batch, now_millis, base) {
            Ok(output) => self.finish_output(output, base),
            Err(error) => {
                self.scratch_reservation.resize(0)?;
                Err(error)
            }
        }
    }

    pub(crate) fn statistics(&self) -> [u64; 8] {
        [
            self.state_read_batches,
            self.state_write_batches,
            self.groups_read,
            self.groups_written,
            self.expired_groups,
            self.comparator_calls,
            self.invalid_retractions,
            self.invalid_top_sizes,
        ]
    }

    pub(crate) fn is_append_limit_saturated(&self) -> bool {
        self.saturated_append_limit
    }

    pub(crate) fn state_memory(&self) -> HostMemoryReservation {
        self.scratch_reservation.sibling("native state transfer")
    }

    pub(crate) fn snapshot_key_group(&self, key_group: u32) -> Result<crate::state::SnapshotBytes> {
        self.state
            .snapshot_key_group(key_group, &self.scratch_reservation)
    }

    pub(crate) fn restore_key_group(&mut self, key_group: u32, bytes: &[u8]) -> Result<()> {
        self.saturated_append_limit = false;
        self.bounded_output = None;
        self.bounded_drained = false;
        self.state
            .restore_key_group(key_group, bytes, &self.scratch_reservation)
    }

    pub(crate) fn checkpoint(&self, directory: &std::path::Path) -> Result<()> {
        self.state.checkpoint(directory)
    }

    fn prepare_schema(&mut self, schema: SchemaRef) -> Result<()> {
        if let Some(expected) = &self.prepared_input_schema {
            if expected.as_ref() != schema.as_ref() {
                return Err(DataFusionError::Execution(
                    "top-n input schema changed while running".to_string(),
                ));
            }
            return Ok(());
        }
        let visible_field_count = self.input_schema.fields().len();
        if schema.fields().len() < visible_field_count
            || !self
                .input_schema
                .fields()
                .iter()
                .zip(&schema.fields()[..visible_field_count])
                .all(|(expected, actual)| fields_compatible(expected, actual, true))
        {
            return Err(DataFusionError::Execution(format!(
                "top-n visible Arrow input does not match its plan: expected {:?}, got {schema:?}",
                self.input_schema
            )));
        }
        self.input_kind_index = metadata_index(&schema, INPUT_KIND_COLUMN);
        if self.input_kind_index.is_none() {
            return Err(DataFusionError::Execution(format!(
                "top-n input is missing {INPUT_KIND_COLUMN}"
            )));
        }
        self.preencoded_key_index = metadata_index(&schema, PREENCODED_KEY_COLUMN);
        if self.preencoded_key_index.is_none() {
            self.key_fields = self
                .plan
                .partition_key_indices
                .iter()
                .map(|&index| {
                    let field =
                        self.input_schema
                            .fields()
                            .get(index as usize)
                            .ok_or_else(|| {
                                DataFusionError::Plan(format!(
                                    "top-n partition index {index} is outside the input"
                                ))
                            })?;
                    Ok((
                        index as usize,
                        KeyField::from_arrow_type(field.data_type())?,
                    ))
                })
                .collect::<Result<Vec<_>>>()?;
        }
        // Arrow Java uses implementation-specific nested child names (for example `$data$`
        // and `entries`) that are not part of the Flink logical type. Keep the exact schema
        // transported across Arrow C Data so state IPC and output remain zero-copy, while the
        // compatibility check above still enforces every logical shape and nullability bit.
        let visible_fields = schema.fields()[..visible_field_count].to_vec();
        self.input_schema = Arc::new(Schema::new(visible_fields.clone()));
        let mut output_fields = visible_fields;
        if self.plan.output_rank_number {
            output_fields.push(
                self.output_schema
                    .field(self.output_schema.fields().len() - 1)
                    .clone()
                    .into(),
            );
        }
        self.output_schema = Arc::new(Schema::new(output_fields));
        self.row_converter = Some(row_converter(&self.input_schema)?);
        self.prepared_input_schema = Some(schema);
        Ok(())
    }

    fn group_key(&self, batch: &RecordBatch, row: usize) -> Result<Vec<u8>> {
        match self.preencoded_key_index {
            Some(index) => {
                let keys = batch
                    .column(index)
                    .as_any()
                    .downcast_ref::<BinaryArray>()
                    .ok_or_else(|| {
                        DataFusionError::Execution(
                            "top-n preencoded partition key is not Arrow Binary".to_string(),
                        )
                    })?;
                if keys.is_null(row) {
                    return Err(DataFusionError::Execution(
                        "top-n preencoded partition key may not be null".to_string(),
                    ));
                }
                Ok(keys.value(row).to_vec())
            }
            None if self.key_fields.is_empty() => Ok(Vec::new()),
            None => Ok(encode_binary_row(batch, row, &self.key_fields)?),
        }
    }

    fn is_expired(&self, last_access_millis: i64, now_millis: i64) -> bool {
        let ttl = self.plan.state_ttl_millis;
        ttl > 0
            && last_access_millis != 0
            && now_millis.saturating_sub(last_access_millis) >= ttl.min(i64::MAX as u64) as i64
    }

    fn finish_output(&mut self, output: RecordBatch, base: usize) -> Result<RecordBatch> {
        let output_bytes = output.get_array_memory_size();
        self.scratch_reservation.resize(output_bytes.max(base))?;
        self.scratch_reservation.transfer_to_arrow(output_bytes)?;
        self.scratch_reservation.resize(0)?;
        Ok(output)
    }
}

fn is_non_expiring_append_limit(plan: &proto::TopN) -> bool {
    is_append_limit(plan) && plan.state_ttl_millis == 0
}

fn is_append_limit(plan: &proto::TopN) -> bool {
    plan.sort_key_indices.is_empty()
        && plan.partition_key_indices.is_empty()
        && plan.rank_end.is_some()
        && plan.strategy == proto::TopNStrategy::AppendFast as i32
}

fn visible_source(sources: &[Arc<RecordBatch>]) -> &RecordBatch {
    sources[0].as_ref()
}

fn output_batch(
    plan: &proto::TopN,
    output_schema: &SchemaRef,
    sources: &[Arc<RecordBatch>],
    events: Vec<OutputEvent>,
) -> Result<RecordBatch> {
    let mut columns = if events.is_empty() {
        output_schema
            .fields()
            .iter()
            .map(|field| arrow::array::new_empty_array(field.data_type()))
            .collect::<Vec<_>>()
    } else {
        let indices = events
            .iter()
            .map(|event| (event.candidate.source, event.candidate.row))
            .collect::<Vec<_>>();
        let source_refs = sources.iter().map(AsRef::as_ref).collect::<Vec<_>>();
        let rows = interleave_record_batch(&source_refs, &indices)?;
        let mut columns = rows.columns().to_vec();
        if plan.output_rank_number {
            columns.push(Arc::new(Int64Array::from(
                events.iter().map(|event| event.rank).collect::<Vec<_>>(),
            )) as ArrayRef);
        }
        columns
    };
    columns.push(Arc::new(Int8Array::from(
        events.iter().map(|event| event.kind).collect::<Vec<_>>(),
    )) as ArrayRef);
    let mut fields = output_schema.fields().iter().cloned().collect::<Vec<_>>();
    fields.push(Arc::new(Field::new(
        OUTPUT_KIND_COLUMN,
        DataType::Int8,
        false,
    )));
    Ok(RecordBatch::try_new(
        Arc::new(Schema::new(fields)),
        columns,
    )?)
}

fn top_n_state_key(key_group: u32, group_key: &[u8]) -> StateKey {
    let mut key = Vec::with_capacity(1 + group_key.len());
    key.push(STATE_KEY_PREFIX);
    key.extend_from_slice(group_key);
    StateKey { key_group, key }
}

fn next_sequence(sequence: u64) -> Result<u64> {
    sequence
        .checked_add(1)
        .ok_or_else(|| DataFusionError::Execution("top-n sequence overflow".to_string()))
}

fn require_insert(kind: i8, strategy: &str) -> Result<()> {
    if kind == INSERT {
        Ok(())
    } else if (0..=3).contains(&kind) {
        Err(DataFusionError::Execution(format!(
            "{strategy} Top-N received RowKind {kind}"
        )))
    } else {
        Err(unknown_row_kind(kind))
    }
}

fn unknown_row_kind(kind: i8) -> DataFusionError {
    DataFusionError::Execution(format!("unknown Flink RowKind byte {kind}"))
}

#[cfg(test)]
mod tests;
