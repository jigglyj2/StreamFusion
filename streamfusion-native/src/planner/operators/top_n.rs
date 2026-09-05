// Copyright 2026 StreamFusion Authors
// Licensed under the Apache License, Version 2.0

pub(crate) mod compare;
mod state;

use std::cmp::Ordering;
use std::sync::Arc;

use ahash::RandomState;
use arrow::array::{Array, ArrayRef, BinaryArray, Int16Array, Int32Array, Int64Array, Int8Array};
use arrow::compute::interleave_record_batch;
use arrow::datatypes::{DataType, Field, Schema, SchemaRef};
use arrow::record_batch::RecordBatch;
use arrow_row::{RowConverter, Rows};
use datafusion::error::{DataFusionError, Result};
use hashbrown::HashMap;

use self::compare::{compare_rows, equal_rows};
#[cfg(test)]
use self::state::{decode_state, encode_state, StoredState};
use self::state::{decode_state_rows, encode_state_rows, row_converter};
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
    state: Box<dyn KeyedState>,
    scratch_reservation: HostMemoryReservation,
    prepared_input_schema: Option<SchemaRef>,
    key_fields: Vec<(usize, KeyField)>,
    preencoded_key_index: Option<usize>,
    input_kind_index: Option<usize>,
    row_converter: Option<RowConverter>,
    state_read_batches: u64,
    state_write_batches: u64,
    groups_read: u64,
    groups_written: u64,
    expired_groups: u64,
    comparator_calls: u64,
    invalid_retractions: u64,
    invalid_top_sizes: u64,
    saturated_append_limit: bool,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
struct CandidateRef {
    source: usize,
    row: usize,
    sequence: u64,
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
    restored_row_offset: usize,
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
        reservation: HostMemoryReservation,
    ) -> Result<Self> {
        let state = Box::new(RocksPluginKeyedState::open(
            plugin_path,
            database_path,
            first_key_group,
            last_key_group,
            memory_limit,
        )?);
        Self::with_state(serialized_plan, max_parallelism, state, reservation)
    }

    fn with_state(
        serialized_plan: &[u8],
        max_parallelism: u32,
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
        Ok(Self {
            plan,
            input_schema,
            output_schema,
            max_parallelism,
            state,
            scratch_reservation,
            prepared_input_schema: None,
            key_fields: Vec::new(),
            preencoded_key_index: None,
            input_kind_index: None,
            row_converter: None,
            state_read_batches: 0,
            state_write_batches: 0,
            groups_read: 0,
            groups_written: 0,
            expired_groups: 0,
            comparator_calls: 0,
            invalid_retractions: 0,
            invalid_top_sizes: 0,
            saturated_append_limit: false,
        })
    }

    pub(crate) fn process_arrow(
        &mut self,
        batch: RecordBatch,
        now_millis: i64,
    ) -> Result<RecordBatch> {
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

    fn process_arrow_accounted(
        &mut self,
        batch: RecordBatch,
        now_millis: i64,
        base_reservation: usize,
    ) -> Result<RecordBatch> {
        let visible = Arc::new(RecordBatch::try_new(
            Arc::clone(&self.input_schema),
            batch.columns()[..self.input_schema.fields().len()].to_vec(),
        )?);
        let kinds = batch
            .column(self.input_kind_index.expect("input schema prepared"))
            .as_any()
            .downcast_ref::<Int8Array>()
            .ok_or_else(|| {
                DataFusionError::Execution("top-n RowKind metadata is not Arrow Int8".to_string())
            })?;
        let mut unique = HashMap::<Vec<u8>, usize, RandomState>::with_capacity_and_hasher(
            batch.num_rows(),
            RandomState::new(),
        );
        let mut row_groups = Vec::with_capacity(batch.num_rows());
        for row in 0..batch.num_rows() {
            let key = self.group_key(&batch, row)?;
            let next = unique.len();
            row_groups.push(*unique.entry(key).or_insert(next));
        }
        let mut ordered_keys = (0..unique.len()).map(|_| None).collect::<Vec<_>>();
        for (key, index) in unique {
            ordered_keys[index] = Some(key);
        }
        let ordered_keys = ordered_keys
            .into_iter()
            .map(|key| key.expect("every top-n group index is populated"))
            .collect::<Vec<_>>();
        let state_keys = ordered_keys
            .iter()
            .map(|key| top_n_state_key(assign_key_group(key, self.max_parallelism), key))
            .collect::<Vec<_>>();
        let state_refs = state_keys
            .iter()
            .map(|key| StateKeyRef {
                key_group: key.key_group,
                key: &key.key,
            })
            .collect::<Vec<_>>();
        let values = self.state.get_batch(&state_refs)?;
        self.state_read_batches = self.state_read_batches.saturating_add(1);
        self.groups_read = self.groups_read.saturating_add(values.len() as u64);
        let state_bytes = values
            .iter()
            .flatten()
            .map(|value| value.len())
            .sum::<usize>();
        self.scratch_reservation
            .resize(base_reservation.saturating_add(state_bytes.saturating_mul(4)))?;

        let mut restored_rows = Vec::new();
        let mut decoded_groups = Vec::with_capacity(values.len());
        for value in &values {
            let decoded = value
                .as_ref()
                .map(|bytes| decode_state_rows(bytes.as_ref()))
                .transpose()?;
            if let Some(decoded) = decoded {
                if self.is_expired(decoded.last_access_millis, now_millis) {
                    self.expired_groups = self.expired_groups.saturating_add(1);
                    decoded_groups.push(DecodedGroup {
                        next_sequence: 0,
                        rank_end: None,
                        sequences: Vec::new(),
                        restored_row_offset: restored_rows.len(),
                    });
                } else {
                    let restored_row_offset = restored_rows.len();
                    restored_rows.extend(decoded.rows);
                    decoded_groups.push(DecodedGroup {
                        next_sequence: decoded.next_sequence,
                        rank_end: decoded.rank_end,
                        sequences: decoded.sequences,
                        restored_row_offset,
                    });
                }
            } else {
                decoded_groups.push(DecodedGroup {
                    next_sequence: 0,
                    rank_end: None,
                    sequences: Vec::new(),
                    restored_row_offset: restored_rows.len(),
                });
            }
        }
        let restored = if restored_rows.is_empty() {
            RecordBatch::new_empty(Arc::clone(&self.input_schema))
        } else {
            let converter = self.row_converter.as_ref().expect("input schema prepared");
            let parser = converter.parser();
            let columns =
                converter.convert_rows(restored_rows.into_iter().map(|row| parser.parse(row)))?;
            RecordBatch::try_new(Arc::clone(&self.input_schema), columns)?
        };
        let sources = vec![visible, Arc::new(restored)];
        let mut groups = state_keys
            .into_iter()
            .zip(decoded_groups)
            .map(|(state_key, decoded)| GroupWork {
                state_key,
                next_sequence: decoded.next_sequence,
                rank_end: decoded.rank_end,
                candidates: decoded
                    .sequences
                    .into_iter()
                    .enumerate()
                    .map(|(row, sequence)| CandidateRef {
                        source: 1,
                        row: decoded.restored_row_offset + row,
                        sequence,
                    })
                    .collect(),
            })
            .collect::<Vec<_>>();
        if is_append_limit(&self.plan) {
            for group in &mut groups {
                group.candidates.clear();
                group.rank_end = Some(0);
            }
        }
        let mut output = Vec::new();
        let append_limit_end = is_append_limit(&self.plan)
            .then(|| usize::try_from(self.plan.rank_end.unwrap()).unwrap_or(usize::MAX));
        for row in 0..batch.num_rows() {
            let group = &mut groups[row_groups[row]];
            if let Some(limit_end) = append_limit_end {
                require_insert(kinds.value(row), "append-fast")?;
                // Flink's append-only Limit stores only the number already observed. Payload
                // rows can never re-enter the output, so retaining them would turn a constant
                // counter into O(limit) state and make OFFSET-only queries unbounded.
                group.rank_end = Some(0);
                if group.next_sequence < limit_end as u64 {
                    let candidate = CandidateRef {
                        source: 0,
                        row,
                        sequence: group.next_sequence,
                    };
                    group.next_sequence = next_sequence(group.next_sequence)?;
                    let rank = group.next_sequence;
                    if rank >= self.plan.rank_start {
                        output.push(OutputEvent {
                            candidate,
                            rank: rank as i64,
                            kind: INSERT,
                        });
                    }
                }
                continue;
            }
            let rank_end = rank_end(
                &self.plan,
                visible_source(&sources),
                row,
                group,
                &mut self.invalid_top_sizes,
            )?;
            let before = selected(group, self.plan.rank_start, rank_end).to_vec();
            let candidate = CandidateRef {
                source: 0,
                row,
                sequence: group.next_sequence,
            };
            match proto::TopNStrategy::try_from(self.plan.strategy)
                .map_err(|_| DataFusionError::Plan("top-n strategy is unknown".to_string()))?
            {
                proto::TopNStrategy::AppendFast => {
                    require_insert(kinds.value(row), "append-fast")?;
                    group.next_sequence = next_sequence(group.next_sequence)?;
                    insert_sorted(
                        &self.plan,
                        &sources,
                        &mut group.candidates,
                        candidate,
                        &mut self.comparator_calls,
                    )?;
                }
                proto::TopNStrategy::UpdateFast => {
                    if !matches!(kinds.value(row), INSERT | UPDATE_AFTER) {
                        return Err(DataFusionError::Execution(format!(
                            "update-fast Top-N received RowKind {}",
                            kinds.value(row)
                        )));
                    }
                    let previous = find_equal(
                        &sources,
                        &group.candidates,
                        candidate,
                        self.plan
                            .primary_key_indices
                            .iter()
                            .map(|&value| value as usize),
                    )?;
                    let candidate = if let Some(index) = previous {
                        let sequence = group.candidates.remove(index).sequence;
                        CandidateRef {
                            sequence,
                            ..candidate
                        }
                    } else {
                        group.next_sequence = next_sequence(group.next_sequence)?;
                        candidate
                    };
                    insert_sorted(
                        &self.plan,
                        &sources,
                        &mut group.candidates,
                        candidate,
                        &mut self.comparator_calls,
                    )?;
                }
                proto::TopNStrategy::Retract => match kinds.value(row) {
                    INSERT | UPDATE_AFTER => {
                        group.next_sequence = next_sequence(group.next_sequence)?;
                        insert_sorted(
                            &self.plan,
                            &sources,
                            &mut group.candidates,
                            candidate,
                            &mut self.comparator_calls,
                        )?;
                    }
                    UPDATE_BEFORE | DELETE => {
                        let previous = find_equal(
                            &sources,
                            &group.candidates,
                            candidate,
                            0..self.input_schema.fields().len(),
                        )?;
                        if let Some(index) = previous {
                            group.candidates.remove(index);
                        } else {
                            self.invalid_retractions = self.invalid_retractions.saturating_add(1);
                        }
                    }
                    other => return Err(unknown_row_kind(other)),
                },
                proto::TopNStrategy::Unspecified => unreachable!("validated Top-N strategy"),
            }
            if self.plan.strategy != proto::TopNStrategy::Retract as i32 {
                let retained = usize::try_from(rank_end.max(0)).unwrap_or(usize::MAX);
                group.candidates.truncate(retained);
            }
            let after = selected(group, self.plan.rank_start, rank_end);
            emit_difference(&self.plan, &sources, &before, after, &mut output)?;
        }

        let saturates_append_limit = is_non_expiring_append_limit(&self.plan)
            && groups
                .first()
                .is_some_and(|group| group.next_sequence >= self.plan.rank_end.unwrap());

        let converter = self.row_converter.as_ref().expect("input schema prepared");
        let encoded_sources = sources
            .iter()
            .map(|source| converter.convert_columns(source.columns()))
            .collect::<std::result::Result<Vec<Rows>, _>>()?;
        let mut mutations = Vec::with_capacity(groups.len());
        for group in groups {
            let sequences = group
                .candidates
                .iter()
                .map(|candidate| candidate.sequence)
                .collect::<Vec<_>>();
            let preserve_empty = group.rank_end.is_some();
            mutations.push(StateMutation {
                key: group.state_key,
                value: (!sequences.is_empty() || preserve_empty)
                    .then(|| {
                        encode_state_rows(
                            group.next_sequence,
                            group.rank_end,
                            now_millis,
                            &sequences,
                            group.candidates.iter().map(|candidate| {
                                encoded_sources[candidate.source].row(candidate.row)
                            }),
                        )
                    })
                    .transpose()?,
            });
        }
        self.groups_written = self.groups_written.saturating_add(mutations.len() as u64);
        if !mutations.is_empty() {
            self.state.write_batch(mutations)?;
            self.state_write_batches = self.state_write_batches.saturating_add(1);
        }
        self.saturated_append_limit = saturates_append_limit;
        output_batch(&self.plan, &self.output_schema, &sources, output)
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

    pub(crate) fn snapshot_key_group(&self, key_group: u32) -> Result<Vec<u8>> {
        self.state.snapshot_key_group(key_group)
    }

    pub(crate) fn restore_key_group(&mut self, key_group: u32, bytes: &[u8]) -> Result<()> {
        self.saturated_append_limit = false;
        self.state.restore_key_group(key_group, bytes)
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

fn rank_end(
    plan: &proto::TopN,
    input: &RecordBatch,
    row: usize,
    group: &mut GroupWork,
    invalid_top_sizes: &mut u64,
) -> Result<i64> {
    if let Some(value) = plan.rank_end {
        return i64::try_from(value)
            .map_err(|_| DataFusionError::Execution("top-n rank end exceeds i64".to_string()));
    }
    let index = plan.variable_rank_end_index.expect("validated") as usize;
    let value = if input.column(index).is_null(row) {
        0
    } else {
        match input.column(index).data_type() {
            DataType::Int16 => input
                .column(index)
                .as_any()
                .downcast_ref::<Int16Array>()
                .unwrap()
                .value(row) as i64,
            DataType::Int32 => input
                .column(index)
                .as_any()
                .downcast_ref::<Int32Array>()
                .unwrap()
                .value(row) as i64,
            DataType::Int64 => input
                .column(index)
                .as_any()
                .downcast_ref::<Int64Array>()
                .unwrap()
                .value(row),
            other => {
                return Err(DataFusionError::Execution(format!(
                    "top-n variable rank end has Arrow type {other}"
                )))
            }
        }
    };
    match group.rank_end {
        None => group.rank_end = Some(value),
        Some(existing) if existing != value => {
            *invalid_top_sizes = invalid_top_sizes.saturating_add(1);
        }
        Some(_) => {}
    }
    Ok(group.rank_end.expect("rank end initialized"))
}

fn selected(group: &GroupWork, rank_start: u64, rank_end: i64) -> &[CandidateRef] {
    if rank_end < rank_start as i64 {
        return &group.candidates[0..0];
    }
    let from = usize::try_from(rank_start - 1)
        .unwrap_or(usize::MAX)
        .min(group.candidates.len());
    let to = usize::try_from(rank_end)
        .unwrap_or(usize::MAX)
        .min(group.candidates.len());
    &group.candidates[from..to]
}

fn insert_sorted(
    plan: &proto::TopN,
    sources: &[Arc<RecordBatch>],
    candidates: &mut Vec<CandidateRef>,
    candidate: CandidateRef,
    comparator_calls: &mut u64,
) -> Result<()> {
    // Flink's LIMIT specialization has no ordering: arrival sequence is its stable order.
    // Avoid a logarithmic binary search whose comparisons can only ever fall through to that
    // same sequence ordering.
    if plan.sort_key_indices.is_empty() {
        candidates.push(candidate);
        return Ok(());
    }
    // Flink's generated floating comparator uses `>`/`<`, so NaN compares equal to every
    // value. Its TopNBuffer (a TreeMap of sort-key buckets) therefore preserves the insertion
    // order of that comparator-equivalent bucket. Keep the same stable bucket behavior without
    // imposing Rust's total float order; the normal path remains logarithmic.
    let can_have_nan = plan.sort_key_indices.iter().any(|&index| {
        self::compare::data_type_can_have_nan(
            sources[candidate.source]
                .schema()
                .field(index as usize)
                .data_type(),
        )
    });
    let mut has_nan = false;
    if can_have_nan {
        has_nan = sort_key_has_nan(plan, sources, candidate)?;
        if !has_nan {
            for &existing in candidates.iter() {
                if sort_key_has_nan(plan, sources, existing)? {
                    has_nan = true;
                    break;
                }
            }
        }
    }
    if has_nan {
        for start in 0..candidates.len() {
            *comparator_calls = comparator_calls.saturating_add(1);
            if sort_key_order(plan, sources, candidates[start], candidate)? == Ordering::Equal {
                let representative = candidates[start];
                let mut end = start + 1;
                while end < candidates.len() {
                    *comparator_calls = comparator_calls.saturating_add(1);
                    if sort_key_order(plan, sources, representative, candidates[end])?
                        != Ordering::Equal
                    {
                        break;
                    }
                    end += 1;
                }
                let position = (start..end)
                    .find(|&index| candidates[index].sequence > candidate.sequence)
                    .unwrap_or(end);
                candidates.insert(position, candidate);
                return Ok(());
            }
        }
    }
    let mut low = 0;
    let mut high = candidates.len();
    while low < high {
        let middle = low + (high - low) / 2;
        *comparator_calls = comparator_calls.saturating_add(1);
        if candidate_order(plan, sources, candidates[middle], candidate)? == Ordering::Less {
            low = middle + 1;
        } else {
            high = middle;
        }
    }
    candidates.insert(low, candidate);
    Ok(())
}

fn candidate_order(
    plan: &proto::TopN,
    sources: &[Arc<RecordBatch>],
    left: CandidateRef,
    right: CandidateRef,
) -> Result<Ordering> {
    let ordering = sort_key_order(plan, sources, left, right)?;
    Ok(ordering.then_with(|| left.sequence.cmp(&right.sequence)))
}

fn sort_key_order(
    plan: &proto::TopN,
    sources: &[Arc<RecordBatch>],
    left: CandidateRef,
    right: CandidateRef,
) -> Result<Ordering> {
    compare_rows(
        sources[left.source].as_ref(),
        left.row,
        sources[right.source].as_ref(),
        right.row,
        &plan.sort_key_indices,
        &plan.sort_ascending,
        &plan.sort_nulls_last,
    )
}

fn sort_key_has_nan(
    plan: &proto::TopN,
    sources: &[Arc<RecordBatch>],
    candidate: CandidateRef,
) -> Result<bool> {
    self::compare::row_has_nan(
        sources[candidate.source].as_ref(),
        candidate.row,
        &plan.sort_key_indices,
    )
}

fn find_equal(
    sources: &[Arc<RecordBatch>],
    candidates: &[CandidateRef],
    candidate: CandidateRef,
    indices: impl IntoIterator<Item = usize> + Clone,
) -> Result<Option<usize>> {
    for (index, existing) in candidates.iter().enumerate() {
        if equal_rows(
            sources[existing.source].as_ref(),
            existing.row,
            sources[candidate.source].as_ref(),
            candidate.row,
            indices.clone(),
        )? {
            return Ok(Some(index));
        }
    }
    Ok(None)
}

fn emit_difference(
    plan: &proto::TopN,
    sources: &[Arc<RecordBatch>],
    before: &[CandidateRef],
    after: &[CandidateRef],
    output: &mut Vec<OutputEvent>,
) -> Result<()> {
    if plan.output_rank_number || plan.rank_start > 1 {
        for index in 0..before.len().max(after.len()) {
            let old = before.get(index).copied();
            let next = after.get(index).copied();
            if same_output(sources, old, next)? {
                continue;
            }
            let rank = i64::try_from(plan.rank_start)
                .unwrap_or(i64::MAX)
                .saturating_add(index as i64);
            match (old, next) {
                (Some(old), Some(next)) => {
                    if plan.generate_update_before {
                        output.push(OutputEvent {
                            candidate: old,
                            rank,
                            kind: UPDATE_BEFORE,
                        });
                    }
                    output.push(OutputEvent {
                        candidate: next,
                        rank,
                        kind: UPDATE_AFTER,
                    });
                }
                (Some(old), None) => output.push(OutputEvent {
                    candidate: old,
                    rank,
                    kind: DELETE,
                }),
                (None, Some(next)) => output.push(OutputEvent {
                    candidate: next,
                    rank,
                    kind: INSERT,
                }),
                (None, None) => unreachable!(),
            }
        }
        return Ok(());
    }
    // Flink selects FastTop1Function for both append-fast and update-fast Top-1. Unlike the
    // general no-rank-number Top-N helper, replacement of the current winner is represented as
    // an optional UPDATE_BEFORE followed by UPDATE_AFTER. This distinction is observable by
    // upsert-aware downstream operators and sinks even though DELETE + INSERT has the same
    // materialized multiset.
    if plan.rank_start == 1
        && plan.rank_end == Some(1)
        && plan.strategy != proto::TopNStrategy::Retract as i32
    {
        let old = before.first().copied();
        let next = after.first().copied();
        if same_output(sources, old, next)? {
            return Ok(());
        }
        match (old, next) {
            (Some(old), Some(next)) => {
                if plan.generate_update_before {
                    output.push(OutputEvent {
                        candidate: old,
                        rank: 0,
                        kind: UPDATE_BEFORE,
                    });
                }
                output.push(OutputEvent {
                    candidate: next,
                    rank: 0,
                    kind: UPDATE_AFTER,
                });
            }
            (None, Some(next)) => output.push(OutputEvent {
                candidate: next,
                rank: 0,
                kind: INSERT,
            }),
            (Some(old), None) => output.push(OutputEvent {
                candidate: old,
                rank: 0,
                kind: DELETE,
            }),
            (None, None) => {}
        }
        return Ok(());
    }
    for old in before {
        match after.iter().find(|next| next.sequence == old.sequence) {
            None => output.push(OutputEvent {
                candidate: *old,
                rank: 0,
                kind: DELETE,
            }),
            // Append/retract strategies keep the exact candidate reference for an unchanged
            // row. Short-circuit that identity before value equality: SQL floating equality
            // deliberately treats NaN differently from the sort comparator, but an untouched
            // NaN row must not turn into a spurious UPDATE_BEFORE/UPDATE_AFTER pair.
            Some(next) if old == next => {}
            Some(next)
                if !equal_rows(
                    sources[old.source].as_ref(),
                    old.row,
                    sources[next.source].as_ref(),
                    next.row,
                    0..sources[old.source].num_columns(),
                )? =>
            {
                if plan.generate_update_before {
                    output.push(OutputEvent {
                        candidate: *old,
                        rank: 0,
                        kind: UPDATE_BEFORE,
                    });
                }
                output.push(OutputEvent {
                    candidate: *next,
                    rank: 0,
                    kind: UPDATE_AFTER,
                });
            }
            Some(_) => {}
        }
    }
    for next in after {
        if !before.iter().any(|old| old.sequence == next.sequence) {
            output.push(OutputEvent {
                candidate: *next,
                rank: 0,
                kind: INSERT,
            });
        }
    }
    Ok(())
}

fn same_output(
    sources: &[Arc<RecordBatch>],
    left: Option<CandidateRef>,
    right: Option<CandidateRef>,
) -> Result<bool> {
    match (left, right) {
        (None, None) => Ok(true),
        (Some(left), Some(right)) if left == right => Ok(true),
        (Some(left), Some(right)) if left.sequence == right.sequence => equal_rows(
            sources[left.source].as_ref(),
            left.row,
            sources[right.source].as_ref(),
            right.row,
            0..sources[left.source].num_columns(),
        ),
        _ => Ok(false),
    }
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

fn validate_plan(plan: &proto::TopN, max_parallelism: u32) -> Result<()> {
    if max_parallelism == 0
        || max_parallelism > 32_768
        || plan.rank_start == 0
        || (plan.rank_end.is_some() == plan.variable_rank_end_index.is_some())
        || plan.input_schema.is_none()
        || plan.output_schema.is_none()
        || !matches!(
            proto::TopNStrategy::try_from(plan.strategy),
            Ok(proto::TopNStrategy::AppendFast)
                | Ok(proto::TopNStrategy::UpdateFast)
                | Ok(proto::TopNStrategy::Retract)
        )
        || plan.sort_key_indices.len() != plan.sort_ascending.len()
        || plan.sort_key_indices.len() != plan.sort_nulls_last.len()
    {
        return Err(DataFusionError::Plan(
            "top-n requires valid schemas, strategy, range, ordering, and max parallelism"
                .to_string(),
        ));
    }
    if let Some(end) = plan.rank_end {
        if end < plan.rank_start || end > i64::MAX as u64 {
            return Err(DataFusionError::Plan(
                "top-n rank end is outside its valid one-based range".to_string(),
            ));
        }
    }
    if plan.sort_key_indices.is_empty()
        && (!plan.partition_key_indices.is_empty()
            || plan.output_rank_number
            || plan.variable_rank_end_index.is_some())
    {
        return Err(DataFusionError::Plan(
            "unordered top-n is valid only for a global constant LIMIT/OFFSET".to_string(),
        ));
    }
    Ok(())
}

fn validate_output_schema(plan: &proto::TopN, input: &SchemaRef, output: &SchemaRef) -> Result<()> {
    let expected = input.fields().len() + usize::from(plan.output_rank_number);
    if output.fields().len() != expected
        || output.fields()[..input.fields().len()] != input.fields()[..]
        || (plan.output_rank_number && output.field(expected - 1).data_type() != &DataType::Int64)
    {
        return Err(DataFusionError::Plan(
            "top-n output schema must be the input plus an optional BIGINT rank".to_string(),
        ));
    }
    for &index in plan
        .partition_key_indices
        .iter()
        .chain(&plan.sort_key_indices)
        .chain(&plan.primary_key_indices)
    {
        if index as usize >= input.fields().len() {
            return Err(DataFusionError::Plan(format!(
                "top-n field index {index} is outside the input"
            )));
        }
    }
    Ok(())
}

fn metadata_index(schema: &SchemaRef, name: &str) -> Option<usize> {
    schema
        .fields()
        .iter()
        .position(|field| field.name() == name)
}

fn fields_compatible(expected: &Field, actual: &Field, compare_name: bool) -> bool {
    (!compare_name || expected.name() == actual.name())
        && expected.is_nullable() == actual.is_nullable()
        && data_types_compatible(expected.data_type(), actual.data_type())
}

fn data_types_compatible(expected: &DataType, actual: &DataType) -> bool {
    match (expected, actual) {
        (DataType::List(left), DataType::List(right))
        | (DataType::LargeList(left), DataType::LargeList(right)) => {
            fields_compatible(left, right, false)
        }
        (DataType::FixedSizeList(left, left_size), DataType::FixedSizeList(right, right_size)) => {
            left_size == right_size && fields_compatible(left, right, false)
        }
        (DataType::Struct(left), DataType::Struct(right)) => {
            left.len() == right.len()
                && left
                    .iter()
                    .zip(right.iter())
                    .all(|(left, right)| fields_compatible(left, right, true))
        }
        (DataType::Map(left, left_sorted), DataType::Map(right, right_sorted)) => {
            left_sorted == right_sorted && fields_compatible(left, right, false)
        }
        _ => expected == actual,
    }
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
mod tests {
    use super::*;
    use crate::memory_pool::{tests_support::TestBroker, HostMemoryReservation};
    use arrow::array::{Int32Array, StringArray};
    use prost::Message;

    fn schema() -> SchemaRef {
        Arc::new(Schema::new(vec![
            Field::new("key", DataType::Int32, false),
            Field::new("value", DataType::Utf8, true),
        ]))
    }

    fn plan() -> Vec<u8> {
        let schema = proto::Schema {
            fields: vec![
                proto::Field {
                    name: "key".to_string(),
                    r#type: Some(proto::LogicalType {
                        nullable: false,
                        r#type: Some(proto::logical_type::Type::Integer(
                            proto::EmptyType::default(),
                        )),
                    }),
                },
                proto::Field {
                    name: "value".to_string(),
                    r#type: Some(proto::LogicalType {
                        nullable: true,
                        r#type: Some(proto::logical_type::Type::Varchar(
                            proto::EmptyType::default(),
                        )),
                    }),
                },
            ],
        };
        let mut output = schema.clone();
        output.fields.push(proto::Field {
            name: "row_num".to_string(),
            r#type: Some(proto::LogicalType {
                nullable: false,
                r#type: Some(proto::logical_type::Type::Bigint(
                    proto::EmptyType::default(),
                )),
            }),
        });
        proto::NativePlan {
            protocol_version: 1,
            root: Some(proto::Operator {
                plan_node_id: 0,
                operator: Some(proto::operator::Operator::TopN(Box::new(proto::TopN {
                    input: Some(Box::new(proto::Operator {
                        plan_node_id: 0,
                        operator: Some(proto::operator::Operator::Input(proto::Input::default())),
                    })),
                    partition_key_indices: vec![0],
                    sort_key_indices: vec![1],
                    primary_key_indices: vec![],
                    rank_start: 1,
                    rank_end: Some(2),
                    variable_rank_end_index: None,
                    output_rank_number: true,
                    generate_update_before: true,
                    strategy: proto::TopNStrategy::AppendFast as i32,
                    input_schema: Some(schema),
                    state_ttl_millis: 0,
                    sort_ascending: vec![false],
                    sort_nulls_last: vec![true],
                    output_schema: Some(output),
                }))),
            }),
        }
        .encode_to_vec()
    }

    fn batch(keys: Vec<i32>, values: Vec<&str>) -> RecordBatch {
        let rows = keys.len();
        batch_with_kinds(keys, values, vec![INSERT; rows])
    }

    fn batch_with_kinds(keys: Vec<i32>, values: Vec<&str>, kinds: Vec<i8>) -> RecordBatch {
        let rows = keys.len();
        assert_eq!(values.len(), rows);
        assert_eq!(kinds.len(), rows);
        RecordBatch::try_new(
            Arc::new(Schema::new(vec![
                Field::new("key", DataType::Int32, false),
                Field::new("value", DataType::Utf8, true),
                Field::new(INPUT_KIND_COLUMN, DataType::Int8, false),
            ])),
            vec![
                Arc::new(Int32Array::from(keys)),
                Arc::new(StringArray::from(values)),
                Arc::new(Int8Array::from(kinds)),
            ],
        )
        .unwrap()
    }

    fn top_one_plan(generate_update_before: bool) -> Vec<u8> {
        let bytes = plan();
        let mut native = proto::NativePlan::decode(bytes.as_slice()).unwrap();
        let Some(proto::operator::Operator::TopN(top_n)) =
            native.root.as_mut().and_then(|root| root.operator.as_mut())
        else {
            unreachable!()
        };
        top_n.rank_end = Some(1);
        top_n.output_rank_number = false;
        top_n.generate_update_before = generate_update_before;
        top_n.output_schema = top_n.input_schema.clone();
        native.encode_to_vec()
    }

    fn output_kinds(batch: &RecordBatch) -> Vec<i8> {
        batch
            .column(batch.num_columns() - 1)
            .as_any()
            .downcast_ref::<Int8Array>()
            .unwrap()
            .values()
            .to_vec()
    }

    #[test]
    fn append_fast_top_one_uses_flinks_update_changelog() {
        for (generate_update_before, expected) in [
            (false, vec![INSERT, UPDATE_AFTER]),
            (true, vec![INSERT, UPDATE_BEFORE, UPDATE_AFTER]),
        ] {
            let broker = Arc::new(TestBroker::new(64 << 20));
            let mut processor = TopNProcessor::new(
                &top_one_plan(generate_update_before),
                128,
                0,
                127,
                HostMemoryReservation::new(broker, "fast top-one changelog"),
            )
            .unwrap();
            let first = processor
                .process_arrow(batch(vec![1], vec!["a"]), 1)
                .unwrap();
            let replacement = processor
                .process_arrow(batch(vec![1], vec!["z"]), 2)
                .unwrap();
            assert_eq!(
                [output_kinds(&first), output_kinds(&replacement)].concat(),
                expected
            );
        }
    }

    fn limit_plan(strategy: proto::TopNStrategy) -> Vec<u8> {
        let schema = proto::Schema {
            fields: vec![
                proto::Field {
                    name: "key".to_string(),
                    r#type: Some(proto::LogicalType {
                        nullable: false,
                        r#type: Some(proto::logical_type::Type::Integer(
                            proto::EmptyType::default(),
                        )),
                    }),
                },
                proto::Field {
                    name: "value".to_string(),
                    r#type: Some(proto::LogicalType {
                        nullable: true,
                        r#type: Some(proto::logical_type::Type::Varchar(
                            proto::EmptyType::default(),
                        )),
                    }),
                },
            ],
        };
        proto::NativePlan {
            protocol_version: 1,
            root: Some(proto::Operator {
                plan_node_id: 0,
                operator: Some(proto::operator::Operator::TopN(Box::new(proto::TopN {
                    input: Some(Box::new(proto::Operator {
                        plan_node_id: 0,
                        operator: Some(proto::operator::Operator::Input(proto::Input::default())),
                    })),
                    partition_key_indices: vec![],
                    sort_key_indices: vec![],
                    primary_key_indices: vec![],
                    rank_start: 2,
                    rank_end: Some(3),
                    variable_rank_end_index: None,
                    output_rank_number: false,
                    generate_update_before: true,
                    strategy: strategy as i32,
                    input_schema: Some(schema.clone()),
                    state_ttl_millis: 0,
                    sort_ascending: vec![],
                    sort_nulls_last: vec![],
                    output_schema: Some(schema),
                }))),
            }),
        }
        .encode_to_vec()
    }

    fn output_values(batch: &RecordBatch) -> Vec<&str> {
        batch
            .column(1)
            .as_any()
            .downcast_ref::<StringArray>()
            .unwrap()
            .iter()
            .map(|value| value.unwrap())
            .collect()
    }

    #[test]
    fn unordered_global_limit_preserves_input_order_and_offset_across_batches() {
        let broker = Arc::new(TestBroker::new(64 << 20));
        let mut processor = TopNProcessor::new(
            &limit_plan(proto::TopNStrategy::AppendFast),
            128,
            0,
            127,
            HostMemoryReservation::new(broker.clone(), "native limit"),
        )
        .unwrap();

        let first = processor
            .process_arrow(batch(vec![1, 2], vec!["skip", "first"]), 1)
            .unwrap();
        assert_eq!(output_values(&first), vec!["first"]);
        let second = processor
            .process_arrow(batch(vec![3, 4], vec!["second", "ignored"]), 2)
            .unwrap();
        assert_eq!(output_values(&second), vec!["second"]);
        let state_io_after_saturation = processor.statistics()[..4].to_vec();
        let ignored = processor
            .process_arrow(batch(vec![5, 6], vec!["ignored", "ignored"]), 3)
            .unwrap();
        assert_eq!(ignored.num_rows(), 0);
        assert_eq!(processor.statistics()[..4], state_io_after_saturation);
        assert_eq!(processor.statistics()[5], 0);

        let key_group = assign_key_group(&[], 128);
        let snapshot = processor.snapshot_key_group(key_group).unwrap();
        let mut restored = TopNProcessor::new(
            &limit_plan(proto::TopNStrategy::AppendFast),
            128,
            0,
            127,
            HostMemoryReservation::new(broker.clone(), "restored native limit"),
        )
        .unwrap();
        restored.restore_key_group(key_group, &snapshot).unwrap();
        assert!(!restored.is_append_limit_saturated());
        let after_restore = restored
            .process_arrow(batch(vec![7], vec!["ignored"]), 4)
            .unwrap();
        assert_eq!(after_restore.num_rows(), 0);
        assert!(restored.is_append_limit_saturated());
        assert_eq!(restored.statistics()[0], 1);
        assert_eq!(restored.statistics()[1], 1);
    }

    #[test]
    fn unordered_global_limit_accepts_retractions() {
        let broker = Arc::new(TestBroker::new(64 << 20));
        let mut processor = TopNProcessor::new(
            &limit_plan(proto::TopNStrategy::Retract),
            128,
            0,
            127,
            HostMemoryReservation::new(broker, "retractable native limit"),
        )
        .unwrap();
        let initial = processor
            .process_arrow(batch(vec![1, 2, 3], vec!["skip", "first", "second"]), 1)
            .unwrap();
        assert_eq!(output_values(&initial), vec!["first", "second"]);

        let retracted = processor
            .process_arrow(batch_with_kinds(vec![2], vec!["first"], vec![DELETE]), 2)
            .unwrap();
        assert_eq!(retracted.num_rows(), 3);
        assert_eq!(processor.statistics()[6], 0);
    }

    #[test]
    fn processes_arrow_and_restores_canonical_state_after_rescaling() {
        let source_broker = Arc::new(TestBroker::new(64 << 20));
        let mut source = TopNProcessor::new(
            &plan(),
            128,
            0,
            127,
            HostMemoryReservation::new(source_broker.clone(), "top-n source"),
        )
        .unwrap();
        let output = source
            .process_arrow(batch(vec![1, 1, 2], vec!["a", "c", "b"]), 1)
            .unwrap();
        assert_eq!(output.num_rows(), 5);
        assert_eq!(source.statistics()[..4], [1, 1, 2, 2]);

        let key_batch = RecordBatch::try_new(
            schema(),
            vec![
                Arc::new(Int32Array::from(vec![1])) as ArrayRef,
                Arc::new(StringArray::from(vec!["unused"])) as ArrayRef,
            ],
        )
        .unwrap();
        let key = encode_binary_row(&key_batch, 0, &[(0, KeyField::Integer)]).unwrap();
        let key_group = assign_key_group(&key, 128);
        let snapshot = source.snapshot_key_group(key_group).unwrap();
        assert!(source_broker.reserved() > 0);
        drop(output);
        drop(source);
        assert_eq!(source_broker.reserved(), 0);

        let restored_broker = Arc::new(TestBroker::new(64 << 20));
        let mut restored = TopNProcessor::new(
            &plan(),
            128,
            key_group,
            key_group,
            HostMemoryReservation::new(restored_broker.clone(), "top-n restored"),
        )
        .unwrap();
        restored.restore_key_group(key_group, &snapshot).unwrap();
        let output = restored
            .process_arrow(batch(vec![1], vec!["b"]), 2)
            .unwrap();
        assert!(output.num_rows() > 0);
        drop(output);
        drop(restored);
        assert_eq!(restored_broker.reserved(), 0);
    }

    #[test]
    fn canonical_arrow_state_round_trips_payloads() {
        let rows = RecordBatch::try_new(
            schema(),
            vec![
                Arc::new(Int32Array::from(vec![1])) as ArrayRef,
                Arc::new(StringArray::from(vec![Some("payload")])) as ArrayRef,
            ],
        )
        .unwrap();
        let state = StoredState {
            next_sequence: 2,
            rank_end: Some(2),
            last_access_millis: 9,
            sequences: vec![1],
            rows: rows.clone(),
        };
        let converter = row_converter(&schema()).unwrap();
        let decoded = decode_state(
            &encode_state(&state, &converter).unwrap(),
            &schema(),
            &converter,
        )
        .unwrap();
        assert_eq!(decoded.next_sequence, 2);
        assert_eq!(decoded.rank_end, Some(2));
        assert_eq!(decoded.sequences, vec![1]);
        assert_eq!(decoded.rows, rows);
    }

    #[test]
    fn rejects_unadmitted_working_memory_and_releases_every_reservation() {
        let broker = Arc::new(TestBroker::new(4 << 10));
        let mut processor = TopNProcessor::new(
            &plan(),
            1,
            0,
            0,
            HostMemoryReservation::new(broker.clone(), "top-n constrained"),
        )
        .unwrap();
        let payload = "x".repeat(8 << 10);
        let result = processor.process_arrow(batch(vec![1], vec![payload.as_str()]), 1);
        assert!(matches!(
            result,
            Err(DataFusionError::ResourcesExhausted(_))
        ));
        drop(processor);
        assert_eq!(broker.reserved(), 0);
    }
}
