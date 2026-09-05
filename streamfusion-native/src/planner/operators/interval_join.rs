// Copyright 2026 StreamFusion Authors
// Licensed under the Apache License, Version 2.0

use std::collections::{BTreeMap, BTreeSet};
use std::sync::Arc;

use ahash::RandomState;
use arrow::array::{Array, Int32Array, Int8Array, UInt32Array};
use arrow::compute::{take, SortOptions};
use arrow::datatypes::{DataType, Field, Schema, SchemaRef};
use arrow::record_batch::RecordBatch;
use arrow_row::{RowConverter, Rows, SortField};
use datafusion::error::{DataFusionError, Result};
use hashbrown::HashMap;

use crate::exchange::{assign_key_group, KeyField};
use crate::memory_pool::HostMemoryReservation;
use crate::planner::arrow_schema;
use crate::state::{
    KeyedState, MemoryKeyedState, NativeTimerService, RocksPluginKeyedState, StateKey, StateKeyRef,
    StateMutation, TimerDomain, TimerKey,
};
use crate::{decode_plan, proto};

use super::stateful_utils::{
    finish_output, group_key, metadata_index, restore_timer_state, timer_statistics,
};
use super::window_table_function::timestamp_millis;

mod state_codec;
#[cfg(test)]
mod tests;

use state_codec::{decode_state, encode_state, IntervalState, StagedState, StoredRow};

const INSERT: i8 = 0;
const UPDATE_BEFORE: i8 = 1;
const UPDATE_AFTER: i8 = 2;
const DELETE: i8 = 3;
const JOIN_STATE_PREFIX: u8 = 1;
const TIMER_STATE_KEY: &[u8] = b"\0streamfusion-interval-join-timers";

#[derive(Clone)]
struct OutputRow {
    left: Option<Vec<u8>>,
    right: Option<Vec<u8>>,
    kind: i8,
    input_ordinal: i32,
}

/// Persistent Arrow-native implementation of Flink's time-bounded streaming join.
pub(crate) struct IntervalJoinProcessor {
    plan: proto::IntervalJoin,
    join_type: proto::RegularJoinType,
    max_parallelism: u32,
    state: Box<dyn KeyedState>,
    timers: NativeTimerService,
    visible_schemas: [SchemaRef; 2],
    output_schema: SchemaRef,
    row_converters: [RowConverter; 2],
    input_schemas: [Option<SchemaRef>; 2],
    key_fields: [Vec<(usize, KeyField)>; 2],
    preencoded_key_indices: [Option<usize>; 2],
    input_kind_indices: [Option<usize>; 2],
    current_event_time: i64,
    current_processing_time: i64,
    scratch_reservation: HostMemoryReservation,
    state_read_batches: u64,
    state_write_batches: u64,
    dirty_timer_groups: BTreeSet<u32>,
    timer_registrations: u64,
    timer_deletions: u64,
    timers_fired: u64,
}

impl IntervalJoinProcessor {
    pub(crate) fn new(
        serialized_plan: &[u8],
        max_parallelism: u32,
        first_key_group: u32,
        last_key_group: u32,
        state_reservation: HostMemoryReservation,
    ) -> Result<Self> {
        let timer_reservation = state_reservation.sibling("native interval join timers");
        let scratch_reservation =
            state_reservation.sibling("native interval join batch scratch and output");
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
            timer_reservation,
            scratch_reservation,
        )
    }

    #[allow(clippy::too_many_arguments)]
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
        let timer_reservation = reservation.sibling("native RocksDB interval join timers");
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
            timer_reservation,
            reservation,
        )
    }

    #[allow(clippy::too_many_arguments)]
    fn with_state(
        serialized_plan: &[u8],
        max_parallelism: u32,
        first_key_group: u32,
        last_key_group: u32,
        state: Box<dyn KeyedState>,
        timer_reservation: HostMemoryReservation,
        scratch_reservation: HostMemoryReservation,
    ) -> Result<Self> {
        let native_plan = decode_plan(serialized_plan)?;
        let root = native_plan
            .root
            .ok_or_else(|| DataFusionError::Plan("interval join plan has no root".to_string()))?;
        let plan = match root.operator {
            Some(proto::operator::Operator::IntervalJoin(plan)) => plan,
            _ => {
                return Err(DataFusionError::Plan(
                    "interval join handle requires an IntervalJoin root".to_string(),
                ));
            }
        };
        validate_plan(&plan, max_parallelism)?;
        let join_type = proto::RegularJoinType::try_from(plan.join_type).map_err(|_| {
            DataFusionError::Plan(format!("unknown interval join type {}", plan.join_type))
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
        for (side, schema) in visible_schemas.iter().enumerate() {
            for (index, field) in schema.fields().iter().enumerate() {
                output_fields.push(Arc::new(Field::new(
                    format!("__streamfusion_interval_join_{side}_{index}"),
                    field.data_type().clone(),
                    field.is_nullable()
                        || (side == 0
                            && matches!(
                                join_type,
                                proto::RegularJoinType::Right | proto::RegularJoinType::Full
                            ))
                        || (side == 1
                            && matches!(
                                join_type,
                                proto::RegularJoinType::Left | proto::RegularJoinType::Full
                            )),
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
            timers: NativeTimerService::new(first_key_group, last_key_group, timer_reservation)?,
            visible_schemas,
            output_schema: Arc::new(Schema::new(output_fields)),
            row_converters,
            input_schemas: [None, None],
            key_fields: [Vec::new(), Vec::new()],
            preencoded_key_indices: [None, None],
            input_kind_indices: [None, None],
            current_event_time: i64::MIN,
            current_processing_time: i64::MIN,
            scratch_reservation,
            state_read_batches: 0,
            state_write_batches: 0,
            dirty_timer_groups: BTreeSet::new(),
            timer_registrations: 0,
            timer_deletions: 0,
            timers_fired: 0,
        })
    }

    pub(crate) fn process_arrow(
        &mut self,
        side: usize,
        batch: RecordBatch,
        processing_time: i64,
    ) -> Result<RecordBatch> {
        if side > 1 {
            return Err(DataFusionError::Execution(
                "interval join side must be zero or one".to_string(),
            ));
        }
        if !self.plan.event_time {
            self.current_processing_time = self.current_processing_time.max(processing_time);
        }
        self.prepare_schema(side, batch.schema())?;
        let visible_count = self.visible_schemas[side].fields().len();
        let input_bytes = batch.columns()[..visible_count]
            .iter()
            .map(|column| column.get_array_memory_size())
            .sum::<usize>();
        let base = input_bytes.saturating_add(batch.num_rows().saturating_mul(320));
        self.scratch_reservation.resize(base)?;
        let encoded = self.row_converters[side].convert_columns(&batch.columns()[..visible_count]);
        let result = match encoded {
            Ok(encoded) => self.process_accounted(side, &batch, &encoded),
            Err(error) => Err(error.into()),
        };
        match result {
            Ok(output) => finish_output(output, base, &mut self.scratch_reservation),
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
                DataFusionError::Execution("interval join RowKinds are not Int8".to_string())
            })?;
        let mut unique = HashMap::<StateKey, usize, RandomState>::with_capacity_and_hasher(
            batch.num_rows(),
            RandomState::new(),
        );
        let mut changes = Vec::with_capacity(batch.num_rows());
        for row in 0..batch.num_rows() {
            let group_key = group_key(
                batch,
                row,
                self.preencoded_key_indices[side],
                &self.key_fields[side],
                "interval join",
            )?;
            let key_group = assign_key_group(&group_key, self.max_parallelism);
            let state_key = join_state_key(key_group, &group_key);
            let next = unique.len();
            let index = *unique.entry(state_key).or_insert(next);
            let time = self.row_time(side, batch, row)?;
            let kind = kinds.value(row);
            let accumulate = match kind {
                INSERT | UPDATE_AFTER => true,
                UPDATE_BEFORE | DELETE => false,
                other => {
                    return Err(DataFusionError::Execution(format!(
                        "unknown Flink RowKind byte {other}"
                    )));
                }
            };
            let ordinal = i32::try_from(row).map_err(|_| {
                DataFusionError::Execution("interval join batch exceeds i32 rows".to_string())
            })?;
            changes.push((
                index,
                time,
                kind,
                accumulate,
                self.row_is_matchable(side, batch, row),
                encoded.row(row).data().to_vec(),
                ordinal,
            ));
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
            .map(|key| key.expect("interval join state index is populated"))
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
            .map(|(key, value)| {
                Ok(StagedState {
                    key,
                    value: value
                        .map(|value| decode_state(value.as_ref()))
                        .transpose()?
                        .unwrap_or_default(),
                    touched: false,
                })
            })
            .collect::<Result<Vec<_>>>()?;
        let mut output = Vec::new();
        let mut dirty_timer_groups = BTreeSet::new();
        for (index, time, kind, accumulate, matchable, row, ordinal) in changes {
            let entry = &mut staged[index];
            self.process_change(
                side,
                time,
                kind,
                accumulate,
                matchable,
                row,
                ordinal,
                entry.key.key_group,
                &entry.key.key,
                &mut entry.value,
                &mut output,
                &mut dirty_timer_groups,
            )?;
            entry.touched = true;
        }
        let mutations = staged
            .into_iter()
            .filter(|entry| entry.touched)
            .map(|entry| StateMutation {
                key: entry.key,
                value: (!state_is_empty(&entry.value)).then(|| encode_state(&entry.value)),
            })
            .collect::<Vec<_>>();
        if !mutations.is_empty() {
            self.state.write_batch(mutations)?;
            self.state_write_batches = self.state_write_batches.saturating_add(1);
        }
        self.dirty_timer_groups.extend(dirty_timer_groups);
        self.output_batch(output)
    }

    #[allow(clippy::too_many_arguments)]
    fn process_change(
        &mut self,
        side: usize,
        time: i64,
        kind: i8,
        accumulate: bool,
        matchable: bool,
        row: Vec<u8>,
        ordinal: i32,
        key_group: u32,
        state_key: &[u8],
        state: &mut IntervalState,
        output: &mut Vec<OutputRow>,
        dirty_timer_groups: &mut BTreeSet<u32>,
    ) -> Result<()> {
        let progress = self.operator_time();
        let other_expiration = self.expiration_time(1 - side, progress);
        self.expire_rows(1 - side, other_expiration, state, output, ordinal);
        let (lower, upper) = self.qualified_bounds(side, time);
        let input_outer = is_outer(self.join_type, side);
        let other_outer = is_outer(self.join_type, 1 - side);
        let matches = if matchable {
            rows_in_range_mut(side, state, lower, upper)
        } else {
            Vec::new()
        };
        let match_count = matches.len();
        if accumulate {
            for candidate in matches {
                if other_outer {
                    candidate.associations = candidate.associations.wrapping_add(1);
                }
                push_pair(
                    output,
                    side,
                    Some(row.clone()),
                    Some(candidate.row.clone()),
                    kind,
                    ordinal,
                );
            }
            if self.operator_time() < upper {
                let own = side_rows_mut(side, state);
                own.entry(time).or_default().push(StoredRow {
                    row,
                    associations: if input_outer { match_count as i32 } else { 0 },
                });
                if state.cleanup_timers[side].is_none() {
                    self.register_cleanup_timer(
                        side,
                        time,
                        key_group,
                        state_key,
                        state,
                        dirty_timer_groups,
                    )?;
                }
            } else if match_count == 0 && input_outer {
                push_pair(output, side, Some(row), None, INSERT, ordinal);
            }
        } else {
            for candidate in matches {
                push_pair(
                    output,
                    side,
                    Some(row.clone()),
                    Some(candidate.row.clone()),
                    kind,
                    ordinal,
                );
                if other_outer {
                    candidate.associations = candidate.associations.wrapping_sub(1);
                }
            }
            let own = side_rows_mut(side, state);
            if let Some(rows) = own.get_mut(&time) {
                if let Some(position) = rows.iter().position(|candidate| candidate.row == row) {
                    rows.remove(position);
                }
                if rows.is_empty() {
                    own.remove(&time);
                }
            }
            if self.operator_time() >= upper && match_count == 0 && input_outer {
                push_pair(output, side, Some(row), None, kind, ordinal);
            }
        }
        Ok(())
    }

    pub(crate) fn advance_event_time(&mut self, watermark: i64) -> Result<RecordBatch> {
        if watermark <= self.current_event_time {
            return self.empty_output();
        }
        self.current_event_time = watermark;
        let output = self.fire(TimerDomain::EventTime, watermark)?;
        finish_output(output, 0, &mut self.scratch_reservation)
    }

    pub(crate) fn advance_processing_time(&mut self, timestamp: i64) -> Result<RecordBatch> {
        if timestamp <= self.current_processing_time {
            return self.empty_output();
        }
        self.current_processing_time = timestamp;
        let output = self.fire(TimerDomain::ProcessingTime, timestamp)?;
        finish_output(output, 0, &mut self.scratch_reservation)
    }

    fn fire(&mut self, domain: TimerDomain, progress: i64) -> Result<RecordBatch> {
        if domain != self.timer_domain() {
            return self.empty_output();
        }
        let fired = self.timers.advance(domain, progress)?;
        self.timers_fired = self.timers_fired.saturating_add(fired.len() as u64);
        if fired.is_empty() {
            return self.empty_output();
        }
        let mut grouped = BTreeMap::<(u32, Vec<u8>), Vec<TimerKey>>::new();
        for fired in fired {
            grouped
                .entry((fired.key_group, fired.timer.key.clone()))
                .or_default()
                .push(fired.timer);
        }
        let keys = grouped
            .keys()
            .map(|(key_group, key)| StateKey {
                key_group: *key_group,
                key: key.clone(),
            })
            .collect::<Vec<_>>();
        let refs = keys
            .iter()
            .map(|key| StateKeyRef {
                key_group: key.key_group,
                key: &key.key,
            })
            .collect::<Vec<_>>();
        let values = self
            .state
            .get_batch(&refs)?
            .into_iter()
            .map(|value| value.map(|value| value.into_owned()))
            .collect::<Vec<_>>();
        self.state_read_batches = self.state_read_batches.saturating_add(1);
        let mut output = Vec::new();
        let mut mutations = Vec::with_capacity(keys.len() * 2);
        let mut dirty_timer_groups = BTreeSet::new();
        for (((key_group, key_bytes), timers), value) in grouped.into_iter().zip(values) {
            let key = StateKey {
                key_group,
                key: key_bytes,
            };
            dirty_timer_groups.insert(key.key_group);
            let Some(value) = value else {
                continue;
            };
            let mut state = decode_state(&value)?;
            for timer in &timers {
                let side = timer_side(&timer.namespace)?;
                if state.cleanup_timers[side] != Some(timer.timestamp) {
                    continue;
                }
                state.cleanup_timers[side] = None;
                let expiration = self.expiration_time(side, progress);
                self.expire_rows(side, expiration, &mut state, &mut output, -1);
                if let Some(earliest) = side_rows(side, &state).keys().next().copied() {
                    self.register_cleanup_timer(
                        side,
                        earliest,
                        key.key_group,
                        &key.key,
                        &mut state,
                        &mut dirty_timer_groups,
                    )?;
                }
            }
            mutations.push(StateMutation {
                key,
                value: (!state_is_empty(&state)).then(|| encode_state(&state)),
            });
        }
        self.state.write_batch(mutations)?;
        self.state_write_batches = self.state_write_batches.saturating_add(1);
        self.dirty_timer_groups.extend(dirty_timer_groups);
        self.output_batch(output)
    }

    fn expire_rows(
        &self,
        side: usize,
        expiration: i64,
        state: &mut IntervalState,
        output: &mut Vec<OutputRow>,
        ordinal: i32,
    ) {
        let outer = is_outer(self.join_type, side);
        let rows = side_rows_mut(side, state);
        let expired = rows
            .range(..=expiration)
            .map(|(&time, _)| time)
            .collect::<Vec<_>>();
        for time in expired {
            if let Some(values) = rows.remove(&time) {
                if outer {
                    for value in values {
                        if value.associations == 0 {
                            push_pair(output, side, Some(value.row), None, INSERT, ordinal);
                        }
                    }
                }
            }
        }
    }

    fn register_cleanup_timer(
        &mut self,
        side: usize,
        row_time: i64,
        key_group: u32,
        state_key: &[u8],
        state: &mut IntervalState,
        dirty_timer_groups: &mut BTreeSet<u32>,
    ) -> Result<()> {
        let timestamp = self.cleanup_time(side, row_time);
        let timer = TimerKey {
            timestamp,
            key: state_key.to_vec(),
            namespace: vec![side as u8],
        };
        if self
            .timers
            .register(key_group, self.timer_domain(), timer)?
        {
            self.timer_registrations = self.timer_registrations.saturating_add(1);
            dirty_timer_groups.insert(key_group);
        }
        state.cleanup_timers[side] = Some(timestamp);
        Ok(())
    }

    fn cleanup_time(&self, side: usize, row_time: i64) -> i64 {
        row_time
            .wrapping_add(self.relative_size(side))
            .wrapping_add(self.plan.min_cleanup_interval_millis as i64)
            .wrapping_add(self.plan.allowed_lateness_millis as i64)
            .wrapping_add(1)
    }

    fn expiration_time(&self, side: usize, progress: i64) -> i64 {
        if progress == i64::MAX {
            i64::MAX
        } else {
            progress
                .wrapping_sub(self.relative_size(side))
                .wrapping_sub(self.plan.allowed_lateness_millis as i64)
                .wrapping_sub(1)
        }
    }

    fn relative_size(&self, side: usize) -> i64 {
        if side == 0 {
            self.plan.left_lower_bound_millis.wrapping_neg()
        } else {
            self.plan.left_upper_bound_millis
        }
    }

    fn qualified_bounds(&self, side: usize, time: i64) -> (i64, i64) {
        if side == 0 {
            (
                time.wrapping_sub(self.plan.left_upper_bound_millis),
                time.wrapping_sub(self.plan.left_lower_bound_millis),
            )
        } else {
            (
                time.wrapping_add(self.plan.left_lower_bound_millis),
                time.wrapping_add(self.plan.left_upper_bound_millis),
            )
        }
    }

    fn operator_time(&self) -> i64 {
        if self.plan.event_time {
            self.current_event_time.max(0)
        } else {
            self.current_processing_time.max(0)
        }
    }

    fn timer_domain(&self) -> TimerDomain {
        if self.plan.event_time {
            TimerDomain::EventTime
        } else {
            TimerDomain::ProcessingTime
        }
    }

    fn row_time(&self, side: usize, batch: &RecordBatch, row: usize) -> Result<i64> {
        if !self.plan.event_time {
            return Ok(self.operator_time());
        }
        Ok(timestamp_millis(batch.column(self.time_index(side)), row)?.unwrap_or(0))
    }

    fn time_index(&self, side: usize) -> usize {
        if side == 0 {
            self.plan.left_time_index as usize
        } else {
            self.plan.right_time_index as usize
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
                    "interval join input {side} schema changed while running"
                )));
            }
            return Ok(());
        }
        self.preencoded_key_indices[side] = metadata_index(&schema, "__streamfusion_key");
        self.input_kind_indices[side] = metadata_index(&schema, "__streamfusion_input_row_kind")
            .or_else(|| metadata_index(&schema, "__streamfusion_row_kind"));
        if self.input_kind_indices[side].is_none() {
            return Err(DataFusionError::Execution(
                "interval join requires RowKind metadata".to_string(),
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
            || self.time_index(side) >= visible_count
        {
            return Err(DataFusionError::Execution(format!(
                "interval join input {side} Arrow schema does not match its protobuf schema"
            )));
        }
        if self.preencoded_key_indices[side].is_none() {
            self.key_fields[side] = self
                .key_indices(side)
                .iter()
                .map(|&index| {
                    let field = schema.fields().get(index as usize).ok_or_else(|| {
                        arrow::error::ArrowError::SchemaError(format!(
                            "interval join input {side} key {index} is outside the visible row"
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

    pub(crate) fn next_processing_timer(&self) -> Option<i64> {
        self.timers.next_timestamp(TimerDomain::ProcessingTime)
    }

    pub(crate) fn statistics(&self) -> [u64; 7] {
        timer_statistics(
            self.state_read_batches,
            self.state_write_batches,
            self.timer_registrations,
            self.timer_deletions,
            self.timers_fired,
            &self.timers,
        )
    }

    pub(crate) fn snapshot_key_group(&mut self, key_group: u32) -> Result<Vec<u8>> {
        self.flush_timer_groups([key_group])?;
        self.state.snapshot_key_group(key_group)
    }

    pub(crate) fn restore_key_group(&mut self, key_group: u32, bytes: &[u8]) -> Result<()> {
        restore_timer_state(
            self.state.as_mut(),
            &mut self.timers,
            key_group,
            bytes,
            TIMER_STATE_KEY,
            &mut self.state_read_batches,
        )?;
        self.dirty_timer_groups.remove(&key_group);
        Ok(())
    }

    pub(crate) fn checkpoint(&mut self, directory: &std::path::Path) -> Result<()> {
        let dirty = self.dirty_timer_groups.iter().copied().collect::<Vec<_>>();
        self.flush_timer_groups(dirty)?;
        self.state.checkpoint(directory)
    }

    fn flush_timer_groups(&mut self, key_groups: impl IntoIterator<Item = u32>) -> Result<()> {
        let key_groups = key_groups
            .into_iter()
            .filter(|key_group| self.dirty_timer_groups.contains(key_group))
            .collect::<Vec<_>>();
        if key_groups.is_empty() {
            return Ok(());
        }
        let mutations = key_groups
            .iter()
            .map(|&key_group| {
                Ok(StateMutation {
                    key: StateKey {
                        key_group,
                        key: TIMER_STATE_KEY.to_vec(),
                    },
                    value: Some(self.timers.snapshot_key_group(key_group)?),
                })
            })
            .collect::<Result<Vec<_>>>()?;
        self.state.write_batch(mutations)?;
        self.state_write_batches = self.state_write_batches.saturating_add(1);
        for key_group in key_groups {
            self.dirty_timer_groups.remove(&key_group);
        }
        Ok(())
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
                            "interval join output exceeds u32 rows".to_string(),
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
        let mut columns = Vec::new();
        for side in 0..2 {
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
}

fn rows_in_range_mut(
    input_side: usize,
    state: &mut IntervalState,
    lower: i64,
    upper: i64,
) -> Vec<&mut StoredRow> {
    // Flink performs these bound calculations with wrapping Java longs. At the
    // extreme values an overflow can invert the range; its ordered-state scan
    // is then empty, whereas BTreeMap::range would panic for the same bounds.
    if lower > upper {
        return Vec::new();
    }
    let rows = side_rows_mut(1 - input_side, state);
    rows.range_mut(lower..=upper)
        .flat_map(|(_, rows)| rows.iter_mut())
        .collect()
}

fn side_rows(side: usize, state: &IntervalState) -> &BTreeMap<i64, Vec<StoredRow>> {
    if side == 0 {
        &state.left
    } else {
        &state.right
    }
}

fn side_rows_mut(side: usize, state: &mut IntervalState) -> &mut BTreeMap<i64, Vec<StoredRow>> {
    if side == 0 {
        &mut state.left
    } else {
        &mut state.right
    }
}

fn state_is_empty(state: &IntervalState) -> bool {
    state.left.is_empty()
        && state.right.is_empty()
        && state.cleanup_timers.iter().all(Option::is_none)
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
    input_ordinal: i32,
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
        input_ordinal,
    });
}

fn join_state_key(key_group: u32, group_key: &[u8]) -> StateKey {
    let mut key = Vec::with_capacity(group_key.len() + 1);
    key.push(JOIN_STATE_PREFIX);
    key.extend_from_slice(group_key);
    StateKey { key_group, key }
}

fn timer_side(namespace: &[u8]) -> Result<usize> {
    match namespace {
        [0] => Ok(0),
        [1] => Ok(1),
        _ => Err(DataFusionError::Execution(
            "interval join timer namespace is invalid".to_string(),
        )),
    }
}

fn validate_plan(plan: &proto::IntervalJoin, max_parallelism: u32) -> Result<()> {
    let join_type = proto::RegularJoinType::try_from(plan.join_type).ok();
    if max_parallelism == 0
        || plan.left_key_indices.len() != plan.right_key_indices.len()
        || plan.left_key_indices.len() != plan.filter_nulls.len()
        || plan.left_schema.is_none()
        || plan.right_schema.is_none()
        || !matches!(
            join_type,
            Some(
                proto::RegularJoinType::Inner
                    | proto::RegularJoinType::Left
                    | proto::RegularJoinType::Right
                    | proto::RegularJoinType::Full
            )
        )
        || plan.left_lower_bound_millis > plan.left_upper_bound_millis
        || plan.allowed_lateness_millis > i64::MAX as u64
        || plan.min_cleanup_interval_millis > i64::MAX as u64
    {
        return Err(DataFusionError::Plan(
            "interval join key/schema/type/bounds contract is invalid".to_string(),
        ));
    }
    Ok(())
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
