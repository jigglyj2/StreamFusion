// Copyright 2026 StreamFusion Authors
// Licensed under the Apache License, Version 2.0

use std::collections::BTreeSet;
use std::sync::Arc;

mod partial;
mod planning;
mod session_batch;
mod session_changes;
pub(crate) mod shared_execution;
mod shared_kernel;
mod shared_processing;
mod shared_sessions;
pub(crate) mod shared_slices;
mod state_codec;
mod time;
use planning::validate_plan;
use session_changes::apply_session_changes;
use state_codec::*;
pub(super) use time::{local_to_epoch, local_to_timer_epoch};

use ahash::RandomState;
use arrow::array::{
    Array, ArrayRef, BinaryArray, Int8Array, Int64Array, TimestampMillisecondArray,
};
use arrow::datatypes::{DataType, Field, Schema, SchemaRef};
use arrow::record_batch::RecordBatch;
use arrow::row::{RowConverter, SortField};
use chrono::{LocalResult, NaiveDateTime, Offset, TimeZone, Utc};
use chrono_tz::Tz;
use datafusion::error::{DataFusionError, Result};
use hashbrown::HashMap;

use crate::exchange::{KeyField, assign_key_group, encode_binary_row_into};
use crate::memory_pool::HostMemoryReservation;
use crate::state::{
    KeyedState, MemoryKeyedState, NativeTimerService, RocksPluginKeyedState, StateKey, StateKeyRef,
    StateMutation, TimerDomain, TimerKey,
};
use crate::{decode_plan, proto};

use super::group_aggregate::{
    AccumulatorState, AggregateValue, Call, aggregate_array, decode_state, encode_state,
    lower_call, row_aggregate_values,
};
use super::window_table_function::{assign_windows_into, timestamp_millis};

const INSERT: i8 = 0;
const UPDATE_BEFORE: i8 = 1;
const UPDATE_AFTER: i8 = 2;
const DELETE: i8 = 3;
const WINDOW_STATE_MAGIC: &[u8; 4] = b"SFWA";
const WINDOW_STATE_VERSION: u8 = 1;
const WINDOW_KEY_PREFIX: u8 = 1;
const SESSION_INDEX_PREFIX: u8 = 2;
const COUNT_INDEX_PREFIX: u8 = 3;
const SESSION_STATE_MAGIC: &[u8; 4] = b"SFWS";
const SESSION_INDEX_MAGIC: &[u8; 4] = b"SFWI";
const COUNT_INDEX_MAGIC: &[u8; 4] = b"SFWC";
const TIMER_STATE_KEY: &[u8] = b"\0streamfusion-window-timers";
const MAX_TIMERS_PER_OUTPUT: usize = 4_096;

/// Persistent native SQL window aggregation shared by the memory and direct RocksDB backends.
pub(crate) struct WindowAggregateProcessor {
    plan: proto::WindowAggregate,
    window: proto::WindowTableFunction,
    shift_time_zone: Tz,
    calls: Vec<Call>,
    max_parallelism: u32,
    state: Box<dyn KeyedState>,
    timers: NativeTimerService,
    input_schema: Option<SchemaRef>,
    output_schema: Option<SchemaRef>,
    grouping_converter: Option<RowConverter>,
    key_fields: Vec<(usize, KeyField)>,
    preencoded_key_index: Option<usize>,
    input_kind_index: Option<usize>,
    scratch_reservation: HostMemoryReservation,
    current_event_time: i64,
    current_processing_time: i64,
    late_records_dropped: u64,
    state_read_batches: u64,
    state_write_batches: u64,
    timer_registrations: u64,
    timer_deletions: u64,
    timers_fired: u64,
}

struct StagedWindow {
    key: StateKey,
    grouping_row: Vec<u8>,
    accumulator: AccumulatorState,
    touched: bool,
}

#[derive(Clone, Debug, PartialEq, Eq)]
struct SessionEvent {
    timestamp: i64,
    values: Vec<Option<AggregateValue>>,
}

struct PendingSessionGroup {
    key_group: u32,
    group_key: Vec<u8>,
    grouping_row: Vec<u8>,
    changes: Vec<(bool, SessionEvent)>,
}

struct ActiveSession {
    start: i64,
    end: i64,
    accumulator: AccumulatorState,
    events: Vec<SessionEvent>,
}

impl WindowAggregateProcessor {
    pub(crate) fn new(
        serialized_plan: &[u8],
        max_parallelism: u32,
        first_key_group: u32,
        last_key_group: u32,
        state_reservation: HostMemoryReservation,
    ) -> Result<Self> {
        let scratch = state_reservation.sibling("native window aggregate batch scratch and output");
        let timer_reservation = state_reservation.sibling("native window aggregate timers");
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
        let timer_reservation = reservation.sibling("native window aggregate timers");
        let state = Box::new(RocksPluginKeyedState::open_for_owner(
            plugin_path,
            database_path,
            first_key_group,
            last_key_group,
            memory_limit,
            &reservation,
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
        let root = native_plan.root.ok_or_else(|| {
            DataFusionError::Plan("window aggregate plan has no root".to_string())
        })?;
        let plan = match root.operator {
            Some(proto::operator::Operator::WindowAggregate(plan)) => *plan,
            _ => {
                return Err(DataFusionError::Plan(
                    "stateful window aggregate handle requires a WindowAggregate root".to_string(),
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
                "changelog window aggregate requires retractable aggregate calls".to_string(),
            ));
        }
        let window = proto::WindowTableFunction {
            input: None,
            time_attribute_index: plan.time_attribute_index,
            kind: plan.kind,
            size_millis: plan.size_millis,
            slide_or_step_millis: plan.slide_or_step_millis,
            offset_millis: plan.offset_millis,
            partition_key_indices: Vec::new(),
            processing_time: plan.processing_time,
            input_schema: None,
            shift_time_zone: plan.shift_time_zone.clone(),
        };
        let shift_time_zone = if plan.shift_time_zone.is_empty() {
            chrono_tz::UTC
        } else {
            plan.shift_time_zone.parse::<Tz>().map_err(|error| {
                DataFusionError::Plan(format!(
                    "invalid window shift time zone {}: {error}",
                    plan.shift_time_zone
                ))
            })?
        };
        let planned_input =
            crate::planner::arrow_schema(plan.input_schema.as_ref().ok_or_else(|| {
                DataFusionError::Plan("window aggregate input schema is missing".to_string())
            })?)?;
        if let Some((start, end)) = plan
            .attached_window_start_index
            .zip(plan.attached_window_end_index)
        {
            for (label, index) in [("start", start), ("end", end)] {
                let field = planned_input.fields().get(index as usize).ok_or_else(|| {
                    DataFusionError::Plan(format!(
                        "attached window {label} index {index} is outside the planned input"
                    ))
                })?;
                if field.data_type()
                    != &DataType::Timestamp(arrow::datatypes::TimeUnit::Millisecond, None)
                {
                    return Err(DataFusionError::Plan(format!(
                        "attached window {label} input must be TIMESTAMP(3), got {}",
                        field.data_type()
                    )));
                }
            }
        }
        let planned_output =
            crate::planner::arrow_schema(plan.output_schema.as_ref().ok_or_else(|| {
                DataFusionError::Plan("window aggregate output schema is missing".to_string())
            })?)?;
        if planned_output.fields().len()
            != plan.grouping_indices.len() + calls.len() + plan.window_properties.len()
        {
            return Err(DataFusionError::Plan(
                "window aggregate output schema does not match keys, calls, and properties"
                    .to_string(),
            ));
        }
        let grouping_fields = plan
            .grouping_indices
            .iter()
            .map(|&index| {
                planned_input
                    .fields()
                    .get(index as usize)
                    .cloned()
                    .ok_or_else(|| {
                        DataFusionError::Plan(format!(
                            "window grouping index {index} is outside the planned input"
                        ))
                    })
            })
            .collect::<Result<Vec<_>>>()?;
        let sort_fields = grouping_fields
            .iter()
            .map(|field| SortField::new(field.data_type().clone()))
            .collect::<Vec<_>>();
        if !RowConverter::supports_fields(&sort_fields) {
            return Err(DataFusionError::Plan(
                "window grouping type is not supported by Arrow row encoding".to_string(),
            ));
        }
        let grouping_converter = RowConverter::new(sort_fields)?;
        let mut output_fields = planned_output.fields().iter().cloned().collect::<Vec<_>>();
        output_fields.push(Arc::new(Field::new(
            "__streamfusion_row_kind",
            DataType::Int8,
            false,
        )));
        Ok(Self {
            plan,
            window,
            shift_time_zone,
            calls,
            max_parallelism,
            state,
            timers: NativeTimerService::new(first_key_group, last_key_group, timer_reservation)?,
            input_schema: None,
            output_schema: Some(Arc::new(Schema::new(output_fields))),
            grouping_converter: Some(grouping_converter),
            key_fields: Vec::new(),
            preencoded_key_index: None,
            input_kind_index: None,
            scratch_reservation,
            current_event_time: i64::MIN,
            current_processing_time: i64::MIN,
            late_records_dropped: 0,
            state_read_batches: 0,
            state_write_batches: 0,
            timer_registrations: 0,
            timer_deletions: 0,
            timers_fired: 0,
        })
    }

    pub(crate) fn process_arrow(
        &mut self,
        batch: RecordBatch,
        processing_time: i64,
    ) -> Result<RecordBatch> {
        self.prepare_schema(batch.schema())?;
        self.current_processing_time = self.current_processing_time.max(processing_time);
        let window_copies = if self.plan.partial_accumulator_index.is_some()
            && !self.plan.partial_windows_are_slices
        {
            1
        } else {
            match proto::WindowKind::try_from(self.plan.kind) {
                Ok(proto::WindowKind::Hop) => {
                    self.plan.size_millis / self.plan.slide_or_step_millis
                }
                Ok(proto::WindowKind::CountHop) => {
                    self.plan
                        .size_millis
                        .saturating_add(self.plan.slide_or_step_millis.saturating_sub(1))
                        / self.plan.slide_or_step_millis
                }
                Ok(proto::WindowKind::Cumulate) => {
                    self.plan.size_millis / self.plan.slide_or_step_millis
                }
                _ => 1,
            }
        };
        let partial_input = self.plan.partial_accumulator_index.is_some();
        let bytes_per_call = if partial_input { 512 } else { 64 };
        let encoded_workspace = if partial_input {
            batch.get_array_memory_size().saturating_mul(8)
        } else {
            0
        };
        let base_reservation = batch
            .num_rows()
            .saturating_mul(
                224usize.saturating_add(self.calls.len().saturating_mul(bytes_per_call)),
            )
            .saturating_add(encoded_workspace)
            .saturating_mul(usize::try_from(window_copies.max(1)).unwrap_or(usize::MAX));
        self.scratch_reservation.resize(base_reservation)?;
        let result = self.process_arrow_accounted(&batch);
        match result {
            Ok(output) => self.finish_output(output, base_reservation),
            Err(error) => {
                self.scratch_reservation.resize(0)?;
                Err(error)
            }
        }
    }

    fn process_arrow_accounted(&mut self, batch: &RecordBatch) -> Result<RecordBatch> {
        if self.plan.partial_accumulator_index.is_some() {
            return self.process_partial_batch(batch);
        }
        if matches!(
            proto::WindowKind::try_from(self.plan.kind),
            Ok(proto::WindowKind::CountTumble | proto::WindowKind::CountHop)
        ) {
            return self.process_count_batch(batch);
        }
        if matches!(
            proto::WindowKind::try_from(self.plan.kind),
            Ok(proto::WindowKind::Session)
        ) {
            return self.process_session_batch(batch);
        }
        let grouping_rows = self.encode_grouping_rows(batch)?;
        let mut unique = HashMap::<StateKey, usize, RandomState>::with_capacity_and_hasher(
            batch.num_rows(),
            RandomState::new(),
        );
        let mut row_windows = Vec::new();
        let mut group_key = Vec::new();
        let mut assigned_windows = Vec::new();
        let attached_columns = self
            .plan
            .attached_window_start_index
            .zip(self.plan.attached_window_end_index)
            .map(|(start, end)| (batch.column(start as usize), batch.column(end as usize)));
        let timestamp_column = (!self.plan.processing_time && attached_columns.is_none())
            .then(|| batch.column(self.plan.time_attribute_index as usize));
        for row in 0..batch.num_rows() {
            self.group_key_into(batch, row, &mut group_key)?;
            let key_group = assign_key_group(&group_key, self.max_parallelism);
            if let Some((start_column, end_column)) = attached_columns {
                let Some(start) = timestamp_millis(start_column, row)? else {
                    continue;
                };
                let Some(end) = timestamp_millis(end_column, row)? else {
                    continue;
                };
                if end <= start {
                    return Err(DataFusionError::Execution(format!(
                        "attached window end {end} must be greater than start {start}"
                    )));
                }
                assigned_windows.clear();
                assigned_windows.push((start, end));
            } else {
                let timestamp = if self.plan.processing_time {
                    self.to_window_time(self.current_processing_time)?
                } else {
                    let Some(timestamp) = timestamp_millis(
                        timestamp_column.expect("event-time window has a timestamp column"),
                        row,
                    )?
                    else {
                        continue;
                    };
                    self.to_window_time(timestamp)?
                };
                assign_windows_into(&self.window, timestamp, &mut assigned_windows);
            }
            for &(start, end) in &assigned_windows {
                let deadline = self.timer_timestamp(end.saturating_sub(1))?;
                let progress = if self.plan.processing_time {
                    self.current_processing_time
                } else {
                    self.current_event_time
                };
                if deadline <= progress {
                    self.late_records_dropped = self.late_records_dropped.saturating_add(1);
                    continue;
                }
                let state_key = window_state_key(key_group, &group_key, start, end);
                let next = unique.len();
                let index = *unique.entry(state_key).or_insert(next);
                row_windows.push((row, index, start, end));
            }
        }
        let mut ordered_keys = (0..unique.len()).map(|_| None).collect::<Vec<_>>();
        for (key, index) in unique.drain() {
            ordered_keys[index] = Some(key);
        }
        let keys = ordered_keys
            .into_iter()
            .map(|key| key.expect("every window state index is populated"))
            .collect::<Vec<_>>();
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
        let mut staged = keys
            .into_iter()
            .zip(existing)
            .map(|(key, value)| {
                let (grouping_row, accumulator) = match value {
                    Some(value) => decode_window_state(value.as_ref(), &self.calls)?,
                    None => (Vec::new(), AccumulatorState::new(&self.calls)),
                };
                Ok(StagedWindow {
                    key,
                    grouping_row,
                    accumulator,
                    touched: false,
                })
            })
            .collect::<Result<Vec<_>>>()?;
        let mut dirty_timer_groups = BTreeSet::new();
        for (row, index, start, end) in row_windows {
            let accumulate = self.accumulates(batch, row)?;
            let entry = &mut staged[index];
            let was_empty = entry.accumulator.row_count == 0;
            if was_empty && !accumulate {
                continue;
            }
            if was_empty {
                entry.grouping_row = grouping_rows[row].clone();
            }
            entry
                .accumulator
                .apply(&self.calls, batch, row, accumulate)?;
            let timer = TimerKey {
                timestamp: self.timer_timestamp(end.saturating_sub(1))?,
                key: entry.key.key.clone(),
                namespace: window_namespace(start, end),
            };
            let domain = self.timer_domain();
            if was_empty && entry.accumulator.row_count != 0 {
                if self.timers.register(entry.key.key_group, domain, timer)? {
                    self.timer_registrations = self.timer_registrations.saturating_add(1);
                    dirty_timer_groups.insert(entry.key.key_group);
                }
            } else if entry.accumulator.row_count == 0
                && self.timers.delete(entry.key.key_group, domain, &timer)?
            {
                self.timer_deletions = self.timer_deletions.saturating_add(1);
                dirty_timer_groups.insert(entry.key.key_group);
            }
            entry.touched = true;
        }
        let mut mutations = staged
            .into_iter()
            .filter(|entry| entry.touched)
            .map(|entry| StateMutation {
                key: entry.key,
                value: (entry.accumulator.row_count != 0)
                    .then(|| encode_window_state(&entry.grouping_row, &entry.accumulator)),
            })
            .collect::<Vec<_>>();
        self.append_timer_mutations(&mut mutations, dirty_timer_groups)?;
        if !mutations.is_empty() {
            self.state.write_batch(mutations)?;
            self.state_write_batches = self.state_write_batches.saturating_add(1);
        }
        self.empty_output()
    }

    fn process_count_batch(&mut self, batch: &RecordBatch) -> Result<RecordBatch> {
        let grouping_rows = self.encode_grouping_rows(batch)?;
        let mut unique_groups = HashMap::<StateKey, usize, RandomState>::with_capacity_and_hasher(
            batch.num_rows(),
            RandomState::new(),
        );
        let mut groups = Vec::<(StateKey, Vec<u8>, i64)>::new();
        let mut row_groups = Vec::with_capacity(batch.num_rows());
        let mut group_key = Vec::new();
        for row in 0..batch.num_rows() {
            self.group_key_into(batch, row, &mut group_key)?;
            let key_group = assign_key_group(&group_key, self.max_parallelism);
            let count_key = count_index_key(key_group, &group_key);
            let next = groups.len();
            let index = *unique_groups.entry(count_key.clone()).or_insert(next);
            if index == groups.len() {
                groups.push((count_key, grouping_rows[row].clone(), 0));
            }
            row_groups.push(index);
        }
        let count_refs = groups
            .iter()
            .map(|(key, _, _)| StateKeyRef {
                key_group: key.key_group,
                key: &key.key,
            })
            .collect::<Vec<_>>();
        let counts = self
            .state
            .get_batch(&count_refs, &self.scratch_reservation)?;
        let _loaded_state_workspace =
            crate::state::reserve_decoded_values(&counts, &self.scratch_reservation)?;
        if !count_refs.is_empty() {
            self.state_read_batches = self.state_read_batches.saturating_add(1);
        }
        for (group, value) in groups.iter_mut().zip(counts) {
            group.2 = value
                .as_deref()
                .map(decode_count_index)
                .transpose()?
                .unwrap_or(0);
        }

        let kind = proto::WindowKind::try_from(self.plan.kind).map_err(|_| {
            DataFusionError::Plan(format!("unknown window kind {}", self.plan.kind))
        })?;
        let size = self.plan.size_millis;
        let slide = self.plan.slide_or_step_millis;
        let mut unique_windows = HashMap::<StateKey, usize, RandomState>::with_capacity_and_hasher(
            batch.num_rows(),
            RandomState::new(),
        );
        let mut window_keys = Vec::new();
        let mut row_windows = Vec::<(usize, usize, bool)>::new();
        for (row, group_index) in row_groups.into_iter().enumerate() {
            let group = &mut groups[group_index];
            let current = group.2;
            group.2 = current.wrapping_add(1);
            let group_key = count_group_key(&group.0.key)?;
            let mut ids = Vec::new();
            match kind {
                proto::WindowKind::CountTumble => ids.push(current / size),
                proto::WindowKind::CountHop => {
                    let mut id = current / slide;
                    loop {
                        let start = id.saturating_mul(slide);
                        let end = start.saturating_add(size).saturating_sub(1);
                        if start <= current && current <= end {
                            ids.push(id);
                        }
                        if id == 0 {
                            break;
                        }
                        id -= 1;
                        if id
                            .saturating_mul(slide)
                            .saturating_add(size)
                            .saturating_sub(1)
                            < current
                        {
                            break;
                        }
                    }
                }
                _ => unreachable!("count processing is called only for count windows"),
            }
            for id in ids {
                let start = id.saturating_mul(if kind == proto::WindowKind::CountTumble {
                    size
                } else {
                    slide
                });
                let end = start.saturating_add(size);
                let state_key = window_state_key(group.0.key_group, group_key, start, end);
                let next = window_keys.len();
                let index = *unique_windows.entry(state_key.clone()).or_insert(next);
                if index == window_keys.len() {
                    window_keys.push(state_key);
                }
                row_windows.push((row, index, current == end.saturating_sub(1)));
            }
        }

        let state_refs = window_keys
            .iter()
            .map(|key| StateKeyRef {
                key_group: key.key_group,
                key: &key.key,
            })
            .collect::<Vec<_>>();
        let existing = self
            .state
            .get_batch(&state_refs, &self.scratch_reservation)?;
        let _loaded_state_workspace =
            crate::state::reserve_decoded_values(&existing, &self.scratch_reservation)?;
        if !state_refs.is_empty() {
            self.state_read_batches = self.state_read_batches.saturating_add(1);
        }
        let mut staged = window_keys
            .into_iter()
            .zip(existing)
            .map(|(key, value)| {
                let (grouping_row, accumulator) = match value {
                    Some(value) => decode_window_state(value.as_ref(), &self.calls)?,
                    None => (Vec::new(), AccumulatorState::new(&self.calls)),
                };
                Ok((key, grouping_row, accumulator, false, false))
            })
            .collect::<Result<Vec<_>>>()?;
        let mut output_grouping = Vec::new();
        let mut output_values = (0..self.calls.len())
            .map(|_| Vec::new())
            .collect::<Vec<_>>();
        let mut output_starts = Vec::new();
        let mut output_ends = Vec::new();
        for (row, index, emit) in row_windows {
            let entry = &mut staged[index];
            if entry.1.is_empty() {
                entry.1 = grouping_rows[row].clone();
            }
            entry
                .2
                .apply(&self.calls, batch, row, self.accumulates(batch, row)?)?;
            entry.3 = true;
            if emit {
                let (start, end) = decode_window_state_key_bounds(&entry.0.key)?;
                output_grouping.push(entry.1.clone());
                for (column, value) in output_values.iter_mut().zip(entry.2.values(&self.calls)) {
                    column.push(value);
                }
                output_starts.push(start);
                output_ends.push(end);
                entry.4 = true;
            }
        }
        let mut mutations = Vec::with_capacity(groups.len() + staged.len());
        for (key, _, count) in groups {
            mutations.push(StateMutation {
                key,
                value: Some(encode_count_index(count)),
            });
        }
        for (key, grouping_row, accumulator, touched, emitted) in staged {
            if touched {
                mutations.push(StateMutation {
                    key,
                    value: (!emitted).then(|| encode_window_state(&grouping_row, &accumulator)),
                });
            }
        }
        if !mutations.is_empty() {
            self.state.write_batch(mutations)?;
            self.state_write_batches = self.state_write_batches.saturating_add(1);
        }
        self.output_batch(output_grouping, output_values, output_starts, output_ends)
    }

    pub(crate) fn advance_event_time(&mut self, watermark: i64) -> Result<RecordBatch> {
        if watermark < self.current_event_time {
            return self.empty_output();
        }
        self.current_event_time = watermark;
        let output = self.fire(TimerDomain::EventTime, watermark)?;
        self.finish_output(output, 0)
    }

    pub(crate) fn advance_processing_time(&mut self, timestamp: i64) -> Result<RecordBatch> {
        if timestamp < self.current_processing_time {
            return self.empty_output();
        }
        self.current_processing_time = timestamp;
        let output = self.fire(TimerDomain::ProcessingTime, timestamp)?;
        self.finish_output(output, 0)
    }

    fn fire(&mut self, domain: TimerDomain, progress: i64) -> Result<RecordBatch> {
        let fired = self
            .timers
            .advance_owned_limited(domain, progress, MAX_TIMERS_PER_OUTPUT)?;
        self.timers_fired = self.timers_fired.saturating_add(fired.len() as u64);
        if fired.is_empty() {
            return self.empty_output();
        }
        let refs = fired
            .iter()
            .map(|timer| StateKeyRef {
                key_group: timer.key_group,
                key: &timer.timer.key,
            })
            .collect::<Vec<_>>();
        let states = self.state.get_batch(&refs, &self.scratch_reservation)?;
        let _loaded_state_workspace =
            crate::state::reserve_decoded_values(&states, &self.scratch_reservation)?;
        self.state_read_batches = self.state_read_batches.saturating_add(1);
        let mut grouping_rows = Vec::new();
        let mut aggregate_values = (0..self.calls.len())
            .map(|_| Vec::new())
            .collect::<Vec<_>>();
        let mut starts = Vec::new();
        let mut ends = Vec::new();
        let mut mutations = Vec::with_capacity(fired.len() * 2);
        let mut dirty_groups = BTreeSet::new();
        let mut session_removals =
            HashMap::<StateKey, Vec<(i64, i64)>, RandomState>::with_hasher(RandomState::new());
        let is_session = matches!(
            proto::WindowKind::try_from(self.plan.kind),
            Ok(proto::WindowKind::Session)
        );
        for (timer, value) in fired.iter().zip(states) {
            dirty_groups.insert(timer.key_group);
            let Some(value) = value else {
                continue;
            };
            let (grouping_row, accumulator) = if is_session {
                let (grouping_row, accumulator, _) =
                    decode_session_state(value.as_ref(), &self.calls)?;
                (grouping_row, accumulator)
            } else {
                decode_window_state(value.as_ref(), &self.calls)?
            };
            let (start, end) = decode_window_namespace(&timer.timer.namespace)?;
            if accumulator.row_count != 0 {
                grouping_rows.push(grouping_row);
                for (column, value) in aggregate_values
                    .iter_mut()
                    .zip(accumulator.values(&self.calls))
                {
                    column.push(value);
                }
                starts.push(start);
                ends.push(end);
            }
            if is_session {
                let group_key = group_key_from_window_state_key(&timer.timer.key)?;
                session_removals
                    .entry(session_index_key(timer.key_group, group_key))
                    .or_default()
                    .push((start, end));
            }
            mutations.push(StateMutation {
                key: StateKey {
                    key_group: timer.key_group,
                    key: timer.timer.key.clone(),
                },
                value: None,
            });
        }
        if !session_removals.is_empty() {
            let index_keys = session_removals.keys().cloned().collect::<Vec<_>>();
            let refs = index_keys
                .iter()
                .map(|key| StateKeyRef {
                    key_group: key.key_group,
                    key: &key.key,
                })
                .collect::<Vec<_>>();
            let values = self.state.get_batch(&refs, &self.scratch_reservation)?;
            let _loaded_state_workspace =
                crate::state::reserve_decoded_values(&values, &self.scratch_reservation)?;
            self.state_read_batches = self.state_read_batches.saturating_add(1);
            for (key, value) in index_keys.into_iter().zip(values) {
                let mut intervals = match value {
                    Some(value) => decode_session_index(value.as_ref())?,
                    None => Vec::new(),
                };
                let removals = session_removals
                    .get(&key)
                    .expect("session index came from removal map");
                intervals.retain(|interval| !removals.contains(interval));
                mutations.push(StateMutation {
                    key,
                    value: (!intervals.is_empty()).then(|| encode_session_index(&intervals)),
                });
            }
        }
        self.append_timer_mutations(&mut mutations, dirty_groups)?;
        self.state.write_batch(mutations)?;
        self.state_write_batches = self.state_write_batches.saturating_add(1);
        self.output_batch(grouping_rows, aggregate_values, starts, ends)
    }

    pub(crate) fn next_processing_timer(&self) -> Option<i64> {
        self.timers.next_timestamp(TimerDomain::ProcessingTime)
    }

    pub(crate) fn next_event_timer(&self) -> Option<i64> {
        self.timers.next_timestamp(TimerDomain::EventTime)
    }

    pub(crate) fn late_records_dropped(&self) -> u64 {
        self.late_records_dropped
    }

    pub(crate) fn statistics(&self) -> [u64; 7] {
        [
            self.state_read_batches,
            self.state_write_batches,
            self.timer_registrations,
            self.timer_deletions,
            self.timers_fired,
            self.timers.timer_count(TimerDomain::EventTime) as u64,
            self.timers.timer_count(TimerDomain::ProcessingTime) as u64,
        ]
    }

    pub(crate) fn state_memory(&self) -> HostMemoryReservation {
        self.scratch_reservation.sibling("native state transfer")
    }

    pub(crate) fn snapshot_key_group(&self, key_group: u32) -> Result<crate::state::SnapshotBytes> {
        self.state
            .snapshot_key_group(key_group, &self.scratch_reservation)
    }

    pub(crate) fn restore_key_group(&mut self, key_group: u32, bytes: &[u8]) -> Result<()> {
        self.state
            .restore_key_group(key_group, bytes, &self.scratch_reservation)?;
        let timer_key = StateKeyRef {
            key_group,
            key: TIMER_STATE_KEY,
        };
        if let Some(timer_state) = self
            .state
            .get_batch(&[timer_key], &self.scratch_reservation)?
            .pop()
            .flatten()
        {
            self.state_read_batches = self.state_read_batches.saturating_add(1);
            self.timers
                .restore_key_group(key_group, timer_state.as_ref())?;
        } else {
            self.state_read_batches = self.state_read_batches.saturating_add(1);
        }
        Ok(())
    }

    pub(crate) fn checkpoint(&self, directory: &std::path::Path) -> Result<()> {
        self.state.checkpoint(directory)
    }

    fn append_timer_mutations(
        &self,
        mutations: &mut Vec<StateMutation>,
        key_groups: BTreeSet<u32>,
    ) -> Result<()> {
        for key_group in key_groups {
            mutations.push(StateMutation {
                key: StateKey {
                    key_group,
                    key: TIMER_STATE_KEY.to_vec(),
                },
                value: Some(self.timers.snapshot_key_group(key_group)?),
            });
        }
        Ok(())
    }

    fn timer_domain(&self) -> TimerDomain {
        if self.plan.processing_time {
            TimerDomain::ProcessingTime
        } else {
            TimerDomain::EventTime
        }
    }

    fn to_window_time(&self, epoch_millis: i64) -> Result<i64> {
        if self.shift_time_zone == chrono_tz::UTC || epoch_millis == i64::MAX {
            return Ok(epoch_millis);
        }
        let instant =
            chrono::DateTime::<Utc>::from_timestamp_millis(epoch_millis).ok_or_else(|| {
                DataFusionError::Execution(format!(
                    "window timestamp {epoch_millis} is outside chrono's range"
                ))
            })?;
        Ok(instant
            .with_timezone(&self.shift_time_zone)
            .naive_local()
            .and_utc()
            .timestamp_millis())
    }

    fn timer_timestamp(&self, window_millis: i64) -> Result<i64> {
        if self.shift_time_zone == chrono_tz::UTC || window_millis == i64::MAX {
            return Ok(window_millis);
        }
        let local = chrono::DateTime::<Utc>::from_timestamp_millis(window_millis)
            .ok_or_else(|| {
                DataFusionError::Execution(format!(
                    "window timer timestamp {window_millis} is outside chrono's range"
                ))
            })?
            .naive_utc();
        local_to_timer_epoch(local, self.shift_time_zone)
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
                    "window aggregate input RowKind metadata is not Arrow Int8".to_string(),
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
            .expect("schema prepared")
            .convert_columns(&columns)?;
        Ok((0..batch.num_rows())
            .map(|row| rows.row(row).as_ref().to_vec())
            .collect())
    }

    fn group_key_into(&self, batch: &RecordBatch, row: usize, output: &mut Vec<u8>) -> Result<()> {
        match self.preencoded_key_index {
            Some(index) => {
                let value = batch
                    .column(index)
                    .as_any()
                    .downcast_ref::<arrow::array::BinaryArray>()
                    .ok_or_else(|| {
                        DataFusionError::Execution(
                            "window aggregate preencoded key column is not Arrow Binary"
                                .to_string(),
                        )
                    })?
                    .value(row);
                output.clear();
                output.extend_from_slice(value);
                Ok(())
            }
            None if self.key_fields.is_empty() => {
                output.clear();
                Ok(())
            }
            None => Ok(encode_binary_row_into(
                batch,
                row,
                &self.key_fields,
                output,
            )?),
        }
    }

    fn prepare_schema(&mut self, schema: SchemaRef) -> Result<()> {
        if let Some(expected) = &self.input_schema {
            if expected.as_ref() != schema.as_ref() {
                return Err(DataFusionError::Execution(
                    "window aggregate input schema changed while running".to_string(),
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
            Some(schema.fields().len()),
        ]
        .into_iter()
        .flatten()
        .min()
        .unwrap();
        if self.plan.input_changelog && self.input_kind_index.is_none() {
            return Err(DataFusionError::Execution(
                "changelog window aggregate requires input RowKind metadata".to_string(),
            ));
        }
        let grouping_fields = self
            .plan
            .grouping_indices
            .iter()
            .map(|&index| {
                schema
                    .fields()
                    .get(index as usize)
                    .filter(|_| (index as usize) < visible_count)
                    .cloned()
                    .ok_or_else(|| {
                        DataFusionError::Plan(format!(
                            "window grouping index {index} is outside the input row"
                        ))
                    })
            })
            .collect::<Result<Vec<_>>>()?;
        let planned_input = crate::planner::arrow_schema(
            self.plan
                .input_schema
                .as_ref()
                .expect("validated input schema"),
        )?;
        for (&index, actual) in self.plan.grouping_indices.iter().zip(&grouping_fields) {
            let planned = planned_input.field(index as usize);
            if !planned.data_type().equals_datatype(actual.data_type()) {
                return Err(DataFusionError::Plan(format!(
                    "window grouping input {index} expected {}, got {}",
                    planned.data_type(),
                    actual.data_type()
                )));
            }
        }
        let actual_sort_fields = grouping_fields
            .iter()
            .map(|field| SortField::new(field.data_type().clone()))
            .collect::<Vec<_>>();
        self.grouping_converter = Some(RowConverter::new(actual_sort_fields)?);
        if !grouping_fields.is_empty() {
            let current = self.output_schema.as_ref().expect("planned output schema");
            let mut fields = current.fields().iter().cloned().collect::<Vec<_>>();
            for (index, actual) in grouping_fields.iter().enumerate() {
                let planned_output = &fields[index];
                fields[index] = Arc::new(Field::new(
                    planned_output.name(),
                    actual.data_type().clone(),
                    planned_output.is_nullable(),
                ));
            }
            self.output_schema = Some(Arc::new(Schema::new(fields)));
        }
        if self.preencoded_key_index.is_none() {
            self.key_fields = self
                .plan
                .grouping_indices
                .iter()
                .map(|&index| {
                    let field = schema.field(index as usize);
                    Ok((
                        index as usize,
                        KeyField::from_arrow_type(field.data_type())?,
                    ))
                })
                .collect::<std::result::Result<Vec<_>, arrow::error::ArrowError>>()?;
        }
        if let Some(accumulator_index) = self.plan.partial_accumulator_index {
            let slice_end_index = self
                .plan
                .partial_slice_end_index
                .expect("partial indices were validated");
            let window_start_index = self
                .plan
                .partial_window_start_index
                .expect("partial indices were validated");
            if schema
                .fields()
                .get(accumulator_index as usize)
                .filter(|_| (accumulator_index as usize) < visible_count)
                .is_none_or(|field| field.data_type() != &DataType::Binary)
                || schema
                    .fields()
                    .get(slice_end_index as usize)
                    .filter(|_| (slice_end_index as usize) < visible_count)
                    .is_none_or(|field| field.data_type() != &DataType::Int64)
                || schema
                    .fields()
                    .get(window_start_index as usize)
                    .filter(|_| (window_start_index as usize) < visible_count)
                    .is_none_or(|field| field.data_type() != &DataType::Int64)
            {
                return Err(DataFusionError::Plan(
                    "global window partial input requires BINARY accumulator and BIGINT slice end"
                        .to_string(),
                ));
            }
        } else {
            for call in &self.calls {
                if let Some(index) = call.input_index {
                    let field = schema
                        .fields()
                        .get(index)
                        .filter(|_| index < visible_count)
                        .ok_or_else(|| {
                            DataFusionError::Plan(format!(
                                "window aggregate input index {index} is outside the input row"
                            ))
                        })?;
                    if Some(field.data_type()) != call.input_type.as_ref() {
                        return Err(DataFusionError::Plan(format!(
                            "window aggregate input {index} expected {:?}, got {}",
                            call.input_type,
                            field.data_type()
                        )));
                    }
                }
            }
        }
        self.input_schema = Some(schema);
        Ok(())
    }

    fn output_batch(
        &self,
        grouping_rows: Vec<Vec<u8>>,
        aggregate_values: Vec<Vec<Option<AggregateValue>>>,
        starts: Vec<i64>,
        ends: Vec<i64>,
    ) -> Result<RecordBatch> {
        let row_count = starts.len();
        let mut columns = if self.plan.grouping_indices.is_empty() {
            Vec::new()
        } else {
            let converter = self.grouping_converter.as_ref().expect("schema prepared");
            let parser = converter.parser();
            converter.convert_rows(grouping_rows.iter().map(|row| parser.parse(row)))?
        };
        for (call, values) in self.calls.iter().zip(aggregate_values) {
            columns.push(aggregate_array(&values, &call.output_type)?);
        }
        for property in &self.plan.window_properties {
            let values = match proto::WindowProperty::try_from(*property) {
                Ok(proto::WindowProperty::Start) => starts.clone(),
                Ok(proto::WindowProperty::End) => ends.clone(),
                Ok(proto::WindowProperty::Time) => ends
                    .iter()
                    .map(|end| self.window_property_timestamp(end.wrapping_sub(1)))
                    .collect::<Result<Vec<_>>>()?,
                _ => {
                    return Err(DataFusionError::Plan(format!(
                        "unknown window property {property}"
                    )));
                }
            };
            columns.push(Arc::new(TimestampMillisecondArray::from(values)) as ArrayRef);
        }
        columns.push(Arc::new(Int8Array::from(vec![INSERT; row_count])));
        Ok(RecordBatch::try_new(
            Arc::clone(self.output_schema.as_ref().expect("schema prepared")),
            columns,
        )?)
    }

    fn empty_output(&self) -> Result<RecordBatch> {
        let schema = self.output_schema.as_ref().ok_or_else(|| {
            DataFusionError::Execution(
                "window aggregate cannot emit before its input schema is negotiated".to_string(),
            )
        })?;
        Ok(RecordBatch::new_empty(Arc::clone(schema)))
    }

    fn finish_output(
        &mut self,
        output: RecordBatch,
        base_reservation: usize,
    ) -> Result<RecordBatch> {
        let output_bytes = output.get_array_memory_size();
        self.scratch_reservation
            .resize(output_bytes.max(base_reservation))?;
        self.scratch_reservation.transfer_to_arrow(output_bytes)?;
        self.scratch_reservation.resize(0)?;
        Ok(output)
    }

    fn window_property_timestamp(&self, window_millis: i64) -> Result<i64> {
        if self.shift_time_zone == chrono_tz::UTC || window_millis == i64::MAX {
            return Ok(window_millis);
        }
        let local = chrono::DateTime::<Utc>::from_timestamp_millis(window_millis)
            .ok_or_else(|| {
                DataFusionError::Execution(format!(
                    "window property timestamp {window_millis} is outside chrono's range"
                ))
            })?
            .naive_utc();
        local_to_epoch(local, self.shift_time_zone)
    }
}

#[cfg(test)]
mod tests;
