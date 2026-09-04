// Copyright 2026 StreamFusion Authors
// Licensed under the Apache License, Version 2.0

use std::collections::BTreeSet;
use std::sync::Arc;

use ahash::RandomState;
use arrow::array::{ArrayRef, Int8Array, TimestampMillisecondArray};
use arrow::datatypes::{DataType, Field, Schema, SchemaRef};
use arrow::record_batch::RecordBatch;
use arrow::row::{RowConverter, SortField};
use chrono::{LocalResult, NaiveDateTime, Offset, TimeZone, Utc};
use chrono_tz::Tz;
use datafusion::error::{DataFusionError, Result};
use hashbrown::HashMap;

use crate::exchange::{assign_key_group, encode_binary_row, KeyField};
use crate::memory_pool::HostMemoryReservation;
use crate::state::{
    KeyedState, MemoryKeyedState, NativeTimerService, RocksPluginKeyedState, StateKey, StateKeyRef,
    StateMutation, TimerDomain, TimerKey,
};
use crate::{decode_plan, proto};

use super::group_aggregate::{
    aggregate_array, decode_state, encode_state, lower_call, row_aggregate_values,
    AccumulatorState, AggregateValue, Call,
};
use super::window_table_function::{assign_windows, timestamp_millis};

const INSERT: i8 = 0;
const UPDATE_BEFORE: i8 = 1;
const UPDATE_AFTER: i8 = 2;
const DELETE: i8 = 3;
const WINDOW_STATE_MAGIC: &[u8; 4] = b"SFWA";
const WINDOW_STATE_VERSION: u8 = 1;
const WINDOW_KEY_PREFIX: u8 = 1;
const SESSION_INDEX_PREFIX: u8 = 2;
const SESSION_STATE_MAGIC: &[u8; 4] = b"SFWS";
const SESSION_INDEX_MAGIC: &[u8; 4] = b"SFWI";
const TIMER_STATE_KEY: &[u8] = b"\0streamfusion-window-timers";

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
        let window_copies = match proto::WindowKind::try_from(self.plan.kind) {
            Ok(proto::WindowKind::Hop) => self.plan.size_millis / self.plan.slide_or_step_millis,
            Ok(proto::WindowKind::Cumulate) => {
                self.plan.size_millis / self.plan.slide_or_step_millis
            }
            _ => 1,
        };
        let base_reservation = batch
            .num_rows()
            .saturating_mul(224usize.saturating_add(self.calls.len().saturating_mul(64)))
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
        let timestamp_column = (!self.plan.processing_time)
            .then(|| batch.column(self.plan.time_attribute_index as usize));
        for row in 0..batch.num_rows() {
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
            let group_key = self.group_key(batch, row)?;
            let key_group = assign_key_group(&group_key, self.max_parallelism);
            for (start, end) in assign_windows(&self.window, timestamp) {
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
        let existing = self.state.get_batch(&refs)?;
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

    fn process_session_batch(&mut self, batch: &RecordBatch) -> Result<RecordBatch> {
        let grouping_rows = self.encode_grouping_rows(batch)?;
        let timestamp_column = (!self.plan.processing_time)
            .then(|| batch.column(self.plan.time_attribute_index as usize));
        let gap = self.plan.size_millis;
        let progress = if self.plan.processing_time {
            self.current_processing_time
        } else {
            self.current_event_time
        };
        let mut unique = HashMap::<StateKey, usize, RandomState>::with_capacity_and_hasher(
            batch.num_rows(),
            RandomState::new(),
        );
        let mut groups = Vec::<PendingSessionGroup>::new();
        for row in 0..batch.num_rows() {
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
            let end = timestamp.saturating_add(gap);
            if self.timer_timestamp(end.saturating_sub(1))? <= progress {
                self.late_records_dropped = self.late_records_dropped.saturating_add(1);
                continue;
            }
            let group_key = self.group_key(batch, row)?;
            let key_group = assign_key_group(&group_key, self.max_parallelism);
            let index_key = session_index_key(key_group, &group_key);
            let next = groups.len();
            let index = *unique.entry(index_key).or_insert(next);
            if index == groups.len() {
                groups.push(PendingSessionGroup {
                    key_group,
                    group_key,
                    grouping_row: grouping_rows[row].clone(),
                    changes: Vec::new(),
                });
            }
            groups[index].changes.push((
                self.accumulates(batch, row)?,
                SessionEvent {
                    timestamp,
                    values: row_aggregate_values(&self.calls, batch, row)?,
                },
            ));
        }
        if groups.is_empty() {
            return self.empty_output();
        }

        let index_keys = groups
            .iter()
            .map(|group| session_index_key(group.key_group, &group.group_key))
            .collect::<Vec<_>>();
        let index_refs = index_keys
            .iter()
            .map(|key| StateKeyRef {
                key_group: key.key_group,
                key: &key.key,
            })
            .collect::<Vec<_>>();
        let index_values = self.state.get_batch(&index_refs)?;
        self.state_read_batches = self.state_read_batches.saturating_add(1);
        let old_intervals = index_values
            .into_iter()
            .map(|value| match value {
                Some(value) => decode_session_index(value.as_ref()),
                None => Ok(Vec::new()),
            })
            .collect::<Result<Vec<_>>>()?;
        let session_keys = groups
            .iter()
            .zip(&old_intervals)
            .flat_map(|(group, intervals)| {
                intervals.iter().map(|&(start, end)| {
                    window_state_key(group.key_group, &group.group_key, start, end)
                })
            })
            .collect::<Vec<_>>();
        let session_refs = session_keys
            .iter()
            .map(|key| StateKeyRef {
                key_group: key.key_group,
                key: &key.key,
            })
            .collect::<Vec<_>>();
        let session_values = self.state.get_batch(&session_refs)?;
        if !session_refs.is_empty() {
            self.state_read_batches = self.state_read_batches.saturating_add(1);
        }
        let mut state_offset = 0usize;
        let mut mutations = Vec::new();
        let mut dirty_timer_groups = BTreeSet::new();
        for ((group, intervals), index_key) in groups.into_iter().zip(old_intervals).zip(index_keys)
        {
            let mut sessions = Vec::new();
            let mut grouping_row = group.grouping_row;
            for &(start, end) in &intervals {
                let state_key = window_state_key(group.key_group, &group.group_key, start, end);
                let value = session_values
                    .get(state_offset)
                    .ok_or_else(|| {
                        DataFusionError::Internal(
                            "session state batch did not match its index".to_string(),
                        )
                    })?
                    .as_ref();
                state_offset += 1;
                if let Some(value) = value {
                    let decoded = decode_session_state(value.as_ref(), &self.calls)?;
                    grouping_row = decoded.0;
                    sessions.push(ActiveSession {
                        start,
                        end,
                        accumulator: decoded.1,
                        events: decoded.2,
                    });
                }
                let timer = TimerKey {
                    timestamp: self.timer_timestamp(end.saturating_sub(1))?,
                    key: state_key.key.clone(),
                    namespace: window_namespace(start, end),
                };
                if self
                    .timers
                    .delete(group.key_group, self.timer_domain(), &timer)?
                {
                    self.timer_deletions = self.timer_deletions.saturating_add(1);
                    dirty_timer_groups.insert(group.key_group);
                }
                mutations.push(StateMutation {
                    key: state_key,
                    value: None,
                });
            }
            apply_session_changes(
                &mut sessions,
                group.changes,
                gap,
                &self.calls,
                self.plan.input_changelog,
            )?;
            sessions.sort_by_key(|session| (session.start, session.end));
            let mut new_intervals = Vec::with_capacity(sessions.len());
            for session in sessions {
                let start = session.start;
                let end = session.end;
                let state_key = window_state_key(group.key_group, &group.group_key, start, end);
                let timer = TimerKey {
                    timestamp: self.timer_timestamp(end.saturating_sub(1))?,
                    key: state_key.key.clone(),
                    namespace: window_namespace(start, end),
                };
                if self
                    .timers
                    .register(group.key_group, self.timer_domain(), timer)?
                {
                    self.timer_registrations = self.timer_registrations.saturating_add(1);
                    dirty_timer_groups.insert(group.key_group);
                }
                mutations.push(StateMutation {
                    key: state_key,
                    value: Some(encode_session_state(
                        &grouping_row,
                        &session.accumulator,
                        &session.events,
                    )),
                });
                new_intervals.push((start, end));
            }
            mutations.push(StateMutation {
                key: index_key,
                value: (!new_intervals.is_empty()).then(|| encode_session_index(&new_intervals)),
            });
        }
        debug_assert_eq!(state_offset, session_values.len());
        self.append_timer_mutations(&mut mutations, dirty_timer_groups)?;
        self.state.write_batch(mutations)?;
        self.state_write_batches = self.state_write_batches.saturating_add(1);
        self.empty_output()
    }

    pub(crate) fn advance_event_time(&mut self, watermark: i64) -> Result<RecordBatch> {
        if watermark <= self.current_event_time {
            return self.empty_output();
        }
        self.current_event_time = watermark;
        let output = self.fire(TimerDomain::EventTime, watermark)?;
        self.finish_output(output, 0)
    }

    pub(crate) fn advance_processing_time(&mut self, timestamp: i64) -> Result<RecordBatch> {
        if timestamp <= self.current_processing_time {
            return self.empty_output();
        }
        self.current_processing_time = timestamp;
        let output = self.fire(TimerDomain::ProcessingTime, timestamp)?;
        self.finish_output(output, 0)
    }

    fn fire(&mut self, domain: TimerDomain, progress: i64) -> Result<RecordBatch> {
        let fired = self.timers.advance(domain, progress)?;
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
        let states = self.state.get_batch(&refs)?;
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
        for (timer, value) in fired.into_iter().zip(states) {
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
                    key: timer.timer.key,
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
            let values = self.state.get_batch(&refs)?;
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

    pub(crate) fn snapshot_key_group(&self, key_group: u32) -> Result<Vec<u8>> {
        self.state.snapshot_key_group(key_group)
    }

    pub(crate) fn restore_key_group(&mut self, key_group: u32, bytes: &[u8]) -> Result<()> {
        self.state.restore_key_group(key_group, bytes)?;
        let timer_key = StateKeyRef {
            key_group,
            key: TIMER_STATE_KEY,
        };
        if let Some(timer_state) = self.state.get_batch(&[timer_key])?.pop().flatten() {
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

    fn group_key(&self, batch: &RecordBatch, row: usize) -> Result<Vec<u8>> {
        match self.preencoded_key_index {
            Some(index) => Ok(batch
                .column(index)
                .as_any()
                .downcast_ref::<arrow::array::BinaryArray>()
                .ok_or_else(|| {
                    DataFusionError::Execution(
                        "window aggregate preencoded key column is not Arrow Binary".to_string(),
                    )
                })?
                .value(row)
                .to_vec()),
            None if self.key_fields.is_empty() => Ok(Vec::new()),
            None => Ok(encode_binary_row(batch, row, &self.key_fields)?),
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

pub(super) fn local_to_epoch(local: NaiveDateTime, zone: Tz) -> Result<i64> {
    match zone.from_local_datetime(&local) {
        LocalResult::Single(value) => Ok(value.timestamp_millis()),
        // LocalDateTime.atZone, which Flink uses for a window-time property, selects the
        // earlier offset during an overlap. Timer registration deliberately differs below.
        LocalResult::Ambiguous(left, right) => {
            Ok(left.timestamp_millis().min(right.timestamp_millis()))
        }
        LocalResult::None => {
            // Java resolves a nonexistent local timestamp by shifting it forward by the
            // transition gap. Applying the last valid pre-transition offset is equivalent.
            let mut distance = 1i64;
            while distance <= 7 * 24 * 60 * 60 * 1_000 {
                let candidate = local
                    .checked_sub_signed(chrono::TimeDelta::milliseconds(distance))
                    .ok_or_else(|| {
                        DataFusionError::Execution(
                            "window property overflow while resolving a DST gap".to_string(),
                        )
                    })?;
                if let LocalResult::Single(value) = zone.from_local_datetime(&candidate) {
                    let offset_millis = i64::from(value.offset().fix().local_minus_utc()) * 1_000;
                    return Ok(local.and_utc().timestamp_millis() - offset_millis);
                }
                distance = distance.saturating_mul(2);
            }
            Err(DataFusionError::Execution(format!(
                "could not resolve local window property {local} in {zone}"
            )))
        }
    }
}

pub(super) fn local_to_timer_epoch(local: NaiveDateTime, zone: Tz) -> Result<i64> {
    match zone.from_local_datetime(&local) {
        LocalResult::Single(value) => Ok(value.timestamp_millis()),
        LocalResult::Ambiguous(left, right) => {
            Ok(left.timestamp_millis().max(right.timestamp_millis()))
        }
        LocalResult::None => {
            // Flink registers every nonexistent local time in a DST gap at the first valid
            // instant after the gap. Find that boundary without assuming a one-hour transition.
            let mut high = 1i64;
            while high <= 7 * 24 * 60 * 60 * 1_000 {
                let candidate = local
                    .checked_add_signed(chrono::TimeDelta::milliseconds(high))
                    .ok_or_else(|| {
                        DataFusionError::Execution(
                            "window timer overflow while resolving a DST gap".to_string(),
                        )
                    })?;
                if !matches!(zone.from_local_datetime(&candidate), LocalResult::None) {
                    let mut low = 0i64;
                    while low + 1 < high {
                        let middle = low + (high - low) / 2;
                        let candidate = local
                            .checked_add_signed(chrono::TimeDelta::milliseconds(middle))
                            .ok_or_else(|| {
                                DataFusionError::Execution(
                                    "window timer overflow while resolving a DST gap".to_string(),
                                )
                            })?;
                        if matches!(zone.from_local_datetime(&candidate), LocalResult::None) {
                            low = middle;
                        } else {
                            high = middle;
                        }
                    }
                    let first_valid = local
                        .checked_add_signed(chrono::TimeDelta::milliseconds(high))
                        .ok_or_else(|| {
                            DataFusionError::Execution(
                                "window timer overflow while resolving a DST gap".to_string(),
                            )
                        })?;
                    return match zone.from_local_datetime(&first_valid) {
                        LocalResult::Single(value) => Ok(value.timestamp_millis()),
                        LocalResult::Ambiguous(left, right) => {
                            Ok(left.timestamp_millis().max(right.timestamp_millis()))
                        }
                        LocalResult::None => {
                            unreachable!("binary search ended at a valid local time")
                        }
                    };
                }
                high = high.saturating_mul(2);
            }
            Err(DataFusionError::Execution(format!(
                "could not resolve local window timer {local} in {zone}"
            )))
        }
    }
}

fn validate_plan(plan: &proto::WindowAggregate, max_parallelism: u32) -> Result<()> {
    if max_parallelism == 0 {
        return Err(DataFusionError::Plan(
            "window aggregate max parallelism must be positive".to_string(),
        ));
    }
    let kind = proto::WindowKind::try_from(plan.kind)
        .map_err(|_| DataFusionError::Plan(format!("unknown window kind {}", plan.kind)))?;
    match kind {
        proto::WindowKind::Tumble if plan.size_millis > 0 => {}
        proto::WindowKind::Hop
            if plan.size_millis > 0
                && plan.slide_or_step_millis > 0
                && plan.size_millis % plan.slide_or_step_millis == 0 => {}
        proto::WindowKind::Cumulate
            if plan.size_millis > 0
                && plan.slide_or_step_millis > 0
                && plan.size_millis % plan.slide_or_step_millis == 0 => {}
        proto::WindowKind::Session if plan.size_millis > 0 => {}
        proto::WindowKind::Tumble => {
            return Err(DataFusionError::Plan(
                "TUMBLE window size must be positive".to_string(),
            ));
        }
        proto::WindowKind::Hop => {
            return Err(DataFusionError::Plan(
                "HOP size must be an integral multiple of its positive slide".to_string(),
            ));
        }
        proto::WindowKind::Cumulate => {
            return Err(DataFusionError::Plan(
                "CUMULATE size must be an integral multiple of its positive step".to_string(),
            ));
        }
        proto::WindowKind::Session => {
            return Err(DataFusionError::Plan(
                "SESSION gap must be positive".to_string(),
            ));
        }
        proto::WindowKind::Unspecified => {
            return Err(DataFusionError::Plan(
                "window aggregate kind is unspecified".to_string(),
            ));
        }
    }
    if plan.window_properties.iter().any(|value| {
        !matches!(
            proto::WindowProperty::try_from(*value),
            Ok(proto::WindowProperty::Start
                | proto::WindowProperty::End
                | proto::WindowProperty::Time)
        )
    }) {
        return Err(DataFusionError::Plan(
            "window aggregate has an unknown output property".to_string(),
        ));
    }
    Ok(())
}

fn window_state_key(key_group: u32, group_key: &[u8], start: i64, end: i64) -> StateKey {
    let mut key = Vec::with_capacity(1 + group_key.len() + 16);
    key.push(WINDOW_KEY_PREFIX);
    key.extend_from_slice(group_key);
    key.extend_from_slice(&start.to_be_bytes());
    key.extend_from_slice(&end.to_be_bytes());
    StateKey { key_group, key }
}

fn session_index_key(key_group: u32, group_key: &[u8]) -> StateKey {
    let mut key = Vec::with_capacity(1 + group_key.len());
    key.push(SESSION_INDEX_PREFIX);
    key.extend_from_slice(group_key);
    StateKey { key_group, key }
}

fn group_key_from_window_state_key(key: &[u8]) -> Result<&[u8]> {
    if key.len() < 17 || key[0] != WINDOW_KEY_PREFIX {
        return Err(DataFusionError::Execution(
            "session window state key is malformed".to_string(),
        ));
    }
    Ok(&key[1..key.len() - 16])
}

fn apply_session_changes(
    sessions: &mut Vec<ActiveSession>,
    changes: Vec<(bool, SessionEvent)>,
    gap: i64,
    calls: &[Call],
    retain_events: bool,
) -> Result<()> {
    for (accumulate, event) in changes {
        if accumulate {
            let mut start = event.timestamp;
            let mut end = event.timestamp.saturating_add(gap);
            let mut merged_sessions = Vec::new();
            // Re-evaluate after every merge: absorbing one session can expand the namespace
            // enough to overlap another session that did not overlap the incoming row itself.
            // Flink's MergingWindowSet computes this transitive closure.
            while let Some(index) = sessions
                .iter()
                .position(|session| session.start <= end && session.end >= start)
            {
                let merged = sessions.remove(index);
                start = start.min(merged.start);
                end = end.max(merged.end);
                merged_sessions.push(merged);
            }
            let mut accumulator = AccumulatorState::new(calls);
            let mut events = if retain_events {
                Vec::with_capacity(
                    1 + merged_sessions
                        .iter()
                        .map(|session| session.events.len())
                        .sum::<usize>(),
                )
            } else {
                Vec::new()
            };
            // Flink merges existing accumulator namespaces before applying the row that caused
            // the merge. Combining their compact accumulators avoids O(n^2) replay for a session
            // receiving n records while retaining byte-exact retraction data only when needed.
            for merged in merged_sessions {
                accumulator.merge(calls, &merged.accumulator)?;
                if retain_events {
                    events.extend(merged.events);
                }
            }
            accumulator.apply_values(calls, &event.values, true)?;
            if retain_events {
                events.push(event);
                events.sort_by(|left, right| left.timestamp.cmp(&right.timestamp));
            }
            sessions.push(ActiveSession {
                start,
                end,
                accumulator,
                events,
            });
        } else {
            let Some((session_index, event_index)) =
                sessions
                    .iter()
                    .enumerate()
                    .find_map(|(session_index, session)| {
                        session
                            .events
                            .iter()
                            .position(|candidate| candidate == &event)
                            .map(|event_index| (session_index, event_index))
                    })
            else {
                return Err(DataFusionError::Execution(
                    "session window received a retraction without a matching accumulated row"
                        .to_string(),
                ));
            };
            let session = &mut sessions[session_index];
            session.events.remove(event_index);
            session
                .accumulator
                .apply_values(calls, &event.values, false)?;
            // Flink's merging-window operator keeps the namespace of a non-empty session on
            // retraction. It does not shrink or split a session after its bridging row retracts.
            if session.accumulator.row_count == 0 {
                sessions.remove(session_index);
            }
        }
    }
    Ok(())
}

fn encode_session_index(intervals: &[(i64, i64)]) -> Vec<u8> {
    let mut bytes = Vec::with_capacity(9 + intervals.len() * 16);
    bytes.extend_from_slice(SESSION_INDEX_MAGIC);
    bytes.push(1);
    bytes.extend_from_slice(&(intervals.len() as u32).to_le_bytes());
    for &(start, end) in intervals {
        bytes.extend_from_slice(&start.to_le_bytes());
        bytes.extend_from_slice(&end.to_le_bytes());
    }
    bytes
}

fn decode_session_index(bytes: &[u8]) -> Result<Vec<(i64, i64)>> {
    let mut reader = WindowBytesReader::new(bytes);
    if reader.read_exact(4)? != SESSION_INDEX_MAGIC || reader.read_u8()? != 1 {
        return Err(DataFusionError::Execution(
            "invalid native session index state".to_string(),
        ));
    }
    let count = reader.read_u32()? as usize;
    let mut intervals = Vec::with_capacity(count);
    for _ in 0..count {
        intervals.push((reader.read_i64()?, reader.read_i64()?));
    }
    if !reader.is_empty() {
        return Err(DataFusionError::Execution(
            "native session index has trailing bytes".to_string(),
        ));
    }
    Ok(intervals)
}

fn encode_session_state(
    grouping_row: &[u8],
    accumulator: &AccumulatorState,
    events: &[SessionEvent],
) -> Vec<u8> {
    let aggregate = encode_state(accumulator);
    let mut bytes = Vec::new();
    bytes.extend_from_slice(SESSION_STATE_MAGIC);
    bytes.push(1);
    bytes.extend_from_slice(&(grouping_row.len() as u32).to_le_bytes());
    bytes.extend_from_slice(&(aggregate.len() as u32).to_le_bytes());
    bytes.extend_from_slice(&(events.len() as u32).to_le_bytes());
    bytes.extend_from_slice(grouping_row);
    bytes.extend_from_slice(&aggregate);
    for event in events {
        bytes.extend_from_slice(&event.timestamp.to_le_bytes());
        bytes.extend_from_slice(&(event.values.len() as u32).to_le_bytes());
        for value in &event.values {
            match value {
                None => bytes.push(0),
                Some(value) => {
                    bytes.push(1);
                    encode_session_value(value, &mut bytes);
                }
            }
        }
    }
    bytes
}

fn decode_session_state(
    bytes: &[u8],
    calls: &[Call],
) -> Result<(Vec<u8>, AccumulatorState, Vec<SessionEvent>)> {
    let mut reader = WindowBytesReader::new(bytes);
    if reader.read_exact(4)? != SESSION_STATE_MAGIC || reader.read_u8()? != 1 {
        return Err(DataFusionError::Execution(
            "invalid native session window state".to_string(),
        ));
    }
    let grouping_length = reader.read_u32()? as usize;
    let aggregate_length = reader.read_u32()? as usize;
    let event_count = reader.read_u32()? as usize;
    let grouping = reader.read_exact(grouping_length)?.to_vec();
    let accumulator = decode_state(reader.read_exact(aggregate_length)?, calls)?;
    let mut events = Vec::with_capacity(event_count);
    for _ in 0..event_count {
        let timestamp = reader.read_i64()?;
        let value_count = reader.read_u32()? as usize;
        if value_count != calls.len() {
            return Err(DataFusionError::Execution(format!(
                "session event has {value_count} aggregate values, expected {}",
                calls.len()
            )));
        }
        let mut values = Vec::with_capacity(value_count);
        for _ in 0..value_count {
            values.push(match reader.read_u8()? {
                0 => None,
                1 => Some(decode_session_value(&mut reader)?),
                other => {
                    return Err(DataFusionError::Execution(format!(
                        "invalid session aggregate value presence {other}"
                    )));
                }
            });
        }
        events.push(SessionEvent { timestamp, values });
    }
    if !reader.is_empty() {
        return Err(DataFusionError::Execution(
            "native session window state has trailing bytes".to_string(),
        ));
    }
    Ok((grouping, accumulator, events))
}

fn encode_session_value(value: &AggregateValue, bytes: &mut Vec<u8>) {
    match value {
        AggregateValue::Boolean(value) => {
            bytes.push(1);
            bytes.push(*value as u8);
        }
        AggregateValue::Int(value) => {
            bytes.push(2);
            bytes.extend_from_slice(&value.to_le_bytes());
        }
        AggregateValue::Float32(value) => {
            bytes.push(3);
            bytes.extend_from_slice(&value.to_le_bytes());
        }
        AggregateValue::Float64(value) => {
            bytes.push(4);
            bytes.extend_from_slice(&value.to_le_bytes());
        }
        AggregateValue::Bytes(value) => {
            bytes.push(5);
            bytes.extend_from_slice(&(value.len() as u32).to_le_bytes());
            bytes.extend_from_slice(value);
        }
    }
}

fn decode_session_value(reader: &mut WindowBytesReader<'_>) -> Result<AggregateValue> {
    match reader.read_u8()? {
        1 => match reader.read_u8()? {
            0 => Ok(AggregateValue::Boolean(false)),
            1 => Ok(AggregateValue::Boolean(true)),
            other => Err(DataFusionError::Execution(format!(
                "invalid session boolean value {other}"
            ))),
        },
        2 => Ok(AggregateValue::Int(i128::from_le_bytes(
            reader.read_exact(16)?.try_into().unwrap(),
        ))),
        3 => Ok(AggregateValue::Float32(u32::from_le_bytes(
            reader.read_exact(4)?.try_into().unwrap(),
        ))),
        4 => Ok(AggregateValue::Float64(u64::from_le_bytes(
            reader.read_exact(8)?.try_into().unwrap(),
        ))),
        5 => {
            let length = reader.read_u32()? as usize;
            Ok(AggregateValue::Bytes(reader.read_exact(length)?.to_vec()))
        }
        other => Err(DataFusionError::Execution(format!(
            "unknown session aggregate value tag {other}"
        ))),
    }
}

struct WindowBytesReader<'a> {
    bytes: &'a [u8],
    offset: usize,
}

impl<'a> WindowBytesReader<'a> {
    fn new(bytes: &'a [u8]) -> Self {
        Self { bytes, offset: 0 }
    }

    fn read_exact(&mut self, length: usize) -> Result<&'a [u8]> {
        let end = self.offset.checked_add(length).ok_or_else(|| {
            DataFusionError::Execution("native window state length overflow".to_string())
        })?;
        let value = self.bytes.get(self.offset..end).ok_or_else(|| {
            DataFusionError::Execution("truncated native window state".to_string())
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

    fn is_empty(&self) -> bool {
        self.offset == self.bytes.len()
    }
}

fn window_namespace(start: i64, end: i64) -> Vec<u8> {
    let mut bytes = Vec::with_capacity(16);
    bytes.extend_from_slice(&start.to_le_bytes());
    bytes.extend_from_slice(&end.to_le_bytes());
    bytes
}

fn decode_window_namespace(bytes: &[u8]) -> Result<(i64, i64)> {
    if bytes.len() != 16 {
        return Err(DataFusionError::Execution(
            "native window timer namespace has the wrong size".to_string(),
        ));
    }
    Ok((
        i64::from_le_bytes(bytes[..8].try_into().unwrap()),
        i64::from_le_bytes(bytes[8..].try_into().unwrap()),
    ))
}

fn encode_window_state(grouping_row: &[u8], accumulator: &AccumulatorState) -> Vec<u8> {
    let aggregate = encode_state(accumulator);
    let mut output = Vec::with_capacity(9 + grouping_row.len() + aggregate.len());
    output.extend_from_slice(WINDOW_STATE_MAGIC);
    output.push(WINDOW_STATE_VERSION);
    output.extend_from_slice(&(grouping_row.len() as u32).to_le_bytes());
    output.extend_from_slice(grouping_row);
    output.extend_from_slice(&aggregate);
    output
}

fn decode_window_state(bytes: &[u8], calls: &[Call]) -> Result<(Vec<u8>, AccumulatorState)> {
    if bytes.len() < 9 || &bytes[..4] != WINDOW_STATE_MAGIC {
        return Err(DataFusionError::Execution(
            "invalid native window aggregate state magic".to_string(),
        ));
    }
    if bytes[4] != WINDOW_STATE_VERSION {
        return Err(DataFusionError::Execution(format!(
            "unsupported native window aggregate state version {}",
            bytes[4]
        )));
    }
    let grouping_length = u32::from_le_bytes(bytes[5..9].try_into().unwrap()) as usize;
    let aggregate_offset = 9usize.checked_add(grouping_length).ok_or_else(|| {
        DataFusionError::Execution("native window grouping row length overflow".to_string())
    })?;
    if aggregate_offset > bytes.len() {
        return Err(DataFusionError::Execution(
            "truncated native window grouping row".to_string(),
        ));
    }
    Ok((
        bytes[9..aggregate_offset].to_vec(),
        decode_state(&bytes[aggregate_offset..], calls)?,
    ))
}

#[cfg(test)]
mod tests {
    use std::sync::Arc;

    use arrow::array::{Int64Array, TimestampMillisecondArray};
    use prost::Message;

    use super::*;
    use crate::memory_pool::tests_support::TestBroker;
    use crate::PLAN_PROTOCOL_VERSION;

    fn logical_bigint(nullable: bool) -> proto::LogicalType {
        proto::LogicalType {
            nullable,
            r#type: Some(proto::logical_type::Type::Bigint(proto::EmptyType {})),
        }
    }

    fn logical_timestamp(nullable: bool) -> proto::LogicalType {
        proto::LogicalType {
            nullable,
            r#type: Some(proto::logical_type::Type::Timestamp(proto::PrecisionType {
                precision: 3,
            })),
        }
    }

    fn field(name: &str, r#type: proto::LogicalType) -> proto::Field {
        proto::Field {
            name: name.to_string(),
            r#type: Some(r#type),
        }
    }

    fn plan(kind: proto::WindowKind, size: i64, slide: i64, input_changelog: bool) -> Vec<u8> {
        proto::NativePlan {
            protocol_version: PLAN_PROTOCOL_VERSION,
            root: Some(proto::Operator {
                operator: Some(proto::operator::Operator::WindowAggregate(Box::new(
                    proto::WindowAggregate {
                        shift_time_zone: "UTC".to_string(),
                        input: None,
                        grouping_indices: vec![0],
                        aggregate_calls: vec![proto::AggregateCall {
                            function: proto::AggregateFunction::CountStar as i32,
                            input_index: None,
                            input_type: None,
                            output_type: Some(logical_bigint(false)),
                            retractable: true,
                            filter_index: None,
                            distinct: false,
                            accumulator_type: None,
                        }],
                        input_changelog,
                        time_attribute_index: 1,
                        kind: kind as i32,
                        size_millis: size,
                        slide_or_step_millis: slide,
                        offset_millis: 0,
                        processing_time: false,
                        window_properties: vec![
                            proto::WindowProperty::Start as i32,
                            proto::WindowProperty::End as i32,
                        ],
                        input_schema: Some(proto::Schema {
                            fields: vec![
                                field("key", logical_bigint(false)),
                                field("ts", logical_timestamp(false)),
                            ],
                        }),
                        output_schema: Some(proto::Schema {
                            fields: vec![
                                field("key", logical_bigint(false)),
                                field("count", logical_bigint(false)),
                                field("window_start", logical_timestamp(false)),
                                field("window_end", logical_timestamp(false)),
                            ],
                        }),
                    },
                ))),
            }),
        }
        .encode_to_vec()
    }

    fn processor(plan: &[u8], broker: Arc<TestBroker>) -> WindowAggregateProcessor {
        WindowAggregateProcessor::new(
            plan,
            128,
            0,
            127,
            HostMemoryReservation::new(broker, "window state test"),
        )
        .unwrap()
    }

    fn batch(keys: Vec<i64>, timestamps: Vec<i64>, kinds: Option<Vec<i8>>) -> RecordBatch {
        let mut fields = vec![
            Field::new("key", DataType::Int64, false),
            Field::new(
                "ts",
                DataType::Timestamp(arrow::datatypes::TimeUnit::Millisecond, None),
                false,
            ),
        ];
        let mut columns: Vec<ArrayRef> = vec![
            Arc::new(Int64Array::from(keys)),
            Arc::new(TimestampMillisecondArray::from(timestamps)),
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
    fn tumble_emits_only_when_the_watermark_closes_the_window() {
        let broker = Arc::new(TestBroker::new(16 << 20));
        let bytes = plan(proto::WindowKind::Tumble, 10_000, 0, false);
        let mut processor = processor(&bytes, broker.clone());
        let pending = processor
            .process_arrow(batch(vec![1, 1, 2], vec![1_000, 2_000, 3_000], None), 0)
            .unwrap();
        assert_eq!(pending.num_rows(), 0);
        assert_eq!(processor.advance_event_time(9_998).unwrap().num_rows(), 0);
        let output = processor.advance_event_time(9_999).unwrap();
        assert_eq!(output.num_rows(), 2);
        let mut counts = output
            .column(1)
            .as_any()
            .downcast_ref::<Int64Array>()
            .unwrap()
            .values()
            .to_vec();
        counts.sort_unstable();
        assert_eq!(counts, [1, 2]);
        drop(output);
        drop(processor);
        assert_eq!(broker.reserved(), 0);
    }

    #[test]
    fn hopping_windows_retract_and_restore_canonical_timer_state() {
        let broker = Arc::new(TestBroker::new(32 << 20));
        let bytes = plan(proto::WindowKind::Hop, 10_000, 5_000, true);
        let mut before = processor(&bytes, broker.clone());
        before
            .process_arrow(
                batch(
                    vec![7, 7, 7],
                    vec![6_000, 6_000, 6_000],
                    Some(vec![INSERT, INSERT, DELETE]),
                ),
                0,
            )
            .unwrap();
        let key_group = assign_key_group(
            &encode_binary_row(
                &batch(vec![7], vec![6_000], None),
                0,
                &[(0, KeyField::BigInt)],
            )
            .unwrap(),
            128,
        );
        let snapshot = before.snapshot_key_group(key_group).unwrap();
        let mut after = processor(&bytes, broker.clone());
        after.restore_key_group(key_group, &snapshot).unwrap();
        let first = after.advance_event_time(9_999).unwrap();
        assert_eq!(first.num_rows(), 1);
        assert_eq!(
            first
                .column(1)
                .as_any()
                .downcast_ref::<Int64Array>()
                .unwrap()
                .value(0),
            1
        );
        let second = after.advance_event_time(14_999).unwrap();
        assert_eq!(second.num_rows(), 1);
    }

    #[test]
    fn drops_null_and_late_event_time_rows() {
        let broker = Arc::new(TestBroker::new(16 << 20));
        let bytes = plan(proto::WindowKind::Tumble, 10_000, 0, false);
        let mut processor = processor(&bytes, broker);
        processor
            .process_arrow(batch(vec![1], vec![1_000], None), 0)
            .unwrap();
        processor.advance_event_time(9_999).unwrap();
        processor
            .process_arrow(batch(vec![1], vec![2_000], None), 0)
            .unwrap();
        assert_eq!(processor.late_records_dropped(), 1);
    }

    #[test]
    fn session_retractions_preserve_flinks_merged_namespace() {
        let broker = Arc::new(TestBroker::new(32 << 20));
        let bytes = plan(proto::WindowKind::Session, 10_000, 0, true);
        let mut before = processor(&bytes, broker.clone());
        before
            .process_arrow(
                batch(vec![7, 7], vec![0, 8_000], Some(vec![INSERT, INSERT])),
                0,
            )
            .unwrap();
        before
            .process_arrow(batch(vec![7], vec![8_000], Some(vec![DELETE])), 0)
            .unwrap();
        let key_group = assign_key_group(
            &encode_binary_row(&batch(vec![7], vec![0], None), 0, &[(0, KeyField::BigInt)])
                .unwrap(),
            128,
        );
        let snapshot = before.snapshot_key_group(key_group).unwrap();
        drop(before);
        let mut after = processor(&bytes, broker);
        after.restore_key_group(key_group, &snapshot).unwrap();
        assert_eq!(after.advance_event_time(9_999).unwrap().num_rows(), 0);
        let output = after.advance_event_time(17_999).unwrap();
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
        assert_eq!(
            output
                .column(3)
                .as_any()
                .downcast_ref::<TimestampMillisecondArray>()
                .unwrap()
                .value(0),
            18_000
        );
    }

    #[test]
    fn dst_timer_resolution_matches_flinks_gap_and_overlap_rules() {
        let zone = "America/Los_Angeles".parse::<Tz>().unwrap();
        let spring_gap =
            NaiveDateTime::parse_from_str("2021-03-14 02:59:59.999", "%Y-%m-%d %H:%M:%S%.3f")
                .unwrap();
        assert_eq!(
            local_to_timer_epoch(spring_gap, zone).unwrap(),
            1_615_716_000_000
        );
        let fall_overlap =
            NaiveDateTime::parse_from_str("2021-11-07 01:59:00.000", "%Y-%m-%d %H:%M:%S%.3f")
                .unwrap();
        assert_eq!(
            local_to_timer_epoch(fall_overlap, zone).unwrap(),
            1_636_279_140_000
        );
    }
}
