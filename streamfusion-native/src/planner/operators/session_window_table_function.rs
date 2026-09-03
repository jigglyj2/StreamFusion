// Copyright 2026 StreamFusion Authors
// Licensed under the Apache License, Version 2.0

use std::collections::BTreeSet;
use std::sync::Arc;

use ahash::RandomState;
#[cfg(test)]
use arrow::array::ArrayRef;
use arrow::array::{Array, BinaryArray, Int8Array, TimestampMillisecondArray};
use arrow::compute::SortOptions;
use arrow::datatypes::{DataType, Field, Schema, SchemaRef};
use arrow::record_batch::RecordBatch;
use arrow_row::{RowConverter, Rows, SortField};
use chrono::Utc;
use chrono_tz::Tz;
use datafusion::error::{DataFusionError, Result};
use hashbrown::HashMap;

use crate::exchange::{assign_key_group, encode_binary_row, KeyField};
use crate::memory_pool::HostMemoryReservation;
use crate::planner::arrow_schema;
use crate::state::{
    KeyedState, MemoryKeyedState, NativeTimerService, RocksPluginKeyedState, StateKey, StateKeyRef,
    StateMutation, TimerDomain, TimerKey,
};
use crate::{decode_plan, proto};

use super::window_aggregate::local_to_timer_epoch;
use super::window_table_function::timestamp_millis;

const STATE_MAGIC: &[u8; 4] = b"SFSW";
const STATE_VERSION: u8 = 2;
const GROUP_KEY_PREFIX: u8 = 1;
const TIMER_STATE_KEY: &[u8] = b"\0streamfusion-session-window-tvf-timers";

/// Stateful SESSION Window TVF with schema-aware Arrow-row state and native Arrow output.
pub(crate) struct SessionWindowTableFunctionProcessor {
    plan: proto::WindowTableFunction,
    shift_time_zone: Tz,
    max_parallelism: u32,
    state: Box<dyn KeyedState>,
    timers: NativeTimerService,
    visible_schema: SchemaRef,
    output_schema: SchemaRef,
    row_converter: RowConverter,
    input_schema: Option<SchemaRef>,
    key_fields: Vec<(usize, KeyField)>,
    preencoded_key_index: Option<usize>,
    input_kind_index: Option<usize>,
    current_event_time: i64,
    current_processing_time: i64,
    scratch_reservation: HostMemoryReservation,
    late_records_dropped: u64,
    null_rowtimes_dropped: u64,
    state_read_batches: u64,
    state_write_batches: u64,
    timer_registrations: u64,
    timer_deletions: u64,
    timers_fired: u64,
}

#[derive(Clone, Debug, PartialEq, Eq)]
struct StoredEvent {
    sequence: u64,
    row_kind: i8,
    row: Vec<u8>,
}

#[derive(Clone, Debug, PartialEq, Eq)]
struct Session {
    start: i64,
    end: i64,
    events: Vec<StoredEvent>,
}

#[derive(Default, Debug, PartialEq, Eq)]
struct GroupState {
    next_sequence: u64,
    sessions: Vec<Session>,
}

struct StagedGroup {
    key: StateKey,
    value: GroupState,
    touched: bool,
}

impl SessionWindowTableFunctionProcessor {
    pub(crate) fn new(
        serialized_plan: &[u8],
        max_parallelism: u32,
        first_key_group: u32,
        last_key_group: u32,
        state_reservation: HostMemoryReservation,
    ) -> Result<Self> {
        let scratch =
            state_reservation.sibling("native session Window TVF batch scratch and output");
        let timers = state_reservation.sibling("native session Window TVF timers");
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
            timers,
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
        let timers = reservation.sibling("native session Window TVF timers");
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
            timers,
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
            DataFusionError::Plan("session Window TVF plan has no root".to_string())
        })?;
        let plan = match root.operator {
            Some(proto::operator::Operator::WindowTableFunction(plan)) => *plan,
            _ => {
                return Err(DataFusionError::Plan(
                    "session Window TVF handle requires a WindowTableFunction root".to_string(),
                ));
            }
        };
        validate_plan(&plan, max_parallelism)?;
        let visible_schema =
            arrow_schema(plan.input_schema.as_ref().expect("validated input schema"))?;
        let row_converter = row_converter(&visible_schema)?;
        let mut output_fields = visible_schema.fields().iter().cloned().collect::<Vec<_>>();
        for name in ["window_start", "window_end", "window_time"] {
            output_fields.push(Arc::new(Field::new(
                name,
                DataType::Timestamp(arrow::datatypes::TimeUnit::Millisecond, None),
                false,
            )));
        }
        output_fields.push(Arc::new(Field::new(
            "__streamfusion_row_kind",
            DataType::Int8,
            false,
        )));
        let output_schema = Arc::new(Schema::new(output_fields));
        let shift_time_zone = if plan.shift_time_zone.is_empty() {
            chrono_tz::UTC
        } else {
            plan.shift_time_zone.parse::<Tz>().map_err(|error| {
                DataFusionError::Plan(format!(
                    "invalid session Window TVF shift time zone {}: {error}",
                    plan.shift_time_zone
                ))
            })?
        };
        Ok(Self {
            plan,
            shift_time_zone,
            max_parallelism,
            state,
            timers: NativeTimerService::new(first_key_group, last_key_group, timer_reservation)?,
            visible_schema,
            output_schema,
            row_converter,
            input_schema: None,
            key_fields: Vec::new(),
            preencoded_key_index: None,
            input_kind_index: None,
            current_event_time: i64::MIN,
            current_processing_time: i64::MIN,
            scratch_reservation,
            late_records_dropped: 0,
            null_rowtimes_dropped: 0,
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
        if self.plan.processing_time {
            self.current_processing_time = self.current_processing_time.max(processing_time);
        }
        let visible_count = self.visible_schema.fields().len();
        let copied_bytes = batch.columns()[..visible_count]
            .iter()
            .map(|column| column.get_array_memory_size())
            .sum::<usize>()
            .saturating_add(batch.num_rows().saturating_mul(160));
        self.scratch_reservation.resize(copied_bytes)?;
        let encoded_rows = self
            .row_converter
            .convert_columns(&batch.columns()[..visible_count]);
        let result = match encoded_rows {
            Ok(encoded_rows) => self.process_arrow_accounted(&batch, &encoded_rows),
            Err(error) => Err(error.into()),
        };
        match result {
            Ok(output) => self.finish_output(output, copied_bytes),
            Err(error) => {
                self.scratch_reservation.resize(0)?;
                Err(error)
            }
        }
    }

    fn process_arrow_accounted(
        &mut self,
        batch: &RecordBatch,
        encoded_rows: &Rows,
    ) -> Result<RecordBatch> {
        let kinds = batch
            .column(self.input_kind_index.expect("schema prepared"))
            .as_any()
            .downcast_ref::<Int8Array>()
            .ok_or_else(|| {
                DataFusionError::Execution("session Window TVF RowKinds are not Int8".to_string())
            })?;
        let timestamp_column = (!self.plan.processing_time)
            .then(|| batch.column(self.plan.time_attribute_index as usize));
        let mut unique = HashMap::<StateKey, usize, RandomState>::with_capacity_and_hasher(
            batch.num_rows(),
            RandomState::new(),
        );
        let mut changes = Vec::with_capacity(batch.num_rows());
        for row in 0..batch.num_rows() {
            let timestamp = if self.plan.processing_time {
                self.to_window_time(self.current_processing_time)?
            } else {
                let Some(timestamp) = timestamp_millis(
                    timestamp_column.expect("event-time session has a time column"),
                    row,
                )?
                else {
                    self.null_rowtimes_dropped = self.null_rowtimes_dropped.saturating_add(1);
                    continue;
                };
                self.to_window_time(timestamp)?
            };
            let group_key = self.group_key(batch, row)?;
            let key_group = assign_key_group(&group_key, self.max_parallelism);
            let state_key = group_state_key(key_group, &group_key);
            let next = unique.len();
            let index = *unique.entry(state_key).or_insert(next);
            changes.push((
                index,
                timestamp,
                kinds.value(row),
                encoded_rows.row(row).data().to_vec(),
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
            .map(|key| key.expect("session group state index is populated"))
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
                Ok(StagedGroup {
                    key,
                    value: bytes
                        .map(|bytes| decode_state(bytes.as_ref()))
                        .transpose()?
                        .unwrap_or_default(),
                    touched: false,
                })
            })
            .collect::<Result<Vec<_>>>()?;
        let domain = self.timer_domain();
        let progress = self.current_progress();
        let mut dirty_timer_groups = BTreeSet::new();
        for (index, timestamp, row_kind, row) in changes {
            let entry = &mut staged[index];
            let mut start = timestamp;
            let mut end = timestamp.saturating_add(self.plan.size_millis);
            let mut merged_indices = Vec::new();
            for (session_index, session) in entry.value.sessions.iter().enumerate() {
                if start <= session.end && end >= session.start {
                    start = start.min(session.start);
                    end = end.max(session.end);
                    merged_indices.push(session_index);
                }
            }
            let deadline = timer_timestamp(self.shift_time_zone, end.saturating_sub(1))?;
            if deadline <= progress {
                self.late_records_dropped = self.late_records_dropped.saturating_add(1);
                continue;
            }
            let sequence = entry.value.next_sequence;
            entry.value.next_sequence =
                entry.value.next_sequence.checked_add(1).ok_or_else(|| {
                    DataFusionError::Execution("session Window TVF sequence overflow".to_string())
                })?;
            let mut events = vec![StoredEvent {
                sequence,
                row_kind,
                row,
            }];
            for &session_index in merged_indices.iter().rev() {
                let session = entry.value.sessions.remove(session_index);
                let old_timer =
                    session_timer(&entry.key, session.start, session.end, self.shift_time_zone)?;
                if self
                    .timers
                    .delete(entry.key.key_group, domain, &old_timer)?
                {
                    self.timer_deletions = self.timer_deletions.saturating_add(1);
                    dirty_timer_groups.insert(entry.key.key_group);
                }
                events.extend(session.events);
            }
            events.sort_unstable_by_key(|event| event.sequence);
            let session = Session { start, end, events };
            let timer = session_timer(&entry.key, start, end, self.shift_time_zone)?;
            if self.timers.register(entry.key.key_group, domain, timer)? {
                self.timer_registrations = self.timer_registrations.saturating_add(1);
                dirty_timer_groups.insert(entry.key.key_group);
            }
            entry.value.sessions.push(session);
            entry
                .value
                .sessions
                .sort_unstable_by_key(|session| (session.start, session.end));
            entry.touched = true;
        }
        let mut mutations = staged
            .into_iter()
            .filter(|entry| entry.touched)
            .map(|entry| StateMutation {
                key: entry.key,
                value: Some(encode_state(&entry.value)),
            })
            .collect::<Vec<_>>();
        self.append_timer_mutations(&mut mutations, dirty_timer_groups)?;
        if !mutations.is_empty() {
            self.state.write_batch(mutations)?;
            self.state_write_batches = self.state_write_batches.saturating_add(1);
        }
        self.empty_output()
    }

    pub(crate) fn advance_event_time(&mut self, timestamp: i64) -> Result<RecordBatch> {
        if self.plan.processing_time || timestamp <= self.current_event_time {
            return self.empty_output();
        }
        self.current_event_time = timestamp;
        self.fire_due(TimerDomain::EventTime, timestamp)
    }

    pub(crate) fn advance_processing_time(&mut self, timestamp: i64) -> Result<RecordBatch> {
        if !self.plan.processing_time || timestamp <= self.current_processing_time {
            return self.empty_output();
        }
        self.current_processing_time = timestamp;
        self.fire_due(TimerDomain::ProcessingTime, timestamp)
    }

    fn fire_due(&mut self, domain: TimerDomain, timestamp: i64) -> Result<RecordBatch> {
        let fired = self.timers.advance(domain, timestamp)?;
        self.timers_fired = self.timers_fired.saturating_add(fired.len() as u64);
        if fired.is_empty() {
            return self.empty_output();
        }
        let mut by_key =
            HashMap::<StateKey, Vec<(i64, i64)>, RandomState>::with_hasher(RandomState::new());
        let mut dirty_groups = BTreeSet::new();
        for timer in fired {
            let (start, end) = decode_namespace(&timer.timer.namespace)?;
            dirty_groups.insert(timer.key_group);
            by_key
                .entry(StateKey {
                    key_group: timer.key_group,
                    key: timer.timer.key,
                })
                .or_default()
                .push((start, end));
        }
        let keys = by_key.keys().cloned().collect::<Vec<_>>();
        let refs = keys
            .iter()
            .map(|key| StateKeyRef {
                key_group: key.key_group,
                key: &key.key,
            })
            .collect::<Vec<_>>();
        let existing = self.state.get_batch(&refs)?;
        self.state_read_batches = self.state_read_batches.saturating_add(1);
        let mut output_rows = Vec::new();
        let mut mutations = Vec::new();
        for (key, bytes) in keys.into_iter().zip(existing) {
            let Some(bytes) = bytes else { continue };
            let mut state = decode_state(bytes.as_ref())?;
            let fired_sessions = by_key.get(&key).expect("fired session key exists");
            let mut kept = Vec::with_capacity(state.sessions.len());
            for mut session in state.sessions.drain(..) {
                if fired_sessions.contains(&(session.start, session.end)) {
                    session.events.sort_unstable_by_key(|event| event.sequence);
                    for event in session.events {
                        output_rows.push((event.row, session.start, session.end, event.row_kind));
                    }
                } else {
                    kept.push(session);
                }
            }
            state.sessions = kept;
            mutations.push(StateMutation {
                key,
                value: (!state.sessions.is_empty()).then(|| encode_state(&state)),
            });
        }
        self.append_timer_mutations(&mut mutations, dirty_groups)?;
        if !mutations.is_empty() {
            self.state.write_batch(mutations)?;
            self.state_write_batches = self.state_write_batches.saturating_add(1);
        }
        let output = self.output_batch(output_rows)?;
        self.finish_output(output, 0)
    }

    pub(crate) fn next_processing_time_timer(&self) -> i64 {
        self.timers
            .next_timestamp(TimerDomain::ProcessingTime)
            .unwrap_or(i64::MAX)
    }

    pub(crate) fn late_records_dropped(&self) -> u64 {
        self.late_records_dropped
    }

    pub(crate) fn null_rowtimes_dropped(&self) -> u64 {
        self.null_rowtimes_dropped
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
        let timer = self.state.get_batch(&[StateKeyRef {
            key_group,
            key: TIMER_STATE_KEY,
        }])?;
        self.state_read_batches = self.state_read_batches.saturating_add(1);
        if let Some(bytes) = timer.into_iter().next().flatten() {
            self.timers.restore_key_group(key_group, bytes.as_ref())?;
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

    fn group_key(&self, batch: &RecordBatch, row: usize) -> Result<Vec<u8>> {
        match self.preencoded_key_index {
            Some(index) => Ok(batch
                .column(index)
                .as_any()
                .downcast_ref::<BinaryArray>()
                .ok_or_else(|| {
                    DataFusionError::Execution("session Window TVF key is not Binary".to_string())
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
                    "session Window TVF input schema changed while running".to_string(),
                ));
            }
            return Ok(());
        }
        self.preencoded_key_index = metadata_index(&schema, "__streamfusion_key");
        self.input_kind_index = metadata_index(&schema, "__streamfusion_input_row_kind");
        if self.input_kind_index.is_none() {
            return Err(DataFusionError::Execution(
                "session Window TVF requires RowKind metadata".to_string(),
            ));
        }
        let visible_count = [
            self.preencoded_key_index,
            self.input_kind_index,
            Some(schema.fields().len()),
        ]
        .into_iter()
        .flatten()
        .min()
        .unwrap();
        if visible_count != self.visible_schema.fields().len()
            || schema.fields()[..visible_count]
                .iter()
                .zip(self.visible_schema.fields())
                .any(|(actual, planned)| actual.data_type() != planned.data_type())
        {
            return Err(DataFusionError::Execution(
                "session Window TVF Arrow schema does not match its protobuf input schema"
                    .to_string(),
            ));
        }
        if !self.plan.processing_time && self.plan.time_attribute_index as usize >= visible_count {
            return Err(DataFusionError::Plan(
                "session Window TVF time attribute is outside the visible row".to_string(),
            ));
        }
        if self.preencoded_key_index.is_none() {
            self.key_fields = self
                .plan
                .partition_key_indices
                .iter()
                .map(|&index| {
                    let field = schema.fields().get(index as usize).ok_or_else(|| {
                        arrow::error::ArrowError::SchemaError(format!(
                            "session partition key {index} is outside the visible row"
                        ))
                    })?;
                    Ok((
                        index as usize,
                        KeyField::from_arrow_type(field.data_type())?,
                    ))
                })
                .collect::<std::result::Result<Vec<_>, arrow::error::ArrowError>>()?;
        }
        self.input_schema = Some(schema);
        Ok(())
    }

    fn to_window_time(&self, epoch_millis: i64) -> Result<i64> {
        if self.shift_time_zone == chrono_tz::UTC || epoch_millis == i64::MAX {
            return Ok(epoch_millis);
        }
        let instant =
            chrono::DateTime::<Utc>::from_timestamp_millis(epoch_millis).ok_or_else(|| {
                DataFusionError::Execution(format!(
                    "session Window TVF timestamp {epoch_millis} is outside chrono's range"
                ))
            })?;
        Ok(instant
            .with_timezone(&self.shift_time_zone)
            .naive_local()
            .and_utc()
            .timestamp_millis())
    }

    fn timer_domain(&self) -> TimerDomain {
        if self.plan.processing_time {
            TimerDomain::ProcessingTime
        } else {
            TimerDomain::EventTime
        }
    }

    fn current_progress(&self) -> i64 {
        if self.plan.processing_time {
            self.current_processing_time
        } else {
            self.current_event_time
        }
    }

    fn empty_output(&self) -> Result<RecordBatch> {
        Ok(RecordBatch::new_empty(self.output_schema.clone()))
    }

    fn output_batch(&self, rows: Vec<(Vec<u8>, i64, i64, i8)>) -> Result<RecordBatch> {
        let parser = self.row_converter.parser();
        let mut columns = self
            .row_converter
            .convert_rows(rows.iter().map(|(row, _, _, _)| parser.parse(row)))?;
        let starts = rows
            .iter()
            .map(|(_, start, _, _)| *start)
            .collect::<Vec<_>>();
        let ends = rows.iter().map(|(_, _, end, _)| *end).collect::<Vec<_>>();
        let times = ends
            .iter()
            .map(|end| end.saturating_sub(1))
            .collect::<Vec<_>>();
        let kinds = rows.iter().map(|(_, _, _, kind)| *kind).collect::<Vec<_>>();
        columns.push(Arc::new(TimestampMillisecondArray::from(starts)));
        columns.push(Arc::new(TimestampMillisecondArray::from(ends)));
        columns.push(Arc::new(TimestampMillisecondArray::from(times)));
        columns.push(Arc::new(Int8Array::from(kinds)));
        Ok(RecordBatch::try_new(self.output_schema.clone(), columns)?)
    }

    fn finish_output(&mut self, output: RecordBatch, base: usize) -> Result<RecordBatch> {
        let output_bytes = output.get_array_memory_size();
        self.scratch_reservation.resize(output_bytes.max(base))?;
        self.scratch_reservation.transfer_to_arrow(output_bytes)?;
        self.scratch_reservation.resize(0)?;
        Ok(output)
    }
}

fn validate_plan(plan: &proto::WindowTableFunction, max_parallelism: u32) -> Result<()> {
    if max_parallelism == 0 {
        return Err(DataFusionError::Plan(
            "session Window TVF max parallelism must be positive".to_string(),
        ));
    }
    if proto::WindowKind::try_from(plan.kind) != Ok(proto::WindowKind::Session)
        || plan.size_millis <= 0
    {
        return Err(DataFusionError::Plan(
            "stateful Window TVF requires a positive SESSION gap".to_string(),
        ));
    }
    if plan.input_schema.is_none() {
        return Err(DataFusionError::Plan(
            "session Window TVF input schema is missing".to_string(),
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

fn group_state_key(key_group: u32, group_key: &[u8]) -> StateKey {
    let mut key = Vec::with_capacity(1 + group_key.len());
    key.push(GROUP_KEY_PREFIX);
    key.extend_from_slice(group_key);
    StateKey { key_group, key }
}

fn session_timer(key: &StateKey, start: i64, end: i64, zone: Tz) -> Result<TimerKey> {
    let mut namespace = Vec::with_capacity(16);
    namespace.extend_from_slice(&start.to_le_bytes());
    namespace.extend_from_slice(&end.to_le_bytes());
    Ok(TimerKey {
        timestamp: timer_timestamp(zone, end.saturating_sub(1))?,
        key: key.key.clone(),
        namespace,
    })
}

fn decode_namespace(bytes: &[u8]) -> Result<(i64, i64)> {
    if bytes.len() != 16 {
        return Err(DataFusionError::Execution(
            "session Window TVF timer namespace is malformed".to_string(),
        ));
    }
    Ok((
        i64::from_le_bytes(bytes[..8].try_into().unwrap()),
        i64::from_le_bytes(bytes[8..].try_into().unwrap()),
    ))
}

fn timer_timestamp(zone: Tz, local_millis: i64) -> Result<i64> {
    if zone == chrono_tz::UTC || local_millis == i64::MAX {
        return Ok(local_millis);
    }
    let local = chrono::DateTime::<Utc>::from_timestamp_millis(local_millis)
        .ok_or_else(|| {
            DataFusionError::Execution(format!(
                "session Window TVF timer {local_millis} is outside chrono's range"
            ))
        })?
        .naive_utc();
    local_to_timer_epoch(local, zone)
}

fn encode_state(state: &GroupState) -> Vec<u8> {
    let mut bytes = Vec::new();
    bytes.extend_from_slice(STATE_MAGIC);
    bytes.push(STATE_VERSION);
    bytes.extend_from_slice(&state.next_sequence.to_le_bytes());
    write_u32(&mut bytes, state.sessions.len());
    for session in &state.sessions {
        bytes.extend_from_slice(&session.start.to_le_bytes());
        bytes.extend_from_slice(&session.end.to_le_bytes());
        write_u32(&mut bytes, session.events.len());
        for event in &session.events {
            bytes.extend_from_slice(&event.sequence.to_le_bytes());
            bytes.push(event.row_kind as u8);
            write_u32(&mut bytes, event.row.len());
            bytes.extend_from_slice(&event.row);
        }
    }
    bytes
}

fn decode_state(bytes: &[u8]) -> Result<GroupState> {
    if bytes.len() < 17 || &bytes[..4] != STATE_MAGIC || bytes[4] != STATE_VERSION {
        return Err(DataFusionError::Execution(
            "invalid native session Window TVF state".to_string(),
        ));
    }
    let mut offset = 5;
    let next_sequence = read_u64(bytes, &mut offset)?;
    let count = read_u32(bytes, &mut offset)? as usize;
    let mut sessions = Vec::with_capacity(count);
    for _ in 0..count {
        let start = read_i64(bytes, &mut offset)?;
        let end = read_i64(bytes, &mut offset)?;
        let event_count = read_u32(bytes, &mut offset)? as usize;
        let mut events = Vec::with_capacity(event_count);
        for _ in 0..event_count {
            let sequence = read_u64(bytes, &mut offset)?;
            let row_kind = *bytes.get(offset).ok_or_else(truncated)? as i8;
            offset += 1;
            let length = read_u32(bytes, &mut offset)? as usize;
            let row = read_slice(bytes, &mut offset, length)?.to_vec();
            events.push(StoredEvent {
                sequence,
                row_kind,
                row,
            });
        }
        sessions.push(Session { start, end, events });
    }
    if offset != bytes.len() {
        return Err(DataFusionError::Execution(
            "session Window TVF state has trailing bytes".to_string(),
        ));
    }
    Ok(GroupState {
        next_sequence,
        sessions,
    })
}

fn write_u32(bytes: &mut Vec<u8>, value: usize) {
    bytes.extend_from_slice(&(value as u32).to_le_bytes());
}

fn read_u32(bytes: &[u8], offset: &mut usize) -> Result<u32> {
    Ok(u32::from_le_bytes(read_exact::<4>(bytes, offset)?))
}

fn read_u64(bytes: &[u8], offset: &mut usize) -> Result<u64> {
    Ok(u64::from_le_bytes(read_exact::<8>(bytes, offset)?))
}

fn read_i64(bytes: &[u8], offset: &mut usize) -> Result<i64> {
    Ok(i64::from_le_bytes(read_exact::<8>(bytes, offset)?))
}

fn read_exact<const N: usize>(bytes: &[u8], offset: &mut usize) -> Result<[u8; N]> {
    Ok(read_slice(bytes, offset, N)?.try_into().unwrap())
}

fn read_slice<'a>(bytes: &'a [u8], offset: &mut usize, length: usize) -> Result<&'a [u8]> {
    let end = offset.checked_add(length).ok_or_else(truncated)?;
    let result = bytes.get(*offset..end).ok_or_else(truncated)?;
    *offset = end;
    Ok(result)
}

fn truncated() -> DataFusionError {
    DataFusionError::Execution("truncated native session Window TVF state".to_string())
}

fn row_converter(schema: &SchemaRef) -> Result<RowConverter> {
    let fields = schema
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
        .collect();
    Ok(RowConverter::new(fields)?)
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::memory_pool::{tests_support::TestBroker, HostMemoryReservation};
    use arrow::array::{BinaryArray, Int64Array, TimestampMillisecondArray};
    use prost::Message;

    #[test]
    fn canonical_state_preserves_sessions_rows_and_changelog() {
        let state = GroupState {
            next_sequence: 4,
            sessions: vec![Session {
                start: 10,
                end: 30,
                events: vec![
                    StoredEvent {
                        sequence: 1,
                        row_kind: 0,
                        row: vec![1, 2],
                    },
                    StoredEvent {
                        sequence: 3,
                        row_kind: 3,
                        row: vec![9],
                    },
                ],
            }],
        };
        assert_eq!(decode_state(&encode_state(&state)).unwrap(), state);
    }

    #[test]
    fn flink_session_boundaries_merge_inclusively() {
        let mut sessions = vec![Session {
            start: 0,
            end: 10,
            events: Vec::new(),
        }];
        let start = 10;
        let end = 20;
        let merged = sessions
            .iter()
            .position(|session| start <= session.end && end >= session.start);
        assert_eq!(merged, Some(0));
        sessions.clear();
    }

    #[test]
    fn emits_merged_sessions_directly_as_arrow_and_accounts_memory() {
        let broker = Arc::new(TestBroker::new(64 << 20));
        let mut processor = SessionWindowTableFunctionProcessor::new(
            &plan(),
            128,
            0,
            127,
            HostMemoryReservation::new(broker.clone(), "session TVF test"),
        )
        .unwrap();
        processor
            .process_arrow(
                batch(&[7, 7], &[10, 12], &[b"insert", b"delete"], &[0, 3]),
                0,
            )
            .unwrap();
        let output = processor.advance_event_time(16).unwrap();
        assert_eq!(output.num_rows(), 2);
        let payloads = output
            .column(2)
            .as_any()
            .downcast_ref::<BinaryArray>()
            .unwrap();
        assert_eq!(payloads.value(0), b"insert");
        assert_eq!(payloads.value(1), b"delete");
        for (column, expected) in [(3, 10), (4, 17), (5, 16)] {
            let values = output
                .column(column)
                .as_any()
                .downcast_ref::<TimestampMillisecondArray>()
                .unwrap();
            assert_eq!(values.values(), &[expected, expected]);
        }
        assert_eq!(
            output
                .column(6)
                .as_any()
                .downcast_ref::<Int8Array>()
                .unwrap()
                .values(),
            &[0, 3]
        );
        drop(output);
        drop(processor);
        assert_eq!(broker.reserved(), 0);
    }

    fn plan() -> Vec<u8> {
        proto::NativePlan {
            protocol_version: crate::PLAN_PROTOCOL_VERSION,
            root: Some(proto::Operator {
                operator: Some(proto::operator::Operator::WindowTableFunction(Box::new(
                    proto::WindowTableFunction {
                        input: None,
                        time_attribute_index: 1,
                        kind: proto::WindowKind::Session as i32,
                        size_millis: 5,
                        slide_or_step_millis: 0,
                        offset_millis: 0,
                        partition_key_indices: vec![0],
                        processing_time: false,
                        input_schema: Some(proto::Schema {
                            fields: vec![
                                proto_field(
                                    "key",
                                    proto::logical_type::Type::Bigint(proto::EmptyType::default()),
                                ),
                                proto_field(
                                    "ts",
                                    proto::logical_type::Type::Timestamp(proto::PrecisionType {
                                        precision: 3,
                                    }),
                                ),
                                proto_field(
                                    "payload",
                                    proto::logical_type::Type::Binary(proto::EmptyType::default()),
                                ),
                            ],
                        }),
                        shift_time_zone: "UTC".to_string(),
                    },
                ))),
            }),
        }
        .encode_to_vec()
    }

    fn proto_field(name: &str, r#type: proto::logical_type::Type) -> proto::Field {
        proto::Field {
            name: name.to_string(),
            r#type: Some(proto::LogicalType {
                nullable: true,
                r#type: Some(r#type),
            }),
        }
    }

    fn batch(keys: &[i64], timestamps: &[i64], payloads: &[&[u8]], kinds: &[i8]) -> RecordBatch {
        RecordBatch::try_from_iter(vec![
            ("key", Arc::new(Int64Array::from(keys.to_vec())) as ArrayRef),
            (
                "ts",
                Arc::new(TimestampMillisecondArray::from(timestamps.to_vec())) as ArrayRef,
            ),
            (
                "payload",
                Arc::new(BinaryArray::from_vec(payloads.to_vec())) as ArrayRef,
            ),
            (
                "__streamfusion_input_row_kind",
                Arc::new(Int8Array::from(kinds.to_vec())) as ArrayRef,
            ),
        ])
        .unwrap()
    }
}
