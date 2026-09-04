// Copyright 2026 StreamFusion Authors
// Licensed under the Apache License, Version 2.0

use std::collections::{BTreeMap, BTreeSet};
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
    KeyedState, MemoryKeyedState, NativeTimerService, RocksPluginKeyedState, StateKey, StateKeyRef,
    StateMutation, TimerDomain, TimerKey,
};
use crate::{decode_plan, proto};

use super::window_table_function::timestamp_millis;

mod state_codec;

use state_codec::{decode_state, encode_state, ProbeRow, StagedState, TemporalState, VersionRow};

const INSERT: i8 = 0;
const UPDATE_BEFORE: i8 = 1;
const UPDATE_AFTER: i8 = 2;
const DELETE: i8 = 3;
const JOIN_STATE_PREFIX: u8 = 1;
const EVENT_NAMESPACE: &[u8] = b"event";
const CLEANUP_NAMESPACE: &[u8] = b"cleanup";
const TIMER_STATE_KEY: &[u8] = b"\0streamfusion-temporal-join-timers";

#[derive(Clone)]
struct OutputRow {
    left: Vec<u8>,
    right: Option<Vec<u8>>,
    matched: bool,
    kind: i8,
    input_ordinal: i32,
}

struct Change {
    state_index: usize,
    timestamp: i64,
    kind: i8,
    matchable: bool,
    row: Vec<u8>,
    ordinal: i32,
}

/// Persistent Arrow-native implementation of Flink's temporal-table join contracts.
pub(crate) struct TemporalJoinProcessor {
    plan: proto::TemporalJoin,
    mode: proto::TemporalJoinTimeMode,
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

impl TemporalJoinProcessor {
    pub(crate) fn new(
        serialized_plan: &[u8],
        max_parallelism: u32,
        first_key_group: u32,
        last_key_group: u32,
        state_reservation: HostMemoryReservation,
    ) -> Result<Self> {
        let timer_reservation = state_reservation.sibling("native temporal join timers");
        let scratch_reservation =
            state_reservation.sibling("native temporal join batch scratch and output");
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
        let timer_reservation = reservation.sibling("native RocksDB temporal join timers");
        let scratch_reservation =
            reservation.sibling("native RocksDB temporal join batch scratch and output");
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
            scratch_reservation,
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
            .ok_or_else(|| DataFusionError::Plan("temporal join plan has no root".to_string()))?;
        let plan = match root.operator {
            Some(proto::operator::Operator::TemporalJoin(plan)) => plan,
            _ => {
                return Err(DataFusionError::Plan(
                    "temporal join handle requires a TemporalJoin root".to_string(),
                ));
            }
        };
        validate_plan(&plan, max_parallelism)?;
        let mode = proto::TemporalJoinTimeMode::try_from(plan.time_mode).map_err(|_| {
            DataFusionError::Plan(format!("unknown temporal join mode {}", plan.time_mode))
        })?;
        let join_type = proto::RegularJoinType::try_from(plan.join_type).map_err(|_| {
            DataFusionError::Plan(format!("unknown temporal join type {}", plan.join_type))
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
                format!("__streamfusion_temporal_left_{index}"),
                field.data_type().clone(),
                field.is_nullable(),
            )));
        }
        for (index, field) in visible_schemas[1].fields().iter().enumerate() {
            output_fields.push(Arc::new(Field::new(
                format!("__streamfusion_temporal_right_{index}"),
                field.data_type().clone(),
                field.is_nullable() || matches!(join_type, proto::RegularJoinType::Left),
            )));
        }
        output_fields.push(Arc::new(Field::new(
            "__streamfusion_temporal_matched",
            DataType::Int8,
            false,
        )));
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
            mode,
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
                "temporal join side must be zero or one".to_string(),
            ));
        }
        self.current_processing_time = self.current_processing_time.max(processing_time);
        self.prepare_schema(side, batch.schema())?;
        let visible_count = self.visible_schemas[side].fields().len();
        let input_bytes = batch.columns()[..visible_count]
            .iter()
            .map(|column| column.get_array_memory_size())
            .sum::<usize>();
        let base = input_bytes.saturating_add(batch.num_rows().saturating_mul(192));
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
                DataFusionError::Execution("temporal join RowKinds are not Int8".to_string())
            })?;
        let mut unique = HashMap::<StateKey, usize, RandomState>::with_capacity_and_hasher(
            batch.num_rows(),
            RandomState::new(),
        );
        let mut changes = Vec::with_capacity(batch.num_rows());
        for row in 0..batch.num_rows() {
            let group_key = self.group_key(side, batch, row)?;
            let key_group = assign_key_group(&group_key, self.max_parallelism);
            let state_key = join_state_key(key_group, &group_key);
            let next = unique.len();
            let state_index = *unique.entry(state_key).or_insert(next);
            let kind = kinds.value(row);
            if !matches!(kind, INSERT | UPDATE_BEFORE | UPDATE_AFTER | DELETE) {
                return Err(DataFusionError::Execution(format!(
                    "unknown Flink RowKind byte {kind}"
                )));
            }
            changes.push(Change {
                state_index,
                timestamp: self.row_time(side, batch, row)?,
                kind,
                matchable: self.row_is_matchable(side, batch, row),
                row: encoded.row(row).data().to_vec(),
                ordinal: i32::try_from(row).map_err(|_| {
                    DataFusionError::Execution("temporal join batch exceeds i32 rows".to_string())
                })?,
            });
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
            .map(|key| key.expect("temporal join state index is populated"))
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
        for change in changes {
            let entry = &mut staged[change.state_index];
            if matches!(self.mode, proto::TemporalJoinTimeMode::ProcessingTime) {
                self.process_processing_change(side, change, entry, &mut output)?;
            } else {
                self.process_event_change(side, change, entry)?;
            }
            entry.touched = true;
        }
        let mutations = staged
            .into_iter()
            .filter(|entry| entry.touched)
            .map(|entry| {
                Ok(StateMutation {
                    key: entry.key,
                    value: (!state_is_empty(&entry.value))
                        .then(|| encode_state(&entry.value))
                        .transpose()?,
                })
            })
            .collect::<Result<Vec<_>>>()?;
        if !mutations.is_empty() {
            self.state.write_batch(mutations)?;
            self.state_write_batches = self.state_write_batches.saturating_add(1);
        }
        self.output_batch(output)
    }

    fn process_event_change(
        &mut self,
        side: usize,
        change: Change,
        entry: &mut StagedState,
    ) -> Result<()> {
        if side == 0 {
            let sequence = entry.value.next_sequence;
            entry.value.next_sequence = sequence.wrapping_add(1);
            entry.value.left.push(ProbeRow {
                sequence,
                timestamp: change.timestamp,
                kind: change.kind,
                matchable: change.matchable,
                row: change.row,
            });
        } else {
            entry.value.right.insert(
                change.timestamp,
                VersionRow {
                    kind: change.kind,
                    matchable: change.matchable,
                    row: change.row,
                },
            );
        }
        self.register_smallest_event_timer(entry, change.timestamp)?;
        self.refresh_cleanup_timer(entry)
    }

    fn process_processing_change(
        &mut self,
        side: usize,
        change: Change,
        entry: &mut StagedState,
        output: &mut Vec<OutputRow>,
    ) -> Result<()> {
        if side == 1 {
            if matches!(change.kind, INSERT | UPDATE_AFTER) {
                entry.value.right.clear();
                entry.value.right.insert(
                    self.current_processing_time,
                    VersionRow {
                        kind: change.kind,
                        matchable: change.matchable,
                        row: change.row,
                    },
                );
                self.refresh_cleanup_timer(entry)?;
            } else {
                entry.value.right.clear();
                self.delete_cleanup_timer(entry)?;
            }
            return Ok(());
        }
        let right = entry.value.right.last_key_value().map(|(_, value)| value);
        if change.matchable {
            if let Some(right) = right.filter(|value| value.matchable) {
                output.push(OutputRow {
                    left: change.row,
                    right: Some(right.row.clone()),
                    matched: true,
                    kind: change.kind,
                    input_ordinal: change.ordinal,
                });
                self.refresh_cleanup_timer(entry)?;
                return Ok(());
            }
        }
        if matches!(self.join_type, proto::RegularJoinType::Left) {
            output.push(OutputRow {
                left: change.row,
                right: None,
                matched: false,
                kind: change.kind,
                input_ordinal: change.ordinal,
            });
        }
        Ok(())
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
        let mut mutations = Vec::with_capacity(keys.len());
        for (((key_group, key_bytes), timers), (key, value)) in
            grouped.into_iter().zip(keys.into_iter().zip(values))
        {
            self.dirty_timer_groups.insert(key_group);
            let Some(value) = value else {
                continue;
            };
            let mut state = decode_state(&value)?;
            for timer in timers {
                if timer.namespace == EVENT_NAMESPACE && state.event_timer == Some(timer.timestamp)
                {
                    state.event_timer = None;
                    self.emit_event_rows(progress, &mut state, &mut output);
                    if let Some(next) = state.left.iter().map(|row| row.timestamp).min() {
                        self.register_event_timer(key_group, &key_bytes, &mut state, next)?;
                    }
                } else if timer.namespace == CLEANUP_NAMESPACE
                    && state.cleanup_timer == Some(timer.timestamp)
                {
                    state.cleanup_timer = None;
                    state.left.clear();
                    state.right.clear();
                    state.next_sequence = 0;
                    if let Some(event) = state.event_timer.take() {
                        self.delete_timer(
                            key_group,
                            &key_bytes,
                            TimerDomain::EventTime,
                            event,
                            EVENT_NAMESPACE,
                        )?;
                    }
                }
            }
            mutations.push(StateMutation {
                key,
                value: (!state_is_empty(&state))
                    .then(|| encode_state(&state))
                    .transpose()?,
            });
        }
        if !mutations.is_empty() {
            self.state.write_batch(mutations)?;
            self.state_write_batches = self.state_write_batches.saturating_add(1);
        }
        self.output_batch(output)
    }

    fn emit_event_rows(
        &self,
        watermark: i64,
        state: &mut TemporalState,
        output: &mut Vec<OutputRow>,
    ) {
        let mut future = Vec::with_capacity(state.left.len());
        let mut due = Vec::new();
        for row in state.left.drain(..) {
            if row.timestamp <= watermark {
                due.push(row);
            } else {
                future.push(row);
            }
        }
        due.sort_by_key(|row| row.sequence);
        for left in due {
            let right = state
                .right
                .range(..=left.timestamp)
                .next_back()
                .map(|(_, row)| row)
                .filter(|row| matches!(row.kind, INSERT | UPDATE_AFTER))
                .filter(|row| row.matchable && left.matchable);
            if let Some(right) = right {
                output.push(OutputRow {
                    left: left.row,
                    right: Some(right.row.clone()),
                    matched: true,
                    kind: left.kind,
                    input_ordinal: -1,
                });
            } else if matches!(self.join_type, proto::RegularJoinType::Left) {
                output.push(OutputRow {
                    left: left.row,
                    right: None,
                    matched: false,
                    kind: left.kind,
                    input_ordinal: -1,
                });
            }
        }
        state.left = future;
        let keep = state
            .right
            .range(..=watermark)
            .next_back()
            .map(|(&time, _)| time);
        if let Some(keep) = keep {
            let expired = state
                .right
                .range(..keep)
                .map(|(&time, _)| time)
                .collect::<Vec<_>>();
            for time in expired {
                state.right.remove(&time);
            }
        }
    }

    fn register_smallest_event_timer(
        &mut self,
        entry: &mut StagedState,
        timestamp: i64,
    ) -> Result<()> {
        if let Some(current) = entry.value.event_timer {
            if current <= timestamp {
                return Ok(());
            }
            self.delete_timer(
                entry.key.key_group,
                &entry.key.key,
                TimerDomain::EventTime,
                current,
                EVENT_NAMESPACE,
            )?;
        }
        self.register_event_timer(
            entry.key.key_group,
            &entry.key.key,
            &mut entry.value,
            timestamp,
        )
    }

    fn register_event_timer(
        &mut self,
        key_group: u32,
        key: &[u8],
        state: &mut TemporalState,
        timestamp: i64,
    ) -> Result<()> {
        let timer = TimerKey {
            timestamp,
            key: key.to_vec(),
            namespace: EVENT_NAMESPACE.to_vec(),
        };
        if self
            .timers
            .register(key_group, TimerDomain::EventTime, timer)?
        {
            self.timer_registrations = self.timer_registrations.saturating_add(1);
            self.dirty_timer_groups.insert(key_group);
        }
        state.event_timer = Some(timestamp);
        Ok(())
    }

    fn refresh_cleanup_timer(&mut self, entry: &mut StagedState) -> Result<()> {
        if self.plan.min_state_retention_millis <= 1 {
            return Ok(());
        }
        let min = self.plan.min_state_retention_millis as i64;
        if entry
            .value
            .cleanup_timer
            .is_some_and(|timer| self.current_processing_time.wrapping_add(min) <= timer)
        {
            return Ok(());
        }
        self.delete_cleanup_timer(entry)?;
        let timestamp = self
            .current_processing_time
            .wrapping_add(self.plan.max_state_retention_millis as i64);
        let timer = TimerKey {
            timestamp,
            key: entry.key.key.clone(),
            namespace: CLEANUP_NAMESPACE.to_vec(),
        };
        if self
            .timers
            .register(entry.key.key_group, TimerDomain::ProcessingTime, timer)?
        {
            self.timer_registrations = self.timer_registrations.saturating_add(1);
            self.dirty_timer_groups.insert(entry.key.key_group);
        }
        entry.value.cleanup_timer = Some(timestamp);
        Ok(())
    }

    fn delete_cleanup_timer(&mut self, entry: &mut StagedState) -> Result<()> {
        if let Some(timestamp) = entry.value.cleanup_timer.take() {
            self.delete_timer(
                entry.key.key_group,
                &entry.key.key,
                TimerDomain::ProcessingTime,
                timestamp,
                CLEANUP_NAMESPACE,
            )?;
        }
        Ok(())
    }

    fn delete_timer(
        &mut self,
        key_group: u32,
        key: &[u8],
        domain: TimerDomain,
        timestamp: i64,
        namespace: &[u8],
    ) -> Result<()> {
        if self.timers.delete(
            key_group,
            domain,
            &TimerKey {
                timestamp,
                key: key.to_vec(),
                namespace: namespace.to_vec(),
            },
        )? {
            self.timer_deletions = self.timer_deletions.saturating_add(1);
            self.dirty_timer_groups.insert(key_group);
        }
        Ok(())
    }

    fn row_time(&self, side: usize, batch: &RecordBatch, row: usize) -> Result<i64> {
        if matches!(self.mode, proto::TemporalJoinTimeMode::ProcessingTime) {
            return Ok(self.current_processing_time);
        }
        let index = if side == 0 {
            self.plan.left_time_index
        } else {
            self.plan.right_time_index
        } as usize;
        Ok(timestamp_millis(batch.column(index), row)?.unwrap_or(0))
    }

    fn group_key(&self, side: usize, batch: &RecordBatch, row: usize) -> Result<Vec<u8>> {
        match self.preencoded_key_indices[side] {
            Some(index) => Ok(batch
                .column(index)
                .as_any()
                .downcast_ref::<BinaryArray>()
                .ok_or_else(|| {
                    DataFusionError::Execution(
                        "temporal join preencoded keys are not Binary".to_string(),
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
                    "temporal join input {side} schema changed while running"
                )));
            }
            return Ok(());
        }
        self.preencoded_key_indices[side] = metadata_index(&schema, "__streamfusion_key");
        self.input_kind_indices[side] = metadata_index(&schema, "__streamfusion_input_row_kind")
            .or_else(|| metadata_index(&schema, "__streamfusion_row_kind"));
        if self.input_kind_indices[side].is_none() {
            return Err(DataFusionError::Execution(
                "temporal join requires RowKind metadata".to_string(),
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
        let time_index = if side == 0 {
            self.plan.left_time_index
        } else {
            self.plan.right_time_index
        } as usize;
        if visible_count != self.visible_schemas[side].fields().len()
            || schema.fields()[..visible_count]
                .iter()
                .zip(self.visible_schemas[side].fields())
                .any(|(actual, planned)| actual.data_type() != planned.data_type())
            || (matches!(self.mode, proto::TemporalJoinTimeMode::EventTime)
                && time_index >= visible_count)
        {
            return Err(DataFusionError::Execution(format!(
                "temporal join input {side} Arrow schema does not match its protobuf schema"
            )));
        }
        if self.preencoded_key_indices[side].is_none() {
            self.key_fields[side] = self
                .key_indices(side)
                .iter()
                .map(|&index| {
                    let field = schema.fields().get(index as usize).ok_or_else(|| {
                        arrow::error::ArrowError::SchemaError(format!(
                            "temporal join input {side} key {index} is outside the visible row"
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

    pub(crate) fn snapshot_key_group(&mut self, key_group: u32) -> Result<Vec<u8>> {
        self.flush_timer_groups([key_group])?;
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
        let mut matched = Vec::with_capacity(rows.len());
        let mut ordinals = Vec::with_capacity(rows.len());
        for row in rows {
            let left_index = u32::try_from(encoded[0].len()).map_err(|_| {
                DataFusionError::Execution("temporal join output exceeds u32 rows".to_string())
            })?;
            encoded[0].push(row.left);
            selections[0].push(Some(left_index));
            if let Some(right) = row.right {
                let right_index = u32::try_from(encoded[1].len()).map_err(|_| {
                    DataFusionError::Execution("temporal join output exceeds u32 rows".to_string())
                })?;
                encoded[1].push(right);
                selections[1].push(Some(right_index));
            } else {
                selections[1].push(None);
            }
            matched.push(row.matched as i8);
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
        columns.push(Arc::new(Int8Array::from(matched)));
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

fn join_state_key(key_group: u32, group_key: &[u8]) -> StateKey {
    let mut key = Vec::with_capacity(group_key.len() + 1);
    key.push(JOIN_STATE_PREFIX);
    key.extend_from_slice(group_key);
    StateKey { key_group, key }
}

fn state_is_empty(state: &TemporalState) -> bool {
    state.left.is_empty()
        && state.right.is_empty()
        && state.event_timer.is_none()
        && state.cleanup_timer.is_none()
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

fn validate_plan(plan: &proto::TemporalJoin, max_parallelism: u32) -> Result<()> {
    let mode = proto::TemporalJoinTimeMode::try_from(plan.time_mode).ok();
    let join_type = proto::RegularJoinType::try_from(plan.join_type).ok();
    if max_parallelism == 0
        || plan.left_key_indices.len() != plan.right_key_indices.len()
        || plan.left_key_indices.len() != plan.filter_nulls.len()
        || plan.left_schema.is_none()
        || plan.right_schema.is_none()
        || !matches!(
            mode,
            Some(
                proto::TemporalJoinTimeMode::EventTime
                    | proto::TemporalJoinTimeMode::ProcessingTime
            )
        )
        || !matches!(
            join_type,
            Some(proto::RegularJoinType::Inner | proto::RegularJoinType::Left)
        )
        || plan.max_state_retention_millis < plan.min_state_retention_millis
    {
        return Err(DataFusionError::Plan(
            "temporal join key/schema/type/time/TTL contract is invalid".to_string(),
        ));
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::memory_pool::{tests_support::TestBroker, HostMemoryReservation};
    use arrow::array::{ArrayRef, Int64Array, StringArray, TimestampMillisecondArray};
    use prost::Message;

    #[test]
    fn event_time_uses_latest_version_and_preserves_probe_changelog() {
        let broker = Arc::new(TestBroker::new(64 << 20));
        let mut join = processor(
            broker.clone(),
            proto::TemporalJoinTimeMode::EventTime,
            0,
            127,
        );
        join.process_arrow(
            1,
            batch(
                &[1, 1, 1],
                &[1, 4, 7],
                &["r1", "r4", "gone"],
                &[INSERT, UPDATE_AFTER, DELETE],
            ),
            10,
        )
        .unwrap();
        join.process_arrow(
            0,
            batch(
                &[1, 1, 1, 1],
                &[0, 2, 5, 8],
                &["l0", "l2", "l5", "l8"],
                &[INSERT, INSERT, UPDATE_BEFORE, DELETE],
            ),
            10,
        )
        .unwrap();
        let output = join.advance_event_time(8).unwrap();
        assert_eq!(kinds(&output), vec![INSERT, INSERT, UPDATE_BEFORE, DELETE]);
        let right_values = output
            .column(5)
            .as_any()
            .downcast_ref::<StringArray>()
            .unwrap();
        assert!(right_values.is_null(0));
        assert_eq!(right_values.value(1), "r1");
        assert_eq!(right_values.value(2), "r4");
        assert!(right_values.is_null(3));
        assert_eq!(join.statistics()[5], 0);
        drop(output);
        drop(join);
        assert_eq!(broker.reserved(), 0);
    }

    #[test]
    fn processing_time_probes_current_value_and_ttl_clears_it() {
        let broker = Arc::new(TestBroker::new(64 << 20));
        let mut join = processor_with_ttl(
            broker.clone(),
            proto::TemporalJoinTimeMode::ProcessingTime,
            100,
            150,
        );
        join.process_arrow(1, batch(&[1], &[0], &["right"], &[INSERT]), 10)
            .unwrap();
        let matched = join
            .process_arrow(0, batch(&[1], &[0], &["left"], &[UPDATE_BEFORE]), 20)
            .unwrap();
        assert_eq!(kinds(&matched), vec![UPDATE_BEFORE]);
        assert_eq!(join.next_processing_timer(), Some(160));
        join.advance_processing_time(160).unwrap();
        let missing = join
            .process_arrow(0, batch(&[1], &[0], &["later"], &[INSERT]), 170)
            .unwrap();
        assert_eq!(missing.num_rows(), 1);
        assert!(missing.column(5).is_null(0));
        drop(matched);
        drop(missing);
        drop(join);
        assert_eq!(broker.reserved(), 0);
    }

    #[test]
    fn canonical_key_group_state_restores_after_rescaling() {
        let broker = Arc::new(TestBroker::new(128 << 20));
        let mut source = processor(
            broker.clone(),
            proto::TemporalJoinTimeMode::EventTime,
            0,
            127,
        );
        source
            .process_arrow(
                1,
                batch(&[1, 2], &[1, 1], &["r1", "r2"], &[INSERT, INSERT]),
                5,
            )
            .unwrap();
        source
            .process_arrow(
                0,
                batch(&[1, 2], &[2, 2], &["l1", "l2"], &[INSERT, INSERT]),
                5,
            )
            .unwrap();
        let snapshots = (0..128)
            .map(|group| source.snapshot_key_group(group).unwrap())
            .collect::<Vec<_>>();
        let mut low = processor(
            broker.clone(),
            proto::TemporalJoinTimeMode::EventTime,
            0,
            63,
        );
        let mut high = processor(
            broker.clone(),
            proto::TemporalJoinTimeMode::EventTime,
            64,
            127,
        );
        for (group, snapshot) in snapshots.iter().enumerate() {
            if group < 64 {
                low.restore_key_group(group as u32, snapshot).unwrap();
            } else {
                high.restore_key_group(group as u32, snapshot).unwrap();
            }
        }
        let mut output = kinds(&low.advance_event_time(2).unwrap());
        output.extend(kinds(&high.advance_event_time(2).unwrap()));
        assert_eq!(output.len(), 2);
        drop(source);
        drop(low);
        drop(high);
        assert_eq!(broker.reserved(), 0);
    }

    #[test]
    fn canonical_state_moves_from_memory_to_rocksdb_with_batched_io() {
        let Ok(plugin_path) = std::env::var("STREAMFUSION_TEST_ROCKSDB_PLUGIN") else {
            return;
        };
        let broker = Arc::new(TestBroker::new(1 << 30));
        let mut memory = processor(
            broker.clone(),
            proto::TemporalJoinTimeMode::EventTime,
            0,
            127,
        );
        memory
            .process_arrow(
                1,
                batch(&[1, 2], &[1, 1], &["r1", "r2"], &[INSERT, INSERT]),
                5,
            )
            .unwrap();
        memory
            .process_arrow(
                0,
                batch(&[1, 2], &[2, 2], &["l1", "l2"], &[INSERT, INSERT]),
                5,
            )
            .unwrap();
        assert_eq!(memory.statistics()[0..2], [2, 2]);
        let snapshots = (0..128)
            .map(|group| memory.snapshot_key_group(group).unwrap())
            .collect::<Vec<_>>();
        let directory = tempfile::tempdir().unwrap();
        let mut rocks = TemporalJoinProcessor::new_rocksdb(
            &plan(proto::TemporalJoinTimeMode::EventTime, 0, 0),
            128,
            0,
            127,
            std::path::Path::new(&plugin_path),
            directory.path(),
            64 << 20,
            HostMemoryReservation::new(broker, "temporal join RocksDB scratch"),
        )
        .unwrap();
        for (group, snapshot) in snapshots.iter().enumerate() {
            rocks.restore_key_group(group as u32, snapshot).unwrap();
            assert_eq!(rocks.snapshot_key_group(group as u32).unwrap(), *snapshot);
        }
        assert_eq!(kinds(&rocks.advance_event_time(2).unwrap()).len(), 2);
        assert_eq!(rocks.statistics()[0], 129);
    }

    fn processor(
        broker: Arc<TestBroker>,
        mode: proto::TemporalJoinTimeMode,
        first: u32,
        last: u32,
    ) -> TemporalJoinProcessor {
        TemporalJoinProcessor::new(
            &plan(mode, 0, 0),
            128,
            first,
            last,
            HostMemoryReservation::new(broker, "temporal join test"),
        )
        .unwrap()
    }

    fn processor_with_ttl(
        broker: Arc<TestBroker>,
        mode: proto::TemporalJoinTimeMode,
        min_ttl: u64,
        max_ttl: u64,
    ) -> TemporalJoinProcessor {
        TemporalJoinProcessor::new(
            &plan(mode, min_ttl, max_ttl),
            128,
            0,
            127,
            HostMemoryReservation::new(broker, "temporal join ttl test"),
        )
        .unwrap()
    }

    fn plan(mode: proto::TemporalJoinTimeMode, min_ttl: u64, max_ttl: u64) -> Vec<u8> {
        proto::NativePlan {
            protocol_version: crate::PLAN_PROTOCOL_VERSION,
            root: Some(proto::Operator {
                operator: Some(proto::operator::Operator::TemporalJoin(
                    proto::TemporalJoin {
                        left_key_indices: vec![0],
                        right_key_indices: vec![0],
                        filter_nulls: vec![true],
                        left_schema: Some(schema()),
                        right_schema: Some(schema()),
                        join_type: proto::RegularJoinType::Left as i32,
                        time_mode: mode as i32,
                        left_time_index: 1,
                        right_time_index: 1,
                        min_state_retention_millis: min_ttl,
                        max_state_retention_millis: max_ttl,
                    },
                )),
            }),
        }
        .encode_to_vec()
    }

    fn schema() -> proto::Schema {
        proto::Schema {
            fields: vec![
                field(
                    "key",
                    proto::logical_type::Type::Bigint(proto::EmptyType {}),
                ),
                field(
                    "time",
                    proto::logical_type::Type::Timestamp(proto::PrecisionType { precision: 3 }),
                ),
                field(
                    "value",
                    proto::logical_type::Type::Varchar(proto::EmptyType {}),
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

    fn batch(keys: &[i64], times: &[i64], values: &[&str], row_kinds: &[i8]) -> RecordBatch {
        RecordBatch::try_from_iter(vec![
            ("key", Arc::new(Int64Array::from(keys.to_vec())) as ArrayRef),
            (
                "time",
                Arc::new(TimestampMillisecondArray::from(times.to_vec())) as ArrayRef,
            ),
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
