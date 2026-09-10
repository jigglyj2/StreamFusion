// Copyright 2026 StreamFusion Authors
// Licensed under the Apache License, Version 2.0

use std::collections::BTreeSet;
use std::sync::Arc;

use ahash::RandomState;
use arrow::array::{Array, BinaryArray, Int32Array, Int8Array, UInt32Array};
use arrow::compute::{take, SortOptions};
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
    KeyedState, NativeTimerService, OrderedMemoryKeyedState, RocksPluginKeyedState, StateKey,
    StateKeyRef, StateMutation, TimerDomain, TimerKey,
};
use crate::{decode_plan, proto};

mod indexed_state;
mod legacy_state;
pub(crate) mod planning;
use indexed_state::{Header, WindowKeys};
use legacy_state::decode_state;
#[cfg(test)]
use legacy_state::{encode_state, JoinWindowState};

use super::window_aggregate::local_to_timer_epoch;
use super::window_table_function::timestamp_millis;

const INSERT: i8 = 0;
const UPDATE_BEFORE: i8 = 1;
const UPDATE_AFTER: i8 = 2;
const DELETE: i8 = 3;
const WINDOW_KEY_PREFIX: u8 = 1;
const TIMER_STATE_KEY: &[u8] = b"\0streamfusion-window-join-timers";

/// Two-input Window Join storage with Arrow-row state and Arrow condition-input output.
pub(crate) struct WindowJoinProcessor {
    plan: proto::WindowJoin,
    shift_time_zone: Tz,
    max_parallelism: u32,
    state: Box<dyn KeyedState>,
    window_key_converter: RowConverter,
    dirty_timer_groups: BTreeSet<u32>,
    timers: NativeTimerService,
    visible_schemas: [SchemaRef; 2],
    output_schema: SchemaRef,
    row_converters: [RowConverter; 2],
    schemas: [Option<SchemaRef>; 2],
    key_fields: [Vec<(usize, KeyField)>; 2],
    preencoded_key_indices: [Option<usize>; 2],
    input_kind_indices: [Option<usize>; 2],
    current_event_time: i64,
    scratch_reservation: HostMemoryReservation,
    late_records_dropped: [u64; 2],
    state_read_batches: u64,
    state_write_batches: u64,
    timer_registrations: u64,
    timer_deletions: u64,
    timers_fired: u64,
}

struct StagedWindow {
    key: StateKey,
    window_end: i64,
    keys: WindowKeys,
    value: Header,
}

impl WindowJoinProcessor {
    pub(crate) fn new(
        serialized_plan: &[u8],
        max_parallelism: u32,
        first_key_group: u32,
        last_key_group: u32,
        state_reservation: HostMemoryReservation,
    ) -> Result<Self> {
        let scratch = state_reservation.sibling("native window join batch scratch and output");
        let timers = state_reservation.sibling("native window join timers");
        let state = Box::new(OrderedMemoryKeyedState::new(
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
        let timers = reservation.sibling("native window join timers");
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
        let root = native_plan
            .root
            .ok_or_else(|| DataFusionError::Plan("window join plan has no root".to_string()))?;
        let plan = match root.operator {
            Some(proto::operator::Operator::WindowJoin(plan)) => *plan,
            _ => {
                return Err(DataFusionError::Plan(
                    "window join handle requires a WindowJoin root".to_string(),
                ));
            }
        };
        validate_plan(&plan, max_parallelism)?;
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
            output_fields.extend(schema.fields().iter().enumerate().map(|(index, field)| {
                Arc::new(Field::new(
                    format!("__streamfusion_join_{side}_{index}"),
                    field.data_type().clone(),
                    true,
                ))
            }));
        }
        output_fields.push(Arc::new(Field::new(
            "__streamfusion_join_side",
            DataType::Int8,
            false,
        )));
        output_fields.push(Arc::new(Field::new(
            "__streamfusion_window_group",
            DataType::Int32,
            false,
        )));
        let output_schema = Arc::new(Schema::new(output_fields));
        let shift_time_zone = if plan.shift_time_zone.is_empty() {
            chrono_tz::UTC
        } else {
            plan.shift_time_zone.parse::<Tz>().map_err(|error| {
                DataFusionError::Plan(format!(
                    "invalid window join shift time zone {}: {error}",
                    plan.shift_time_zone
                ))
            })?
        };
        Ok(Self {
            plan,
            shift_time_zone,
            max_parallelism,
            state,
            window_key_converter: RowConverter::new(vec![SortField::new(DataType::Int64)])?,
            dirty_timer_groups: BTreeSet::new(),
            timers: NativeTimerService::new(first_key_group, last_key_group, timer_reservation)?,
            visible_schemas,
            output_schema,
            row_converters,
            schemas: [None, None],
            key_fields: [Vec::new(), Vec::new()],
            preencoded_key_indices: [None, None],
            input_kind_indices: [None, None],
            current_event_time: i64::MIN,
            scratch_reservation,
            late_records_dropped: [0, 0],
            state_read_batches: 0,
            state_write_batches: 0,
            timer_registrations: 0,
            timer_deletions: 0,
            timers_fired: 0,
        })
    }

    pub(crate) fn process_arrow(&mut self, side: usize, batch: RecordBatch) -> Result<RecordBatch> {
        if side > 1 {
            return Err(DataFusionError::Execution(
                "window join side must be zero or one".to_string(),
            ));
        }
        self.prepare_schema(side, batch.schema())?;
        let visible_count = self.visible_schemas[side].fields().len();
        let copied_rows = batch.columns()[..visible_count]
            .iter()
            .map(|column| column.get_array_memory_size())
            .sum::<usize>();
        let copied_keys = self.preencoded_key_indices[side]
            .and_then(|index| batch.column(index).as_any().downcast_ref::<BinaryArray>())
            .map(|keys| keys.iter().flatten().map(<[u8]>::len).sum::<usize>())
            .unwrap_or(0);
        let base = copied_rows
            .saturating_mul(8)
            .saturating_add(copied_keys.saturating_mul(8))
            .saturating_add(batch.num_rows().saturating_mul(512));
        self.scratch_reservation.resize(base)?;
        let encoded_rows =
            self.row_converters[side].convert_columns(&batch.columns()[..visible_count]);
        let result = match encoded_rows {
            Ok(encoded_rows) => self.process_arrow_accounted(side, &batch, &encoded_rows),
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

    fn process_arrow_accounted(
        &mut self,
        side: usize,
        batch: &RecordBatch,
        encoded_rows: &Rows,
    ) -> Result<RecordBatch> {
        let kinds = batch
            .column(self.input_kind_indices[side].expect("schema prepared"))
            .as_any()
            .downcast_ref::<Int8Array>()
            .ok_or_else(|| {
                DataFusionError::Execution("window join RowKinds are not Int8".to_string())
            })?;
        let window_end_column = batch.column(self.window_end_index(side));
        let mut unique = HashMap::<StateKey, usize, RandomState>::with_capacity_and_hasher(
            batch.num_rows(),
            RandomState::new(),
        );
        let mut changes = Vec::with_capacity(batch.num_rows());
        for row in 0..batch.num_rows() {
            let Some(window_end) = timestamp_millis(window_end_column, row)? else {
                continue;
            };
            let deadline = self.timer_timestamp(window_end.wrapping_sub(1))?;
            if window_end != i64::MAX && deadline <= self.current_event_time {
                self.late_records_dropped[side] = self.late_records_dropped[side].saturating_add(1);
                continue;
            }
            let group_key = self.group_key(side, batch, row)?;
            let key_group = assign_key_group(&group_key, self.max_parallelism);
            let state_key = window_state_key(key_group, &group_key, window_end);
            let next = unique.len();
            let index = *unique.entry(state_key).or_insert(next);
            match kinds.value(row) {
                INSERT | UPDATE_AFTER => {}
                UPDATE_BEFORE | DELETE => {
                    return Err(DataFusionError::Execution(
                        "Flink Window Join does not support on-time retraction input".into(),
                    ))
                }
                other => {
                    return Err(DataFusionError::Execution(format!(
                        "unknown Flink RowKind byte {other}"
                    )));
                }
            };
            changes.push((index, encoded_rows.row(row).data().to_vec()));
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
            .map(|key| key.expect("window join state index is populated"))
            .collect::<Vec<_>>();
        let window_keys = keys
            .iter()
            .map(|key| WindowKeys::new(key, &mut self.window_key_converter))
            .collect::<Result<Vec<_>>>()?;
        let refs = window_keys
            .iter()
            .map(|keys| StateKeyRef {
                key_group: keys.header.key_group,
                key: &keys.header.key,
            })
            .collect::<Vec<_>>();
        let existing = self.state.get_batch(&refs, &self.scratch_reservation)?;
        self.state_read_batches = self.state_read_batches.saturating_add(1);
        let mut staged = keys
            .into_iter()
            .zip(window_keys)
            .zip(existing)
            .map(|((key, keys), bytes)| {
                Ok(StagedWindow {
                    window_end: decode_window_end(&key.key)?,
                    key,
                    keys,
                    value: bytes
                        .map(|v| Header::decode(v.as_ref()))
                        .transpose()?
                        .unwrap_or_default(),
                })
            })
            .collect::<Result<Vec<_>>>()?;
        let mut mutations = Vec::with_capacity(changes.len() + staged.len());
        for (index, row) in changes {
            let entry = &mut staged[index];
            let sequence = entry.value.append(side, row.len())?;
            mutations.push(StateMutation {
                key: entry.keys.payload_key(side, sequence),
                value: Some(row),
            });
        }
        for entry in staged {
            let timer = TimerKey {
                timestamp: self.timer_timestamp(entry.window_end.wrapping_sub(1))?,
                key: entry.key.key,
                namespace: entry.window_end.to_le_bytes().to_vec(),
            };
            if self
                .timers
                .register(entry.key.key_group, TimerDomain::EventTime, timer)?
            {
                self.timer_registrations = self.timer_registrations.saturating_add(1);
                self.dirty_timer_groups.insert(entry.key.key_group);
            }
            mutations.push(StateMutation {
                key: entry.keys.header,
                value: Some(entry.value.encode()),
            });
        }
        self.state.write_batch(mutations)?;
        self.state_write_batches = self.state_write_batches.saturating_add(1);
        self.empty_output()
    }

    pub(crate) fn advance_event_time(&mut self, watermark: i64) -> Result<RecordBatch> {
        if watermark <= self.current_event_time {
            return self.empty_output();
        }
        let mut workspace = self
            .scratch_reservation
            .sibling("window join closed-window payload and output workspace");
        workspace.resize(
            self.timers
                .due_timer_bytes(TimerDomain::EventTime, watermark)
                .saturating_mul(4),
        )?;
        self.current_event_time = watermark;
        let fired = self.timers.advance(TimerDomain::EventTime, watermark)?;
        self.timers_fired = self.timers_fired.saturating_add(fired.len() as u64);
        if fired.is_empty() {
            return self.empty_output();
        }
        let keys = fired
            .iter()
            .map(|timer| {
                WindowKeys::new(
                    &StateKey {
                        key_group: timer.key_group,
                        key: timer.timer.key.clone(),
                    },
                    &mut self.window_key_converter,
                )
            })
            .collect::<Result<Vec<_>>>()?;
        let refs = keys
            .iter()
            .map(|key| StateKeyRef {
                key_group: key.header.key_group,
                key: &key.header.key,
            })
            .collect::<Vec<_>>();
        let states = self.state.get_batch(&refs, &self.scratch_reservation)?;
        self.state_read_batches = self.state_read_batches.saturating_add(1);
        let headers = states
            .into_iter()
            .map(|state| {
                state
                    .map(|v| Header::decode(v.as_ref()))
                    .transpose()?
                    .ok_or_else(|| {
                        DataFusionError::Execution("window join timer has no window index".into())
                    })
            })
            .collect::<Result<Vec<_>>>()?;
        let bound = headers.iter().zip(&keys).fold(0usize, |n, (header, keys)| {
            n.saturating_add(header.workspace_bound(keys))
        });
        let field_headroom = self
            .visible_schemas
            .iter()
            .map(|schema| schema.fields().len())
            .sum::<usize>()
            .saturating_mul(1024);
        workspace.resize(
            workspace
                .size()
                .saturating_add(bound)
                .saturating_add(field_headroom)
                .saturating_add(super::sortable_state::PAGE_BYTES),
        )?;
        let mut rows = Vec::new();
        let mut mutations = Vec::new();
        for (group, ((timer, keys), header)) in fired.into_iter().zip(keys).zip(headers).enumerate()
        {
            let group = i32::try_from(group).map_err(|_| {
                DataFusionError::Execution("window join fired group count exceeds i32".into())
            })?;
            indexed_state::read_window(
                self.state.as_ref(),
                &keys,
                &header,
                group,
                &mut rows,
                &mut mutations,
            )?;
            self.state_read_batches = self.state_read_batches.saturating_add(1);
            self.dirty_timer_groups.insert(timer.key_group);
            mutations.push(StateMutation {
                key: keys.header,
                value: None,
            });
        }
        // Construct before deleting durable payloads. A failed allocation must fail the task,
        // whose last Flink checkpoint remains the recovery authority.
        let output = self.output_batch(rows)?;
        self.state.write_batch(mutations)?;
        self.state_write_batches = self.state_write_batches.saturating_add(1);
        self.scratch_reservation
            .grow_from(&mut workspace, output.get_array_memory_size())?;
        self.finish_output(output, 0)
    }

    pub(crate) fn late_records_dropped(&self) -> [u64; 2] {
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
            0,
        ]
    }

    pub(crate) fn state_memory(&self) -> HostMemoryReservation {
        self.scratch_reservation.sibling("native state transfer")
    }

    pub(crate) fn snapshot_key_group(
        &mut self,
        key_group: u32,
    ) -> Result<crate::state::SnapshotBytes> {
        self.flush_timers(key_group)?;
        self.state
            .snapshot_key_group(key_group, &self.scratch_reservation)
    }

    pub(crate) fn restore_key_group(&mut self, key_group: u32, bytes: &[u8]) -> Result<()> {
        self.state
            .restore_key_group(key_group, bytes, &self.scratch_reservation)?;
        indexed_state::migrate_legacy(
            self.state.as_mut(),
            key_group,
            bytes,
            &mut self.window_key_converter,
            &self.scratch_reservation,
        )?;
        let timer = self.state.get_batch(
            &[StateKeyRef {
                key_group,
                key: TIMER_STATE_KEY,
            }],
            &self.scratch_reservation,
        )?;
        let _loaded_state_workspace =
            crate::state::reserve_decoded_values(&timer, &self.scratch_reservation)?;
        self.state_read_batches = self.state_read_batches.saturating_add(1);
        if let Some(bytes) = timer.into_iter().next().flatten() {
            self.timers.restore_key_group(key_group, bytes.as_ref())?;
        }
        Ok(())
    }

    pub(crate) fn checkpoint(&mut self, directory: &std::path::Path) -> Result<()> {
        for group in self.dirty_timer_groups.iter().copied().collect::<Vec<_>>() {
            self.flush_timers(group)?;
        }
        self.state.checkpoint(directory)
    }

    fn flush_timers(&mut self, key_group: u32) -> Result<()> {
        if self.dirty_timer_groups.contains(&key_group) {
            let mut workspace = self
                .scratch_reservation
                .sibling("window join timer checkpoint workspace");
            // Timer serialization scales with retained timer keys, not incoming records.
            workspace.resize(self.timers.snapshot_size_bound(key_group)?)?;
            self.state.write_batch(vec![StateMutation {
                key: StateKey {
                    key_group,
                    key: TIMER_STATE_KEY.to_vec(),
                },
                value: Some(self.timers.snapshot_key_group(key_group)?),
            }])?;
            self.dirty_timer_groups.remove(&key_group);
        }
        Ok(())
    }

    fn group_key(&self, side: usize, batch: &RecordBatch, row: usize) -> Result<Vec<u8>> {
        match self.preencoded_key_indices[side] {
            Some(index) => Ok(binary_column(batch, Some(index), "preencoded key")?
                .value(row)
                .to_vec()),
            None if self.key_fields[side].is_empty() => Ok(Vec::new()),
            None => Ok(encode_binary_row(batch, row, &self.key_fields[side])?),
        }
    }

    fn prepare_schema(&mut self, side: usize, schema: SchemaRef) -> Result<()> {
        if let Some(expected) = &self.schemas[side] {
            if expected.as_ref() != schema.as_ref() {
                return Err(DataFusionError::Execution(format!(
                    "window join input {side} schema changed while running"
                )));
            }
            return Ok(());
        }
        self.preencoded_key_indices[side] = metadata_index(&schema, "__streamfusion_key");
        self.input_kind_indices[side] = metadata_index(&schema, "__streamfusion_input_row_kind");
        if self.input_kind_indices[side].is_none() {
            return Err(DataFusionError::Execution(
                "window join requires RowKind metadata".to_string(),
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
                "window join input {side} Arrow schema does not match its protobuf schema"
            )));
        }
        if self.window_end_index(side) >= visible_count {
            return Err(DataFusionError::Plan(format!(
                "window join input {side} window end is outside the visible row"
            )));
        }
        if self.preencoded_key_indices[side].is_none() {
            self.key_fields[side] = self
                .key_indices(side)
                .iter()
                .map(|&index| {
                    let field = schema.fields().get(index as usize).ok_or_else(|| {
                        arrow::error::ArrowError::SchemaError(format!(
                            "window join input {side} key {index} is outside the visible row"
                        ))
                    })?;
                    Ok((
                        index as usize,
                        KeyField::from_arrow_type(field.data_type())?,
                    ))
                })
                .collect::<std::result::Result<Vec<_>, arrow::error::ArrowError>>()?;
        }
        self.schemas[side] = Some(schema);
        Ok(())
    }

    fn key_indices(&self, side: usize) -> &[u32] {
        if side == 0 {
            &self.plan.left_key_indices
        } else {
            &self.plan.right_key_indices
        }
    }

    fn window_end_index(&self, side: usize) -> usize {
        if side == 0 {
            self.plan.left_window_end_index as usize
        } else {
            self.plan.right_window_end_index as usize
        }
    }

    fn timer_timestamp(&self, local_millis: i64) -> Result<i64> {
        if self.shift_time_zone == chrono_tz::UTC || local_millis == i64::MAX {
            return Ok(local_millis);
        }
        let local = chrono::DateTime::<Utc>::from_timestamp_millis(local_millis)
            .ok_or_else(|| {
                DataFusionError::Execution(format!(
                    "window join timer {local_millis} is outside chrono's range"
                ))
            })?
            .naive_utc();
        local_to_timer_epoch(local, self.shift_time_zone)
    }

    fn empty_output(&self) -> Result<RecordBatch> {
        Ok(RecordBatch::new_empty(self.output_schema.clone()))
    }

    fn output_batch(&self, rows: Vec<(i32, i8, Vec<u8>)>) -> Result<RecordBatch> {
        let mut encoded = [Vec::new(), Vec::new()];
        let mut selections = [
            Vec::with_capacity(rows.len()),
            Vec::with_capacity(rows.len()),
        ];
        let mut sides = Vec::with_capacity(rows.len());
        let mut groups = Vec::with_capacity(rows.len());
        for (group, side, row) in rows {
            let side_index = usize::try_from(side).map_err(|_| {
                DataFusionError::Execution("window join produced a negative side".to_string())
            })?;
            if side_index > 1 {
                return Err(DataFusionError::Execution(
                    "window join produced an invalid side".to_string(),
                ));
            }
            let row_index = u32::try_from(encoded[side_index].len()).map_err(|_| {
                DataFusionError::Execution("window join side exceeds u32 rows".to_string())
            })?;
            encoded[side_index].push(row);
            selections[side_index].push(Some(row_index));
            selections[1 - side_index].push(None);
            sides.push(side);
            groups.push(group);
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
        columns.push(Arc::new(Int8Array::from(sides)));
        columns.push(Arc::new(Int32Array::from(groups)));
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

fn validate_plan(plan: &proto::WindowJoin, max_parallelism: u32) -> Result<()> {
    if max_parallelism == 0
        || plan.left_key_indices.len() != plan.right_key_indices.len()
        || plan.left_schema.is_none()
        || plan.right_schema.is_none()
    {
        return Err(DataFusionError::Plan(
            "window join key/schema contract is invalid".to_string(),
        ));
    }
    if planning::is_native_contract(plan) {
        planning::filter(plan)?;
    }
    Ok(())
}

fn metadata_index(schema: &SchemaRef, name: &str) -> Option<usize> {
    schema
        .fields()
        .iter()
        .position(|field| field.name() == name)
}

fn binary_column<'a>(
    batch: &'a RecordBatch,
    index: Option<usize>,
    description: &str,
) -> Result<&'a BinaryArray> {
    batch
        .column(index.ok_or_else(|| {
            DataFusionError::Execution(format!("window join has no {description} column"))
        })?)
        .as_any()
        .downcast_ref::<BinaryArray>()
        .ok_or_else(|| {
            DataFusionError::Execution(format!("window join {description} is not Arrow Binary"))
        })
}

fn window_state_key(key_group: u32, group_key: &[u8], window_end: i64) -> StateKey {
    let mut key = Vec::with_capacity(1 + group_key.len() + 8);
    key.push(WINDOW_KEY_PREFIX);
    key.extend_from_slice(group_key);
    key.extend_from_slice(&window_end.to_be_bytes());
    StateKey { key_group, key }
}

fn decode_window_end(key: &[u8]) -> Result<i64> {
    if key.len() < 9 || key[0] != WINDOW_KEY_PREFIX {
        return Err(DataFusionError::Execution(
            "window join state key is malformed".to_string(),
        ));
    }
    Ok(i64::from_be_bytes(key[key.len() - 8..].try_into().unwrap()))
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
mod datafusion_probe;

#[cfg(test)]
mod indexed_tests;
#[cfg(test)]
mod tests;
