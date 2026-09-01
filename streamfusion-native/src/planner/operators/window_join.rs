// Copyright 2026 StreamFusion Authors
// Licensed under the Apache License, Version 2.0

use std::collections::BTreeSet;
use std::sync::Arc;

use ahash::RandomState;
use arrow::array::{Array, ArrayRef, BinaryArray, BinaryBuilder, Int32Array, Int8Array};
use arrow::datatypes::{DataType, Field, Schema, SchemaRef};
use arrow::record_batch::RecordBatch;
use chrono::Utc;
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

use super::window_aggregate::local_to_timer_epoch;
use super::window_table_function::timestamp_millis;

const INSERT: i8 = 0;
const UPDATE_BEFORE: i8 = 1;
const UPDATE_AFTER: i8 = 2;
const DELETE: i8 = 3;
const STATE_MAGIC: &[u8; 4] = b"SFWJ";
const STATE_VERSION: u8 = 1;
const WINDOW_KEY_PREFIX: u8 = 1;
const TIMER_STATE_KEY: &[u8] = b"\0streamfusion-window-join-timers";

/// Two-input Window Join storage. Complete rows are opaque until Java applies Flink's condition.
pub(crate) struct WindowJoinProcessor {
    plan: proto::WindowJoin,
    shift_time_zone: Tz,
    max_parallelism: u32,
    state: Box<dyn KeyedState>,
    timers: NativeTimerService,
    schemas: [Option<SchemaRef>; 2],
    key_fields: [Vec<(usize, KeyField)>; 2],
    preencoded_key_indices: [Option<usize>; 2],
    stored_row_indices: [Option<usize>; 2],
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

#[derive(Default, Debug, PartialEq, Eq)]
struct JoinWindowState {
    left: Vec<Vec<u8>>,
    right: Vec<Vec<u8>>,
}

struct StagedWindow {
    key: StateKey,
    window_end: i64,
    value: JoinWindowState,
    touched: bool,
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
        let timers = reservation.sibling("native window join timers");
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
        let root = native_plan
            .root
            .ok_or_else(|| DataFusionError::Plan("window join plan has no root".to_string()))?;
        let plan = match root.operator {
            Some(proto::operator::Operator::WindowJoin(plan)) => plan,
            _ => {
                return Err(DataFusionError::Plan(
                    "window join handle requires a WindowJoin root".to_string(),
                ));
            }
        };
        validate_plan(&plan, max_parallelism)?;
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
            timers: NativeTimerService::new(first_key_group, last_key_group, timer_reservation)?,
            schemas: [None, None],
            key_fields: [Vec::new(), Vec::new()],
            preencoded_key_indices: [None, None],
            stored_row_indices: [None, None],
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
        let stored_rows = binary_column(&batch, self.stored_row_indices[side], "stored row")?;
        let copied_rows = stored_rows.iter().flatten().map(<[u8]>::len).sum::<usize>();
        let copied_keys = self.preencoded_key_indices[side]
            .and_then(|index| batch.column(index).as_any().downcast_ref::<BinaryArray>())
            .map(|keys| keys.iter().flatten().map(<[u8]>::len).sum::<usize>())
            .unwrap_or(0);
        let base = copied_rows
            .saturating_add(copied_keys)
            .saturating_add(batch.num_rows().saturating_mul(144));
        self.scratch_reservation.resize(base)?;
        let result = self.process_arrow_accounted(side, &batch);
        match result {
            Ok(output) => self.finish_output(output, base),
            Err(error) => {
                self.scratch_reservation.resize(0)?;
                Err(error)
            }
        }
    }

    fn process_arrow_accounted(&mut self, side: usize, batch: &RecordBatch) -> Result<RecordBatch> {
        let stored_rows = binary_column(batch, self.stored_row_indices[side], "stored row")?;
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
            let deadline = self.timer_timestamp(window_end.saturating_sub(1))?;
            if deadline <= self.current_event_time {
                self.late_records_dropped[side] = self.late_records_dropped[side].saturating_add(1);
                continue;
            }
            let group_key = self.group_key(side, batch, row)?;
            let key_group = assign_key_group(&group_key, self.max_parallelism);
            let state_key = window_state_key(key_group, &group_key, window_end);
            let next = unique.len();
            let index = *unique.entry(state_key).or_insert(next);
            let accumulate = match kinds.value(row) {
                INSERT | UPDATE_AFTER => true,
                UPDATE_BEFORE | DELETE => false,
                other => {
                    return Err(DataFusionError::Execution(format!(
                        "unknown Flink RowKind byte {other}"
                    )));
                }
            };
            changes.push((index, accumulate, stored_rows.value(row).to_vec()));
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
                Ok(StagedWindow {
                    window_end: decode_window_end(&key.key)?,
                    key,
                    value: bytes
                        .map(|bytes| decode_state(bytes.as_ref()))
                        .transpose()?
                        .unwrap_or_default(),
                    touched: false,
                })
            })
            .collect::<Result<Vec<_>>>()?;
        let mut dirty_groups = BTreeSet::new();
        for (index, accumulate, row) in changes {
            let entry = &mut staged[index];
            let was_empty = entry.value.left.is_empty() && entry.value.right.is_empty();
            let values = if side == 0 {
                &mut entry.value.left
            } else {
                &mut entry.value.right
            };
            if accumulate {
                values.push(row);
            } else {
                let position = values
                    .iter()
                    .position(|candidate| *candidate == row)
                    .ok_or_else(|| {
                        DataFusionError::Execution(
                            "window join received a retraction without a matching row".to_string(),
                        )
                    })?;
                values.remove(position);
            }
            let is_empty = entry.value.left.is_empty() && entry.value.right.is_empty();
            let timer = TimerKey {
                timestamp: self.timer_timestamp(entry.window_end.saturating_sub(1))?,
                key: entry.key.key.clone(),
                namespace: entry.window_end.to_le_bytes().to_vec(),
            };
            if was_empty && !is_empty {
                if self
                    .timers
                    .register(entry.key.key_group, TimerDomain::EventTime, timer)?
                {
                    self.timer_registrations = self.timer_registrations.saturating_add(1);
                    dirty_groups.insert(entry.key.key_group);
                }
            } else if !was_empty
                && is_empty
                && self
                    .timers
                    .delete(entry.key.key_group, TimerDomain::EventTime, &timer)?
            {
                self.timer_deletions = self.timer_deletions.saturating_add(1);
                dirty_groups.insert(entry.key.key_group);
            }
            entry.touched = true;
        }
        let mut mutations = staged
            .into_iter()
            .filter(|entry| entry.touched)
            .map(|entry| StateMutation {
                key: entry.key,
                value: (!(entry.value.left.is_empty() && entry.value.right.is_empty()))
                    .then(|| encode_state(&entry.value)),
            })
            .collect::<Vec<_>>();
        self.append_timer_mutations(&mut mutations, dirty_groups)?;
        if !mutations.is_empty() {
            self.state.write_batch(mutations)?;
            self.state_write_batches = self.state_write_batches.saturating_add(1);
        }
        self.empty_output()
    }

    pub(crate) fn advance_event_time(&mut self, watermark: i64) -> Result<RecordBatch> {
        if watermark <= self.current_event_time {
            return self.empty_output();
        }
        self.current_event_time = watermark;
        let fired = self.timers.advance(TimerDomain::EventTime, watermark)?;
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
        let mut rows = Vec::new();
        let mut mutations = Vec::with_capacity(fired.len() * 2);
        let mut dirty_groups = BTreeSet::new();
        for (group, (timer, state)) in fired.into_iter().zip(states).enumerate() {
            dirty_groups.insert(timer.key_group);
            if let Some(state) = state {
                let state = decode_state(state.as_ref())?;
                let group = i32::try_from(group).map_err(|_| {
                    DataFusionError::Execution(
                        "window join fired group count exceeds i32".to_string(),
                    )
                })?;
                rows.extend(state.left.into_iter().map(|row| (group, 0, row)));
                rows.extend(state.right.into_iter().map(|row| (group, 1, row)));
            }
            mutations.push(StateMutation {
                key: StateKey {
                    key_group: timer.key_group,
                    key: timer.timer.key,
                },
                value: None,
            });
        }
        self.append_timer_mutations(&mut mutations, dirty_groups)?;
        self.state.write_batch(mutations)?;
        self.state_write_batches = self.state_write_batches.saturating_add(1);
        let output = output_batch(rows)?;
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
        self.stored_row_indices[side] = metadata_index(&schema, "__streamfusion_stored_row");
        self.input_kind_indices[side] = metadata_index(&schema, "__streamfusion_input_row_kind");
        if self.stored_row_indices[side].is_none() || self.input_kind_indices[side].is_none() {
            return Err(DataFusionError::Execution(
                "window join requires stored-row and RowKind metadata".to_string(),
            ));
        }
        let visible_count = [
            self.preencoded_key_indices[side],
            self.stored_row_indices[side],
            self.input_kind_indices[side],
            Some(schema.fields().len()),
        ]
        .into_iter()
        .flatten()
        .min()
        .unwrap();
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
        Ok(RecordBatch::new_empty(output_schema()))
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

fn encode_state(state: &JoinWindowState) -> Vec<u8> {
    let mut bytes = Vec::new();
    bytes.extend_from_slice(STATE_MAGIC);
    bytes.push(STATE_VERSION);
    for rows in [&state.left, &state.right] {
        bytes.extend_from_slice(&(rows.len() as u32).to_le_bytes());
        for row in rows {
            bytes.extend_from_slice(&(row.len() as u32).to_le_bytes());
            bytes.extend_from_slice(row);
        }
    }
    bytes
}

fn decode_state(bytes: &[u8]) -> Result<JoinWindowState> {
    if bytes.len() < 13 || &bytes[..4] != STATE_MAGIC || bytes[4] != STATE_VERSION {
        return Err(DataFusionError::Execution(
            "invalid native window join state".to_string(),
        ));
    }
    let mut offset = 5;
    let left = decode_rows(bytes, &mut offset)?;
    let right = decode_rows(bytes, &mut offset)?;
    if offset != bytes.len() {
        return Err(DataFusionError::Execution(
            "window join state has trailing bytes".to_string(),
        ));
    }
    Ok(JoinWindowState { left, right })
}

fn decode_rows(bytes: &[u8], offset: &mut usize) -> Result<Vec<Vec<u8>>> {
    let count = read_u32(bytes, offset)? as usize;
    let mut rows = Vec::with_capacity(count);
    for _ in 0..count {
        let length = read_u32(bytes, offset)? as usize;
        let end = offset.checked_add(length).ok_or_else(truncated)?;
        rows.push(bytes.get(*offset..end).ok_or_else(truncated)?.to_vec());
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

fn truncated() -> DataFusionError {
    DataFusionError::Execution("truncated native window join state".to_string())
}

fn output_schema() -> SchemaRef {
    Arc::new(Schema::new(vec![
        Field::new("__streamfusion_stored_row", DataType::Binary, false),
        Field::new("__streamfusion_join_side", DataType::Int8, false),
        Field::new("__streamfusion_window_group", DataType::Int32, false),
    ]))
}

fn output_batch(rows: Vec<(i32, i8, Vec<u8>)>) -> Result<RecordBatch> {
    let mut stored = BinaryBuilder::new();
    let mut sides = Vec::with_capacity(rows.len());
    let mut groups = Vec::with_capacity(rows.len());
    for (group, side, row) in rows {
        stored.append_value(row);
        sides.push(side);
        groups.push(group);
    }
    let columns: Vec<ArrayRef> = vec![
        Arc::new(stored.finish()),
        Arc::new(Int8Array::from(sides)),
        Arc::new(Int32Array::from(groups)),
    ];
    Ok(RecordBatch::try_new(output_schema(), columns)?)
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::memory_pool::{tests_support::TestBroker, HostMemoryReservation};
    use arrow::array::{BinaryArray, Int64Array, TimestampMillisecondArray};
    use prost::Message;

    #[test]
    fn canonical_state_preserves_both_sides_and_duplicate_rows() {
        let state = JoinWindowState {
            left: vec![b"left".to_vec(), b"left".to_vec()],
            right: vec![b"right".to_vec()],
        };
        assert_eq!(decode_state(&encode_state(&state)).unwrap(), state);
    }

    #[test]
    fn accounts_join_rows_keys_timers_and_state_in_host_memory() {
        let broker = Arc::new(TestBroker::new(64 << 20));
        let mut processor = WindowJoinProcessor::new(
            &plan(),
            128,
            0,
            127,
            HostMemoryReservation::new(broker.clone(), "window join accounting"),
        )
        .unwrap();
        let empty_state = broker.reserved();
        let output = processor
            .process_arrow(
                0,
                batch(
                    &[7, 8],
                    &[100, 100],
                    &[b"left", b"right"],
                    &[INSERT, INSERT],
                ),
            )
            .unwrap();

        assert!(broker.reserved() > empty_state);
        drop(output);
        drop(processor);
        assert_eq!(broker.reserved(), 0);
    }

    #[test]
    fn retractions_restore_and_rescale_without_changing_window_contents() {
        let broker = Arc::new(TestBroker::new(1 << 30));
        let mut source = WindowJoinProcessor::new(
            &plan(),
            128,
            0,
            127,
            HostMemoryReservation::new(broker.clone(), "window join source"),
        )
        .unwrap();
        source
            .process_arrow(
                0,
                batch(&[7, 7], &[100, 100], &[b"left", b"left"], &[INSERT, INSERT]),
            )
            .unwrap();
        source
            .process_arrow(0, batch(&[7], &[100], &[b"left"], &[DELETE]))
            .unwrap();
        source
            .process_arrow(1, batch(&[7], &[100], &[b"right"], &[INSERT]))
            .unwrap();
        assert_eq!(&source.statistics()[..2], &[3, 3]);
        let snapshots = (0..128)
            .map(|key_group| source.snapshot_key_group(key_group).unwrap())
            .collect::<Vec<_>>();

        let mut lower = WindowJoinProcessor::new(
            &plan(),
            128,
            0,
            63,
            HostMemoryReservation::new(broker.clone(), "window join lower"),
        )
        .unwrap();
        let mut upper = WindowJoinProcessor::new(
            &plan(),
            128,
            64,
            127,
            HostMemoryReservation::new(broker, "window join upper"),
        )
        .unwrap();
        for (key_group, snapshot) in snapshots.iter().enumerate() {
            let target = if key_group < 64 {
                &mut lower
            } else {
                &mut upper
            };
            target
                .restore_key_group(key_group as u32, snapshot)
                .unwrap();
        }

        let outputs = [
            lower.advance_event_time(99).unwrap(),
            upper.advance_event_time(99).unwrap(),
        ];
        let rows = outputs
            .iter()
            .flat_map(|output| {
                output
                    .column(0)
                    .as_any()
                    .downcast_ref::<BinaryArray>()
                    .unwrap()
                    .iter()
                    .flatten()
            })
            .collect::<Vec<_>>();
        assert_eq!(rows, vec![b"left".as_slice(), b"right".as_slice()]);
        assert_eq!(lower.statistics()[5] + upper.statistics()[5], 0);
    }

    fn plan() -> Vec<u8> {
        proto::NativePlan {
            protocol_version: crate::PLAN_PROTOCOL_VERSION,
            root: Some(proto::Operator {
                operator: Some(proto::operator::Operator::WindowJoin(proto::WindowJoin {
                    left_key_indices: vec![0],
                    right_key_indices: vec![0],
                    left_window_end_index: 1,
                    right_window_end_index: 1,
                    left_schema: Some(proto::Schema { fields: Vec::new() }),
                    right_schema: Some(proto::Schema { fields: Vec::new() }),
                    shift_time_zone: "UTC".to_string(),
                })),
            }),
        }
        .encode_to_vec()
    }

    fn batch(keys: &[i64], window_end: &[i64], rows: &[&[u8]], kinds: &[i8]) -> RecordBatch {
        RecordBatch::try_from_iter(vec![
            ("key", Arc::new(Int64Array::from(keys.to_vec())) as ArrayRef),
            (
                "window_end",
                Arc::new(TimestampMillisecondArray::from(window_end.to_vec())) as ArrayRef,
            ),
            (
                "__streamfusion_stored_row",
                Arc::new(BinaryArray::from_vec(rows.to_vec())) as ArrayRef,
            ),
            (
                "__streamfusion_input_row_kind",
                Arc::new(Int8Array::from(kinds.to_vec())) as ArrayRef,
            ),
        ])
        .unwrap()
    }
}
