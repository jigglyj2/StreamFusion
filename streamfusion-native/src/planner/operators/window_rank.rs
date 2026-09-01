// Copyright 2026 StreamFusion Authors
// Licensed under the Apache License, Version 2.0

use std::collections::BTreeSet;
use std::sync::Arc;

use ahash::RandomState;
use arrow::array::{
    Array, ArrayRef, BinaryArray, BinaryBuilder, Int32Array, Int64Array, Int8Array,
};
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
const STATE_MAGIC: &[u8; 4] = b"SFWR";
const STATE_VERSION: u8 = 1;
const WINDOW_KEY_PREFIX: u8 = 1;
const TIMER_STATE_KEY: &[u8] = b"\0streamfusion-window-rank-timers";

/// Native window Top-N buffering. Java performs one generated-comparator sort per fired window.
pub(crate) struct WindowRankProcessor {
    plan: proto::WindowRank,
    shift_time_zone: Tz,
    max_parallelism: u32,
    state: Box<dyn KeyedState>,
    timers: NativeTimerService,
    input_schema: Option<SchemaRef>,
    key_fields: Vec<(usize, KeyField)>,
    preencoded_key_index: Option<usize>,
    stored_row_index: Option<usize>,
    sort_row_index: Option<usize>,
    input_kind_index: Option<usize>,
    current_event_time: i64,
    scratch_reservation: HostMemoryReservation,
    late_records_dropped: u64,
    state_read_batches: u64,
    state_write_batches: u64,
    timer_registrations: u64,
    timer_deletions: u64,
    timers_fired: u64,
}

#[derive(Clone, Debug, PartialEq, Eq)]
struct Candidate {
    sequence: u64,
    row: Vec<u8>,
    sort_key: Vec<u8>,
}

#[derive(Default)]
struct WindowState {
    next_sequence: u64,
    candidates: Vec<Candidate>,
}

struct StagedWindow {
    key: StateKey,
    window_end: i64,
    value: WindowState,
    touched: bool,
}

impl WindowRankProcessor {
    pub(crate) fn new(
        serialized_plan: &[u8],
        max_parallelism: u32,
        first_key_group: u32,
        last_key_group: u32,
        state_reservation: HostMemoryReservation,
    ) -> Result<Self> {
        let scratch = state_reservation.sibling("native window rank batch scratch and output");
        let timers = state_reservation.sibling("native window rank timers");
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
        let timers = reservation.sibling("native window rank timers");
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
        let root = decode_plan(serialized_plan)?
            .root
            .ok_or_else(|| DataFusionError::Plan("window rank plan has no root".to_string()))?;
        let plan = match root.operator {
            Some(proto::operator::Operator::WindowRank(plan)) => *plan,
            _ => {
                return Err(DataFusionError::Plan(
                    "window rank handle requires a WindowRank root".to_string(),
                ));
            }
        };
        validate_plan(&plan, max_parallelism)?;
        let shift_time_zone = if plan.shift_time_zone.is_empty() {
            chrono_tz::UTC
        } else {
            plan.shift_time_zone.parse::<Tz>().map_err(|error| {
                DataFusionError::Plan(format!(
                    "invalid window rank shift time zone {}: {error}",
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
            input_schema: None,
            key_fields: Vec::new(),
            preencoded_key_index: None,
            stored_row_index: None,
            sort_row_index: None,
            input_kind_index: None,
            current_event_time: i64::MIN,
            scratch_reservation,
            late_records_dropped: 0,
            state_read_batches: 0,
            state_write_batches: 0,
            timer_registrations: 0,
            timer_deletions: 0,
            timers_fired: 0,
        })
    }

    pub(crate) fn process_arrow(&mut self, batch: RecordBatch) -> Result<RecordBatch> {
        self.prepare_schema(batch.schema())?;
        let base = batch.num_rows().saturating_mul(224);
        self.scratch_reservation.resize(base)?;
        let result = self.process_arrow_accounted(&batch);
        match result {
            Ok(output) => self.finish_output(output, base),
            Err(error) => {
                self.scratch_reservation.resize(0)?;
                Err(error)
            }
        }
    }

    fn process_arrow_accounted(&mut self, batch: &RecordBatch) -> Result<RecordBatch> {
        let stored_rows = binary_column(batch, self.stored_row_index, "stored row")?;
        let sort_rows = binary_column(batch, self.sort_row_index, "sort key")?;
        let kinds = batch
            .column(self.input_kind_index.expect("schema prepared"))
            .as_any()
            .downcast_ref::<Int8Array>()
            .ok_or_else(|| {
                DataFusionError::Execution(
                    "window rank RowKind metadata is not Arrow Int8".to_string(),
                )
            })?;
        let window_end_column = batch.column(self.plan.window_end_index as usize);
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
                self.late_records_dropped = self.late_records_dropped.saturating_add(1);
                continue;
            }
            let group_key = self.group_key(batch, row)?;
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
            changes.push((
                index,
                accumulate,
                stored_rows.value(row).to_vec(),
                sort_rows.value(row).to_vec(),
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
            .map(|key| key.expect("window rank state index is populated"))
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
        let mut dirty_timer_groups = BTreeSet::new();
        for (index, accumulate, row, sort_key) in changes {
            let entry = &mut staged[index];
            let was_empty = entry.value.candidates.is_empty();
            if accumulate {
                let sequence = entry.value.next_sequence;
                entry.value.next_sequence = sequence.checked_add(1).ok_or_else(|| {
                    DataFusionError::Execution("window rank sequence number overflow".to_string())
                })?;
                entry.value.candidates.push(Candidate {
                    sequence,
                    row,
                    sort_key,
                });
            } else {
                let Some(index) =
                    entry.value.candidates.iter().position(|candidate| {
                        candidate.row == row && candidate.sort_key == sort_key
                    })
                else {
                    return Err(DataFusionError::Execution(
                        "window rank received a retraction without a matching row".to_string(),
                    ));
                };
                entry.value.candidates.remove(index);
            }
            let timer = TimerKey {
                timestamp: self.timer_timestamp(entry.window_end.saturating_sub(1))?,
                key: entry.key.key.clone(),
                namespace: entry.window_end.to_le_bytes().to_vec(),
            };
            if was_empty && !entry.value.candidates.is_empty() {
                if self
                    .timers
                    .register(entry.key.key_group, TimerDomain::EventTime, timer)?
                {
                    self.timer_registrations = self.timer_registrations.saturating_add(1);
                    dirty_timer_groups.insert(entry.key.key_group);
                }
            } else if !was_empty
                && entry.value.candidates.is_empty()
                && self
                    .timers
                    .delete(entry.key.key_group, TimerDomain::EventTime, &timer)?
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
                value: (!entry.value.candidates.is_empty()).then(|| encode_state(&entry.value)),
            })
            .collect::<Vec<_>>();
        self.append_timer_mutations(&mut mutations, dirty_timer_groups)?;
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
        let mut output = Vec::<(i32, Candidate)>::new();
        let mut mutations = Vec::with_capacity(fired.len() * 2);
        let mut dirty_groups = BTreeSet::new();
        for (group, (timer, state)) in fired.into_iter().zip(states).enumerate() {
            dirty_groups.insert(timer.key_group);
            if let Some(state) = state {
                let state = decode_state(state.as_ref())?;
                let group = i32::try_from(group).map_err(|_| {
                    DataFusionError::Execution("window rank timer batch exceeds Int32".to_string())
                })?;
                output.extend(
                    state
                        .candidates
                        .into_iter()
                        .map(|candidate| (group, candidate)),
                );
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
        let output = output_batch(output)?;
        self.finish_output(output, 0)
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

    fn group_key(&self, batch: &RecordBatch, row: usize) -> Result<Vec<u8>> {
        match self.preencoded_key_index {
            Some(index) => Ok(batch
                .column(index)
                .as_any()
                .downcast_ref::<BinaryArray>()
                .ok_or_else(|| {
                    DataFusionError::Execution(
                        "window rank preencoded key is not Binary".to_string(),
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
                    "window rank input schema changed while running".to_string(),
                ));
            }
            return Ok(());
        }
        self.preencoded_key_index = metadata_index(&schema, "__streamfusion_key");
        self.stored_row_index = metadata_index(&schema, "__streamfusion_stored_row");
        self.sort_row_index = metadata_index(&schema, "__streamfusion_sort_key");
        self.input_kind_index = metadata_index(&schema, "__streamfusion_input_row_kind");
        if self.stored_row_index.is_none()
            || self.sort_row_index.is_none()
            || self.input_kind_index.is_none()
        {
            return Err(DataFusionError::Execution(
                "window rank requires stored-row, sort-key, and RowKind metadata".to_string(),
            ));
        }
        let visible_count = [
            self.preencoded_key_index,
            self.stored_row_index,
            self.sort_row_index,
            self.input_kind_index,
            Some(schema.fields().len()),
        ]
        .into_iter()
        .flatten()
        .min()
        .unwrap();
        if self.plan.window_end_index as usize >= visible_count {
            return Err(DataFusionError::Plan(
                "window rank end index is outside the visible row".to_string(),
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
                            "window rank partition index {index} is outside the input"
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

    fn timer_timestamp(&self, window_millis: i64) -> Result<i64> {
        if self.shift_time_zone == chrono_tz::UTC || window_millis == i64::MAX {
            return Ok(window_millis);
        }
        let local = chrono::DateTime::<Utc>::from_timestamp_millis(window_millis)
            .ok_or_else(|| {
                DataFusionError::Execution(format!(
                    "window rank timer {window_millis} is outside chrono's range"
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

fn validate_plan(plan: &proto::WindowRank, max_parallelism: u32) -> Result<()> {
    if max_parallelism == 0 || plan.rank_start == 0 || plan.rank_end < plan.rank_start {
        return Err(DataFusionError::Plan(
            "window rank requires positive max parallelism and a valid one-based range".to_string(),
        ));
    }
    if plan.sort_key_indices.is_empty()
        || plan.sort_key_indices.len() != plan.sort_ascending.len()
        || plan.sort_key_indices.len() != plan.sort_nulls_last.len()
        || plan.input_schema.is_none()
    {
        return Err(DataFusionError::Plan(
            "window rank sort contract or input schema is invalid".to_string(),
        ));
    }
    Ok(())
}

fn binary_column<'a>(
    batch: &'a RecordBatch,
    index: Option<usize>,
    description: &str,
) -> Result<&'a BinaryArray> {
    batch
        .column(index.expect("schema prepared"))
        .as_any()
        .downcast_ref::<BinaryArray>()
        .ok_or_else(|| {
            DataFusionError::Execution(format!("window rank {description} is not Arrow Binary"))
        })
}

fn metadata_index(schema: &SchemaRef, name: &str) -> Option<usize> {
    schema
        .fields()
        .iter()
        .position(|field| field.name() == name)
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
            "window rank state key is malformed".to_string(),
        ));
    }
    Ok(i64::from_be_bytes(key[key.len() - 8..].try_into().unwrap()))
}

fn encode_state(state: &WindowState) -> Vec<u8> {
    let mut bytes = Vec::new();
    bytes.extend_from_slice(STATE_MAGIC);
    bytes.push(STATE_VERSION);
    bytes.extend_from_slice(&state.next_sequence.to_le_bytes());
    bytes.extend_from_slice(&(state.candidates.len() as u32).to_le_bytes());
    for candidate in &state.candidates {
        bytes.extend_from_slice(&candidate.sequence.to_le_bytes());
        bytes.extend_from_slice(&(candidate.row.len() as u32).to_le_bytes());
        bytes.extend_from_slice(&(candidate.sort_key.len() as u32).to_le_bytes());
        bytes.extend_from_slice(&candidate.row);
        bytes.extend_from_slice(&candidate.sort_key);
    }
    bytes
}

fn decode_state(bytes: &[u8]) -> Result<WindowState> {
    if bytes.len() < 17 || &bytes[..4] != STATE_MAGIC || bytes[4] != STATE_VERSION {
        return Err(DataFusionError::Execution(
            "invalid native window rank state".to_string(),
        ));
    }
    let mut offset = 5;
    let next_sequence = read_u64(bytes, &mut offset)?;
    let count = read_u32(bytes, &mut offset)? as usize;
    let mut candidates = Vec::with_capacity(count);
    for _ in 0..count {
        let sequence = read_u64(bytes, &mut offset)?;
        let row_length = read_u32(bytes, &mut offset)? as usize;
        let sort_length = read_u32(bytes, &mut offset)? as usize;
        let row_end = offset.checked_add(row_length).ok_or_else(|| {
            DataFusionError::Execution("window rank state length overflow".to_string())
        })?;
        let sort_end = row_end.checked_add(sort_length).ok_or_else(|| {
            DataFusionError::Execution("window rank state length overflow".to_string())
        })?;
        let row = bytes
            .get(offset..row_end)
            .ok_or_else(|| DataFusionError::Execution("truncated window rank row".to_string()))?;
        let sort_key = bytes.get(row_end..sort_end).ok_or_else(|| {
            DataFusionError::Execution("truncated window rank sort key".to_string())
        })?;
        candidates.push(Candidate {
            sequence,
            row: row.to_vec(),
            sort_key: sort_key.to_vec(),
        });
        offset = sort_end;
    }
    if offset != bytes.len() {
        return Err(DataFusionError::Execution(
            "window rank state has trailing bytes".to_string(),
        ));
    }
    Ok(WindowState {
        next_sequence,
        candidates,
    })
}

fn read_u32(bytes: &[u8], offset: &mut usize) -> Result<u32> {
    Ok(u32::from_le_bytes(read_exact::<4>(bytes, offset)?))
}

fn read_u64(bytes: &[u8], offset: &mut usize) -> Result<u64> {
    Ok(u64::from_le_bytes(read_exact::<8>(bytes, offset)?))
}

fn read_exact<const N: usize>(bytes: &[u8], offset: &mut usize) -> Result<[u8; N]> {
    let end = offset.checked_add(N).ok_or_else(|| {
        DataFusionError::Execution("window rank state offset overflow".to_string())
    })?;
    let result = bytes
        .get(*offset..end)
        .ok_or_else(|| DataFusionError::Execution("truncated window rank state".to_string()))?
        .try_into()
        .unwrap();
    *offset = end;
    Ok(result)
}

fn output_schema() -> SchemaRef {
    Arc::new(Schema::new(vec![
        Field::new("__streamfusion_stored_row", DataType::Binary, false),
        Field::new("__streamfusion_sort_key", DataType::Binary, false),
        Field::new("__streamfusion_window_group", DataType::Int32, false),
        Field::new("__streamfusion_sequence", DataType::Int64, false),
        Field::new("__streamfusion_row_kind", DataType::Int8, false),
    ]))
}

fn output_batch(rows: Vec<(i32, Candidate)>) -> Result<RecordBatch> {
    let mut stored = BinaryBuilder::new();
    let mut sort = BinaryBuilder::new();
    let mut groups = Vec::with_capacity(rows.len());
    let mut sequences = Vec::with_capacity(rows.len());
    for (group, candidate) in rows {
        stored.append_value(candidate.row);
        sort.append_value(candidate.sort_key);
        groups.push(group);
        sequences.push(i64::try_from(candidate.sequence).map_err(|_| {
            DataFusionError::Execution("window rank sequence exceeds Java long".to_string())
        })?);
    }
    let count = groups.len();
    let columns: Vec<ArrayRef> = vec![
        Arc::new(stored.finish()),
        Arc::new(sort.finish()),
        Arc::new(Int32Array::from(groups)),
        Arc::new(Int64Array::from(sequences)),
        Arc::new(Int8Array::from(vec![INSERT; count])),
    ];
    Ok(RecordBatch::try_new(output_schema(), columns)?)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn state_round_trips_sort_keys_and_retractions() {
        let state = WindowState {
            next_sequence: 2,
            candidates: vec![
                Candidate {
                    sequence: 0,
                    row: b"row-a".to_vec(),
                    sort_key: b"sort-a".to_vec(),
                },
                Candidate {
                    sequence: 1,
                    row: b"row-b".to_vec(),
                    sort_key: b"sort-b".to_vec(),
                },
            ],
        };
        let decoded = decode_state(&encode_state(&state)).unwrap();
        assert_eq!(decoded.next_sequence, 2);
        assert_eq!(decoded.candidates, state.candidates);
    }
}
