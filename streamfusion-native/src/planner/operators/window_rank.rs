// Copyright 2026 StreamFusion Authors
// Licensed under the Apache License, Version 2.0

use std::cell::RefCell;
use std::cmp::Ordering;
use std::collections::BTreeSet;
use std::sync::Arc;

use ahash::RandomState;
#[cfg(test)]
use arrow::array::ArrayRef;
use arrow::array::{Array, BinaryArray, Int64Array, Int8Array};
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

use super::top_n::compare::compare_rows;
use super::window_aggregate::local_to_timer_epoch;
use super::window_table_function::timestamp_millis;

const INSERT: i8 = 0;
const UPDATE_BEFORE: i8 = 1;
const UPDATE_AFTER: i8 = 2;
const DELETE: i8 = 3;
const STATE_MAGIC: &[u8; 4] = b"SFWR";
const STATE_VERSION: u8 = 2;
const WINDOW_KEY_PREFIX: u8 = 1;
const TIMER_STATE_KEY: &[u8] = b"\0streamfusion-window-rank-timers";

/// Native window Top-N buffering, Flink-compatible ordering, and Arrow output.
pub(crate) struct WindowRankProcessor {
    plan: proto::WindowRank,
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
        let visible_schema =
            arrow_schema(plan.input_schema.as_ref().expect("validated input schema"))?;
        let row_converter = row_converter(&visible_schema)?;
        let mut output_fields = visible_schema.fields().iter().cloned().collect::<Vec<_>>();
        if plan.output_rank_number {
            output_fields.push(Arc::new(Field::new(
                "__streamfusion_rank_number",
                DataType::Int64,
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
            visible_schema,
            output_schema,
            row_converter,
            input_schema: None,
            key_fields: Vec::new(),
            preencoded_key_index: None,
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
        // Arrow input buffers have already consumed Flink managed memory. This reservation covers
        // only the Arrow-row/key copies and Rust collection nodes below.
        let visible_count = self.visible_schema.fields().len();
        let row_bytes = batch.columns()[..visible_count]
            .iter()
            .map(|column| column.get_array_memory_size())
            .sum::<usize>();
        let copied_keys = self
            .preencoded_key_index
            .and_then(|index| batch.column(index).as_any().downcast_ref::<BinaryArray>())
            .map(|keys| keys.iter().flatten().map(<[u8]>::len).sum::<usize>())
            .unwrap_or(0);
        let base = row_bytes
            .saturating_add(copied_keys)
            .saturating_add(batch.num_rows().saturating_mul(176));
        self.scratch_reservation.resize(base)?;
        let encoded_rows = self
            .row_converter
            .convert_columns(&batch.columns()[..visible_count]);
        let result = match encoded_rows {
            Ok(encoded_rows) => self.process_arrow_accounted(&batch, &encoded_rows),
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
        batch: &RecordBatch,
        encoded_rows: &Rows,
    ) -> Result<RecordBatch> {
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
            changes.push((index, accumulate, encoded_rows.row(row).data().to_vec()));
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
        for (index, accumulate, row) in changes {
            let entry = &mut staged[index];
            let was_empty = entry.value.candidates.is_empty();
            if accumulate {
                let sequence = entry.value.next_sequence;
                entry.value.next_sequence = sequence.checked_add(1).ok_or_else(|| {
                    DataFusionError::Execution("window rank sequence number overflow".to_string())
                })?;
                entry.value.candidates.push(Candidate { sequence, row });
            } else {
                let Some(index) = entry
                    .value
                    .candidates
                    .iter()
                    .position(|candidate| candidate.row == row)
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
        let mut output = Vec::<(usize, Candidate)>::new();
        let mut mutations = Vec::with_capacity(fired.len() * 2);
        let mut dirty_groups = BTreeSet::new();
        for (group, (timer, state)) in fired.into_iter().zip(states).enumerate() {
            dirty_groups.insert(timer.key_group);
            if let Some(state) = state {
                let state = decode_state(state.as_ref())?;
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
        let output = self.output_batch(output)?;
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
        self.input_kind_index = metadata_index(&schema, "__streamfusion_input_row_kind");
        if self.input_kind_index.is_none() {
            return Err(DataFusionError::Execution(
                "window rank requires RowKind metadata".to_string(),
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
                "window rank Arrow schema does not match its protobuf input schema".to_string(),
            ));
        }
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
        Ok(RecordBatch::new_empty(self.output_schema.clone()))
    }

    fn output_batch(&self, rows: Vec<(usize, Candidate)>) -> Result<RecordBatch> {
        if rows.is_empty() {
            return self.empty_output();
        }
        let parser = self.row_converter.parser();
        let visible_columns = self.row_converter.convert_rows(
            rows.iter()
                .map(|(_, candidate)| parser.parse(&candidate.row)),
        )?;
        let visible = RecordBatch::try_new(self.visible_schema.clone(), visible_columns)?;
        let mut selected = Vec::new();
        let mut ranks = Vec::new();
        let mut start = 0;
        while start < rows.len() {
            let group = rows[start].0;
            let mut end = start + 1;
            while end < rows.len() && rows[end].0 == group {
                end += 1;
            }
            let mut indices = (start..end).collect::<Vec<_>>();
            let failure = RefCell::new(None);
            indices.sort_by(|&left, &right| {
                if failure.borrow().is_some() {
                    return Ordering::Equal;
                }
                match compare_rows(
                    &visible,
                    left,
                    &visible,
                    right,
                    &self.plan.sort_key_indices,
                    &self.plan.sort_ascending,
                    &self.plan.sort_nulls_last,
                ) {
                    Ok(Ordering::Equal) => rows[left].1.sequence.cmp(&rows[right].1.sequence),
                    Ok(ordering) => ordering,
                    Err(error) => {
                        *failure.borrow_mut() = Some(error);
                        Ordering::Equal
                    }
                }
            });
            if let Some(error) = failure.into_inner() {
                return Err(error);
            }
            let first = usize::try_from(self.plan.rank_start - 1).unwrap_or(usize::MAX);
            let exclusive_end = usize::try_from(self.plan.rank_end)
                .unwrap_or(usize::MAX)
                .min(indices.len());
            for rank in first..exclusive_end {
                selected.push(rows[indices[rank]].1.row.as_slice());
                ranks.push(i64::try_from(rank + 1).map_err(|_| {
                    DataFusionError::Execution("window rank number exceeds i64".to_string())
                })?);
            }
            start = end;
        }
        let mut columns = self
            .row_converter
            .convert_rows(selected.iter().map(|row| parser.parse(row)))?;
        if self.plan.output_rank_number {
            columns.push(Arc::new(Int64Array::from(ranks)));
        }
        columns.push(Arc::new(Int8Array::from(vec![INSERT; selected.len()])));
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
        bytes.extend_from_slice(&candidate.row);
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
        let row_end = offset.checked_add(row_length).ok_or_else(|| {
            DataFusionError::Execution("window rank state length overflow".to_string())
        })?;
        let row = bytes
            .get(offset..row_end)
            .ok_or_else(|| DataFusionError::Execution("truncated window rank row".to_string()))?;
        candidates.push(Candidate {
            sequence,
            row: row.to_vec(),
        });
        offset = row_end;
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

#[cfg(test)]
mod tests {
    use super::*;
    use crate::memory_pool::{tests_support::TestBroker, HostMemoryReservation};
    use arrow::array::{BinaryArray, Int64Array, TimestampMillisecondArray};
    use prost::Message;

    #[test]
    fn state_round_trips_arrow_rows_and_retractions() {
        let state = WindowState {
            next_sequence: 2,
            candidates: vec![
                Candidate {
                    sequence: 0,
                    row: b"row-a".to_vec(),
                },
                Candidate {
                    sequence: 1,
                    row: b"row-b".to_vec(),
                },
            ],
        };
        let decoded = decode_state(&encode_state(&state)).unwrap();
        assert_eq!(decoded.next_sequence, 2);
        assert_eq!(decoded.candidates, state.candidates);
    }

    #[test]
    fn accounts_rank_rows_sort_keys_timers_and_state_in_host_memory() {
        let broker = Arc::new(TestBroker::new(64 << 20));
        let mut processor = WindowRankProcessor::new(
            &plan(),
            128,
            0,
            127,
            HostMemoryReservation::new(broker.clone(), "window rank accounting"),
        )
        .unwrap();
        let empty_state = broker.reserved();
        let output = processor
            .process_arrow(batch(
                &[7, 8],
                &[100, 100],
                &[b"left", b"right"],
                &[INSERT, INSERT],
            ))
            .unwrap();

        assert!(broker.reserved() > empty_state);
        drop(output);
        drop(processor);
        assert_eq!(broker.reserved(), 0);
    }

    #[test]
    fn retractions_restore_and_rescale_with_timer_and_sequence_state() {
        let broker = Arc::new(TestBroker::new(1 << 30));
        let mut source = WindowRankProcessor::new(
            &plan(),
            128,
            0,
            127,
            HostMemoryReservation::new(broker.clone(), "window rank source"),
        )
        .unwrap();
        source
            .process_arrow(batch(
                &[7, 7, 9],
                &[100, 100, 200],
                &[b"left", b"right", b"other"],
                &[INSERT, UPDATE_AFTER, INSERT],
            ))
            .unwrap();
        source
            .process_arrow(batch(&[7], &[100], &[b"left"], &[UPDATE_BEFORE]))
            .unwrap();
        let snapshots = (0..128)
            .map(|key_group| source.snapshot_key_group(key_group).unwrap())
            .collect::<Vec<_>>();

        let mut lower = WindowRankProcessor::new(
            &plan(),
            128,
            0,
            63,
            HostMemoryReservation::new(broker.clone(), "window rank lower"),
        )
        .unwrap();
        let mut upper = WindowRankProcessor::new(
            &plan(),
            128,
            64,
            127,
            HostMemoryReservation::new(broker, "window rank upper"),
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
            lower.advance_event_time(199).unwrap(),
            upper.advance_event_time(199).unwrap(),
        ];
        let mut rows = outputs
            .iter()
            .flat_map(|output| {
                output
                    .column(2)
                    .as_any()
                    .downcast_ref::<BinaryArray>()
                    .unwrap()
                    .iter()
                    .flatten()
            })
            .collect::<Vec<_>>();
        rows.sort_unstable();
        assert_eq!(rows, vec![b"other".as_slice(), b"right".as_slice()]);
        assert_eq!(lower.statistics()[5] + upper.statistics()[5], 0);
    }

    fn plan() -> Vec<u8> {
        proto::NativePlan {
            protocol_version: crate::PLAN_PROTOCOL_VERSION,
            root: Some(proto::Operator {
                plan_node_id: 0,
                operator: Some(proto::operator::Operator::WindowRank(Box::new(
                    proto::WindowRank {
                        input: None,
                        partition_key_indices: vec![0],
                        sort_key_indices: vec![2],
                        sort_ascending: vec![true],
                        sort_nulls_last: vec![true],
                        window_end_index: 1,
                        rank_start: 1,
                        rank_end: 2,
                        output_rank_number: true,
                        input_changelog: true,
                        input_schema: Some(proto::Schema {
                            fields: vec![
                                proto_field(
                                    "key",
                                    proto::logical_type::Type::Bigint(proto::EmptyType::default()),
                                ),
                                proto_field(
                                    "window_end",
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

    fn batch(keys: &[i64], window_end: &[i64], rows: &[&[u8]], kinds: &[i8]) -> RecordBatch {
        RecordBatch::try_from_iter(vec![
            ("key", Arc::new(Int64Array::from(keys.to_vec())) as ArrayRef),
            (
                "window_end",
                Arc::new(TimestampMillisecondArray::from(window_end.to_vec())) as ArrayRef,
            ),
            (
                "payload",
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
