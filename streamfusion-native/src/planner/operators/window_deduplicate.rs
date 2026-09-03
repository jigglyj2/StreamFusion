// Copyright 2026 StreamFusion Authors
// Licensed under the Apache License, Version 2.0

use std::collections::BTreeSet;
use std::sync::Arc;

use ahash::RandomState;
#[cfg(test)]
use arrow::array::ArrayRef;
use arrow::array::{Array, BinaryArray, Int8Array};
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

const INSERT: i8 = 0;
const UPDATE_BEFORE: i8 = 1;
const UPDATE_AFTER: i8 = 2;
const DELETE: i8 = 3;
const STATE_MAGIC: &[u8; 4] = b"SFWD";
const STATE_VERSION: u8 = 2;
const WINDOW_KEY_PREFIX: u8 = 1;
const TIMER_STATE_KEY: &[u8] = b"\0streamfusion-window-dedup-timers";

/// Window-scoped first/last row selection with opaque Arrow-row payload state.
pub(crate) struct WindowDeduplicateProcessor {
    plan: proto::WindowDeduplicate,
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
    order: i64,
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

impl WindowDeduplicateProcessor {
    pub(crate) fn new(
        serialized_plan: &[u8],
        max_parallelism: u32,
        first_key_group: u32,
        last_key_group: u32,
        state_reservation: HostMemoryReservation,
    ) -> Result<Self> {
        let scratch =
            state_reservation.sibling("native window deduplicate batch scratch and output");
        let timers = state_reservation.sibling("native window deduplicate timers");
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
        let timers = reservation.sibling("native window deduplicate timers");
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
            DataFusionError::Plan("window deduplicate plan has no root".to_string())
        })?;
        let plan = match root.operator {
            Some(proto::operator::Operator::WindowDeduplicate(plan)) => *plan,
            _ => {
                return Err(DataFusionError::Plan(
                    "window deduplicate handle requires a WindowDeduplicate root".to_string(),
                ));
            }
        };
        validate_plan(&plan, max_parallelism)?;
        let visible_schema =
            arrow_schema(plan.input_schema.as_ref().expect("validated input schema"))?;
        let row_converter = row_converter(&visible_schema)?;
        let mut output_fields = visible_schema.fields().iter().cloned().collect::<Vec<_>>();
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
                    "invalid window deduplicate shift time zone {}: {error}",
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
        // Input Arrow buffers are already charged by the Flink-backed Arrow allocator. Reserve
        // only Rust-owned Arrow-row copies and collection nodes created while staging this batch.
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
        let base_reservation = row_bytes
            .saturating_add(copied_keys)
            .saturating_add(batch.num_rows().saturating_mul(160));
        self.scratch_reservation.resize(base_reservation)?;
        let encoded_rows = self
            .row_converter
            .convert_columns(&batch.columns()[..visible_count]);
        let result = match encoded_rows {
            Ok(encoded_rows) => self.process_arrow_accounted(&batch, &encoded_rows),
            Err(error) => Err(error.into()),
        };
        match result {
            Ok(output) => self.finish_output(output, base_reservation),
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
                    "window deduplicate RowKind metadata is not Arrow Int8".to_string(),
                )
            })?;
        let order_column = batch.column(self.plan.order_index as usize);
        let window_end_column = batch.column(self.plan.window_end_index as usize);
        let mut unique = HashMap::<StateKey, usize, RandomState>::with_capacity_and_hasher(
            batch.num_rows(),
            RandomState::new(),
        );
        let mut changes = Vec::with_capacity(batch.num_rows());
        for row in 0..batch.num_rows() {
            let Some(order) = timestamp_millis(order_column, row)? else {
                continue;
            };
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
                order,
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
            .map(|key| key.expect("window deduplicate state index is populated"))
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
                let window_end = decode_window_end(&key.key)?;
                Ok(StagedWindow {
                    key,
                    window_end,
                    value: bytes
                        .map(|bytes| decode_state(bytes.as_ref()))
                        .transpose()?
                        .unwrap_or_default(),
                    touched: false,
                })
            })
            .collect::<Result<Vec<_>>>()?;
        let mut dirty_timer_groups = BTreeSet::new();
        for (index, accumulate, order, row) in changes {
            let entry = &mut staged[index];
            let was_empty = entry.value.candidates.is_empty();
            if accumulate {
                let sequence = entry.value.next_sequence;
                entry.value.next_sequence =
                    entry.value.next_sequence.checked_add(1).ok_or_else(|| {
                        DataFusionError::Execution(
                            "window deduplicate sequence number overflow".to_string(),
                        )
                    })?;
                entry.value.candidates.push(Candidate {
                    order,
                    sequence,
                    row,
                });
            } else {
                let Some(candidate) = entry
                    .value
                    .candidates
                    .iter()
                    .position(|candidate| candidate.order == order && candidate.row == row)
                else {
                    return Err(DataFusionError::Execution(
                        "window deduplicate received a retraction without a matching row"
                            .to_string(),
                    ));
                };
                entry.value.candidates.remove(candidate);
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
        let mut rows = Vec::new();
        let mut mutations = Vec::with_capacity(fired.len() * 2);
        let mut dirty_groups = BTreeSet::new();
        for (timer, state) in fired.into_iter().zip(states) {
            dirty_groups.insert(timer.key_group);
            if let Some(state) = state {
                let state = decode_state(state.as_ref())?;
                if let Some(winner) = winner(&state.candidates, self.plan.keep_last) {
                    rows.push(winner.row.clone());
                }
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
        let output = self.output_batch(rows)?;
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
                        "window deduplicate preencoded key is not Arrow Binary".to_string(),
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
                    "window deduplicate input schema changed while running".to_string(),
                ));
            }
            return Ok(());
        }
        self.preencoded_key_index = metadata_index(&schema, "__streamfusion_key");
        self.input_kind_index = metadata_index(&schema, "__streamfusion_input_row_kind");
        if self.input_kind_index.is_none() {
            return Err(DataFusionError::Execution(
                "window deduplicate requires RowKind metadata".to_string(),
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
            return Err(DataFusionError::Execution(format!(
                "window deduplicate Arrow schema does not match its protobuf input schema: actual {:?}, planned {:?}",
                schema.fields()[..visible_count]
                    .iter()
                    .map(|field| field.data_type())
                    .collect::<Vec<_>>(),
                self.visible_schema
                    .fields()
                    .iter()
                    .map(|field| field.data_type())
                    .collect::<Vec<_>>()
            )));
        }
        for &index in self
            .plan
            .partition_key_indices
            .iter()
            .chain([self.plan.order_index, self.plan.window_end_index].iter())
        {
            if index as usize >= visible_count {
                return Err(DataFusionError::Plan(format!(
                    "window deduplicate input index {index} is outside the visible row"
                )));
            }
        }
        if self.preencoded_key_index.is_none() {
            self.key_fields = self
                .plan
                .partition_key_indices
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
                    "window deduplicate timer {window_millis} is outside chrono's range"
                ))
            })?
            .naive_utc();
        local_to_timer_epoch(local, self.shift_time_zone)
    }

    fn empty_output(&self) -> Result<RecordBatch> {
        Ok(RecordBatch::new_empty(self.output_schema.clone()))
    }

    fn output_batch(&self, rows: Vec<Vec<u8>>) -> Result<RecordBatch> {
        let parser = self.row_converter.parser();
        let mut columns = self
            .row_converter
            .convert_rows(rows.iter().map(|row| parser.parse(row)))?;
        columns.push(Arc::new(Int8Array::from(vec![INSERT; rows.len()])));
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

fn validate_plan(plan: &proto::WindowDeduplicate, max_parallelism: u32) -> Result<()> {
    if max_parallelism == 0 {
        return Err(DataFusionError::Plan(
            "window deduplicate max parallelism must be positive".to_string(),
        ));
    }
    if plan.input_schema.is_none() {
        return Err(DataFusionError::Plan(
            "window deduplicate input schema is missing".to_string(),
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
            "window deduplicate state key is malformed".to_string(),
        ));
    }
    Ok(i64::from_be_bytes(key[key.len() - 8..].try_into().unwrap()))
}

fn winner(candidates: &[Candidate], keep_last: bool) -> Option<&Candidate> {
    if keep_last {
        candidates
            .iter()
            .max_by_key(|candidate| (candidate.order, candidate.sequence))
    } else {
        candidates
            .iter()
            .min_by_key(|candidate| (candidate.order, candidate.sequence))
    }
}

fn encode_state(state: &WindowState) -> Vec<u8> {
    let capacity = 17usize.saturating_add(
        state
            .candidates
            .iter()
            .map(|candidate| 20usize.saturating_add(candidate.row.len()))
            .sum::<usize>(),
    );
    let mut bytes = Vec::with_capacity(capacity);
    bytes.extend_from_slice(STATE_MAGIC);
    bytes.push(STATE_VERSION);
    bytes.extend_from_slice(&state.next_sequence.to_le_bytes());
    bytes.extend_from_slice(&(state.candidates.len() as u32).to_le_bytes());
    for candidate in &state.candidates {
        bytes.extend_from_slice(&candidate.order.to_le_bytes());
        bytes.extend_from_slice(&candidate.sequence.to_le_bytes());
        bytes.extend_from_slice(&(candidate.row.len() as u32).to_le_bytes());
        bytes.extend_from_slice(&candidate.row);
    }
    bytes
}

fn decode_state(bytes: &[u8]) -> Result<WindowState> {
    if bytes.len() < 17 || &bytes[..4] != STATE_MAGIC || bytes[4] != STATE_VERSION {
        return Err(DataFusionError::Execution(
            "invalid native window deduplicate state".to_string(),
        ));
    }
    let mut offset = 5;
    let next_sequence = read_u64(bytes, &mut offset)?;
    let count = read_u32(bytes, &mut offset)? as usize;
    let mut candidates = Vec::with_capacity(count);
    for _ in 0..count {
        let order = read_i64(bytes, &mut offset)?;
        let sequence = read_u64(bytes, &mut offset)?;
        let length = read_u32(bytes, &mut offset)? as usize;
        let end = offset.checked_add(length).ok_or_else(|| {
            DataFusionError::Execution("window deduplicate state length overflow".to_string())
        })?;
        let row = bytes.get(offset..end).ok_or_else(|| {
            DataFusionError::Execution("truncated window deduplicate row state".to_string())
        })?;
        candidates.push(Candidate {
            order,
            sequence,
            row: row.to_vec(),
        });
        offset = end;
    }
    if offset != bytes.len() {
        return Err(DataFusionError::Execution(
            "window deduplicate state has trailing bytes".to_string(),
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

fn read_i64(bytes: &[u8], offset: &mut usize) -> Result<i64> {
    Ok(i64::from_le_bytes(read_exact::<8>(bytes, offset)?))
}

fn read_exact<const N: usize>(bytes: &[u8], offset: &mut usize) -> Result<[u8; N]> {
    let end = offset.checked_add(N).ok_or_else(|| {
        DataFusionError::Execution("window deduplicate state offset overflow".to_string())
    })?;
    let result = bytes
        .get(*offset..end)
        .ok_or_else(|| {
            DataFusionError::Execution("truncated window deduplicate state".to_string())
        })?
        .try_into()
        .unwrap();
    *offset = end;
    Ok(result)
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
    fn state_round_trip_and_retraction_winner() {
        let state = WindowState {
            next_sequence: 3,
            candidates: vec![
                Candidate {
                    order: 10,
                    sequence: 0,
                    row: vec![1, 2],
                },
                Candidate {
                    order: 20,
                    sequence: 1,
                    row: vec![3],
                },
                Candidate {
                    order: 20,
                    sequence: 2,
                    row: vec![4],
                },
            ],
        };
        let decoded = decode_state(&encode_state(&state)).unwrap();
        assert_eq!(winner(&decoded.candidates, true).unwrap().row, vec![4]);
        assert_eq!(winner(&decoded.candidates, false).unwrap().row, vec![1, 2]);
    }

    #[test]
    fn accounts_dedup_rows_timers_and_state_in_host_memory() {
        let broker = Arc::new(TestBroker::new(64 << 20));
        let mut processor = WindowDeduplicateProcessor::new(
            &plan(true),
            128,
            0,
            127,
            HostMemoryReservation::new(broker.clone(), "window dedup accounting"),
        )
        .unwrap();
        let empty_state = broker.reserved();
        let output = processor
            .process_arrow(batch(
                &[7, 8],
                &[10, 20],
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
    fn retraction_restores_the_previous_winner_after_canonical_restore() {
        let broker = Arc::new(TestBroker::new(1 << 30));
        let mut source = WindowDeduplicateProcessor::new(
            &plan(true),
            128,
            0,
            127,
            HostMemoryReservation::new(broker.clone(), "window dedup source"),
        )
        .unwrap();
        source
            .process_arrow(batch(
                &[7, 7],
                &[10, 20],
                &[100, 100],
                &[b"old", b"new"],
                &[INSERT, INSERT],
            ))
            .unwrap();
        let snapshots = (0..128)
            .map(|key_group| source.snapshot_key_group(key_group).unwrap())
            .collect::<Vec<_>>();

        let mut restored = WindowDeduplicateProcessor::new(
            &plan(true),
            128,
            0,
            127,
            HostMemoryReservation::new(broker, "window dedup restored"),
        )
        .unwrap();
        for (key_group, snapshot) in snapshots.iter().enumerate() {
            restored
                .restore_key_group(key_group as u32, snapshot)
                .unwrap();
        }
        restored
            .process_arrow(batch(&[7], &[20], &[100], &[b"new"], &[DELETE]))
            .unwrap();
        let output = restored.advance_event_time(99).unwrap();
        let rows = output
            .column(3)
            .as_any()
            .downcast_ref::<BinaryArray>()
            .unwrap();
        assert_eq!(rows.len(), 1);
        assert_eq!(rows.value(0), b"old");
        assert_eq!(restored.statistics()[5], 0);
    }

    #[test]
    fn first_row_ties_use_input_sequence_order() {
        let broker = Arc::new(TestBroker::new(1 << 30));
        let mut processor = WindowDeduplicateProcessor::new(
            &plan(false),
            128,
            0,
            127,
            HostMemoryReservation::new(broker, "window first dedup"),
        )
        .unwrap();
        processor
            .process_arrow(batch(
                &[1, 1],
                &[10, 10],
                &[50, 50],
                &[b"first", b"second"],
                &[INSERT, INSERT],
            ))
            .unwrap();
        let output = processor.advance_event_time(49).unwrap();
        let rows = output
            .column(3)
            .as_any()
            .downcast_ref::<BinaryArray>()
            .unwrap();
        assert_eq!(rows.value(0), b"first");
    }

    fn plan(keep_last: bool) -> Vec<u8> {
        proto::NativePlan {
            protocol_version: crate::PLAN_PROTOCOL_VERSION,
            root: Some(proto::Operator {
                operator: Some(proto::operator::Operator::WindowDeduplicate(Box::new(
                    proto::WindowDeduplicate {
                        input: None,
                        partition_key_indices: vec![0],
                        order_index: 1,
                        window_end_index: 2,
                        keep_last,
                        input_changelog: true,
                        input_schema: Some(proto::Schema {
                            fields: vec![
                                proto_field(
                                    "key",
                                    proto::logical_type::Type::Bigint(proto::EmptyType::default()),
                                ),
                                proto_field(
                                    "order",
                                    proto::logical_type::Type::Timestamp(proto::PrecisionType {
                                        precision: 3,
                                    }),
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

    fn batch(
        keys: &[i64],
        order: &[i64],
        window_end: &[i64],
        rows: &[&[u8]],
        kinds: &[i8],
    ) -> RecordBatch {
        RecordBatch::try_from_iter(vec![
            ("key", Arc::new(Int64Array::from(keys.to_vec())) as ArrayRef),
            (
                "order",
                Arc::new(TimestampMillisecondArray::from(order.to_vec())) as ArrayRef,
            ),
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
