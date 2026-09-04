// Copyright 2026 StreamFusion Authors
// Licensed under the Apache License, Version 2.0

use std::collections::BTreeSet;
use std::mem::size_of;
use std::sync::Arc;

use ahash::RandomState;
use arrow::array::{Array, Int64Array, Int8Array};
use arrow::compute::SortOptions;
use arrow::datatypes::{DataType, Field, Schema, SchemaRef};
use arrow::record_batch::RecordBatch;
use arrow_row::{RowConverter, SortField};
use datafusion::error::{DataFusionError, Result};
use hashbrown::HashMap;

use super::window_table_function::timestamp_millis;
use crate::memory_pool::HostMemoryReservation;
use crate::planner::arrow_schema;
use crate::state::{
    KeyedState, MemoryKeyedState, NativeTimerService, RocksPluginKeyedState, StateKey, StateKeyRef,
    StateMutation, TimerDomain, TimerKey,
};
use crate::{decode_plan, proto};

const INSERT: i8 = 0;
const UPDATE_BEFORE: i8 = 1;
const UPDATE_AFTER: i8 = 2;
const DELETE: i8 = 3;
const STATE_MAGIC: &[u8; 4] = b"SFTS";
const STATE_VERSION: u8 = 2;
const ROWS_KEY_PREFIX: u8 = 7;
const PROCESSING_TIME_ROWS_KEY: &[u8] = b"\x07processing-time-rows";
const TIMER_STATE_KEY: &[u8] = b"\0streamfusion-temporal-sort-timers";
const LAST_TRIGGER_STATE_KEY: &[u8] = b"\0streamfusion-temporal-sort-last-trigger";
const INPUT_KIND_COLUMN: &str = "__streamfusion_input_row_kind";
const PROCESSING_TIME_COLUMN: &str = "__streamfusion_processing_time";
const MAX_EVENT_TIME_TIMERS_PER_OUTPUT: usize = 4_096;

#[derive(Clone, Debug, PartialEq, Eq)]
struct BufferedRow {
    kind: i8,
    sort_key: Vec<u8>,
    row: Vec<u8>,
}

/// Arrow-native temporal sort with backend-neutral row and timer state.
pub(crate) struct TemporalSortProcessor {
    plan: proto::TemporalSort,
    state: Box<dyn KeyedState>,
    timers: NativeTimerService,
    key_group: u32,
    visible_schema: SchemaRef,
    output_schema: SchemaRef,
    row_converter: RowConverter,
    secondary_converter: Option<RowConverter>,
    input_schema: Option<SchemaRef>,
    input_kind_index: Option<usize>,
    processing_time_index: Option<usize>,
    last_triggering_timestamp: i64,
    scratch_reservation: HostMemoryReservation,
    state_read_batches: u64,
    state_write_batches: u64,
    timer_registrations: u64,
    timers_fired: u64,
    late_records_dropped: u64,
}

impl TemporalSortProcessor {
    pub(crate) fn new(
        serialized_plan: &[u8],
        max_parallelism: u32,
        first_key_group: u32,
        last_key_group: u32,
        state_reservation: HostMemoryReservation,
    ) -> Result<Self> {
        let timers = state_reservation.sibling("native temporal sort timers");
        let scratch = state_reservation.sibling("native temporal sort batch scratch and output");
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
        let timers = reservation.sibling("native temporal sort timers");
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
        if max_parallelism == 0 || first_key_group > last_key_group {
            return Err(DataFusionError::Plan(
                "temporal sort requires a valid keyed subtask range".to_string(),
            ));
        }
        let root = decode_plan(serialized_plan)?
            .root
            .ok_or_else(|| DataFusionError::Plan("temporal sort plan has no root".to_string()))?;
        let plan = match root.operator {
            Some(proto::operator::Operator::TemporalSort(plan)) => *plan,
            _ => {
                return Err(DataFusionError::Plan(
                    "temporal sort handle requires a TemporalSort root".to_string(),
                ));
            }
        };
        validate_plan(&plan)?;
        let visible_schema = arrow_schema(plan.input_schema.as_ref().expect("validated"))?;
        let row_converter = row_converter(&visible_schema)?;
        let secondary_converter = secondary_converter(&visible_schema, &plan)?;
        let mut output_fields = visible_schema.fields().iter().cloned().collect::<Vec<_>>();
        output_fields.push(Arc::new(Field::new(
            "__streamfusion_row_kind",
            DataType::Int8,
            false,
        )));
        Ok(Self {
            plan,
            state,
            timers: NativeTimerService::new(first_key_group, last_key_group, timer_reservation)?,
            key_group: first_key_group,
            output_schema: Arc::new(Schema::new(output_fields)),
            visible_schema,
            row_converter,
            secondary_converter,
            input_schema: None,
            input_kind_index: None,
            processing_time_index: None,
            last_triggering_timestamp: i64::MIN,
            scratch_reservation,
            state_read_batches: 0,
            state_write_batches: 0,
            timer_registrations: 0,
            timers_fired: 0,
            late_records_dropped: 0,
        })
    }

    pub(crate) fn process_arrow(&mut self, batch: RecordBatch) -> Result<()> {
        self.prepare_schema(batch.schema())?;
        let visible_count = self.visible_schema.fields().len();
        let visible_bytes = batch.columns()[..visible_count]
            .iter()
            .map(|column| column.get_array_memory_size())
            .sum::<usize>();
        let base = visible_bytes.saturating_add(batch.num_rows().saturating_mul(160));
        self.scratch_reservation.resize(base)?;
        let rows = self
            .row_converter
            .convert_columns(&batch.columns()[..visible_count]);
        let sort_rows = self.secondary_converter.as_ref().map(|converter| {
            let columns = self
                .plan
                .secondary_key_indices
                .iter()
                .map(|&index| Arc::clone(batch.column(index as usize)))
                .collect::<Vec<_>>();
            converter.convert_columns(&columns)
        });
        let result = match (rows, sort_rows.transpose()) {
            (Ok(rows), Ok(sort_rows)) => {
                self.process_arrow_accounted(&batch, &rows, sort_rows.as_ref())
            }
            (Err(error), _) | (_, Err(error)) => Err(error.into()),
        };
        self.scratch_reservation.resize(0)?;
        result
    }

    fn process_arrow_accounted(
        &mut self,
        batch: &RecordBatch,
        encoded_rows: &arrow_row::Rows,
        sort_rows: Option<&arrow_row::Rows>,
    ) -> Result<()> {
        let kinds = batch
            .column(self.input_kind_index.expect("schema prepared"))
            .as_any()
            .downcast_ref::<Int8Array>()
            .ok_or_else(|| {
                DataFusionError::Execution("temporal sort RowKind metadata is not Int8".to_string())
            })?;
        let processing_times = self
            .processing_time_index
            .map(|index| {
                batch
                    .column(index)
                    .as_any()
                    .downcast_ref::<Int64Array>()
                    .ok_or_else(|| {
                        DataFusionError::Execution(
                            "temporal sort processing-time metadata is not Int64".to_string(),
                        )
                    })
            })
            .transpose()?;
        let time_column =
            (!self.plan.processing_time).then(|| batch.column(self.plan.time_index as usize));
        let mut groups = HashMap::<i64, Vec<BufferedRow>, RandomState>::with_capacity_and_hasher(
            batch.num_rows(),
            RandomState::new(),
        );
        let mut processing_rows = Vec::with_capacity(batch.num_rows());
        let mut processing_timestamps = BTreeSet::new();
        for row in 0..batch.num_rows() {
            let kind = kinds.value(row);
            if !matches!(kind, INSERT | UPDATE_BEFORE | UPDATE_AFTER | DELETE) {
                return Err(DataFusionError::Execution(format!(
                    "unknown Flink RowKind byte {kind}"
                )));
            }
            let timestamp = if let Some(processing_times) = processing_times {
                processing_times.value(row).saturating_add(1)
            } else {
                timestamp_millis(time_column.expect("event-time column"), row)?.ok_or_else(
                    || {
                        DataFusionError::Execution(
                            "temporal sort time attribute must not be null".to_string(),
                        )
                    },
                )?
            };
            if !self.plan.processing_time && timestamp <= self.last_triggering_timestamp {
                self.late_records_dropped = self.late_records_dropped.saturating_add(1);
                continue;
            }
            let buffered = BufferedRow {
                kind,
                sort_key: sort_rows
                    .map(|rows| rows.row(row).data().to_vec())
                    .unwrap_or_default(),
                row: encoded_rows.row(row).data().to_vec(),
            };
            if self.plan.processing_time {
                processing_rows.push(buffered);
                processing_timestamps.insert(timestamp);
            } else {
                groups.entry(timestamp).or_default().push(buffered);
            }
        }
        if self.plan.processing_time {
            return self.process_processing_time_rows(processing_rows, processing_timestamps);
        }
        if groups.is_empty() {
            return Ok(());
        }
        let mut timestamps = groups.keys().copied().collect::<Vec<_>>();
        timestamps.sort_unstable();
        let keys = timestamps
            .iter()
            .map(|&timestamp| rows_state_key(self.key_group, timestamp))
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
        let domain = self.domain();
        let mut timer_dirty = false;
        let mut mutations = Vec::with_capacity(keys.len() + 1);
        for ((timestamp, key), existing) in timestamps.into_iter().zip(keys).zip(existing) {
            let mut rows = existing
                .map(|bytes| decode_rows(bytes.as_ref()))
                .transpose()?
                .unwrap_or_default();
            let was_empty = rows.is_empty();
            rows.append(groups.get_mut(&timestamp).expect("timestamp was grouped"));
            if was_empty
                && self.timers.register(
                    self.key_group,
                    domain,
                    TimerKey {
                        timestamp,
                        key: key.key.clone(),
                        namespace: Vec::new(),
                    },
                )?
            {
                self.timer_registrations = self.timer_registrations.saturating_add(1);
                timer_dirty = true;
            }
            mutations.push(StateMutation {
                key,
                value: Some(encode_rows(&rows)?),
            });
        }
        if timer_dirty {
            self.append_timer_mutation(&mut mutations)?;
        }
        self.state.write_batch(mutations)?;
        self.state_write_batches = self.state_write_batches.saturating_add(1);
        Ok(())
    }

    fn process_processing_time_rows(
        &mut self,
        incoming: Vec<BufferedRow>,
        timer_timestamps: BTreeSet<i64>,
    ) -> Result<()> {
        if incoming.is_empty() {
            return Ok(());
        }
        // Flink's ProcTimeSortOperator stores every pending row in one ListState. The first due
        // callback sorts and clears that complete list; later callbacks registered before that
        // firing consequently observe an empty list. Keep the same state shape, including when a
        // delayed mailbox callback lets more than one processing-time timestamp accumulate.
        let key = processing_time_rows_state_key(self.key_group);
        let existing = self.state.get_batch(&[StateKeyRef {
            key_group: key.key_group,
            key: &key.key,
        }])?;
        self.state_read_batches = self.state_read_batches.saturating_add(1);
        let mut rows = existing
            .into_iter()
            .next()
            .flatten()
            .map(|bytes| decode_rows(bytes.as_ref()))
            .transpose()?
            .unwrap_or_default();
        rows.extend(incoming);

        let mut timer_dirty = false;
        for timestamp in timer_timestamps {
            if self.timers.register(
                self.key_group,
                TimerDomain::ProcessingTime,
                TimerKey {
                    timestamp,
                    key: key.key.clone(),
                    namespace: Vec::new(),
                },
            )? {
                self.timer_registrations = self.timer_registrations.saturating_add(1);
                timer_dirty = true;
            }
        }
        let mut mutations = vec![StateMutation {
            key,
            value: Some(encode_rows(&rows)?),
        }];
        if timer_dirty {
            self.append_timer_mutation(&mut mutations)?;
        }
        self.state.write_batch(mutations)?;
        self.state_write_batches = self.state_write_batches.saturating_add(1);
        Ok(())
    }

    pub(crate) fn advance_event_time(&mut self, watermark: i64) -> Result<RecordBatch> {
        if self.plan.processing_time {
            return Err(DataFusionError::Execution(
                "processing-time temporal sort cannot advance event time".to_string(),
            ));
        }
        self.advance(TimerDomain::EventTime, watermark)
    }

    pub(crate) fn advance_processing_time(&mut self, timestamp: i64) -> Result<RecordBatch> {
        if !self.plan.processing_time {
            return Err(DataFusionError::Execution(
                "event-time temporal sort cannot advance processing time".to_string(),
            ));
        }
        self.advance(TimerDomain::ProcessingTime, timestamp)
    }

    fn advance(&mut self, domain: TimerDomain, progress: i64) -> Result<RecordBatch> {
        // A watermark can make many timestamp groups ready at once. Drain a bounded group of them
        // with one backend multi-get/write and one Arrow C Data export, preserving timer order.
        // One timer per callback degenerates into thousands of JNI calls, while an unbounded drain
        // can exceed the task's output-memory slice. Processing time deliberately retains Flink's
        // one shared ListState callback semantics.
        let fired = if domain == TimerDomain::EventTime {
            self.timers
                .advance_limited(domain, progress, MAX_EVENT_TIME_TIMERS_PER_OUTPUT)?
        } else {
            self.timers.advance(domain, progress)?
        };
        self.timers_fired = self.timers_fired.saturating_add(fired.len() as u64);
        if fired.is_empty() {
            return Ok(RecordBatch::new_empty(self.output_schema.clone()));
        }
        let last_event_timestamp = fired
            .iter()
            .map(|fired| fired.timer.timestamp)
            .max()
            .unwrap_or(i64::MIN);
        let mut seen = hashbrown::HashSet::<StateKey, RandomState>::with_hasher(RandomState::new());
        let mut keys = Vec::with_capacity(fired.len());
        for fired in fired {
            let key = StateKey {
                key_group: fired.key_group,
                key: fired.timer.key,
            };
            if seen.insert(key.clone()) {
                keys.push(key);
            }
        }
        let refs = keys
            .iter()
            .map(|key| StateKeyRef {
                key_group: key.key_group,
                key: &key.key,
            })
            .collect::<Vec<_>>();
        let state = self.state.get_batch(&refs)?;
        self.state_read_batches = self.state_read_batches.saturating_add(1);
        let mut row_groups = Vec::with_capacity(keys.len());
        for value in state {
            let rows = value
                .map(|bytes| decode_rows(bytes.as_ref()))
                .transpose()?
                .unwrap_or_default();
            if !rows.is_empty() {
                row_groups.push(rows);
            }
        }
        let mut mutations = keys
            .into_iter()
            .map(|key| StateMutation { key, value: None })
            .collect::<Vec<_>>();
        if domain == TimerDomain::EventTime && !row_groups.is_empty() {
            self.last_triggering_timestamp = last_event_timestamp;
            mutations.push(StateMutation {
                key: StateKey {
                    key_group: self.key_group,
                    key: LAST_TRIGGER_STATE_KEY.to_vec(),
                },
                value: Some(self.last_triggering_timestamp.to_le_bytes().to_vec()),
            });
        }
        self.append_timer_mutation(&mut mutations)?;
        self.state.write_batch(mutations)?;
        self.state_write_batches = self.state_write_batches.saturating_add(1);
        self.output_row_groups(row_groups)
    }

    fn output_row_groups(&mut self, mut row_groups: Vec<Vec<BufferedRow>>) -> Result<RecordBatch> {
        let row_count = row_groups.iter().map(Vec::len).sum::<usize>();
        if row_count == 0 {
            return Ok(RecordBatch::new_empty(self.output_schema.clone()));
        }
        if self.secondary_converter.is_some() {
            for rows in &mut row_groups {
                // Arrow row keys encode the planned ascending/descending and null placement.
                // Rust's stable sort retains input sequence for equal secondary keys.
                rows.sort_by(|left, right| left.sort_key.cmp(&right.sort_key));
            }
        }
        let row_bytes = row_groups
            .iter()
            .flatten()
            .map(|row| row.row.capacity().saturating_add(size_of::<BufferedRow>()))
            .sum::<usize>();
        let sort_key_bytes = row_groups
            .iter()
            .flatten()
            .map(|row| row.sort_key.capacity())
            .sum::<usize>();
        // Encoded rows and the emitted arrays coexist until the C Data owner takes over. Admit the
        // complete sort keys plus both the encoded rows and their decoded-array equivalent before
        // allocation. Sort keys are not decoded into the result and therefore are not doubled.
        let working_bytes = row_bytes.saturating_mul(2).saturating_add(sort_key_bytes);
        self.scratch_reservation.resize(working_bytes)?;

        let result = (|| {
            let parser = self.row_converter.parser();
            let mut output_columns = self.row_converter.convert_rows(
                row_groups
                    .iter()
                    .flatten()
                    .map(|row| parser.parse(&row.row)),
            )?;
            output_columns.push(Arc::new(Int8Array::from(
                row_groups
                    .iter()
                    .flatten()
                    .map(|row| row.kind)
                    .collect::<Vec<_>>(),
            )));
            RecordBatch::try_new(self.output_schema.clone(), output_columns).map_err(Into::into)
        })();

        match result {
            Ok(output) => {
                let output_bytes = output.get_array_memory_size();
                if let Err(error) = self
                    .scratch_reservation
                    .resize(working_bytes.max(output_bytes))
                {
                    self.scratch_reservation.resize(0)?;
                    return Err(error);
                }
                if let Err(error) = self.scratch_reservation.transfer_to_arrow(output_bytes) {
                    self.scratch_reservation.resize(0)?;
                    return Err(error);
                }
                self.scratch_reservation.resize(0)?;
                Ok(output)
            }
            Err(error) => {
                self.scratch_reservation.resize(0)?;
                Err(error)
            }
        }
    }

    pub(crate) fn next_event_time_timer(&self) -> i64 {
        self.timers
            .next_timestamp(TimerDomain::EventTime)
            .unwrap_or(i64::MAX)
    }

    pub(crate) fn next_processing_time_timer(&self) -> i64 {
        self.timers
            .next_timestamp(TimerDomain::ProcessingTime)
            .unwrap_or(i64::MAX)
    }

    pub(crate) fn statistics(&self) -> [u64; 8] {
        [
            self.state_read_batches,
            self.state_write_batches,
            self.timer_registrations,
            0,
            self.timers_fired,
            self.timers.timer_count(TimerDomain::EventTime) as u64,
            self.timers.timer_count(TimerDomain::ProcessingTime) as u64,
            self.late_records_dropped,
        ]
    }

    pub(crate) fn snapshot_key_group(&self, key_group: u32) -> Result<Vec<u8>> {
        self.state.snapshot_key_group(key_group)
    }

    pub(crate) fn restore_key_group(&mut self, key_group: u32, bytes: &[u8]) -> Result<()> {
        self.state.restore_key_group(key_group, bytes)?;
        let restored = self.state.get_batch(&[
            StateKeyRef {
                key_group,
                key: TIMER_STATE_KEY,
            },
            StateKeyRef {
                key_group,
                key: LAST_TRIGGER_STATE_KEY,
            },
        ])?;
        self.state_read_batches = self.state_read_batches.saturating_add(1);
        if let Some(bytes) = restored[0].as_ref() {
            self.timers.restore_key_group(key_group, bytes.as_ref())?;
        }
        if key_group == self.key_group {
            if let Some(bytes) = restored[1].as_ref() {
                self.last_triggering_timestamp =
                    i64::from_le_bytes(bytes.as_ref().try_into().map_err(|_| {
                        DataFusionError::Execution(
                            "invalid temporal sort last-trigger state".to_string(),
                        )
                    })?);
            }
        }
        Ok(())
    }

    pub(crate) fn checkpoint(&self, directory: &std::path::Path) -> Result<()> {
        self.state.checkpoint(directory)
    }

    fn append_timer_mutation(&self, mutations: &mut Vec<StateMutation>) -> Result<()> {
        mutations.push(StateMutation {
            key: StateKey {
                key_group: self.key_group,
                key: TIMER_STATE_KEY.to_vec(),
            },
            value: Some(self.timers.snapshot_key_group(self.key_group)?),
        });
        Ok(())
    }

    fn domain(&self) -> TimerDomain {
        if self.plan.processing_time {
            TimerDomain::ProcessingTime
        } else {
            TimerDomain::EventTime
        }
    }

    fn prepare_schema(&mut self, schema: SchemaRef) -> Result<()> {
        if let Some(expected) = self.input_schema.as_ref() {
            if expected.as_ref() != schema.as_ref() {
                return Err(DataFusionError::Execution(
                    "temporal sort input schema changed while running".to_string(),
                ));
            }
            return Ok(());
        }
        self.input_kind_index = metadata_index(&schema, INPUT_KIND_COLUMN);
        self.processing_time_index = metadata_index(&schema, PROCESSING_TIME_COLUMN);
        if self.input_kind_index.is_none()
            || (self.plan.processing_time && self.processing_time_index.is_none())
            || (!self.plan.processing_time && self.processing_time_index.is_some())
        {
            return Err(DataFusionError::Execution(
                "temporal sort Arrow metadata does not match its time mode".to_string(),
            ));
        }
        let visible_count = [
            self.input_kind_index,
            self.processing_time_index,
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
                "temporal sort Arrow schema does not match its protobuf schema".to_string(),
            ));
        }
        self.input_schema = Some(schema);
        Ok(())
    }
}

fn validate_plan(plan: &proto::TemporalSort) -> Result<()> {
    let schema = plan.input_schema.as_ref().ok_or_else(|| {
        DataFusionError::Plan("temporal sort requires an input schema".to_string())
    })?;
    if plan.time_index as usize >= schema.fields.len()
        || plan.secondary_key_indices.len() != plan.secondary_ascending.len()
        || plan.secondary_key_indices.len() != plan.secondary_nulls_last.len()
        || plan
            .secondary_key_indices
            .iter()
            .any(|&index| index as usize >= schema.fields.len())
    {
        return Err(DataFusionError::Plan(
            "temporal sort has an invalid time or secondary ordering contract".to_string(),
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

fn secondary_converter(
    schema: &SchemaRef,
    plan: &proto::TemporalSort,
) -> Result<Option<RowConverter>> {
    if plan.secondary_key_indices.is_empty() {
        return Ok(None);
    }
    let fields = plan
        .secondary_key_indices
        .iter()
        .zip(&plan.secondary_ascending)
        .zip(&plan.secondary_nulls_last)
        .map(|((&index, &ascending), &nulls_last)| {
            SortField::new_with_options(
                schema.field(index as usize).data_type().clone(),
                SortOptions {
                    descending: !ascending,
                    nulls_first: !nulls_last,
                },
            )
        })
        .collect::<Vec<_>>();
    Ok(Some(RowConverter::new(fields)?))
}

fn rows_state_key(key_group: u32, timestamp: i64) -> StateKey {
    let mut key = Vec::with_capacity(9);
    key.push(ROWS_KEY_PREFIX);
    key.extend_from_slice(&timestamp.to_be_bytes());
    StateKey { key_group, key }
}

fn processing_time_rows_state_key(key_group: u32) -> StateKey {
    StateKey {
        key_group,
        key: PROCESSING_TIME_ROWS_KEY.to_vec(),
    }
}

fn encode_rows(rows: &[BufferedRow]) -> Result<Vec<u8>> {
    let count = u32::try_from(rows.len()).map_err(|_| {
        DataFusionError::Execution("temporal sort timestamp group is too large".to_string())
    })?;
    let mut output = Vec::new();
    output.extend_from_slice(STATE_MAGIC);
    output.push(STATE_VERSION);
    output.extend_from_slice(&count.to_le_bytes());
    for row in rows {
        output.push(row.kind as u8);
        let sort_key_length = u32::try_from(row.sort_key.len()).map_err(|_| {
            DataFusionError::Execution("temporal sort key is too large".to_string())
        })?;
        output.extend_from_slice(&sort_key_length.to_le_bytes());
        output.extend_from_slice(&row.sort_key);
        let length = u32::try_from(row.row.len()).map_err(|_| {
            DataFusionError::Execution("temporal sort row is too large".to_string())
        })?;
        output.extend_from_slice(&length.to_le_bytes());
        output.extend_from_slice(&row.row);
    }
    Ok(output)
}

fn decode_rows(bytes: &[u8]) -> Result<Vec<BufferedRow>> {
    if bytes.len() < 9 || &bytes[..4] != STATE_MAGIC || bytes[4] != STATE_VERSION {
        return Err(DataFusionError::Execution(
            "invalid temporal sort row state".to_string(),
        ));
    }
    let mut offset = 5;
    let count = read_u32(bytes, &mut offset)? as usize;
    let mut rows = Vec::with_capacity(count);
    for _ in 0..count {
        let kind = *bytes.get(offset).ok_or_else(|| {
            DataFusionError::Execution("truncated temporal sort RowKind".to_string())
        })? as i8;
        offset += 1;
        let sort_key_length = read_u32(bytes, &mut offset)? as usize;
        let sort_key_end = offset.checked_add(sort_key_length).ok_or_else(|| {
            DataFusionError::Execution("temporal sort key length overflow".to_string())
        })?;
        let sort_key = bytes
            .get(offset..sort_key_end)
            .ok_or_else(|| DataFusionError::Execution("truncated temporal sort key".to_string()))?;
        offset = sort_key_end;
        let length = read_u32(bytes, &mut offset)? as usize;
        let end = offset.checked_add(length).ok_or_else(|| {
            DataFusionError::Execution("temporal sort row length overflow".to_string())
        })?;
        let row = bytes
            .get(offset..end)
            .ok_or_else(|| DataFusionError::Execution("truncated temporal sort row".to_string()))?;
        rows.push(BufferedRow {
            kind,
            sort_key: sort_key.to_vec(),
            row: row.to_vec(),
        });
        offset = end;
    }
    if offset != bytes.len() {
        return Err(DataFusionError::Execution(
            "temporal sort state has trailing bytes".to_string(),
        ));
    }
    Ok(rows)
}

fn read_u32(bytes: &[u8], offset: &mut usize) -> Result<u32> {
    let end = offset.checked_add(4).ok_or_else(|| {
        DataFusionError::Execution("temporal sort state offset overflow".to_string())
    })?;
    let value = bytes
        .get(*offset..end)
        .ok_or_else(|| DataFusionError::Execution("truncated temporal sort state".to_string()))?;
    *offset = end;
    Ok(u32::from_le_bytes(value.try_into().unwrap()))
}

fn metadata_index(schema: &SchemaRef, name: &str) -> Option<usize> {
    schema
        .fields()
        .iter()
        .position(|field| field.name() == name)
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::memory_pool::{tests_support::TestBroker, HostMemoryReservation};
    use arrow::array::{ArrayRef, Int32Array, StringArray, TimestampMillisecondArray};
    use prost::Message;

    #[test]
    fn row_state_round_trips_every_changelog_kind() {
        let rows = vec![
            BufferedRow {
                kind: INSERT,
                sort_key: b"sort-insert".to_vec(),
                row: b"insert".to_vec(),
            },
            BufferedRow {
                kind: UPDATE_BEFORE,
                sort_key: b"sort-before".to_vec(),
                row: b"before".to_vec(),
            },
            BufferedRow {
                kind: UPDATE_AFTER,
                sort_key: b"sort-after".to_vec(),
                row: b"after".to_vec(),
            },
            BufferedRow {
                kind: DELETE,
                sort_key: b"sort-delete".to_vec(),
                row: b"delete".to_vec(),
            },
        ];
        assert_eq!(decode_rows(&encode_rows(&rows).unwrap()).unwrap(), rows);
    }

    #[test]
    fn event_time_sorts_secondary_fields_stably_and_drops_already_fired_rows() {
        let broker = Arc::new(TestBroker::new(64 << 20));
        let mut processor = processor(false, broker.clone());
        processor
            .process_arrow(batch(
                &[1_000, 1_000, 2_000],
                &[2, 1, 9],
                &["second", "first", "later"],
                &[UPDATE_AFTER, DELETE, INSERT],
                None,
            ))
            .unwrap();
        let output = processor.advance_event_time(1_000).unwrap();

        assert_eq!(
            output
                .column(1)
                .as_any()
                .downcast_ref::<Int32Array>()
                .unwrap()
                .values(),
            &[1, 2]
        );
        assert_eq!(
            output
                .column(3)
                .as_any()
                .downcast_ref::<Int8Array>()
                .unwrap()
                .values(),
            &[DELETE, UPDATE_AFTER]
        );
        processor
            .process_arrow(batch(&[1_000], &[0], &["late"], &[INSERT], None))
            .unwrap();
        assert_eq!(processor.statistics()[7], 1);
        assert_eq!(processor.next_event_time_timer(), 2_000);
        drop(output);
        drop(processor);
        assert_eq!(broker.reserved(), 0);
    }

    #[test]
    fn event_time_coalesces_all_due_timestamp_groups_in_timer_order() {
        let broker = Arc::new(TestBroker::new(64 << 20));
        let mut processor = processor(false, broker.clone());
        processor
            .process_arrow(batch(
                &[3_000, 1_000, 2_000, 1_000],
                &[1, 2, 4, 1],
                &["last", "second", "middle", "first"],
                &[INSERT, INSERT, INSERT, INSERT],
                None,
            ))
            .unwrap();

        let output = processor.advance_event_time(3_000).unwrap();
        assert_eq!(output.num_rows(), 4);
        assert_eq!(
            output
                .column(1)
                .as_any()
                .downcast_ref::<Int32Array>()
                .unwrap()
                .values(),
            &[1, 2, 4, 1]
        );
        assert_eq!(processor.statistics()[4], 3);
        assert_eq!(processor.statistics()[5], 0);
        drop(output);
        drop(processor);
        assert_eq!(broker.reserved(), 0);
    }

    #[test]
    fn processing_time_and_timer_state_restore_from_canonical_key_group_bytes() {
        let source_broker = Arc::new(TestBroker::new(64 << 20));
        let mut source = processor(true, source_broker);
        source
            .process_arrow(batch(
                &[0, 0, 0],
                &[3, 1, 2],
                &["third", "first", "second"],
                &[INSERT, UPDATE_BEFORE, UPDATE_AFTER],
                Some(&[40, 40, 40]),
            ))
            .unwrap();
        let snapshot = source.snapshot_key_group(0).unwrap();

        let target_broker = Arc::new(TestBroker::new(64 << 20));
        let mut target = processor(true, target_broker.clone());
        target.restore_key_group(0, &snapshot).unwrap();
        assert_eq!(target.next_processing_time_timer(), 41);
        let output = target.advance_processing_time(41).unwrap();
        assert_eq!(
            output
                .column(1)
                .as_any()
                .downcast_ref::<Int32Array>()
                .unwrap()
                .values(),
            &[1, 2, 3]
        );
        assert_eq!(target.statistics()[6], 0);
        drop(output);
        drop(target);
        assert_eq!(target_broker.reserved(), 0);
    }

    #[test]
    fn first_processing_time_callback_sorts_and_clears_the_complete_flink_list_state() {
        let broker = Arc::new(TestBroker::new(64 << 20));
        let mut processor = processor(true, broker.clone());
        processor
            .process_arrow(batch(
                &[0, 0, 0],
                &[3, 1, 2],
                &["third", "first", "second"],
                &[INSERT, INSERT, INSERT],
                Some(&[42, 40, 42]),
            ))
            .unwrap();

        let output = processor.advance_processing_time(41).unwrap();
        assert_eq!(
            output
                .column(1)
                .as_any()
                .downcast_ref::<Int32Array>()
                .unwrap()
                .values(),
            &[1, 2, 3]
        );
        assert_eq!(processor.next_processing_time_timer(), 43);
        assert_eq!(processor.advance_processing_time(43).unwrap().num_rows(), 0);
        drop(output);
        drop(processor);
        assert_eq!(broker.reserved(), 0);
    }

    fn processor(processing_time: bool, broker: Arc<TestBroker>) -> TemporalSortProcessor {
        TemporalSortProcessor::new(
            &plan(processing_time),
            1,
            0,
            0,
            HostMemoryReservation::new(broker, "temporal sort test"),
        )
        .unwrap()
    }

    fn plan(processing_time: bool) -> Vec<u8> {
        proto::NativePlan {
            protocol_version: crate::PLAN_PROTOCOL_VERSION,
            root: Some(proto::Operator {
                operator: Some(proto::operator::Operator::TemporalSort(Box::new(
                    proto::TemporalSort {
                        input: None,
                        input_schema: Some(proto::Schema {
                            fields: vec![
                                proto_field(
                                    "rowtime",
                                    proto::logical_type::Type::Timestamp(proto::PrecisionType {
                                        precision: 3,
                                    }),
                                ),
                                proto_field(
                                    "number",
                                    proto::logical_type::Type::Integer(proto::EmptyType::default()),
                                ),
                                proto_field(
                                    "payload",
                                    proto::logical_type::Type::Varchar(proto::EmptyType::default()),
                                ),
                            ],
                        }),
                        time_index: 0,
                        processing_time,
                        secondary_key_indices: vec![1],
                        secondary_ascending: vec![true],
                        secondary_nulls_last: vec![true],
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
                nullable: false,
                r#type: Some(r#type),
            }),
        }
    }

    fn batch(
        timestamps: &[i64],
        values: &[i32],
        payloads: &[&str],
        kinds: &[i8],
        processing_times: Option<&[i64]>,
    ) -> RecordBatch {
        let mut columns = vec![
            (
                "rowtime",
                Arc::new(TimestampMillisecondArray::from(timestamps.to_vec())) as ArrayRef,
            ),
            (
                "number",
                Arc::new(Int32Array::from(values.to_vec())) as ArrayRef,
            ),
            (
                "payload",
                Arc::new(StringArray::from(payloads.to_vec())) as ArrayRef,
            ),
            (
                INPUT_KIND_COLUMN,
                Arc::new(Int8Array::from(kinds.to_vec())) as ArrayRef,
            ),
        ];
        if let Some(processing_times) = processing_times {
            columns.push((
                PROCESSING_TIME_COLUMN,
                Arc::new(Int64Array::from(processing_times.to_vec())) as ArrayRef,
            ));
        }
        RecordBatch::try_from_iter(columns).unwrap()
    }
}
