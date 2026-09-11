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
    KeyedState, NativeTimerService, OrderedMemoryKeyedState, RocksPluginKeyedState, StateKey,
    StateKeyRef, StateMutation, TimerDomain, TimerKey,
};
use crate::{decode_plan, proto};

mod draining;
mod legacy_state;
mod migration;
mod row_state;
mod sorting;
mod timer_state;

const INSERT: i8 = 0;
const UPDATE_BEFORE: i8 = 1;
const UPDATE_AFTER: i8 = 2;
const DELETE: i8 = 3;
const ROWS_KEY_PREFIX: u8 = 7;
const PROCESSING_TIME_ROWS_KEY: &[u8] = b"\x07processing-time-rows";
const TIMER_STATE_KEY: &[u8] = b"\0streamfusion-temporal-sort-timers";
const LAST_TRIGGER_STATE_KEY: &[u8] = b"\0streamfusion-temporal-sort-last-trigger";
const INPUT_KIND_COLUMN: &str = "__streamfusion_input_row_kind";
const PROCESSING_TIME_COLUMN: &str = "__streamfusion_processing_time";
const MAX_TIMERS_PER_OUTPUT: usize = 4_096;

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
    pending_drain: Option<draining::Drain>,
    failed: bool,
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
        let timers = reservation.sibling("native temporal sort timers");
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
            pending_drain: None,
            failed: false,
            scratch_reservation,
            state_read_batches: 0,
            state_write_batches: 0,
            timer_registrations: 0,
            timers_fired: 0,
            late_records_dropped: 0,
        })
    }

    pub(crate) fn process_arrow(&mut self, batch: RecordBatch) -> Result<()> {
        self.require_idle()?;
        let result = self.process_arrow_inner(batch);
        if result.is_err() {
            self.failed = true;
        }
        result
    }

    fn process_arrow_inner(&mut self, batch: RecordBatch) -> Result<()> {
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
        let incoming = timestamps
            .iter()
            .map(|&timestamp| {
                (
                    rows_state_key(self.key_group, timestamp),
                    groups.remove(&timestamp).unwrap(),
                )
            })
            .collect();
        let mut pending =
            row_state::append(self.state.as_ref(), incoming, &self.scratch_reservation)?;
        self.state_read_batches = self.state_read_batches.saturating_add(1);
        let timestamps = timestamps
            .into_iter()
            .zip(&pending.was_empty)
            .filter_map(|(timestamp, was_empty)| (*was_empty).then_some(timestamp))
            .collect();
        let domain = self.domain();
        let (registered, _timer_workspace) = timer_state::register(
            &mut self.timers,
            self.key_group,
            domain,
            timestamps,
            &mut pending.mutations,
            &self.scratch_reservation,
        )?;
        self.timer_registrations = self.timer_registrations.saturating_add(registered as u64);
        self.state.write_batch(pending.mutations)?;
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
        // Flink clears the entire pending list at the first due callback, even when a delayed
        // mailbox has accumulated several processing-time timers. Keep that logical scope while
        // storing each row independently so append cost does not grow with the pending list.
        let key = processing_time_rows_state_key(self.key_group);
        let mut pending = row_state::append(
            self.state.as_ref(),
            vec![(key.clone(), incoming)],
            &self.scratch_reservation,
        )?;
        self.state_read_batches = self.state_read_batches.saturating_add(1);
        let (registered, _timer_workspace) = timer_state::register(
            &mut self.timers,
            self.key_group,
            TimerDomain::ProcessingTime,
            timer_timestamps.into_iter().collect(),
            &mut pending.mutations,
            &self.scratch_reservation,
        )?;
        self.timer_registrations = self.timer_registrations.saturating_add(registered as u64);
        self.state.write_batch(pending.mutations)?;
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

    fn output_row_groups(&mut self, row_groups: Vec<Vec<BufferedRow>>) -> Result<RecordBatch> {
        let row_count = row_groups.iter().map(Vec::len).sum::<usize>();
        if row_count == 0 {
            return Ok(RecordBatch::new_empty(self.output_schema.clone()));
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
        let working_bytes = row_bytes
            .saturating_mul(2)
            .saturating_add(sort_key_bytes)
            .saturating_add(if self.secondary_converter.is_some() {
                // DataFusion's key arrays, sorted keys, row conversion and selection indices.
                sort_key_bytes
                    .saturating_mul(3)
                    .saturating_add(row_count.saturating_mul(128))
            } else {
                row_count.saturating_mul(size_of::<&BufferedRow>())
            });
        self.scratch_reservation.resize(working_bytes)?;

        let result = (|| {
            let rows = if self.secondary_converter.is_some() {
                sorting::ordered_rows(&row_groups)?
            } else {
                row_groups.iter().flatten().collect()
            };
            let parser = self.row_converter.parser();
            let mut output_columns = self
                .row_converter
                .convert_rows(rows.iter().map(|row| parser.parse(&row.row)))?;
            output_columns.push(Arc::new(Int8Array::from(
                rows.iter().map(|row| row.kind).collect::<Vec<_>>(),
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
        if !self.plan.processing_time {
            if let Some(drain) = &self.pending_drain {
                return drain.deadline;
            }
        }
        self.timers
            .next_timestamp(TimerDomain::EventTime)
            .unwrap_or(i64::MAX)
    }

    pub(crate) fn next_processing_time_timer(&self) -> i64 {
        if self.plan.processing_time {
            if let Some(drain) = &self.pending_drain {
                return drain.deadline;
            }
        }
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

    pub(crate) fn state_memory(&self) -> HostMemoryReservation {
        self.scratch_reservation.sibling("native state transfer")
    }

    pub(crate) fn snapshot_key_group(&self, key_group: u32) -> Result<crate::state::SnapshotBytes> {
        self.require_idle()?;
        self.state
            .snapshot_key_group(key_group, &self.scratch_reservation)
    }

    pub(crate) fn restore_key_group(&mut self, key_group: u32, bytes: &[u8]) -> Result<()> {
        self.restore_with(key_group, |state, owner| {
            state.restore_key_group(key_group, bytes, owner)
        })
    }

    pub(crate) fn restore_physical_key_group(
        &mut self,
        key_group: u32,
        source: &dyn KeyedState,
    ) -> Result<()> {
        self.restore_with(key_group, |state, owner| {
            crate::state::import_key_group(state, source, key_group, owner, &mut |_, _| Ok(()))
        })
    }

    fn restore_with(
        &mut self,
        key_group: u32,
        import: impl FnOnce(&mut dyn KeyedState, &HostMemoryReservation) -> Result<()>,
    ) -> Result<()> {
        self.require_idle()?;
        let result = import(self.state.as_mut(), &self.scratch_reservation)
            .and_then(|_| self.restore_control_state(key_group));
        if result.is_err() {
            self.failed = true;
        }
        result
    }

    fn restore_control_state(&mut self, key_group: u32) -> Result<()> {
        migration::migrate_legacy_groups(
            self.state.as_mut(),
            key_group,
            &self.scratch_reservation,
            &mut self.state_write_batches,
        )?;
        let domain = self.domain();
        let last_trigger = timer_state::restore(
            self.state.as_mut(),
            &mut self.timers,
            key_group,
            domain,
            &self.scratch_reservation,
        )?;
        self.state_read_batches = self.state_read_batches.saturating_add(1);
        if key_group == self.key_group {
            self.last_triggering_timestamp = last_trigger;
        }
        Ok(())
    }

    pub(crate) fn checkpoint(&self, directory: &std::path::Path) -> Result<()> {
        self.require_idle()?;
        self.state.checkpoint(directory)
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

fn metadata_index(schema: &SchemaRef, name: &str) -> Option<usize> {
    schema
        .fields()
        .iter()
        .position(|field| field.name() == name)
}

#[cfg(test)]
mod tests;

#[cfg(test)]
mod row_state_tests;
