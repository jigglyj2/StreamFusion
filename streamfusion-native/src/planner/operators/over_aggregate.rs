// Copyright 2026 StreamFusion Authors
// Licensed under the Apache License, Version 2.0

use std::sync::Arc;

use ahash::RandomState;
use arrow::array::{Array, ArrayRef, BinaryArray, Int32Array, Int8Array};
use arrow::compute::SortOptions;
use arrow::datatypes::{DataType, Field, Schema, SchemaRef};
use arrow::record_batch::RecordBatch;
use arrow_row::{RowConverter, SortField};
use datafusion::error::{DataFusionError, Result};
use hashbrown::HashMap;

use crate::exchange::{assign_key_group, encode_binary_row, KeyField};
use crate::memory_pool::HostMemoryReservation;
use crate::planner::arrow_schema;
use crate::planner::operators::group_aggregate::{
    aggregate_array, lower_call, row_aggregate_values, AccumulatorState, AggregateValue, Call,
};
use crate::state::{
    KeyedState, MemoryKeyedState, RocksPluginKeyedState, StateKey, StateKeyRef, StateMutation,
};
use crate::{decode_plan, proto};

mod state_codec;
#[cfg(test)]
mod tests;

use state_codec::{decode_state, encode_state, OverState, StoredRow};

const INSERT: i8 = 0;
const UPDATE_BEFORE: i8 = 1;
const UPDATE_AFTER: i8 = 2;
const DELETE: i8 = 3;
const OVER_STATE_PREFIX: u8 = 1;

#[derive(Clone)]
struct OutputEvent {
    payload: Vec<u8>,
    values: Vec<Option<AggregateValue>>,
    kind: i8,
    input_ordinal: i32,
}

/// Persistent ordered keyed state for Flink streaming OVER aggregation.
pub(crate) struct OverAggregateProcessor {
    plan: proto::OverAggregate,
    calls: Vec<Call>,
    max_parallelism: u32,
    state: Box<dyn KeyedState>,
    input_schema: Option<SchemaRef>,
    visible_schema: SchemaRef,
    output_schema: SchemaRef,
    payload_converter: RowConverter,
    order_converter: Option<RowConverter>,
    key_fields: Vec<(usize, KeyField)>,
    preencoded_key_index: Option<usize>,
    input_kind_index: Option<usize>,
    scratch_reservation: HostMemoryReservation,
    state_read_batches: u64,
    state_write_batches: u64,
    missing_ids: u64,
    missing_sort_keys: u64,
}

impl OverAggregateProcessor {
    pub(crate) fn new(
        serialized_plan: &[u8],
        max_parallelism: u32,
        first_key_group: u32,
        last_key_group: u32,
        state_reservation: HostMemoryReservation,
    ) -> Result<Self> {
        let scratch = state_reservation.sibling("native over aggregate batch scratch and output");
        let state = Box::new(MemoryKeyedState::new(
            first_key_group,
            last_key_group,
            state_reservation,
        )?);
        Self::with_state(serialized_plan, max_parallelism, state, scratch)
    }

    pub(crate) fn new_rocksdb(
        serialized_plan: &[u8],
        max_parallelism: u32,
        first_key_group: u32,
        last_key_group: u32,
        plugin_path: &std::path::Path,
        database_path: &std::path::Path,
        memory_limit: usize,
        scratch_reservation: HostMemoryReservation,
    ) -> Result<Self> {
        let state = Box::new(RocksPluginKeyedState::open(
            plugin_path,
            database_path,
            first_key_group,
            last_key_group,
            memory_limit,
        )?);
        Self::with_state(serialized_plan, max_parallelism, state, scratch_reservation)
    }

    fn with_state(
        serialized_plan: &[u8],
        max_parallelism: u32,
        state: Box<dyn KeyedState>,
        scratch_reservation: HostMemoryReservation,
    ) -> Result<Self> {
        let root = decode_plan(serialized_plan)?
            .root
            .ok_or_else(|| DataFusionError::Plan("OVER aggregate plan has no root".to_string()))?;
        let plan = match root.operator {
            Some(proto::operator::Operator::OverAggregate(plan)) => *plan,
            _ => {
                return Err(DataFusionError::Plan(
                    "stateful OVER aggregate handle requires an OverAggregate root".to_string(),
                ));
            }
        };
        validate_plan(&plan, max_parallelism)?;
        let calls = plan
            .aggregate_calls
            .iter()
            .map(lower_call)
            .collect::<Result<Vec<_>>>()?;
        let visible_schema =
            arrow_schema(plan.input_schema.as_ref().expect("validated input schema"))?;
        let output_visible = arrow_schema(
            plan.output_schema
                .as_ref()
                .expect("validated output schema"),
        )?;
        let payload_converter = row_converter(&visible_schema, None)?;
        let mut output_fields = output_visible.fields().iter().cloned().collect::<Vec<_>>();
        output_fields.push(Arc::new(Field::new(
            "__streamfusion_row_kind",
            DataType::Int8,
            false,
        )));
        output_fields.push(Arc::new(Field::new(
            "__streamfusion_input_ordinal",
            DataType::Int32,
            false,
        )));
        Ok(Self {
            plan,
            calls,
            max_parallelism,
            state,
            input_schema: None,
            visible_schema,
            output_schema: Arc::new(Schema::new(output_fields)),
            payload_converter,
            order_converter: None,
            key_fields: Vec::new(),
            preencoded_key_index: None,
            input_kind_index: None,
            scratch_reservation,
            state_read_batches: 0,
            state_write_batches: 0,
            missing_ids: 0,
            missing_sort_keys: 0,
        })
    }

    pub(crate) fn process_arrow(&mut self, batch: RecordBatch) -> Result<RecordBatch> {
        let base = batch
            .num_rows()
            .saturating_mul(256usize.saturating_add(self.calls.len().saturating_mul(96)));
        self.scratch_reservation.resize(base)?;
        match self.process_accounted(batch) {
            Ok(output) => {
                let bytes = output.get_array_memory_size();
                self.scratch_reservation.resize(base.max(bytes))?;
                self.scratch_reservation.transfer_to_arrow(bytes)?;
                self.scratch_reservation.resize(0)?;
                Ok(output)
            }
            Err(error) => {
                self.scratch_reservation.resize(0)?;
                Err(error)
            }
        }
    }

    fn process_accounted(&mut self, batch: RecordBatch) -> Result<RecordBatch> {
        self.prepare_schema(batch.schema())?;
        let visible_count = self.visible_schema.fields().len();
        let visible_columns = batch.columns()[..visible_count].to_vec();
        let payload_rows = self.payload_converter.convert_columns(&visible_columns)?;
        let order_rows = self
            .order_converter
            .as_mut()
            .expect("schema prepared")
            .convert_columns(&[batch.column(self.plan.order_key_index as usize).clone()])?;

        let mut unique = HashMap::<StateKey, usize, RandomState>::with_capacity_and_hasher(
            batch.num_rows(),
            RandomState::new(),
        );
        let mut row_state_indices = Vec::with_capacity(batch.num_rows());
        for row in 0..batch.num_rows() {
            let key = self.state_key(&batch, row)?;
            let next = unique.len();
            row_state_indices.push(*unique.entry(key).or_insert(next));
        }
        let mut ordered_keys = vec![None; unique.len()];
        for (key, index) in unique.drain() {
            ordered_keys[index] = Some(key);
        }
        let state_keys = ordered_keys
            .into_iter()
            .map(|key| key.expect("each OVER partition index is populated"))
            .collect::<Vec<_>>();
        let refs = state_keys
            .iter()
            .map(|key| StateKeyRef {
                key_group: key.key_group,
                key: &key.key,
            })
            .collect::<Vec<_>>();
        let existing = self.state.get_batch(&refs)?;
        self.state_read_batches = self.state_read_batches.saturating_add(1);
        let mut states = existing
            .iter()
            .map(|value| {
                value
                    .as_deref()
                    .map(|bytes| decode_state(bytes, self.calls.len()))
                    .transpose()
                    .map(Option::unwrap_or_default)
            })
            .collect::<Result<Vec<_>>>()?;
        let mut touched = vec![false; states.len()];
        let mut events = Vec::new();

        for row in 0..batch.num_rows() {
            let state = &mut states[row_state_indices[row]];
            let payload = payload_rows.row(row).as_ref().to_vec();
            let order = order_rows.row(row).as_ref().to_vec();
            let contributions = row_aggregate_values(&self.calls, &batch, row)?;
            let kind = self.input_kind(&batch, row)?;
            let ordinal = i32::try_from(row).map_err(|_| {
                DataFusionError::Execution("OVER aggregate batch exceeds i32 rows".to_string())
            })?;
            match kind {
                INSERT | UPDATE_AFTER => {
                    let id = state.next_id;
                    state.next_id = state.next_id.wrapping_add(1);
                    let rows = state.rows.entry(order.clone()).or_default();
                    let start_index = rows.len();
                    rows.push(StoredRow {
                        id,
                        payload,
                        contributions,
                        output: Vec::new(),
                    });
                    let changes = recompute_from(
                        state,
                        &self.calls,
                        self.plan.rows_frame,
                        &order,
                        start_index,
                    )?;
                    emit_insert_and_changes(&mut events, changes, id, kind, ordinal);
                }
                UPDATE_BEFORE | DELETE => {
                    let order_exists = state.rows.contains_key(order.as_slice());
                    let removed = remove_row(state, &order, &payload);
                    if let Some((removed, start_index)) = removed {
                        events.push(OutputEvent {
                            payload: removed.payload,
                            values: removed.output,
                            // Flink's non-time OVER functions canonicalize both DELETE and
                            // UPDATE_BEFORE inputs to a DELETE for the row being removed. The
                            // affected suffix is still emitted as UPDATE_BEFORE/UPDATE_AFTER.
                            kind: DELETE,
                            input_ordinal: ordinal,
                        });
                        let changes = recompute_from(
                            state,
                            &self.calls,
                            self.plan.rows_frame,
                            &order,
                            start_index,
                        )?;
                        emit_existing_changes(&mut events, changes, None, ordinal);
                    } else {
                        if order_exists {
                            self.missing_ids = self.missing_ids.saturating_add(1);
                        } else {
                            self.missing_sort_keys = self.missing_sort_keys.saturating_add(1);
                        }
                    }
                }
                other => {
                    return Err(DataFusionError::Execution(format!(
                        "unknown Flink RowKind byte {other}"
                    )));
                }
            }
            touched[row_state_indices[row]] = true;
        }

        let mutations = state_keys
            .into_iter()
            .zip(states.into_iter().zip(touched))
            .filter_map(|(key, (state, touched))| {
                touched.then(|| StateMutation {
                    key,
                    value: (!state.rows.is_empty()).then(|| encode_state(&state)),
                })
            })
            .collect::<Vec<_>>();
        if !mutations.is_empty() {
            self.state.write_batch(mutations)?;
            self.state_write_batches = self.state_write_batches.saturating_add(1);
        }
        self.output_batch(events)
    }

    fn prepare_schema(&mut self, schema: SchemaRef) -> Result<()> {
        if let Some(expected) = &self.input_schema {
            if expected.as_ref() != schema.as_ref() {
                return Err(DataFusionError::Execution(
                    "OVER aggregate input schema changed while running".to_string(),
                ));
            }
            return Ok(());
        }
        self.preencoded_key_index = metadata_index(&schema, "__streamfusion_key");
        self.input_kind_index = metadata_index(&schema, "__streamfusion_input_row_kind");
        let visible_count = self.visible_schema.fields().len();
        if schema.fields().len() < visible_count
            || schema.fields()[..visible_count]
                .iter()
                .zip(self.visible_schema.fields())
                .any(|(actual, planned)| actual.data_type() != planned.data_type())
        {
            return Err(DataFusionError::Execution(
                "OVER aggregate visible input schema differs from its plan".to_string(),
            ));
        }
        if self.plan.input_changelog && self.input_kind_index.is_none() {
            return Err(DataFusionError::Execution(
                "changelog OVER aggregate requires RowKind metadata".to_string(),
            ));
        }
        if self.preencoded_key_index.is_none() {
            self.key_fields = self
                .plan
                .partition_key_indices
                .iter()
                .map(|&index| {
                    let index = index as usize;
                    let field = self.visible_schema.fields().get(index).ok_or_else(|| {
                        DataFusionError::Plan(format!(
                            "OVER partition index {index} is outside {visible_count} fields"
                        ))
                    })?;
                    Ok((index, KeyField::from_arrow_type(field.data_type())?))
                })
                .collect::<std::result::Result<Vec<_>, arrow::error::ArrowError>>()?;
        }
        let order_field = self
            .visible_schema
            .fields()
            .get(self.plan.order_key_index as usize)
            .ok_or_else(|| {
                DataFusionError::Plan("OVER order index is outside input".to_string())
            })?;
        self.order_converter = Some(row_converter(
            &Arc::new(Schema::new(vec![order_field.clone()])),
            Some(SortOptions {
                descending: !self.plan.sort_ascending,
                nulls_first: !self.plan.sort_nulls_last,
            }),
        )?);
        self.input_schema = Some(schema);
        Ok(())
    }

    fn state_key(&self, batch: &RecordBatch, row: usize) -> Result<StateKey> {
        let key = match self.preencoded_key_index {
            Some(index) => batch
                .column(index)
                .as_any()
                .downcast_ref::<BinaryArray>()
                .ok_or_else(|| {
                    DataFusionError::Execution(
                        "OVER preencoded key column is not Arrow Binary".to_string(),
                    )
                })?
                .value(row)
                .to_vec(),
            None => encode_binary_row(batch, row, &self.key_fields)?,
        };
        let mut state_key = Vec::with_capacity(key.len() + 1);
        state_key.push(OVER_STATE_PREFIX);
        state_key.extend_from_slice(&key);
        Ok(StateKey {
            key_group: assign_key_group(&key, self.max_parallelism),
            key: state_key,
        })
    }

    fn input_kind(&self, batch: &RecordBatch, row: usize) -> Result<i8> {
        if !self.plan.input_changelog {
            return Ok(INSERT);
        }
        Ok(batch
            .column(self.input_kind_index.expect("schema prepared"))
            .as_any()
            .downcast_ref::<Int8Array>()
            .ok_or_else(|| {
                DataFusionError::Execution("OVER RowKind metadata is not Int8".to_string())
            })?
            .value(row))
    }

    fn output_batch(&self, events: Vec<OutputEvent>) -> Result<RecordBatch> {
        if events.is_empty() {
            return Ok(RecordBatch::new_empty(self.output_schema.clone()));
        }
        let parser = self.payload_converter.parser();
        let mut columns = self.payload_converter.convert_rows(
            events
                .iter()
                .map(|event| parser.parse(event.payload.as_slice())),
        )?;
        for (call_index, call) in self.calls.iter().enumerate() {
            let values = events
                .iter()
                .map(|event| event.values[call_index].clone())
                .collect::<Vec<_>>();
            columns.push(aggregate_array(&values, &call.output_type)?);
        }
        columns.push(Arc::new(Int8Array::from(
            events.iter().map(|event| event.kind).collect::<Vec<_>>(),
        )) as ArrayRef);
        columns.push(Arc::new(Int32Array::from(
            events
                .iter()
                .map(|event| event.input_ordinal)
                .collect::<Vec<_>>(),
        )) as ArrayRef);
        Ok(RecordBatch::try_new(self.output_schema.clone(), columns)?)
    }

    pub(crate) fn statistics(&self) -> [u64; 4] {
        [
            self.state_read_batches,
            self.state_write_batches,
            self.missing_ids,
            self.missing_sort_keys,
        ]
    }

    pub(crate) fn snapshot_key_group(&self, key_group: u32) -> Result<Vec<u8>> {
        self.state.snapshot_key_group(key_group)
    }

    pub(crate) fn restore_key_group(&mut self, key_group: u32, bytes: &[u8]) -> Result<()> {
        self.state.restore_key_group(key_group, bytes)
    }

    pub(crate) fn checkpoint(&self, directory: &std::path::Path) -> Result<()> {
        self.state.checkpoint(directory)
    }
}

#[derive(Clone)]
struct ChangedRow {
    id: i64,
    payload: Vec<u8>,
    old: Vec<Option<AggregateValue>>,
    new: Vec<Option<AggregateValue>>,
}

fn recompute(state: &mut OverState, calls: &[Call], rows_frame: bool) -> Result<Vec<ChangedRow>> {
    let mut accumulator = AccumulatorState::new(calls);
    let mut changed = Vec::new();
    for rows in state.rows.values_mut() {
        if rows_frame {
            for row in rows {
                accumulator.apply_values(calls, &row.contributions, true)?;
                let new = accumulator.values(calls);
                if row.output != new {
                    changed.push(ChangedRow {
                        id: row.id,
                        payload: row.payload.clone(),
                        old: std::mem::replace(&mut row.output, new.clone()),
                        new,
                    });
                }
            }
        } else {
            for row in rows.iter() {
                accumulator.apply_values(calls, &row.contributions, true)?;
            }
            let new = accumulator.values(calls);
            for row in rows {
                if row.output != new {
                    changed.push(ChangedRow {
                        id: row.id,
                        payload: row.payload.clone(),
                        old: std::mem::replace(&mut row.output, new.clone()),
                        new: new.clone(),
                    });
                }
            }
        }
    }
    Ok(changed)
}

fn recompute_from(
    state: &mut OverState,
    calls: &[Call],
    rows_frame: bool,
    start_order: &[u8],
    start_index: usize,
) -> Result<Vec<ChangedRow>> {
    let prefix = if rows_frame && start_index > 0 {
        state
            .rows
            .get(start_order)
            .and_then(|rows| rows.get(start_index - 1))
    } else {
        state
            .rows
            .range(..start_order.to_vec())
            .next_back()
            .and_then(|(_, rows)| rows.last())
    };
    let Some(prefix) = prefix else {
        return recompute(state, calls, rows_frame);
    };
    if prefix.output.len() != calls.len() {
        return recompute(state, calls, rows_frame);
    }
    let mut accumulator = AccumulatorState::from_prefix_values(calls, &prefix.output)?;
    let mut changed = Vec::new();
    let mut first_order = true;
    for rows in state
        .rows
        .range_mut(start_order.to_vec()..)
        .map(|(_, rows)| rows)
    {
        if rows_frame {
            let first_row = if first_order {
                start_index.min(rows.len())
            } else {
                0
            };
            for row in rows.iter_mut().skip(first_row) {
                accumulator.apply_values(calls, &row.contributions, true)?;
                update_row_if_changed(row, accumulator.values(calls), &mut changed);
            }
        } else {
            for row in rows.iter() {
                accumulator.apply_values(calls, &row.contributions, true)?;
            }
            let new = accumulator.values(calls);
            for row in rows {
                update_row_if_changed(row, new.clone(), &mut changed);
            }
        }
        first_order = false;
    }
    Ok(changed)
}

fn update_row_if_changed(
    row: &mut StoredRow,
    new: Vec<Option<AggregateValue>>,
    changed: &mut Vec<ChangedRow>,
) {
    if row.output != new {
        changed.push(ChangedRow {
            id: row.id,
            payload: row.payload.clone(),
            old: std::mem::replace(&mut row.output, new.clone()),
            new,
        });
    }
}

fn emit_insert_and_changes(
    output: &mut Vec<OutputEvent>,
    changes: Vec<ChangedRow>,
    inserted_id: i64,
    input_kind: i8,
    ordinal: i32,
) {
    for change in changes {
        if change.id == inserted_id {
            output.push(OutputEvent {
                payload: change.payload,
                values: change.new,
                kind: input_kind,
                input_ordinal: ordinal,
            });
        } else if !change.old.is_empty() {
            output.push(OutputEvent {
                payload: change.payload.clone(),
                values: change.old,
                kind: UPDATE_BEFORE,
                input_ordinal: ordinal,
            });
            output.push(OutputEvent {
                payload: change.payload,
                values: change.new,
                kind: UPDATE_AFTER,
                input_ordinal: ordinal,
            });
        }
    }
}

fn emit_existing_changes(
    output: &mut Vec<OutputEvent>,
    changes: Vec<ChangedRow>,
    ignored_id: Option<i64>,
    ordinal: i32,
) {
    for change in changes {
        if Some(change.id) == ignored_id || change.old.is_empty() {
            continue;
        }
        output.push(OutputEvent {
            payload: change.payload.clone(),
            values: change.old,
            kind: UPDATE_BEFORE,
            input_ordinal: ordinal,
        });
        output.push(OutputEvent {
            payload: change.payload,
            values: change.new,
            kind: UPDATE_AFTER,
            input_ordinal: ordinal,
        });
    }
}

fn remove_row(state: &mut OverState, order: &[u8], payload: &[u8]) -> Option<(StoredRow, usize)> {
    let rows = state.rows.get_mut(order)?;
    let index = rows.iter().position(|row| row.payload == payload)?;
    let removed = rows.remove(index);
    if rows.is_empty() {
        state.rows.remove(order);
    }
    Some((removed, index))
}

fn validate_plan(plan: &proto::OverAggregate, max_parallelism: u32) -> Result<()> {
    let time = proto::OverTimeAttribute::try_from(plan.time_attribute).ok();
    if max_parallelism == 0 {
        return Err(DataFusionError::Plan(
            "OVER aggregate max parallelism must be positive".to_string(),
        ));
    }
    if !plan.sort_ascending
        || time != Some(proto::OverTimeAttribute::NonTime)
        || plan.preceding_offset.is_some()
        || plan.state_ttl_millis != 0
    {
        return Err(DataFusionError::Plan(
            "initial native OVER aggregate requires an ascending non-time unbounded-preceding frame"
                .to_string(),
        ));
    }
    if plan.aggregate_calls.is_empty() {
        return Err(DataFusionError::Plan(
            "OVER aggregate requires at least one aggregate call".to_string(),
        ));
    }
    let input = plan
        .input_schema
        .as_ref()
        .ok_or_else(|| DataFusionError::Plan("OVER aggregate has no input schema".to_string()))?;
    let output = plan
        .output_schema
        .as_ref()
        .ok_or_else(|| DataFusionError::Plan("OVER aggregate has no output schema".to_string()))?;
    let input_fields = input.fields.len();
    if plan.order_key_index as usize >= input_fields {
        return Err(DataFusionError::Plan(format!(
            "OVER order index {} is outside {input_fields} input fields",
            plan.order_key_index
        )));
    }
    if let Some(index) = plan
        .partition_key_indices
        .iter()
        .find(|&&index| index as usize >= input_fields)
    {
        return Err(DataFusionError::Plan(format!(
            "OVER partition index {index} is outside {input_fields} input fields"
        )));
    }
    let expected_output = input_fields + plan.aggregate_calls.len();
    if output.fields.len() != expected_output {
        return Err(DataFusionError::Plan(format!(
            "OVER output has {} fields, expected {expected_output}",
            output.fields.len()
        )));
    }
    Ok(())
}

fn metadata_index(schema: &SchemaRef, name: &str) -> Option<usize> {
    schema
        .fields()
        .iter()
        .position(|field| field.name() == name)
}

fn row_converter(schema: &SchemaRef, options: Option<SortOptions>) -> Result<RowConverter> {
    Ok(RowConverter::new(
        schema
            .fields()
            .iter()
            .map(|field| match options {
                Some(options) => SortField::new_with_options(field.data_type().clone(), options),
                None => SortField::new(field.data_type().clone()),
            })
            .collect(),
    )?)
}
