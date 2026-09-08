// Copyright 2026 StreamFusion Authors
// Licensed under the Apache License, Version 2.0

use std::sync::Arc;

use ahash::RandomState;
use arrow::array::{Array, ArrayRef, BinaryArray, Int64Array, Int8Array};
use arrow::datatypes::{DataType, SchemaRef};
use arrow::record_batch::RecordBatch;
use arrow::row::{RowConverter, SortField};
use chrono_tz::Tz;
use datafusion::error::{DataFusionError, Result};
use hashbrown::HashMap;

use super::group_aggregate::{encode_state, lower_call, AccumulatorState, Call};
use super::window_table_function::{timestamp_millis, to_window_time, window_start};
use crate::memory_pool::HostMemoryReservation;
use crate::proto;

mod admission;
// Tested prerequisite for the shared Flink buffer/control lifecycle; the legacy kernel stays
// gated and does not yet use this capacity model.
#[cfg(test)]
mod buffer_layout;
#[cfg(test)]
mod buffered;
mod planning;

const INSERT: i8 = 0;
const UPDATE_BEFORE: i8 = 1;
const UPDATE_AFTER: i8 = 2;
const DELETE: i8 = 3;

/// State-free local half of Flink's two-stage slicing window aggregate.
///
/// Every raw row contributes to exactly one base slice. The opaque partial is expanded to the
/// corresponding logical TUMBLE/HOP/CUMULATE windows only after the normal keyed exchange, which
/// keeps the local CPU and network work proportional to the input rather than to overlapping
/// logical windows.
pub(crate) struct LocalWindowAggregateProcessor {
    plan: proto::LocalWindowAggregate,
    calls: Vec<Call>,
    grouped_compute: Option<super::group_aggregate::grouped_compute::GroupedCompute>,
    input_schema: SchemaRef,
    output_schema: SchemaRef,
    grouping_converter: RowConverter,
    shift_time_zone: Tz,
    reservation: HostMemoryReservation,
    output_reservation: HostMemoryReservation,
    _plan_reservation: HostMemoryReservation,
    _schema_reservation: HostMemoryReservation,
}

#[derive(Clone, Hash, PartialEq, Eq)]
struct SliceKey {
    grouping_row: Vec<u8>,
    window_start: i64,
    slice_end: i64,
}

impl LocalWindowAggregateProcessor {
    pub(crate) fn process_arrow(&mut self, batch: RecordBatch) -> Result<RecordBatch> {
        self.validate_batch(&batch)?;
        self.reservation.resize(self.batch_admission(&batch)?)?;
        let result = self.process_accounted(&batch);
        self.finish_legacy_output(result)
    }

    fn process_accounted(&mut self, batch: &RecordBatch) -> Result<RecordBatch> {
        let grouping_rows = self.grouping_rows(batch)?;
        let mut pending = HashMap::<SliceKey, Vec<usize>, RandomState>::with_capacity_and_hasher(
            batch.num_rows(),
            RandomState::new(),
        );
        for (row, grouping_row) in grouping_rows.into_iter().enumerate() {
            let Some((slice_start, slice_end)) = self.slice_bounds(batch, row)? else {
                continue;
            };
            let key = SliceKey {
                grouping_row,
                window_start: slice_start,
                slice_end,
            };
            pending.entry(key).or_default().push(row);
        }
        if !self.plan.input_changelog {
            if let Some(compute) = self.grouped_compute.as_mut() {
                if pending.is_empty() {
                    return Ok(RecordBatch::new_empty(self.output_schema.clone()));
                }
                let mut indices = vec![0; batch.num_rows()];
                let mut valid = vec![false; batch.num_rows()];
                let mut keys = Vec::with_capacity(pending.len());
                for (key, rows) in pending {
                    let index = keys.len();
                    for row in rows {
                        indices[row] = index;
                        valid[row] = true;
                    }
                    keys.push((index, key));
                }
                let valid = arrow::array::BooleanArray::from(valid);
                compute.update(&self.calls, batch, &indices, Some(&valid), keys.len())?;
                let output = compute.finish()?;
                keys.sort_unstable_by(|(_, left), (_, right)| slice_order(left, right));
                return self.output_partials(keys.into_iter().map(|(index, key)| {
                    output.state(&self.calls, index).map(|state| (key, state))
                }));
            }
        }
        let kernels = super::group_aggregate::datafusion_compute::Kernels::new(&self.calls)?;
        let accumulate = (0..batch.num_rows())
            .map(|row| self.accumulates(batch, row))
            .collect::<Result<Vec<_>>>()?;
        let row_inputs = accumulate
            .contains(&false)
            .then(|| super::group_aggregate::datafusion_rows::RowInputs::new(&self.calls, batch))
            .transpose()?;
        let mut entries = Vec::with_capacity(pending.len());
        for (key, rows) in pending {
            let mut accumulator = AccumulatorState::new(&self.calls);
            accumulator.apply_input_rows(
                &self.calls,
                &kernels,
                batch,
                &rows,
                &accumulate,
                row_inputs.as_ref(),
                false,
            )?;
            if accumulator.has_delta() {
                entries.push((key, accumulator));
            }
        }
        entries.sort_unstable_by(|(left, _), (right, _)| slice_order(left, right));
        self.output_partials(entries.into_iter().map(Ok))
    }

    fn slice_bounds(&self, batch: &RecordBatch, row: usize) -> Result<Option<(i64, i64)>> {
        let attached_columns = self
            .plan
            .attached_window_start_index
            .zip(self.plan.attached_window_end_index)
            .map(|(start, end)| (batch.column(start as usize), batch.column(end as usize)));
        let timestamp_column = attached_columns
            .is_none()
            .then(|| batch.column(self.plan.time_attribute_index as usize));
        let slice_size = match proto::WindowKind::try_from(self.plan.kind) {
            Ok(proto::WindowKind::Tumble) => self.plan.size_millis,
            Ok(proto::WindowKind::Hop | proto::WindowKind::Cumulate) => {
                self.plan.slide_or_step_millis
            }
            _ => unreachable!("validated local window kind"),
        };
        let bounds = if let Some((start_column, end_column)) = attached_columns {
            let Some(start) = timestamp_millis(start_column, row)? else {
                return Ok(None);
            };
            let Some(end) = timestamp_millis(end_column, row)? else {
                return Ok(None);
            };
            (start, end)
        } else {
            let Some(epoch_millis) = timestamp_millis(
                timestamp_column.expect("direct local window has a time column"),
                row,
            )?
            else {
                return Ok(None);
            };
            let timestamp = to_window_time(epoch_millis, self.shift_time_zone)?;
            let start = window_start(timestamp, self.plan.offset_millis, slice_size);
            (start, start.wrapping_add(slice_size))
        };
        Ok(Some(bounds))
    }

    fn output_partials(
        &self,
        entries: impl IntoIterator<Item = Result<(SliceKey, AccumulatorState)>>,
    ) -> Result<RecordBatch> {
        let entries = entries.into_iter();
        let capacity = entries.size_hint().0;
        let mut grouping = Vec::with_capacity(capacity);
        let mut accumulators = Vec::with_capacity(capacity);
        let mut window_starts = Vec::with_capacity(capacity);
        let mut slice_ends = Vec::with_capacity(capacity);
        for entry in entries {
            let (key, accumulator) = entry?;
            grouping.push(key.grouping_row);
            accumulators.push(encode_state(&accumulator));
            window_starts.push(key.window_start);
            slice_ends.push(key.slice_end);
        }
        let mut columns = if self.plan.grouping_indices.is_empty() {
            Vec::new()
        } else {
            let parser = self.grouping_converter.parser();
            self.grouping_converter
                .convert_rows(grouping.iter().map(|row| parser.parse(row)))?
        };
        columns.push(Arc::new(BinaryArray::from_iter_values(accumulators)) as ArrayRef);
        columns.push(Arc::new(Int64Array::from(window_starts)) as ArrayRef);
        columns.push(Arc::new(Int64Array::from(slice_ends)) as ArrayRef);
        Ok(RecordBatch::try_new(
            Arc::clone(&self.output_schema),
            columns,
        )?)
    }

    fn grouping_rows(&self, batch: &RecordBatch) -> Result<Vec<Vec<u8>>> {
        if self.plan.grouping_indices.is_empty() {
            return Ok((0..batch.num_rows()).map(|_| Vec::new()).collect());
        }
        let columns = self
            .plan
            .grouping_indices
            .iter()
            .map(|&index| Arc::clone(batch.column(index as usize)))
            .collect::<Vec<_>>();
        let rows = self.grouping_converter.convert_columns(&columns)?;
        Ok((0..batch.num_rows())
            .map(|row| rows.row(row).as_ref().to_vec())
            .collect())
    }

    fn accumulates(&self, batch: &RecordBatch, row: usize) -> Result<bool> {
        if !self.plan.input_changelog {
            return Ok(true);
        }
        let kinds = batch
            .column(self.input_schema.fields().len())
            .as_any()
            .downcast_ref::<Int8Array>()
            .expect("validated changelog metadata");
        if kinds.is_null(row) {
            return Err(DataFusionError::Execution(
                "local window RowKind must not be null".into(),
            ));
        }
        match kinds.value(row) {
            INSERT | UPDATE_AFTER => Ok(true),
            UPDATE_BEFORE | DELETE => Ok(false),
            value => Err(DataFusionError::Execution(format!(
                "unknown local window RowKind byte {value}"
            ))),
        }
    }

    fn validate_batch(&self, batch: &RecordBatch) -> Result<()> {
        let expected = self.input_schema.fields().len() + usize::from(self.plan.input_changelog);
        if batch.num_columns() != expected {
            return Err(DataFusionError::Plan(format!(
                "local window aggregate expected {expected} columns, got {}",
                batch.num_columns()
            )));
        }
        for (index, planned) in self.input_schema.fields().iter().enumerate() {
            if !planned
                .data_type()
                .equals_datatype(batch.schema().field(index).data_type())
            {
                return Err(DataFusionError::Plan(format!(
                    "local window input {index} expected {}, got {}",
                    planned.data_type(),
                    batch.schema().field(index).data_type()
                )));
            }
        }
        if self.plan.input_changelog
            && batch
                .column(self.input_schema.fields().len())
                .as_any()
                .downcast_ref::<Int8Array>()
                .is_none()
        {
            return Err(DataFusionError::Plan(
                "local window changelog metadata must be Int8".to_string(),
            ));
        }
        Ok(())
    }
}

fn slice_order(left: &SliceKey, right: &SliceKey) -> std::cmp::Ordering {
    left.slice_end
        .cmp(&right.slice_end)
        .then_with(|| left.window_start.cmp(&right.window_start))
        .then_with(|| left.grouping_row.cmp(&right.grouping_row))
}

#[cfg(test)]
mod tests;
