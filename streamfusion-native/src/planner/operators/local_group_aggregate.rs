// Copyright 2026 StreamFusion Authors
// Licensed under the Apache License, Version 2.0

use std::sync::Arc;

use ahash::RandomState;
use arrow::array::{Array, ArrayRef, BinaryArray, Int8Array};
use arrow::datatypes::SchemaRef;
use arrow::record_batch::RecordBatch;
use arrow::row::{RowConverter, Rows, SortField};
use datafusion::error::{DataFusionError, Result};
use hashbrown::HashMap;
use prost::Message;

use super::group_aggregate::{
    encode_state, lower_call, sort_flink_hashmap_keys, AccumulatorState, Call,
};
use crate::exchange::{encode_binary_row, KeyField};
use crate::memory_pool::HostMemoryReservation;
use crate::planner::persistent::unary::InvocationState;
use crate::proto;

mod admission;
mod control;
pub(crate) mod execution_plan;
mod planning;

/// Flink-compatible local bundle aggregation with no persistent keyed state.
///
/// The output is intentionally a native internal contract: grouping columns followed by one
/// canonical opaque accumulator. The paired native global stage consumes it after the normal
/// Flink-owned exchange, avoiding Java interpretation of aggregate state and retaining one Arrow
/// payload throughout the accelerated portion of each task.
pub(crate) struct LocalGroupAggregateProcessor {
    plan: proto::LocalGroupAggregate,
    calls: Vec<Call>,
    input_schema: SchemaRef,
    output_schema: SchemaRef,
    grouping_converter: RowConverter,
    key_fields: Vec<(usize, KeyField)>,
    pending: HashMap<Vec<u8>, PendingLocal, RandomState>,
    pending_order: Vec<Vec<u8>>,
    pending_elements: usize,
    invocation: InvocationState,
    native_output_schema: Option<SchemaRef>,
    control_flushing: bool,
    pending_reservation: HostMemoryReservation,
    output_reservation: HostMemoryReservation,
    workspace: HostMemoryReservation,
    _plan_reservation: HostMemoryReservation,
    _schema_reservation: HostMemoryReservation,
}

struct PendingLocal {
    grouping_row: Vec<u8>,
    accumulator: AccumulatorState,
}

impl LocalGroupAggregateProcessor {
    pub(crate) fn process_arrow(&mut self, batch: RecordBatch) -> Result<RecordBatch> {
        self.invocation.require_idle("local aggregate")?;
        self.validate_batch(&batch)?;
        self.workspace.resize(self.batch_admission(&batch)?)?;
        let result = self.process_accounted(batch);
        self.finish_legacy_output(result)
    }

    fn process_accounted(&mut self, batch: RecordBatch) -> Result<RecordBatch> {
        let grouping_rows = self.encode_grouping_rows(&batch)?;
        let mut output_keys = Vec::new();
        let mut output_accumulators = Vec::new();
        let trigger = if self.plan.bounded_batch {
            batch.num_rows().max(1)
        } else {
            self.plan.mini_batch_size as usize
        };
        let mut offset = 0;
        while offset < batch.num_rows() {
            let length = (trigger - self.pending_elements).min(batch.num_rows() - offset);
            for row in offset..offset + length {
                let key = self.key(&batch, row)?;
                let accumulate = self.accumulates(&batch, row)?;
                let pending = match self.pending.entry(key) {
                    hashbrown::hash_map::Entry::Occupied(entry) => entry.into_mut(),
                    hashbrown::hash_map::Entry::Vacant(entry) => {
                        self.pending_order.push(entry.key().clone());
                        entry.insert(PendingLocal {
                            grouping_row: grouping_rows
                                .as_ref()
                                .map_or_else(Vec::new, |rows| rows.row(row).as_ref().to_vec()),
                            accumulator: AccumulatorState::new(&self.calls),
                        })
                    }
                };
                pending
                    .accumulator
                    .apply(&self.calls, &batch, row, accumulate)?;
            }
            self.pending_elements += length;
            offset += length;
            if self.pending_elements == trigger {
                self.drain_pending(&mut output_keys, &mut output_accumulators);
            }
        }
        self.resize_reservation()?;
        self.output_batch(output_keys, output_accumulators)
    }

    pub(crate) fn finish_bundle(&mut self) -> Result<RecordBatch> {
        self.invocation.require_idle("local aggregate")?;
        self.workspace.resize(self.flush_admission()?)?;
        let result = self.finish_accounted();
        self.finish_legacy_output(result)
    }

    fn finish_accounted(&mut self) -> Result<RecordBatch> {
        let mut output_keys = Vec::new();
        let mut output_accumulators = Vec::new();
        self.drain_pending(&mut output_keys, &mut output_accumulators);
        self.resize_reservation()?;
        self.output_batch(output_keys, output_accumulators)
    }

    pub(crate) fn pending_element_count(&self) -> usize {
        self.pending_elements
    }

    pub(crate) fn pending_key_count(&self) -> usize {
        self.pending.len()
    }

    fn validate_batch(&self, batch: &RecordBatch) -> Result<()> {
        let visible = self.input_schema.fields().len();
        let preencoded = batch
            .schema()
            .fields()
            .iter()
            .position(|field| field.name() == "__streamfusion_key");
        let expected =
            visible + usize::from(self.plan.input_changelog) + usize::from(preencoded.is_some());
        if batch.num_columns() != expected {
            return Err(DataFusionError::Plan(format!(
                "local group aggregate expected {expected} input columns but received {}",
                batch.num_columns()
            )));
        }
        let batch_schema = batch.schema();
        for (index, expected) in self.input_schema.fields().iter().enumerate() {
            let actual = batch_schema.field(index);
            if actual.data_type() != expected.data_type() {
                return Err(DataFusionError::Plan(format!(
                    "local group aggregate input field {index} has type {} instead of {}",
                    actual.data_type(),
                    expected.data_type()
                )));
            }
        }
        if self.plan.input_changelog {
            let kinds = batch
                .column(visible)
                .as_any()
                .downcast_ref::<Int8Array>()
                .ok_or_else(|| {
                    DataFusionError::Plan(
                        "local group aggregate changelog metadata must be Int8".into(),
                    )
                })?;
            if kinds.null_count() != 0 || kinds.values().iter().any(|kind| !(0..=3).contains(kind))
            {
                return Err(DataFusionError::Execution(
                    "local group aggregate input has null or invalid RowKind".into(),
                ));
            }
        }
        if let Some(index) = preencoded {
            let keys = batch
                .column(index)
                .as_any()
                .downcast_ref::<BinaryArray>()
                .ok_or_else(|| {
                    DataFusionError::Plan(
                        "local group aggregate preencoded key metadata must be Binary".to_string(),
                    )
                })?;
            if keys.null_count() != 0 {
                return Err(DataFusionError::Execution(
                    "local group aggregate preencoded key may not be null".into(),
                ));
            }
        } else if self.key_fields.len() != self.plan.grouping_indices.len() {
            return Err(DataFusionError::Plan(
                "local group aggregate requires Flink BinaryRow key metadata for this grouping type"
                    .to_string(),
            ));
        }
        Ok(())
    }

    fn key(&self, batch: &RecordBatch, row: usize) -> Result<Vec<u8>> {
        if let Some(index) = batch
            .schema()
            .fields()
            .iter()
            .position(|field| field.name() == "__streamfusion_key")
        {
            let keys = batch
                .column(index)
                .as_any()
                .downcast_ref::<BinaryArray>()
                .expect("preencoded keys were validated");
            if keys.is_null(row) {
                return Err(DataFusionError::Execution(
                    "local group aggregate preencoded key may not be null".to_string(),
                ));
            }
            Ok(keys.value(row).to_vec())
        } else {
            Ok(encode_binary_row(batch, row, &self.key_fields)?)
        }
    }

    fn encode_grouping_rows(&self, batch: &RecordBatch) -> Result<Option<Rows>> {
        if self.plan.grouping_indices.is_empty() {
            return Ok(None);
        }
        let columns = self
            .plan
            .grouping_indices
            .iter()
            .map(|&index| Arc::clone(batch.column(index as usize)))
            .collect::<Vec<_>>();
        Ok(Some(self.grouping_converter.convert_columns(&columns)?))
    }

    fn accumulates(&self, batch: &RecordBatch, row: usize) -> Result<bool> {
        if !self.plan.input_changelog {
            return Ok(true);
        }
        let kinds = batch
            .column(self.input_schema.fields().len())
            .as_any()
            .downcast_ref::<Int8Array>()
            .expect("changelog metadata was validated");
        if kinds.is_null(row) {
            return Err(DataFusionError::Execution(
                "local group aggregate row kind cannot be null".to_string(),
            ));
        }
        match kinds.value(row) {
            0 | 2 => Ok(true),
            1 | 3 => Ok(false),
            kind => Err(DataFusionError::Execution(format!(
                "unknown local group aggregate row kind {kind}"
            ))),
        }
    }

    fn drain_pending(&mut self, keys: &mut Vec<Vec<u8>>, accumulators: &mut Vec<Vec<u8>>) {
        let mut order = std::mem::take(&mut self.pending_order);
        sort_flink_hashmap_keys(&mut order, Vec::as_slice);
        for key in order {
            let pending = self
                .pending
                .remove(&key)
                .expect("local aggregate order and map remain synchronized");
            keys.push(pending.grouping_row);
            accumulators.push(encode_state(&pending.accumulator));
        }
        self.pending_elements = 0;
    }

    fn resize_reservation(&mut self) -> Result<()> {
        let bytes = self.pending_bytes();
        if bytes > self.pending_reservation.size() {
            self.pending_reservation
                .grow_from(&mut self.workspace, bytes - self.pending_reservation.size())
        } else {
            self.pending_reservation.resize(bytes)
        }
    }

    fn pending_bytes(&self) -> usize {
        // capacity() measures insertion capacity, which can shrink with tombstones even
        // when no backing storage is freed. Account the actual retained table allocation.
        let map_bytes = self.pending.allocation_size();
        let order_bytes = self
            .pending_order
            .capacity()
            .saturating_mul(std::mem::size_of::<Vec<u8>>());
        let dynamic = self.pending.iter().fold(0usize, |bytes, (key, pending)| {
            bytes
                .saturating_add(key.capacity().saturating_mul(2))
                .saturating_add(pending.grouping_row.capacity())
                .saturating_add(pending.accumulator.estimated_dynamic_bytes())
        });
        map_bytes
            .saturating_add(order_bytes)
            .saturating_add(dynamic)
    }

    fn output_batch(
        &mut self,
        keys: Vec<Vec<u8>>,
        accumulators: Vec<Vec<u8>>,
    ) -> Result<RecordBatch> {
        let mut columns = if self.plan.grouping_indices.is_empty() {
            Vec::new()
        } else {
            let parser = self.grouping_converter.parser();
            self.grouping_converter
                .convert_rows(keys.iter().map(|key| parser.parse(key)))?
        };
        columns.push(Arc::new(BinaryArray::from_iter_values(accumulators)) as ArrayRef);
        Ok(RecordBatch::try_new(
            Arc::clone(&self.output_schema),
            columns,
        )?)
    }
}

#[cfg(test)]
mod tests;
