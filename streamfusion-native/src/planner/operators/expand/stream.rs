// Copyright 2026 StreamFusion Authors
// Licensed under the Apache License, Version 2.0

use std::sync::Arc;

use arrow::array::{Array, ArrayRef, RecordBatch};
use arrow::compute::interleave;
use arrow::datatypes::SchemaRef;
use datafusion::error::{DataFusionError, Result};
use datafusion::execution::memory_pool::{MemoryPool, MemoryReservation};
use datafusion::physical_expr::PhysicalExpr;

use super::Envelope;

pub(super) const MAX_OUTPUT_ROWS: usize = 4096;

/// A cursor over (input row, projection), never a queue of materialized output batches.
/// Input slices and expression references retain Arrow/plan owners without copying payloads.
pub(super) struct ExpansionWork {
    batch: RecordBatch,
    projections: Arc<Vec<Vec<Arc<dyn PhysicalExpr>>>>,
    schema: SchemaRef,
    row: usize,
    projection: usize,
    reservation: MemoryReservation,
    registry: Option<Arc<crate::memory_pool::arrow_lease::Registry>>,
    pool: Arc<dyn MemoryPool>,
}

impl ExpansionWork {
    pub(super) fn new(
        batch: RecordBatch,
        projections: Arc<Vec<Vec<Arc<dyn PhysicalExpr>>>>,
        schema: SchemaRef,
        reservation: MemoryReservation,
        registry: Option<Arc<crate::memory_pool::arrow_lease::Registry>>,
        pool: Arc<dyn MemoryPool>,
    ) -> Self {
        Self {
            batch,
            projections,
            schema,
            row: 0,
            projection: 0,
            reservation,
            registry,
            pool,
        }
    }

    pub(super) fn next_batch(&mut self) -> Result<Option<RecordBatch>> {
        if self.row == self.batch.num_rows() {
            return Ok(None);
        }
        let projection_count = self.projections.len();
        // Usually process several complete input rows. Very large grouping-set lists instead
        // split a single input row across pulls, keeping the same row-major ordering.
        let selected_projections = (projection_count - self.projection).min(MAX_OUTPUT_ROWS);
        let rows = (MAX_OUTPUT_ROWS / selected_projections).min(self.batch.num_rows() - self.row);
        let rows = if selected_projections != projection_count {
            1
        } else {
            rows
        };
        let output_rows = rows * selected_projections;
        let width = self.schema.fields().len();
        let control_bytes = selected_projections
            .checked_mul(width)
            .and_then(|slots| slots.checked_mul(std::mem::size_of::<ArrayRef>() * 2))
            .and_then(|bytes| {
                bytes.checked_add(output_rows * std::mem::size_of::<(usize, usize)>())
            })
            .and_then(|bytes| bytes.checked_add(selected_projections * 128))
            .and_then(|bytes| bytes.checked_add(4096))
            .ok_or_else(|| {
                DataFusionError::ResourcesExhausted("Expand workspace size overflowed".into())
            })?;
        // Admit the selection vector and projection descriptors before allocating either.
        self.reservation.try_resize(control_bytes)?;
        let batch = self.batch.slice(self.row, rows);
        let envelope = Envelope::from_schema(batch.schema().as_ref())?;
        let mut evaluated = self.projections
            [self.projection..self.projection + selected_projections]
            .iter()
            .map(|projection| {
                let mut columns = projection
                    .iter()
                    .map(|expression| {
                        // The same projection-root materializer as Calc borrows cached literals,
                        // admits scalar broadcast before allocation, and forwards arrays unchanged.
                        super::managed_expression::evaluate_projection(
                            expression, &batch, &self.pool,
                        )?
                        .into_array(rows)
                    })
                    .collect::<Result<Vec<_>>>()?;
                columns.extend(
                    envelope
                        .indices()
                        .map(|index| Arc::clone(batch.column(index))),
                );
                Ok(columns)
            })
            .collect::<Result<Vec<Vec<ArrayRef>>>>()?;
        // Every evaluated row is selected once. Retained sliced buffers conservatively count
        // their complete allocation, not an average row width (which is unsafe under skew).
        // General expression evaluation above still needs its own pre-allocation audit.
        let evaluated_bytes = evaluated
            .iter()
            .flatten()
            .try_fold(0usize, |bytes, array| {
                bytes.checked_add(array.get_array_memory_size())
            })
            .ok_or_else(|| {
                DataFusionError::ResourcesExhausted(
                    "Expand evaluated arrays size overflowed".into(),
                )
            })?;
        let workspace_bytes = evaluated_bytes
            .checked_mul(if selected_projections == 1 { 1 } else { 4 })
            .and_then(|bytes| bytes.checked_add(control_bytes))
            .ok_or_else(|| {
                DataFusionError::ResourcesExhausted("Expand materialization size overflowed".into())
            })?;
        self.reservation.try_resize(workspace_bytes)?;
        let columns = if selected_projections == 1 {
            // A single projection is already in row-major order. Keep its buffers instead of
            // running an identity gather over every payload column.
            evaluated.pop().expect("one selected projection")
        } else {
            let indices = (0..rows)
                .flat_map(|row| (0..selected_projections).map(move |projection| (projection, row)))
                .collect::<Vec<_>>();
            (0..width)
                .map(|column| {
                    let values = evaluated
                        .iter()
                        .map(|projection| projection[column].as_ref() as &dyn Array)
                        .collect::<Vec<_>>();
                    interleave(&values, &indices).map_err(DataFusionError::from)
                })
                .collect::<Result<Vec<_>>>()?
        };
        let output = RecordBatch::try_new(Arc::clone(&self.schema), columns)?;
        drop(evaluated);
        drop(batch);
        self.reservation
            .try_resize(output.get_array_memory_size())?;
        let memory = self.reservation.split(output.get_array_memory_size());
        let output = crate::memory_pool::arrow_lease::datafusion_batch_registered(
            output,
            memory,
            self.registry.clone(),
        )?;
        self.projection += selected_projections;
        if self.projection == projection_count {
            self.row += rows;
            self.projection = 0;
        }
        Ok(Some(output))
    }
}

#[cfg(test)]
mod tests;
