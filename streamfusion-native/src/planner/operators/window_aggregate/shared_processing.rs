// Copyright 2026 StreamFusion Authors
// Licensed under the Apache License, Version 2.0.

//! One processing-time operator: Flink buffer progression, DataFusion grouped compute,
//! ordered slice state, and absolute processing-time timers. No intermediate JNI edge.
use super::shared_kernel::SharedWindowKernel;
use super::shared_slices::SharedSlices;
use super::*;
use crate::planner::operators::local_window_aggregate::{
    buffered::BufferedWindow, LocalWindowAggregateProcessor,
};
use crate::planner::persistent::control::ControlEvent;

mod planning;
#[cfg(test)]
mod tests;

pub(super) struct SharedProcessingWindows {
    buffer: BufferedWindow,
    slices: SharedSlices,
    input_schema: SchemaRef,
    grouping_indices: Vec<u32>,
    failed: bool,
}

impl SharedProcessingWindows {
    pub(super) fn new(
        kernel: WindowAggregateProcessor,
        capacity: usize,
        page: usize,
    ) -> Result<Self> {
        planning::validate(&kernel.plan)?;
        let input_schema =
            crate::planner::arrow_schema(kernel.plan.input_schema.as_ref().unwrap())?;
        let grouping_indices = kernel.plan.grouping_indices.clone();
        let local = planning::buffer_plan(&kernel.plan)?;
        let partial_schema = local.output_schema.as_ref().unwrap().clone();
        let reservation = kernel
            .scratch_reservation
            .sibling("processing-time DataFusion buffer");
        let mut plan_memory = reservation.sibling("processing-time buffer plan");
        plan_memory.resize(
            super::super::group_aggregate::schema_admission::planned_schemas(
                local.input_schema.as_ref(),
                local.output_schema.as_ref(),
                &local.aggregate_calls,
            )?
            .saturating_add(64 << 10),
        )?;
        let buffer = BufferedWindow::new_processing_time(
            LocalWindowAggregateProcessor::from_plan(local, reservation, plan_memory)?,
            capacity,
            page,
        )?;
        let slices = SharedSlices::new_processing_time(kernel, partial_schema)?;
        Ok(Self {
            buffer,
            slices,
            input_schema,
            grouping_indices,
            failed: false,
        })
    }

    pub(super) fn process(&mut self, batch: RecordBatch, clocks: Int64Array) -> Result<()> {
        self.require_healthy()?;
        // Reject malformed transport before registering any timer or retaining any input.
        if batch.schema() != self.input_schema
            || batch.num_rows() != clocks.len()
            || clocks.null_count() != 0
        {
            return Err(DataFusionError::Execution(
                "processing-time payload or clock differs from its planned input".into(),
            ));
        }
        let result = (|| {
            self.slices
                .register_processing_timers(&batch, &self.grouping_indices, &clocks)?;
            let first = self.buffer.push_processing_time(batch, clocks)?;
            self.publish(first)
        })();
        self.failed |= result.is_err();
        result
    }

    fn publish(&mut self, mut partial: Option<RecordBatch>) -> Result<()> {
        loop {
            if let Some(batch) = partial {
                // Each bounded flush batch reads its namespaces together, merges through
                // DataFusion, and writes dirty state together on either state backend.
                self.slices.process(&batch)?;
            }
            if !self.buffer.has_pending() {
                return Ok(());
            }
            partial = self.buffer.poll_pending()?;
        }
    }

    pub(super) fn control(&mut self, event: ControlEvent) -> Result<()> {
        self.require_healthy()?;
        let result = (|| {
            let first = self.buffer.control(event)?;
            self.publish(first)?;
            if let ControlEvent::Watermark(watermark) = event {
                self.slices.observe_processing_watermark(watermark)?;
            }
            Ok(())
        })();
        self.failed |= result.is_err();
        result
    }

    /// Invoke after buffer control, draining one bounded timer frontier at a time.
    /// Older/repeated callback deadlines still fire timers, even when they do not flush.
    pub(super) fn fire(&mut self, deadline: i64) -> Result<RecordBatch> {
        self.require_healthy()?;
        let result = self.slices.advance(deadline);
        self.failed |= result.is_err();
        result
    }

    pub(super) fn next_timer(&self) -> Option<i64> {
        self.slices.next_timer()
    }

    pub(super) fn require_healthy(&self) -> Result<()> {
        if self.failed {
            return Err(DataFusionError::Execution(
                "processing-time window failed; restore a new context".into(),
            ));
        }
        self.slices.require_healthy()
    }

    pub(super) fn snapshot(&mut self, group: u32) -> Result<crate::state::SnapshotBytes> {
        self.require_healthy()?;
        if self.buffer.has_buffered_updates() {
            return Err(DataFusionError::Execution(
                "processing-time snapshot requires Flink's pre-checkpoint buffer flush".into(),
            ));
        }
        self.slices.snapshot(group)
    }

    pub(super) fn restore(&mut self, group: u32, bytes: &[u8], watermark: i64) -> Result<()> {
        self.require_healthy()?;
        self.slices.restore(group, bytes, watermark)
    }
}
