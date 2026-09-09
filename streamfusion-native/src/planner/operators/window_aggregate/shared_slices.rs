// Copyright 2026 StreamFusion Authors
// Licensed under the Apache License, Version 2.0.

//! TUMBLE and shared/attached HOP slice execution. Flink retains each base slice once and combines the slices
//! when a window fires. Attached windows retain one completed namespace and fire once.
//! Explicit shared-plan bindings are available; ordinary planner admission still requires
//! the complete Flink resource, operator-clock and metric lifecycle.

use super::*;
use crate::planner::operators::group_aggregate::grouped_compute::GroupedMerge;
use crate::planner::operators::sortable_state;

mod checkpoint;
mod codec;
mod firing;
mod input;
#[cfg(test)]
pub(super) mod tests;

const STATE_NAMESPACE: u8 = 0x53;
const STATE_MAGIC: &[u8; 5] = b"SFSL\x01";
const READ_ROWS: usize = 4096;
pub(super) const OUTPUT_ROWS: usize = 1024;

pub(super) struct SharedSlices {
    // Drop compute/codec owners before the kernel's workspace reservation.
    compute: GroupedMerge,
    end_codec: RowConverter,
    failed: bool,
    started: bool,
    restored_watermark: Option<i64>,
    kernel: WindowAggregateProcessor,
}

impl SharedSlices {
    pub(super) fn new(kernel: WindowAggregateProcessor) -> Result<Self> {
        let plan = &kernel.plan;
        if (plan.kind != proto::WindowKind::Hop as i32
            && plan.kind != proto::WindowKind::Tumble as i32)
            || plan.partial_accumulator_index.is_none()
            || plan.input_changelog
            || plan.processing_time
            || kernel.shift_time_zone != chrono_tz::UTC
        {
            return Err(DataFusionError::Plan(
                "shared slice execution requires append-only UTC event-time TUMBLE/HOP partials"
                    .into(),
            ));
        }
        let compute = GroupedMerge::new(&kernel.calls)?.ok_or_else(|| {
            DataFusionError::Plan(
                "shared slice execution requires DataFusion grouped COUNT or append-only extrema"
                    .into(),
            )
        })?;
        Ok(Self {
            compute,
            end_codec: RowConverter::new(vec![SortField::new(DataType::Int64)])?,
            failed: false,
            started: false,
            restored_watermark: None,
            kernel,
        })
    }

    fn require_healthy(&self) -> Result<()> {
        if self.failed {
            return Err(DataFusionError::Execution(
                "shared slice execution failed; restore a new context".into(),
            ));
        }
        Ok(())
    }

    fn admit(&mut self, bytes: usize) -> Result<()> {
        if bytes > self.kernel.scratch_reservation.size() {
            self.kernel.scratch_reservation.resize(bytes)?;
        }
        Ok(())
    }

    fn process(&mut self, batch: &RecordBatch) -> Result<()> {
        self.require_healthy()?;
        self.started = true;
        let result = self.process_inner(batch);
        if result.is_err() {
            // Failed computation may retain large DataFusion vectors; keep their allowance
            // until the failed context closes. A partially mutated context cannot be reused.
            self.failed = true;
        } else {
            self.kernel.scratch_reservation.resize(0)?;
        }
        result
    }

    fn advance(&mut self, watermark: i64) -> Result<RecordBatch> {
        self.require_healthy()?;
        self.started = true;
        let result = self.advance_inner(watermark);
        if result.is_err() {
            self.failed = true;
        } else {
            self.kernel.scratch_reservation.resize(0)?;
        }
        result
    }

    fn shares_slices(&self) -> bool {
        self.kernel.plan.kind == proto::WindowKind::Hop as i32
            && self.kernel.plan.partial_windows_are_slices
    }

    fn last_window_end(&self, slice_end: i64) -> i64 {
        if !self.shares_slices() {
            return slice_end;
        }
        slice_end
            .wrapping_sub(self.kernel.plan.slide_or_step_millis)
            .wrapping_add(self.kernel.plan.size_millis)
    }

    fn first_unfired(&self, slice_end: i64) -> i64 {
        let watermark = self.kernel.current_event_time;
        if !self.shares_slices() || slice_end.wrapping_sub(1) > watermark {
            return slice_end;
        }
        let interval = i128::from(self.kernel.plan.slide_or_step_millis);
        let steps = (i128::from(watermark) + 1 - i128::from(slice_end)) / interval + 1;
        slice_end.wrapping_add((steps * interval) as i64)
    }

    fn next_timer(&self) -> Option<i64> {
        self.kernel.next_event_timer()
    }
}

impl super::shared_kernel::SharedWindowKernel for SharedSlices {
    fn kernel(&self) -> &WindowAggregateProcessor {
        &self.kernel
    }
    fn set_restored_watermark(&mut self, watermark: i64) {
        self.kernel.current_event_time = watermark;
        self.restored_watermark = Some(watermark);
    }
    fn require_healthy(&self) -> Result<()> {
        SharedSlices::require_healthy(self)
    }
    fn process(&mut self, batch: &RecordBatch) -> Result<()> {
        SharedSlices::process(self, batch)
    }
    fn advance(&mut self, watermark: i64) -> Result<RecordBatch> {
        SharedSlices::advance(self, watermark)
    }
    fn next_timer(&self) -> Option<i64> {
        SharedSlices::next_timer(self)
    }
    fn snapshot(&mut self, group: u32) -> Result<crate::state::SnapshotBytes> {
        SharedSlices::snapshot(self, group)
    }
    fn restore(&mut self, group: u32, bytes: &[u8], watermark: i64) -> Result<()> {
        SharedSlices::restore(self, group, bytes, watermark)
    }
    fn checkpoint(&mut self, directory: &std::path::Path) -> Result<()> {
        SharedSlices::checkpoint(self, directory)
    }
}
