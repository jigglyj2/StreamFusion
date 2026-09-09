// Copyright 2026 StreamFusion Authors
// Licensed under the Apache License, Version 2.0.

//! Ordered append-only session state. Flink owns planning, operator clocks and recovery;
//! custom code assigns merging namespaces, and DataFusion computes grouped aggregates.

use super::super::group_aggregate::grouped_compute::{GroupedCompute, GroupedMerge};
use super::super::sortable_state;
use super::*;
use arrow::array::BooleanArray;

mod assignments;
mod checkpoint;
mod codec;
mod firing;
mod input;
mod migration;

const STATE_NAMESPACE: u8 = 0x54;
const STATE_MAGIC: &[u8; 5] = b"SFSN\x01";
const OUTPUT_ROWS: usize = 1024;
const COMPUTE_ROWS: usize = 2048;

pub(super) struct SharedSessions {
    compute: GroupedCompute,
    merge: GroupedMerge,
    end_codec: RowConverter,
    failed: bool,
    started: bool,
    restored_watermark: Option<i64>,
    kernel: WindowAggregateProcessor,
}

impl SharedSessions {
    pub(super) fn new(kernel: WindowAggregateProcessor) -> Result<Self> {
        validate(&kernel.plan)?;
        let merge = GroupedMerge::new(&kernel.calls)?.ok_or_else(|| {
            DataFusionError::Plan(
                "shared sessions require DataFusion grouped COUNT or append-only extrema".into(),
            )
        })?;
        let compute = GroupedCompute::new(&kernel.calls)?.ok_or_else(|| {
            DataFusionError::Plan("shared session calls lack DataFusion grouped computation".into())
        })?;
        Ok(Self {
            compute,
            merge,
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
                "shared session execution failed; restore a new context".into(),
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
    fn next_timer(&self) -> Option<i64> {
        self.kernel.next_event_timer()
    }
}

#[cfg(test)]
pub(super) mod tests;

impl super::shared_kernel::SharedWindowKernel for SharedSessions {
    fn kernel(&self) -> &WindowAggregateProcessor {
        &self.kernel
    }
    fn set_restored_watermark(&mut self, watermark: i64) {
        self.kernel.current_event_time = watermark;
        self.restored_watermark = Some(watermark);
    }
    fn require_healthy(&self) -> Result<()> {
        SharedSessions::require_healthy(self)
    }
    fn process(&mut self, batch: &RecordBatch) -> Result<()> {
        SharedSessions::process(self, batch)
    }
    fn advance(&mut self, watermark: i64) -> Result<RecordBatch> {
        SharedSessions::advance(self, watermark)
    }
    fn next_timer(&self) -> Option<i64> {
        SharedSessions::next_timer(self)
    }
    fn snapshot(&mut self, group: u32) -> Result<crate::state::SnapshotBytes> {
        SharedSessions::snapshot(self, group)
    }
    fn restore(&mut self, group: u32, bytes: &[u8], watermark: i64) -> Result<()> {
        SharedSessions::restore(self, group, bytes, watermark)
    }
    fn checkpoint(&mut self, directory: &std::path::Path) -> Result<()> {
        SharedSessions::checkpoint(self, directory)
    }
}

pub(super) fn validate(plan: &proto::WindowAggregate) -> Result<()> {
    if plan.kind != proto::WindowKind::Session as i32
        || plan.input_changelog
        || plan.processing_time
        || !(plan.shift_time_zone.is_empty() || plan.shift_time_zone == "UTC")
        || plan.partial_accumulator_index.is_some()
        || plan.attached_window_end_index.is_some()
        || plan.aggregate_calls.iter().any(|call| call.retractable)
    {
        return Err(DataFusionError::Plan(
            "shared sessions require append-only UTC event-time input".into(),
        ));
    }
    let calls = plan
        .aggregate_calls
        .iter()
        .map(lower_call)
        .collect::<Result<Vec<_>>>()?;
    if GroupedMerge::new(&calls)?.is_none() || GroupedCompute::new(&calls)?.is_none() {
        return Err(DataFusionError::Plan(
            "shared sessions require DataFusion grouped COUNT or append-only extrema".into(),
        ));
    }
    Ok(())
}
