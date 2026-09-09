// Copyright 2026 StreamFusion Authors
// Licensed under the Apache License, Version 2.0.

//! Common execution/control ownership for slicing and merging windows.
use super::*;
use crate::planner::persistent::control::ControlEvent;

pub(super) trait SharedWindowKernel: Send {
    fn kernel(&self) -> &WindowAggregateProcessor;
    fn set_restored_watermark(&mut self, watermark: i64);
    fn require_healthy(&self) -> Result<()>;
    fn process(&mut self, batch: &RecordBatch) -> Result<()>;
    fn advance(&mut self, watermark: i64) -> Result<RecordBatch>;
    fn next_timer(&self) -> Option<i64>;
    fn snapshot(&mut self, group: u32) -> Result<crate::state::SnapshotBytes>;
    fn restore(&mut self, group: u32, bytes: &[u8], watermark: i64) -> Result<()>;
    fn checkpoint(&mut self, directory: &std::path::Path) -> Result<()>;

    fn input_schema(&self) -> Result<SchemaRef> {
        crate::planner::arrow_schema(self.kernel().plan.input_schema.as_ref().unwrap())
    }

    fn process_with_clock(
        &mut self,
        batch: &RecordBatch,
        clock: Option<&Int64Array>,
    ) -> Result<()> {
        if clock.is_some() {
            return Err(DataFusionError::Plan(
                "event-time window cannot consume a clock input".into(),
            ));
        }
        self.process(batch)
    }

    fn poll_control(&mut self, event: ControlEvent) -> Result<Option<RecordBatch>> {
        match event {
            ControlEvent::ProcessingTime(_) => Err(DataFusionError::Plan(
                "event-time window cannot consume a processing-time timer".into(),
            )),
            ControlEvent::Watermark(watermark) => loop {
                let output = self.advance(watermark)?;
                if output.num_rows() != 0 {
                    return Ok(Some(output));
                }
                if self.next_timer().is_none_or(|next| next > watermark) {
                    return Ok(None);
                }
            },
            ControlEvent::BeforeCheckpoint(_) | ControlEvent::EndInput => {
                self.require_healthy()?;
                Ok(None)
            }
        }
    }
}
