// Copyright 2026 StreamFusion Authors
// Licensed under the Apache License, Version 2.0.
use super::*;

impl SharedWindowKernel for SharedProcessingWindows {
    fn kernel(&self) -> &WindowAggregateProcessor {
        self.slices.kernel()
    }
    fn input_schema(&self) -> Result<SchemaRef> {
        Ok(self.input_schema.clone())
    }
    fn set_restored_watermark(&mut self, watermark: i64) {
        // The Flink union watermark never initializes processing-time buffer progress.
        self.slices.set_restored_watermark(watermark);
    }
    fn require_healthy(&self) -> Result<()> {
        Self::require_healthy(self)
    }
    fn process(&mut self, _: &RecordBatch) -> Result<()> {
        Err(DataFusionError::Plan(
            "processing-time window requires its per-record clock input".into(),
        ))
    }
    fn process_with_clock(
        &mut self,
        batch: &RecordBatch,
        clock: Option<&Int64Array>,
    ) -> Result<()> {
        let clock = clock.ok_or_else(|| {
            DataFusionError::Plan(
                "processing-time window is missing its per-record clock input".into(),
            )
        })?;
        // Physical child field names may differ from the SQL schema. Only schema metadata
        // changes here; payload and clock arrays retain their producer-owned buffers.
        let input = RecordBatch::try_new(self.input_schema.clone(), batch.columns().to_vec())?;
        Self::process(self, input, clock.clone())
    }
    fn advance(&mut self, deadline: i64) -> Result<RecordBatch> {
        self.fire(deadline)
    }
    fn next_timer(&self) -> Option<i64> {
        Self::next_timer(self)
    }
    fn poll_control(&mut self, event: ControlEvent) -> Result<Option<RecordBatch>> {
        self.control(event)?;
        if let ControlEvent::ProcessingTime(deadline) = event {
            while self.next_timer().is_some_and(|next| next <= deadline) {
                let output = self.fire(deadline)?;
                if output.num_rows() != 0 {
                    return Ok(Some(output));
                }
            }
        }
        Ok(None)
    }
    fn snapshot(&mut self, group: u32) -> Result<crate::state::SnapshotBytes> {
        Self::snapshot(self, group)
    }
    fn restore(&mut self, group: u32, bytes: &[u8], watermark: i64) -> Result<()> {
        Self::restore(self, group, bytes, watermark)
    }
    fn checkpoint(&mut self, directory: &std::path::Path) -> Result<()> {
        self.require_healthy()?;
        if self.buffer.has_buffered_updates() {
            return Err(DataFusionError::Execution(
                "processing-time checkpoint requires Flink's pre-checkpoint buffer flush".into(),
            ));
        }
        self.slices.checkpoint(directory)
    }
}
