// Copyright 2026 StreamFusion Authors
// Licensed under the Apache License, Version 2.0.

//! Common execution/control ownership for slicing and merging windows.
use super::*;

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
}
