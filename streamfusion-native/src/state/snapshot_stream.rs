// Copyright 2026 StreamFusion Authors
// Licensed under the Apache License, Version 2.0.

use datafusion::error::{DataFusionError, Result};

pub(crate) type SnapshotSink<'a> = dyn FnMut(&[u8]) -> Result<()> + 'a;

pub(crate) fn write_materialized(bytes: &[u8], sink: &mut SnapshotSink<'_>) -> Result<usize> {
    sink(&length(bytes.len())?.to_be_bytes())?;
    sink(bytes)?;
    Ok(4 + bytes.len())
}

fn length(bytes: usize) -> Result<i32> {
    i32::try_from(bytes).map_err(|_| {
        DataFusionError::ResourcesExhausted(
            "canonical snapshot exceeds the version-one signed frame length".into(),
        )
    })
}

#[derive(Default)]
pub(crate) struct Shape {
    entries: usize,
    payload_bytes: usize,
}
impl Shape {
    pub(crate) fn add(&mut self, key: &[u8], value: &[u8]) -> Result<()> {
        self.entries = self.entries.checked_add(1).ok_or_else(overflow)?;
        self.payload_bytes = self
            .payload_bytes
            .checked_add(8)
            .and_then(|bytes| bytes.checked_add(key.len()))
            .and_then(|bytes| bytes.checked_add(value.len()))
            .ok_or_else(overflow)?;
        Ok(())
    }
    pub(crate) fn start(&self, group: u32, sink: &mut SnapshotSink<'_>) -> Result<usize> {
        let bytes = self.payload_bytes.checked_add(16).ok_or_else(overflow)?;
        let header = streamfusion_state_abi::key_group_snapshot_header(group, self.entries, bytes)
            .map_err(|error| DataFusionError::Execution(error.to_string()))?;
        sink(&length(bytes)?.to_be_bytes())?;
        sink(&header)?;
        Ok(4 + bytes)
    }
}
fn overflow() -> DataFusionError {
    DataFusionError::ResourcesExhausted("canonical snapshot size overflow".into())
}
pub(crate) fn entry(key: &[u8], value: &[u8], sink: &mut SnapshotSink<'_>) -> Result<()> {
    let key_len = u32::try_from(key.len()).map_err(|_| overflow())?;
    let value_len = u32::try_from(value.len()).map_err(|_| overflow())?;
    sink(&key_len.to_le_bytes())?;
    sink(key)?;
    sink(&value_len.to_le_bytes())?;
    sink(value)
}

pub(crate) fn write_entries<'a>(
    group: u32,
    entries: impl Iterator<Item = (&'a [u8], &'a [u8])> + Clone,
    sink: &mut SnapshotSink<'_>,
) -> Result<usize> {
    let mut shape = Shape::default();
    for (key, value) in entries.clone() {
        shape.add(key, value)?;
    }
    let bytes = shape.start(group, sink)?;
    for (key, value) in entries {
        entry(key, value, sink)?;
    }
    Ok(bytes)
}
