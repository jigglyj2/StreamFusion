// Copyright 2026 StreamFusion Authors
// Licensed under the Apache License, Version 2.0.

use datafusion::error::{DataFusionError, Result};

pub(crate) type SnapshotSink<'a> = dyn FnMut(&[u8]) -> Result<()> + 'a;

pub(crate) fn write_materialized(bytes: &[u8], sink: &mut SnapshotSink<'_>) -> Result<usize> {
    let framed = write_length(bytes.len(), sink)?;
    sink(bytes)?;
    Ok(framed)
}

/// Positive Int32 lengths retain their original bytes. Frame extension v2 uses -1 followed by
/// a positive Int64 payload length; older runtimes reject it instead of truncating a large group.
fn write_length(bytes: usize, sink: &mut SnapshotSink<'_>) -> Result<usize> {
    let header = if bytes <= i32::MAX as usize { 4 } else { 12 };
    let framed = bytes
        .checked_add(header)
        .filter(|length| i64::try_from(*length).is_ok())
        .ok_or_else(overflow)?;
    if header == 4 {
        sink(&(bytes as i32).to_be_bytes())?;
    } else {
        sink(&(-1i32).to_be_bytes())?;
        sink(&(bytes as i64).to_be_bytes())?;
    }
    Ok(framed)
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
        let framed = write_length(bytes, sink)?;
        sink(&header)?;
        Ok(framed)
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

#[cfg(test)]
mod frame_tests {
    use super::*;
    #[test]
    fn frame_lengths_preserve_small_bytes_and_version_large_key_groups() {
        for size in [0usize, 16, i32::MAX as usize] {
            let mut bytes = Vec::new();
            assert_eq!(
                write_length(size, &mut |part| {
                    bytes.extend_from_slice(part);
                    Ok(())
                })
                .unwrap(),
                size + 4
            );
            assert_eq!(bytes, (size as i32).to_be_bytes());
        }
        #[cfg(target_pointer_width = "64")]
        for size in [i32::MAX as usize + 1, 5usize << 30] {
            let mut bytes = Vec::new();
            assert_eq!(
                write_length(size, &mut |part| {
                    bytes.extend_from_slice(part);
                    Ok(())
                })
                .unwrap(),
                size + 12
            );
            assert_eq!(&bytes[..4], &(-1i32).to_be_bytes());
            assert_eq!(&bytes[4..], &(size as i64).to_be_bytes());
        }
        let mut called = false;
        assert!(write_length(usize::MAX, &mut |_| {
            called = true;
            Ok(())
        })
        .is_err());
        assert!(!called);
    }
}
