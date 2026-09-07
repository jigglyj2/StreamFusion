// Copyright 2026 StreamFusion Authors
// Licensed under the Apache License, Version 2.0

use crate::{SnapshotError, SNAPSHOT_MAGIC, SNAPSHOT_VERSION};

/// Builds canonical bytes after a caller has measured and admitted their exact size.
/// In particular, no copied collection of historical keys/values is required.
pub struct SnapshotWriter {
    bytes: Vec<u8>,
    expected_bytes: usize,
    remaining_entries: u32,
}

impl SnapshotWriter {
    pub fn new(key_group: u32, entries: usize, bytes: usize) -> Result<Self, SnapshotError> {
        let entries = u32::try_from(entries)
            .map_err(|_| SnapshotError("canonical snapshot entry count exceeds UInt32".into()))?;
        if bytes < 16 || bytes.checked_sub(16).unwrap() / 8 < entries as usize {
            return Err(SnapshotError("invalid canonical snapshot size".into()));
        }
        let mut output = Vec::with_capacity(bytes);
        output.extend_from_slice(SNAPSHOT_MAGIC);
        output.extend_from_slice(&SNAPSHOT_VERSION.to_le_bytes());
        output.extend_from_slice(&key_group.to_le_bytes());
        output.extend_from_slice(&entries.to_le_bytes());
        Ok(Self {
            bytes: output,
            expected_bytes: bytes,
            remaining_entries: entries,
        })
    }

    pub fn append(&mut self, key: &[u8], value: &[u8]) -> Result<(), SnapshotError> {
        let key_len = u32::try_from(key.len())
            .map_err(|_| SnapshotError("snapshot key exceeds UInt32".into()))?;
        let value_len = u32::try_from(value.len())
            .map_err(|_| SnapshotError("snapshot value exceeds UInt32".into()))?;
        let end = self
            .bytes
            .len()
            .checked_add(8)
            .and_then(|bytes| bytes.checked_add(key.len()))
            .and_then(|bytes| bytes.checked_add(value.len()))
            .ok_or_else(|| SnapshotError("canonical snapshot size overflow".into()))?;
        if self.remaining_entries == 0 || end > self.expected_bytes {
            return Err(SnapshotError(
                "canonical snapshot exceeded its admitted size".into(),
            ));
        }
        self.bytes.extend_from_slice(&key_len.to_le_bytes());
        self.bytes.extend_from_slice(key);
        self.bytes.extend_from_slice(&value_len.to_le_bytes());
        self.bytes.extend_from_slice(value);
        self.remaining_entries -= 1;
        Ok(())
    }

    pub fn finish(self) -> Result<Vec<u8>, SnapshotError> {
        if self.remaining_entries != 0 || self.bytes.len() != self.expected_bytes {
            return Err(SnapshotError(
                "canonical snapshot changed while being encoded".into(),
            ));
        }
        Ok(self.bytes)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn preserves_canonical_bytes_and_refuses_growth() {
        let mut writer = SnapshotWriter::new(7, 1, 28).unwrap();
        writer.append(b"a", b"one").unwrap();
        assert!(writer.append(b"b", b"two").is_err());
        let bytes = writer.finish().unwrap();
        assert_eq!(
            bytes,
            crate::encode_key_group_snapshot(7, [(b"a".as_slice(), b"one".as_slice())].into_iter())
                .unwrap()
        );
        let mut too_small = SnapshotWriter::new(7, 1, 24).unwrap();
        assert!(too_small.append(b"a", b"one").is_err());
        assert!(too_small.finish().is_err());
    }
}
