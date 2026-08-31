// Copyright 2026 StreamFusion Authors
// Licensed under the Apache License, Version 2.0

use std::ffi::{c_char, c_void};
use std::fmt::{Display, Formatter};

use arrow::ffi::{FFI_ArrowArray, FFI_ArrowSchema};

pub const STATE_BACKEND_ABI_VERSION: u32 = 3;
pub const STATE_BACKEND_OK: i32 = 0;

pub type OpenBackend = unsafe extern "C" fn(
    path: *const u8,
    path_len: usize,
    first_key_group: u32,
    last_key_group: u32,
    memory_limit: usize,
    output: *mut *mut c_void,
) -> i32;
pub type CloseBackend = unsafe extern "C" fn(handle: *mut c_void);
pub type ArrowOperation = unsafe extern "C" fn(
    handle: *mut c_void,
    input_array: *mut FFI_ArrowArray,
    input_schema: *mut FFI_ArrowSchema,
    output_array: *mut FFI_ArrowArray,
    output_schema: *mut FFI_ArrowSchema,
) -> i32;
pub type LastError = unsafe extern "C" fn() -> *const c_char;

#[repr(C)]
pub struct StateBackendApiV1 {
    pub abi_version: u32,
    pub open: OpenBackend,
    pub close: CloseBackend,
    pub get_batch: ArrowOperation,
    pub write_batch: ArrowOperation,
    pub snapshot_key_group: ArrowOperation,
    pub restore_key_group: ArrowOperation,
    /// Creates a consistent physical backend checkpoint at the path supplied as one Binary row.
    pub checkpoint: ArrowOperation,
    pub last_error: LastError,
}

pub type InitializeStateBackend =
    unsafe extern "C" fn(requested_version: u32, output: *mut *const StateBackendApiV1) -> i32;

const SNAPSHOT_MAGIC: &[u8; 4] = b"SFS1";
const SNAPSHOT_VERSION: u32 = 1;

/// Error produced while encoding or decoding the backend-neutral keyed-state snapshot.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct SnapshotError(String);

impl Display for SnapshotError {
    fn fmt(&self, formatter: &mut Formatter<'_>) -> std::fmt::Result {
        formatter.write_str(&self.0)
    }
}

impl std::error::Error for SnapshotError {}

/// Encodes one key group in the canonical format shared by every native backend.
pub fn encode_key_group_snapshot<'a>(
    key_group: u32,
    entries: impl ExactSizeIterator<Item = (&'a [u8], &'a [u8])>,
) -> Result<Vec<u8>, SnapshotError> {
    let mut output = Vec::new();
    output.extend_from_slice(SNAPSHOT_MAGIC);
    output.extend_from_slice(&SNAPSHOT_VERSION.to_le_bytes());
    output.extend_from_slice(&key_group.to_le_bytes());
    write_len(&mut output, entries.len(), "entry count")?;
    for (key, value) in entries {
        write_len(&mut output, key.len(), "key")?;
        output.extend_from_slice(key);
        write_len(&mut output, value.len(), "value")?;
        output.extend_from_slice(value);
    }
    Ok(output)
}

/// Decodes one canonical key group. The format is deliberately independent of RocksDB or memory.
pub fn decode_key_group_snapshot(
    expected_key_group: u32,
    bytes: &[u8],
) -> Result<Vec<(Vec<u8>, Vec<u8>)>, SnapshotError> {
    let mut input = SnapshotInput::new(bytes);
    if input.read_exact(4, "magic")? != SNAPSHOT_MAGIC {
        return Err(SnapshotError(
            "invalid StreamFusion state magic".to_string(),
        ));
    }
    let version = input.read_u32("version")?;
    if version != SNAPSHOT_VERSION {
        return Err(SnapshotError(format!(
            "unsupported StreamFusion state version {version}"
        )));
    }
    let key_group = input.read_u32("key group")?;
    if key_group != expected_key_group {
        return Err(SnapshotError(format!(
            "state key group {key_group} does not match assigned group {expected_key_group}"
        )));
    }
    let count = input.read_u32("entry count")? as usize;
    let mut entries = Vec::with_capacity(count);
    for _ in 0..count {
        entries.push((input.read_bytes("key")?, input.read_bytes("value")?));
    }
    if !input.is_empty() {
        return Err(SnapshotError(
            "StreamFusion state has trailing bytes".to_string(),
        ));
    }
    Ok(entries)
}

fn write_len(output: &mut Vec<u8>, value: usize, description: &str) -> Result<(), SnapshotError> {
    let value = u32::try_from(value)
        .map_err(|_| SnapshotError(format!("StreamFusion state {description} exceeds 4 GiB")))?;
    output.extend_from_slice(&value.to_le_bytes());
    Ok(())
}

struct SnapshotInput<'a> {
    bytes: &'a [u8],
    offset: usize,
}

impl<'a> SnapshotInput<'a> {
    fn new(bytes: &'a [u8]) -> Self {
        Self { bytes, offset: 0 }
    }

    fn is_empty(&self) -> bool {
        self.offset == self.bytes.len()
    }

    fn read_u32(&mut self, description: &str) -> Result<u32, SnapshotError> {
        Ok(u32::from_le_bytes(
            self.read_exact(4, description)?.try_into().unwrap(),
        ))
    }

    fn read_bytes(&mut self, description: &str) -> Result<Vec<u8>, SnapshotError> {
        let len = self.read_u32(&format!("{description} length"))? as usize;
        Ok(self.read_exact(len, description)?.to_vec())
    }

    fn read_exact(&mut self, len: usize, description: &str) -> Result<&'a [u8], SnapshotError> {
        let end = self.offset.checked_add(len).ok_or_else(|| {
            SnapshotError(format!("StreamFusion state {description} length overflow"))
        })?;
        let value = self
            .bytes
            .get(self.offset..end)
            .ok_or_else(|| SnapshotError(format!("truncated StreamFusion state {description}")))?;
        self.offset = end;
        Ok(value)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn canonical_snapshot_has_a_stable_fixture() {
        let entries = [(b"a".as_slice(), b"one".as_slice())];
        let encoded = encode_key_group_snapshot(7, entries.into_iter()).unwrap();
        assert_eq!(
            encoded,
            b"SFS1\x01\0\0\0\x07\0\0\0\x01\0\0\0\x01\0\0\0a\x03\0\0\0one"
        );
        assert_eq!(
            decode_key_group_snapshot(7, &encoded).unwrap(),
            vec![(b"a".to_vec(), b"one".to_vec())]
        );
    }
}
