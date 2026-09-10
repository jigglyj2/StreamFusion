// Copyright 2026 StreamFusion Authors
// Licensed under the Apache License, Version 2.0

//! Read-only migration codec for the former whole-window SFWJ/2 value format.
use datafusion::error::{DataFusionError, Result};

const STATE_MAGIC: &[u8; 4] = b"SFWJ";
const STATE_VERSION: u8 = 2;

#[derive(Default, Debug, PartialEq, Eq)]
pub(super) struct JoinWindowState {
    pub(super) left: Vec<Vec<u8>>,
    pub(super) right: Vec<Vec<u8>>,
}

#[cfg(test)]
pub(super) fn encode_state(state: &JoinWindowState) -> Vec<u8> {
    let mut bytes = Vec::new();
    bytes.extend_from_slice(STATE_MAGIC);
    bytes.push(STATE_VERSION);
    for rows in [&state.left, &state.right] {
        bytes.extend_from_slice(&(rows.len() as u32).to_le_bytes());
        for row in rows {
            bytes.extend_from_slice(&(row.len() as u32).to_le_bytes());
            bytes.extend_from_slice(row);
        }
    }
    bytes
}

pub(super) fn decode_state(bytes: &[u8]) -> Result<JoinWindowState> {
    if bytes.len() < 13 || &bytes[..4] != STATE_MAGIC || bytes[4] != STATE_VERSION {
        return Err(DataFusionError::Execution(
            "invalid native window join state".to_string(),
        ));
    }
    let mut offset = 5;
    let left = decode_rows(bytes, &mut offset)?;
    let right = decode_rows(bytes, &mut offset)?;
    if offset != bytes.len() {
        return Err(DataFusionError::Execution(
            "window join state has trailing bytes".to_string(),
        ));
    }
    Ok(JoinWindowState { left, right })
}

fn decode_rows(bytes: &[u8], offset: &mut usize) -> Result<Vec<Vec<u8>>> {
    let count = read_u32(bytes, offset)? as usize;
    if count > bytes.len().saturating_sub(*offset) / 4 {
        return Err(truncated());
    }
    let mut rows = Vec::with_capacity(count);
    for _ in 0..count {
        let length = read_u32(bytes, offset)? as usize;
        let end = offset.checked_add(length).ok_or_else(truncated)?;
        rows.push(bytes.get(*offset..end).ok_or_else(truncated)?.to_vec());
        *offset = end;
    }
    Ok(rows)
}

fn read_u32(bytes: &[u8], offset: &mut usize) -> Result<u32> {
    let end = offset.checked_add(4).ok_or_else(truncated)?;
    let value = bytes.get(*offset..end).ok_or_else(truncated)?;
    *offset = end;
    Ok(u32::from_le_bytes(value.try_into().unwrap()))
}

fn truncated() -> DataFusionError {
    DataFusionError::Execution("truncated native window join state".to_string())
}
