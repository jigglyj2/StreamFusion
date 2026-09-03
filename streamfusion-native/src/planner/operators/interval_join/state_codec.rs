// Copyright 2026 StreamFusion Authors
// Licensed under the Apache License, Version 2.0

use std::collections::BTreeMap;

use datafusion::error::{DataFusionError, Result};

use crate::state::StateKey;

const STATE_MAGIC: &[u8; 4] = b"SFIJ";
const STATE_VERSION: u8 = 1;

#[derive(Clone, Debug, PartialEq, Eq)]
pub(super) struct StoredRow {
    pub(super) row: Vec<u8>,
    pub(super) associations: i32,
}

#[derive(Default, Debug, PartialEq, Eq)]
pub(super) struct IntervalState {
    pub(super) left: BTreeMap<i64, Vec<StoredRow>>,
    pub(super) right: BTreeMap<i64, Vec<StoredRow>>,
    pub(super) cleanup_timers: [Option<i64>; 2],
}

pub(super) struct StagedState {
    pub(super) key: StateKey,
    pub(super) value: IntervalState,
    pub(super) touched: bool,
}

pub(super) fn encode_state(state: &IntervalState) -> Vec<u8> {
    let mut bytes = Vec::new();
    bytes.extend_from_slice(STATE_MAGIC);
    bytes.push(STATE_VERSION);
    for timer in state.cleanup_timers {
        bytes.extend_from_slice(&timer.unwrap_or(i64::MIN).to_le_bytes());
    }
    for timestamps in [&state.left, &state.right] {
        bytes.extend_from_slice(&(timestamps.len() as u32).to_le_bytes());
        for (timestamp, rows) in timestamps {
            bytes.extend_from_slice(&timestamp.to_le_bytes());
            bytes.extend_from_slice(&(rows.len() as u32).to_le_bytes());
            for row in rows {
                bytes.extend_from_slice(&row.associations.to_le_bytes());
                bytes.extend_from_slice(&(row.row.len() as u32).to_le_bytes());
                bytes.extend_from_slice(&row.row);
            }
        }
    }
    bytes
}

pub(super) fn decode_state(bytes: &[u8]) -> Result<IntervalState> {
    if bytes.len() < 29 || &bytes[..4] != STATE_MAGIC || bytes[4] != STATE_VERSION {
        return Err(DataFusionError::Execution(
            "invalid native interval join state".to_string(),
        ));
    }
    let mut offset = 5;
    let mut cleanup_timers = [None, None];
    for timer in &mut cleanup_timers {
        let value = read_i64(bytes, &mut offset)?;
        *timer = (value != i64::MIN).then_some(value);
    }
    let left = decode_timestamps(bytes, &mut offset)?;
    let right = decode_timestamps(bytes, &mut offset)?;
    if offset != bytes.len() {
        return Err(DataFusionError::Execution(
            "interval join state has trailing bytes".to_string(),
        ));
    }
    Ok(IntervalState {
        left,
        right,
        cleanup_timers,
    })
}

fn decode_timestamps(bytes: &[u8], offset: &mut usize) -> Result<BTreeMap<i64, Vec<StoredRow>>> {
    let count = read_u32(bytes, offset)? as usize;
    let mut timestamps = BTreeMap::new();
    for _ in 0..count {
        let timestamp = read_i64(bytes, offset)?;
        let row_count = read_u32(bytes, offset)? as usize;
        let mut rows = Vec::with_capacity(row_count);
        for _ in 0..row_count {
            let associations = read_i32(bytes, offset)?;
            let length = read_u32(bytes, offset)? as usize;
            let end = offset.checked_add(length).ok_or_else(truncated)?;
            rows.push(StoredRow {
                row: bytes.get(*offset..end).ok_or_else(truncated)?.to_vec(),
                associations,
            });
            *offset = end;
        }
        if timestamps.insert(timestamp, rows).is_some() {
            return Err(DataFusionError::Execution(
                "interval join state contains a duplicate timestamp".to_string(),
            ));
        }
    }
    Ok(timestamps)
}

fn read_u32(bytes: &[u8], offset: &mut usize) -> Result<u32> {
    let end = offset.checked_add(4).ok_or_else(truncated)?;
    let value = bytes.get(*offset..end).ok_or_else(truncated)?;
    *offset = end;
    Ok(u32::from_le_bytes(value.try_into().unwrap()))
}

fn read_i32(bytes: &[u8], offset: &mut usize) -> Result<i32> {
    let end = offset.checked_add(4).ok_or_else(truncated)?;
    let value = bytes.get(*offset..end).ok_or_else(truncated)?;
    *offset = end;
    Ok(i32::from_le_bytes(value.try_into().unwrap()))
}

fn read_i64(bytes: &[u8], offset: &mut usize) -> Result<i64> {
    let end = offset.checked_add(8).ok_or_else(truncated)?;
    let value = bytes.get(*offset..end).ok_or_else(truncated)?;
    *offset = end;
    Ok(i64::from_le_bytes(value.try_into().unwrap()))
}

fn truncated() -> DataFusionError {
    DataFusionError::Execution("truncated native interval join state".to_string())
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn rejects_trailing_bytes() {
        let state = IntervalState {
            left: BTreeMap::from([(
                7,
                vec![StoredRow {
                    row: vec![1, 2, 3],
                    associations: 4,
                }],
            )]),
            right: BTreeMap::new(),
            cleanup_timers: [Some(11), None],
        };
        assert_eq!(decode_state(&encode_state(&state)).unwrap(), state);
        let mut malformed = encode_state(&state);
        malformed.push(0);
        assert!(decode_state(&malformed).is_err());
    }
}
