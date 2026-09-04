// Copyright 2026 StreamFusion Authors
// Licensed under the Apache License, Version 2.0

use std::collections::BTreeMap;

use datafusion::error::{DataFusionError, Result};

const STATE_MAGIC: &[u8; 4] = b"SFTJ";
const STATE_VERSION: u8 = 1;

#[derive(Clone, Debug, PartialEq, Eq)]
pub(super) struct ProbeRow {
    pub(super) sequence: u64,
    pub(super) timestamp: i64,
    pub(super) kind: i8,
    pub(super) matchable: bool,
    pub(super) row: Vec<u8>,
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub(super) struct VersionRow {
    pub(super) kind: i8,
    pub(super) matchable: bool,
    pub(super) row: Vec<u8>,
}

#[derive(Default, Debug, PartialEq, Eq)]
pub(super) struct TemporalState {
    pub(super) next_sequence: u64,
    pub(super) left: Vec<ProbeRow>,
    pub(super) right: BTreeMap<i64, VersionRow>,
    pub(super) event_timer: Option<i64>,
    pub(super) cleanup_timer: Option<i64>,
}

pub(super) struct StagedState {
    pub(super) key: crate::state::StateKey,
    pub(super) value: TemporalState,
    pub(super) touched: bool,
}

pub(super) fn encode_state(state: &TemporalState) -> Result<Vec<u8>> {
    let estimated = 5usize
        .checked_add(8 + 8 + 8 + 4 + 4)
        .and_then(|size| {
            state.left.iter().try_fold(size, |size, row| {
                size.checked_add(8 + 8 + 1 + 1 + 4 + row.row.len())
            })
        })
        .and_then(|size| {
            state.right.values().try_fold(size, |size, row| {
                size.checked_add(8 + 1 + 1 + 4 + row.row.len())
            })
        })
        .ok_or_else(|| {
            DataFusionError::Execution("temporal join state size overflow".to_string())
        })?;
    let mut bytes = Vec::with_capacity(estimated);
    bytes.extend_from_slice(STATE_MAGIC);
    bytes.push(STATE_VERSION);
    bytes.extend_from_slice(&state.next_sequence.to_le_bytes());
    write_option_i64(&mut bytes, state.event_timer);
    write_option_i64(&mut bytes, state.cleanup_timer);
    write_count(&mut bytes, state.left.len(), "left row count")?;
    for row in &state.left {
        bytes.extend_from_slice(&row.sequence.to_le_bytes());
        bytes.extend_from_slice(&row.timestamp.to_le_bytes());
        bytes.push(row.kind as u8);
        bytes.push(u8::from(row.matchable));
        write_bytes(&mut bytes, &row.row)?;
    }
    write_count(&mut bytes, state.right.len(), "right version count")?;
    for (&timestamp, row) in &state.right {
        bytes.extend_from_slice(&timestamp.to_le_bytes());
        bytes.push(row.kind as u8);
        bytes.push(u8::from(row.matchable));
        write_bytes(&mut bytes, &row.row)?;
    }
    debug_assert_eq!(bytes.len(), estimated);
    Ok(bytes)
}

pub(super) fn decode_state(bytes: &[u8]) -> Result<TemporalState> {
    if bytes.len() < 5 || &bytes[..4] != STATE_MAGIC || bytes[4] != STATE_VERSION {
        return Err(invalid());
    }
    let mut offset = 5;
    let next_sequence = read_u64(bytes, &mut offset)?;
    let event_timer = read_option_i64(bytes, &mut offset)?;
    let cleanup_timer = read_option_i64(bytes, &mut offset)?;
    let left_count = read_u32(bytes, &mut offset)? as usize;
    let mut left = Vec::with_capacity(left_count);
    for _ in 0..left_count {
        left.push(ProbeRow {
            sequence: read_u64(bytes, &mut offset)?,
            timestamp: read_i64(bytes, &mut offset)?,
            kind: read_u8(bytes, &mut offset)? as i8,
            matchable: read_u8(bytes, &mut offset)? != 0,
            row: read_bytes(bytes, &mut offset)?.to_vec(),
        });
    }
    let right_count = read_u32(bytes, &mut offset)? as usize;
    let mut right = BTreeMap::new();
    for _ in 0..right_count {
        let timestamp = read_i64(bytes, &mut offset)?;
        right.insert(
            timestamp,
            VersionRow {
                kind: read_u8(bytes, &mut offset)? as i8,
                matchable: read_u8(bytes, &mut offset)? != 0,
                row: read_bytes(bytes, &mut offset)?.to_vec(),
            },
        );
    }
    if offset != bytes.len() {
        return Err(invalid());
    }
    Ok(TemporalState {
        next_sequence,
        left,
        right,
        event_timer,
        cleanup_timer,
    })
}

fn write_option_i64(bytes: &mut Vec<u8>, value: Option<i64>) {
    bytes.extend_from_slice(&value.unwrap_or(i64::MIN).to_le_bytes());
}

fn read_option_i64(bytes: &[u8], offset: &mut usize) -> Result<Option<i64>> {
    let value = read_i64(bytes, offset)?;
    Ok((value != i64::MIN).then_some(value))
}

fn write_count(bytes: &mut Vec<u8>, value: usize, what: &str) -> Result<()> {
    let value = u32::try_from(value)
        .map_err(|_| DataFusionError::Execution(format!("temporal join {what} exceeds u32")))?;
    bytes.extend_from_slice(&value.to_le_bytes());
    Ok(())
}

fn write_bytes(bytes: &mut Vec<u8>, value: &[u8]) -> Result<()> {
    write_count(bytes, value.len(), "row length")?;
    bytes.extend_from_slice(value);
    Ok(())
}

fn read_bytes<'a>(bytes: &'a [u8], offset: &mut usize) -> Result<&'a [u8]> {
    let length = read_u32(bytes, offset)? as usize;
    let end = offset.checked_add(length).ok_or_else(invalid)?;
    let value = bytes.get(*offset..end).ok_or_else(invalid)?;
    *offset = end;
    Ok(value)
}

fn read_u8(bytes: &[u8], offset: &mut usize) -> Result<u8> {
    let value = *bytes.get(*offset).ok_or_else(invalid)?;
    *offset += 1;
    Ok(value)
}

fn read_u32(bytes: &[u8], offset: &mut usize) -> Result<u32> {
    let end = offset.checked_add(4).ok_or_else(invalid)?;
    let value = bytes.get(*offset..end).ok_or_else(invalid)?;
    *offset = end;
    Ok(u32::from_le_bytes(value.try_into().unwrap()))
}

fn read_u64(bytes: &[u8], offset: &mut usize) -> Result<u64> {
    let end = offset.checked_add(8).ok_or_else(invalid)?;
    let value = bytes.get(*offset..end).ok_or_else(invalid)?;
    *offset = end;
    Ok(u64::from_le_bytes(value.try_into().unwrap()))
}

fn read_i64(bytes: &[u8], offset: &mut usize) -> Result<i64> {
    let end = offset.checked_add(8).ok_or_else(invalid)?;
    let value = bytes.get(*offset..end).ok_or_else(invalid)?;
    *offset = end;
    Ok(i64::from_le_bytes(value.try_into().unwrap()))
}

fn invalid() -> DataFusionError {
    DataFusionError::Execution("invalid native temporal join state".to_string())
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn state_round_trips_changelog_versions_and_timers() {
        let mut state = TemporalState {
            next_sequence: 9,
            event_timer: Some(30),
            cleanup_timer: Some(90),
            ..TemporalState::default()
        };
        state.left.push(ProbeRow {
            sequence: 8,
            timestamp: 29,
            kind: 3,
            matchable: true,
            row: vec![1, 2],
        });
        state.right.insert(
            20,
            VersionRow {
                kind: 2,
                matchable: false,
                row: vec![3, 4],
            },
        );
        assert_eq!(decode_state(&encode_state(&state).unwrap()).unwrap(), state);
    }
}
