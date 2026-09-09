// Copyright 2026 StreamFusion Authors
// Licensed under the Apache License, Version 2.0.

use super::*;

pub(super) fn prefix(grouping: &[u8]) -> Result<Vec<u8>> {
    sortable_state::prefix(STATE_NAMESPACE, grouping)
}

pub(super) fn grouping(prefix: &[u8]) -> Result<&[u8]> {
    if prefix.len() < 11 || prefix[0] != STATE_NAMESPACE || &prefix[1..7] != sortable_state::FORMAT
    {
        return Err(DataFusionError::Execution(
            "unsupported ordered session key version".into(),
        ));
    }
    let len = u32::from_be_bytes(prefix[7..11].try_into().unwrap()) as usize;
    if len != prefix.len() - 11 {
        return Err(DataFusionError::Execution(
            "invalid ordered session partition framing".into(),
        ));
    }
    Ok(&prefix[11..])
}

pub(super) fn key(group: u32, prefix: &[u8], end_row: &[u8]) -> StateKey {
    let mut key = Vec::with_capacity(prefix.len() + end_row.len());
    key.extend_from_slice(prefix);
    key.extend_from_slice(end_row);
    StateKey {
        key_group: group,
        key,
    }
}

pub(super) fn bounds(bytes: &[u8]) -> Result<(i64, i64)> {
    if bytes.len() < STATE_MAGIC.len() + 16 || !bytes.starts_with(STATE_MAGIC) {
        return Err(DataFusionError::Execution(
            "unsupported ordered session accumulator version".into(),
        ));
    }
    let start = i64::from_le_bytes(bytes[5..13].try_into().unwrap());
    let end = i64::from_le_bytes(bytes[13..21].try_into().unwrap());
    if start >= end {
        return Err(DataFusionError::Execution(
            "invalid ordered session interval".into(),
        ));
    }
    Ok((start, end))
}

pub(super) fn decode(bytes: &[u8], calls: &[Call]) -> Result<(i64, i64, AccumulatorState)> {
    let (start, end) = bounds(bytes)?;
    let state = decode_state(&bytes[21..], calls)?;
    if state.row_count <= 0 {
        return Err(DataFusionError::Execution(
            "ordered session state must have positive row count".into(),
        ));
    }
    Ok((start, end, state))
}

pub(super) fn encode(start: i64, end: i64, state: &AccumulatorState) -> Vec<u8> {
    let value = encode_state(state);
    let mut bytes = Vec::with_capacity(21 + value.len());
    bytes.extend_from_slice(STATE_MAGIC);
    bytes.extend_from_slice(&start.to_le_bytes());
    bytes.extend_from_slice(&end.to_le_bytes());
    bytes.extend_from_slice(&value);
    bytes
}
