// Copyright 2026 StreamFusion Authors
// Licensed under the Apache License, Version 2.0.

use super::*;

pub(super) fn prefix(grouping_row: &[u8]) -> Result<Vec<u8>> {
    sortable_state::prefix(STATE_NAMESPACE, grouping_row)
}

pub(super) fn grouping_row(prefix: &[u8]) -> Result<&[u8]> {
    if prefix.len() < 11 || prefix[0] != STATE_NAMESPACE || &prefix[1..7] != sortable_state::FORMAT
    {
        return Err(DataFusionError::Execution(
            "unsupported shared-slice key encoding".into(),
        ));
    }
    let length = u32::from_be_bytes(prefix[7..11].try_into().unwrap()) as usize;
    if length != prefix.len() - 11 {
        return Err(DataFusionError::Execution(
            "invalid shared-slice partition framing".into(),
        ));
    }
    Ok(&prefix[11..])
}

pub(super) fn key(key_group: u32, prefix: &[u8], end_row: &[u8]) -> StateKey {
    let mut key = Vec::with_capacity(prefix.len() + end_row.len());
    key.extend_from_slice(prefix);
    key.extend_from_slice(end_row);
    StateKey { key_group, key }
}

pub(super) fn encode(state: &AccumulatorState) -> Vec<u8> {
    let encoded = encode_state(state);
    let mut bytes = Vec::with_capacity(STATE_MAGIC.len() + encoded.len());
    bytes.extend_from_slice(STATE_MAGIC);
    bytes.extend_from_slice(&encoded);
    bytes
}

pub(super) fn decode(bytes: &[u8], calls: &[Call]) -> Result<AccumulatorState> {
    if !bytes.starts_with(STATE_MAGIC) {
        return Err(DataFusionError::Execution(
            "unsupported shared-slice accumulator encoding".into(),
        ));
    }
    decode_state(&bytes[STATE_MAGIC.len()..], calls)
}
