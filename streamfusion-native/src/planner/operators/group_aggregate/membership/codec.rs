// Copyright 2026 StreamFusion Authors
// Licensed under the Apache License, Version 2.0.

use super::*;

const HEADER_MAGIC: &[u8; 4] = b"SFGD";
const HEADER_VERSION: u8 = 1;
const MEMBER_MAGIC: &[u8; 5] = b"SFDV\x01";

pub(in crate::planner::operators::group_aggregate) fn is_header(bytes: &[u8]) -> bool {
    bytes.starts_with(HEADER_MAGIC)
}

pub(in crate::planner::operators::group_aggregate) fn header_bytes(bytes: &[u8]) -> Result<&[u8]> {
    if !is_header(bytes) {
        return Ok(bytes);
    }
    if bytes.get(4) != Some(&HEADER_VERSION) {
        return Err(DataFusionError::Execution(
            "unsupported external DISTINCT header version".into(),
        ));
    }
    Ok(&bytes[5..])
}

pub(super) fn encode_header(state: &AccumulatorState) -> Vec<u8> {
    let inline = encode_state(state);
    let mut bytes = Vec::with_capacity(5 + inline.len());
    bytes.extend_from_slice(HEADER_MAGIC);
    bytes.push(HEADER_VERSION);
    bytes.extend_from_slice(&inline);
    bytes
}

pub(super) fn encode_counts(counts: &[i64]) -> Vec<u8> {
    let mut bytes = Vec::with_capacity(9 + counts.len() * 8);
    bytes.extend_from_slice(MEMBER_MAGIC);
    bytes.extend_from_slice(&(counts.len() as u32).to_le_bytes());
    for count in counts {
        bytes.extend_from_slice(&count.to_le_bytes());
    }
    bytes
}

pub(super) fn decode_counts(bytes: &[u8], count: usize) -> Result<Vec<i64>> {
    if bytes.len() != 9 + count * 8
        || !bytes.starts_with(MEMBER_MAGIC)
        || u32::from_le_bytes(bytes[5..9].try_into().unwrap()) as usize != count
    {
        return Err(DataFusionError::Execution(
            "invalid external DISTINCT count vector".into(),
        ));
    }
    Ok(bytes[9..]
        .chunks_exact(8)
        .map(|count| i64::from_le_bytes(count.try_into().unwrap()))
        .collect())
}
