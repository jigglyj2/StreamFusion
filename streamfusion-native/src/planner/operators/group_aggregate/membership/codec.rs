// Copyright 2026 StreamFusion Authors
// Licensed under the Apache License, Version 2.0.

use super::*;

const HEADER_MAGIC: &[u8; 4] = b"SFGD";
const MEMBER_MAGIC: &[u8; 5] = b"SFDV\x01";

pub(in crate::planner::operators::group_aggregate) fn is_header(bytes: &[u8]) -> bool {
    bytes.starts_with(HEADER_MAGIC)
}

pub(in crate::planner::operators::group_aggregate) fn header_bytes(bytes: &[u8]) -> Result<&[u8]> {
    if !is_header(bytes) {
        return Ok(bytes);
    }
    if !matches!(bytes.get(4), Some(1 | 2)) {
        return Err(DataFusionError::Execution(
            "unsupported external DISTINCT header version".into(),
        ));
    }
    Ok(&bytes[5..])
}

pub(super) fn encode_header(state: &AccumulatorState, mode: Mode) -> Vec<u8> {
    let inline = encode_state(state);
    let mut bytes = Vec::with_capacity(5 + inline.len());
    bytes.extend_from_slice(HEADER_MAGIC);
    bytes.push(if mode == Mode::Presence { 2 } else { 1 });
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

pub(super) fn encode_presence(counts: &[i64]) -> Vec<u8> {
    let mut bytes = vec![0; 9 + counts.len().div_ceil(8)];
    bytes[..5].copy_from_slice(b"SFDV\x02");
    bytes[5..9].copy_from_slice(&(counts.len() as u32).to_le_bytes());
    for (index, &count) in counts.iter().enumerate() {
        if count != 0 {
            bytes[9 + index / 8] |= 1 << (index % 8);
        }
    }
    bytes
}

pub(super) fn decode_members(bytes: &[u8], count: usize, mode: Mode) -> Result<Vec<i64>> {
    if mode == Mode::Counted {
        return decode_counts(bytes, count);
    }
    if bytes.len() != 9 + count.div_ceil(8)
        || !bytes.starts_with(b"SFDV\x02")
        || u32::from_le_bytes(bytes[5..9].try_into().unwrap()) as usize != count
        || (count % 8 != 0 && bytes.last().unwrap() >> (count % 8) != 0)
    {
        return Err(DataFusionError::Execution(
            "invalid external DISTINCT presence vector".into(),
        ));
    }
    Ok((0..count)
        .map(|index| i64::from((bytes[9 + index / 8] >> (index % 8)) & 1))
        .collect())
}
