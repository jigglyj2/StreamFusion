// Copyright 2026 StreamFusion Authors
// Licensed under the Apache License, Version 2.0

use super::*;

pub(super) fn encode_state(state: &JoinState) -> Vec<u8> {
    let mut bytes = Vec::new();
    bytes.extend_from_slice(STATE_MAGIC);
    bytes.push(STATE_VERSION);
    bytes.push(encode_matchable(state.left_matchable));
    bytes.push(encode_matchable(state.right_matchable));
    for rows in [&state.left, &state.right] {
        bytes.extend_from_slice(&(rows.len() as u32).to_le_bytes());
        for row in rows {
            bytes.extend_from_slice(&row.associations.to_le_bytes());
            bytes.extend_from_slice(&(row.row.len() as u32).to_le_bytes());
            bytes.extend_from_slice(&row.row);
        }
    }
    bytes
}

pub(super) fn decode_state(bytes: &[u8]) -> Result<JoinState> {
    if bytes.len() < 13
        || &bytes[..4] != STATE_MAGIC
        || !matches!(bytes[4], LEGACY_STATE_VERSION | STATE_VERSION)
    {
        return Err(DataFusionError::Execution(
            "invalid native regular join state".to_string(),
        ));
    }
    let version = bytes[4];
    let (left_matchable, right_matchable, mut offset) = if version == STATE_VERSION {
        if bytes.len() < 15 {
            return Err(truncated());
        }
        (decode_matchable(bytes[5])?, decode_matchable(bytes[6])?, 7)
    } else {
        (None, None, 5)
    };
    let left = decode_rows(bytes, &mut offset)?;
    let right = decode_rows(bytes, &mut offset)?;
    if offset != bytes.len() {
        return Err(DataFusionError::Execution(
            "regular join state has trailing bytes".to_string(),
        ));
    }
    Ok(JoinState {
        next_row_id: [left.len() as u64, right.len() as u64],
        left,
        right,
        left_matchable,
        right_matchable,
    })
}

fn encode_matchable(value: Option<bool>) -> u8 {
    match value {
        None => 0,
        Some(false) => 1,
        Some(true) => 2,
    }
}

fn decode_matchable(value: u8) -> Result<Option<bool>> {
    match value {
        0 => Ok(None),
        1 => Ok(Some(false)),
        2 => Ok(Some(true)),
        other => Err(DataFusionError::Execution(format!(
            "invalid bounded regular join matchability byte {other}"
        ))),
    }
}

fn decode_rows(bytes: &[u8], offset: &mut usize) -> Result<Vec<StoredRow>> {
    let count = read_u32(bytes, offset)? as usize;
    if count > bytes.len().saturating_sub(*offset) / 8 {
        return Err(truncated());
    }
    let mut rows = Vec::with_capacity(count);
    for id in 0..count {
        let associations = read_i32(bytes, offset)?;
        let length = read_u32(bytes, offset)? as usize;
        let end = offset.checked_add(length).ok_or_else(truncated)?;
        rows.push(StoredRow {
            id: id as u64,
            associations,
            row: Arc::from(bytes.get(*offset..end).ok_or_else(truncated)?),
        });
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

fn read_i32(bytes: &[u8], offset: &mut usize) -> Result<i32> {
    let end = offset.checked_add(4).ok_or_else(truncated)?;
    let value = bytes.get(*offset..end).ok_or_else(truncated)?;
    *offset = end;
    Ok(i32::from_le_bytes(value.try_into().unwrap()))
}

fn truncated() -> DataFusionError {
    DataFusionError::Execution("truncated native regular join state".to_string())
}
