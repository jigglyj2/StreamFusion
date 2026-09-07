// Copyright 2026 StreamFusion Authors
// Licensed under the Apache License, Version 2.0.

use super::*;
pub(super) fn encode_state(state: &MultiJoinState) -> Vec<u8> {
    let mut bytes = Vec::new();
    bytes.extend_from_slice(STATE_MAGIC);
    bytes.push(STATE_VERSION);
    bytes.extend_from_slice(&(state.inputs.len() as u32).to_le_bytes());
    for rows in &state.inputs {
        bytes.extend_from_slice(&(rows.len() as u32).to_le_bytes());
        for row in rows {
            bytes.extend_from_slice(&row.slot.to_le_bytes());
            bytes.extend_from_slice(&(row.row.len() as u32).to_le_bytes());
            bytes.extend_from_slice(&row.row);
            bytes.extend_from_slice(&(row.condition_values.len() as u32).to_le_bytes());
            for value in &row.condition_values {
                match value {
                    Some(value) => {
                        bytes.extend_from_slice(&(value.len() as u32).to_le_bytes());
                        bytes.extend_from_slice(value);
                    }
                    None => bytes.extend_from_slice(&u32::MAX.to_le_bytes()),
                }
            }
        }
    }
    bytes
}

pub(super) fn decode_state(bytes: &[u8], expected_inputs: usize) -> Result<MultiJoinState> {
    if bytes.len() < 9 || &bytes[..4] != STATE_MAGIC || bytes[4] != STATE_VERSION {
        return Err(DataFusionError::Execution(
            "invalid native multi-join state".to_string(),
        ));
    }
    let mut offset = 5;
    let input_count = read_u32(bytes, &mut offset)? as usize;
    if input_count != expected_inputs {
        return Err(DataFusionError::Execution(format!(
            "multi-join state has {input_count} inputs, expected {expected_inputs}"
        )));
    }
    let mut inputs = Vec::with_capacity(input_count);
    for _ in 0..input_count {
        let count = read_u32(bytes, &mut offset)? as usize;
        if count > bytes.len().saturating_sub(offset) / 16 {
            return Err(truncated());
        }
        let mut rows = Vec::with_capacity(count);
        for _ in 0..count {
            let slot = pages::read_u64(bytes, &mut offset)?;
            let row = read_bytes(bytes, &mut offset)?;
            let condition_count = read_u32(bytes, &mut offset)? as usize;
            if condition_count > bytes.len().saturating_sub(offset) / 4 {
                return Err(truncated());
            }
            let mut condition_values = Vec::with_capacity(condition_count);
            for _ in 0..condition_count {
                let length = read_u32(bytes, &mut offset)?;
                condition_values.push(if length == u32::MAX {
                    None
                } else {
                    Some(read_bytes_with_length(bytes, &mut offset, length as usize)?)
                });
            }
            rows.push(StoredRow {
                slot,
                row,
                condition_values,
            });
        }
        inputs.push(rows);
    }
    if offset != bytes.len() {
        return Err(DataFusionError::Execution(
            "multi-join state has trailing bytes".to_string(),
        ));
    }
    Ok(MultiJoinState { inputs })
}

fn read_bytes(bytes: &[u8], offset: &mut usize) -> Result<Vec<u8>> {
    let length = read_u32(bytes, offset)? as usize;
    read_bytes_with_length(bytes, offset, length)
}

fn read_bytes_with_length(bytes: &[u8], offset: &mut usize, length: usize) -> Result<Vec<u8>> {
    let end = offset.checked_add(length).ok_or_else(truncated)?;
    let value = bytes.get(*offset..end).ok_or_else(truncated)?.to_vec();
    *offset = end;
    Ok(value)
}

pub(super) fn read_u32(bytes: &[u8], offset: &mut usize) -> Result<u32> {
    let end = offset.checked_add(4).ok_or_else(truncated)?;
    let value = bytes.get(*offset..end).ok_or_else(truncated)?;
    *offset = end;
    Ok(u32::from_le_bytes(value.try_into().unwrap()))
}

pub(super) fn truncated() -> DataFusionError {
    DataFusionError::Execution("truncated native multi-join state".to_string())
}
