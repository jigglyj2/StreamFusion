// Copyright 2026 StreamFusion Authors
// Licensed under the Apache License, Version 2.0.

use super::*;

const STATE_MAGIC: &[u8; 4] = b"SFTS";
const STATE_VERSION: u8 = 2;

#[cfg(test)]
pub(super) fn encode_rows(rows: &[BufferedRow]) -> Result<Vec<u8>> {
    let count = u32::try_from(rows.len()).map_err(|_| {
        DataFusionError::Execution("temporal sort timestamp group is too large".to_string())
    })?;
    let capacity = rows.iter().try_fold(9usize, |capacity, row| {
        capacity
            .checked_add(1 + 4 + row.sort_key.len() + 4 + row.row.len())
            .ok_or_else(|| {
                DataFusionError::Execution(
                    "temporal sort encoded timestamp group is too large".to_string(),
                )
            })
    })?;
    let mut output = Vec::with_capacity(capacity);
    output.extend_from_slice(STATE_MAGIC);
    output.push(STATE_VERSION);
    output.extend_from_slice(&count.to_le_bytes());
    for row in rows {
        output.push(row.kind as u8);
        let sort_key_length = u32::try_from(row.sort_key.len()).map_err(|_| {
            DataFusionError::Execution("temporal sort key is too large".to_string())
        })?;
        output.extend_from_slice(&sort_key_length.to_le_bytes());
        output.extend_from_slice(&row.sort_key);
        let length = u32::try_from(row.row.len()).map_err(|_| {
            DataFusionError::Execution("temporal sort row is too large".to_string())
        })?;
        output.extend_from_slice(&length.to_le_bytes());
        output.extend_from_slice(&row.row);
    }
    Ok(output)
}

pub(super) fn workspace_size(bytes: &[u8], root_key_bytes: usize) -> Result<usize> {
    let count = row_count(bytes)?;
    Ok(bytes.len().saturating_mul(2).saturating_add(
        count
            .saturating_mul(root_key_bytes.saturating_add(256))
            .saturating_mul(2),
    ))
}

fn row_count(bytes: &[u8]) -> Result<usize> {
    if bytes.len() < 9 || &bytes[..4] != STATE_MAGIC || bytes[4] != STATE_VERSION {
        return Err(DataFusionError::Execution(
            "invalid temporal sort row state".into(),
        ));
    }
    let count = read_u32(bytes, &mut 5)? as usize;
    if count > (bytes.len() - 9) / 9 {
        return Err(DataFusionError::Execution(
            "invalid temporal sort row count".into(),
        ));
    }
    Ok(count)
}

pub(super) fn decode_rows(bytes: &[u8]) -> Result<Vec<BufferedRow>> {
    let count = row_count(bytes)?;
    let mut offset = 9;
    let mut rows = Vec::with_capacity(count);
    for _ in 0..count {
        let kind = *bytes.get(offset).ok_or_else(|| {
            DataFusionError::Execution("truncated temporal sort RowKind".to_string())
        })? as i8;
        if !matches!(kind, INSERT | UPDATE_BEFORE | UPDATE_AFTER | DELETE) {
            return Err(DataFusionError::Execution(
                "invalid temporal sort RowKind".into(),
            ));
        }
        offset += 1;
        let sort_key_length = read_u32(bytes, &mut offset)? as usize;
        let sort_key_end = offset.checked_add(sort_key_length).ok_or_else(|| {
            DataFusionError::Execution("temporal sort key length overflow".to_string())
        })?;
        let sort_key = bytes
            .get(offset..sort_key_end)
            .ok_or_else(|| DataFusionError::Execution("truncated temporal sort key".to_string()))?;
        offset = sort_key_end;
        let length = read_u32(bytes, &mut offset)? as usize;
        let end = offset.checked_add(length).ok_or_else(|| {
            DataFusionError::Execution("temporal sort row length overflow".to_string())
        })?;
        let row = bytes
            .get(offset..end)
            .ok_or_else(|| DataFusionError::Execution("truncated temporal sort row".to_string()))?;
        rows.push(BufferedRow {
            kind,
            sort_key: sort_key.to_vec(),
            row: row.to_vec(),
        });
        offset = end;
    }
    if offset != bytes.len() {
        return Err(DataFusionError::Execution(
            "temporal sort state has trailing bytes".to_string(),
        ));
    }
    Ok(rows)
}

fn read_u32(bytes: &[u8], offset: &mut usize) -> Result<u32> {
    let end = offset.checked_add(4).ok_or_else(|| {
        DataFusionError::Execution("temporal sort state offset overflow".to_string())
    })?;
    let value = bytes
        .get(*offset..end)
        .ok_or_else(|| DataFusionError::Execution("truncated temporal sort state".to_string()))?;
    *offset = end;
    Ok(u32::from_le_bytes(value.try_into().unwrap()))
}
