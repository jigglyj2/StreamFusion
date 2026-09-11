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

/// Validated views borrow opaque payloads from the checkpoint; migration does not decode a
/// complete hot-key history into Arc payloads or a row directory merely to rewrite it.
pub(super) struct StateView<'a> {
    pub(super) matchable: [Option<bool>; 2],
    pub(super) rows: [RowsView<'a>; 2],
}

pub(super) struct RowsView<'a> {
    bytes: &'a [u8],
    count: usize,
}

impl<'a> StateView<'a> {
    pub(super) fn parse(bytes: &'a [u8]) -> Result<Self> {
        if bytes.len() < 13
            || &bytes[..4] != STATE_MAGIC
            || !matches!(bytes[4], LEGACY_STATE_VERSION | STATE_VERSION)
        {
            return Err(DataFusionError::Execution(
                "invalid native regular join state".into(),
            ));
        }
        let (matchable, mut offset) = if bytes[4] == STATE_VERSION {
            if bytes.len() < 15 {
                return Err(truncated());
            }
            (
                [decode_matchable(bytes[5])?, decode_matchable(bytes[6])?],
                7,
            )
        } else {
            ([None; 2], 5)
        };
        let rows = [
            RowsView::parse(bytes, &mut offset)?,
            RowsView::parse(bytes, &mut offset)?,
        ];
        if offset != bytes.len() {
            return Err(DataFusionError::Execution(
                "regular join state has trailing bytes".into(),
            ));
        }
        Ok(Self { matchable, rows })
    }

    pub(super) fn to_owned(&self) -> JoinState {
        let [left, right] = self.rows.each_ref().map(|rows| {
            rows.iter()
                .map(|(id, associations, row)| StoredRow {
                    id,
                    associations,
                    row: Arc::from(row),
                })
                .collect::<Vec<_>>()
        });
        JoinState {
            next_row_id: [left.len() as u64, right.len() as u64],
            left,
            right,
            left_matchable: self.matchable[0],
            right_matchable: self.matchable[1],
        }
    }
}

impl<'a> RowsView<'a> {
    pub(super) fn len(&self) -> usize {
        self.count
    }

    fn parse(bytes: &'a [u8], offset: &mut usize) -> Result<Self> {
        let count = read_u32(bytes, offset)? as usize;
        if count > bytes.len().saturating_sub(*offset) / 8 {
            return Err(truncated());
        }
        let start = *offset;
        for _ in 0..count {
            read_row(bytes, offset)?;
        }
        Ok(Self {
            bytes: &bytes[start..*offset],
            count,
        })
    }

    pub(super) fn iter(&self) -> impl Iterator<Item = (u64, i32, &'a [u8])> + '_ {
        let mut offset = 0;
        (0..self.count).map(move |id| {
            let (associations, row) =
                read_row(self.bytes, &mut offset).expect("validated immutable legacy join rows");
            (id as u64, associations, row)
        })
    }
}

fn read_row<'a>(bytes: &'a [u8], offset: &mut usize) -> Result<(i32, &'a [u8])> {
    let associations = read_i32(bytes, offset)?;
    let length = read_u32(bytes, offset)? as usize;
    let end = offset.checked_add(length).ok_or_else(truncated)?;
    let row = bytes.get(*offset..end).ok_or_else(truncated)?;
    *offset = end;
    Ok((associations, row))
}

pub(super) fn decode_state(bytes: &[u8]) -> Result<JoinState> {
    Ok(StateView::parse(bytes)?.to_owned())
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
