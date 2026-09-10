// Copyright 2026 StreamFusion Authors
// Licensed under the Apache License, Version 2.0.

//! Immutable bounded payload pages. Keys retain the first arrival ordinal; framing does
//! not change Arrow row ordering or Flink partition identity. Old v3 windows retire in place.

use super::*;

const MAGIC: &[u8; 5] = b"SFWP\x01";
const MAX_ROWS: usize = 256;
const TARGET_BYTES: usize = 16 << 10;

pub(super) fn append(
    keys: &WindowKeys,
    header: &mut Header,
    side: usize,
    rows: Vec<Vec<u8>>,
    mutations: &mut Vec<StateMutation>,
) -> Result<()> {
    let mut start = 0;
    while start < rows.len() {
        let mut end = start;
        let mut size = 9usize;
        while end < rows.len() && end - start < if header.paged { MAX_ROWS } else { 1 } {
            let next = size.saturating_add(4).saturating_add(rows[end].len());
            // A single wide row is admitted by the enclosing batch reservation.
            if end > start && next > TARGET_BYTES {
                break;
            }
            size = next;
            end += 1;
        }
        let first = header.counts()[side];
        let value = if header.paged {
            let mut value = Vec::with_capacity(size);
            value.extend_from_slice(MAGIC);
            value.extend_from_slice(&((end - start) as u32).to_le_bytes());
            for row in &rows[start..end] {
                let len = u32::try_from(row.len()).map_err(|_| invalid())?;
                value.extend_from_slice(&len.to_le_bytes());
                value.extend_from_slice(row);
                header.append(side, row.len())?;
            }
            value
        } else {
            header.append(side, rows[start].len())?;
            rows[start].clone()
        };
        mutations.push(StateMutation {
            key: keys.payload_key(side, first),
            value: Some(value),
        });
        start = end;
    }
    Ok(())
}

pub(super) struct Rows<'a> {
    value: &'a [u8],
    paged: bool,
    count: usize,
    bytes: usize,
}

impl<'a> Rows<'a> {
    pub(super) fn new(value: &'a [u8], paged: bool) -> Result<Self> {
        if !paged {
            return Ok(Self {
                value,
                paged,
                count: 1,
                bytes: value.len(),
            });
        }
        if value.len() < 9 || &value[..5] != MAGIC {
            return Err(invalid());
        }
        let count = u32::from_le_bytes(value[5..9].try_into().unwrap()) as usize;
        if count == 0 || count > MAX_ROWS {
            return Err(invalid());
        }
        let mut offset = 9usize;
        let mut bytes = 0usize;
        for _ in 0..count {
            let length = value
                .get(offset..offset.saturating_add(4))
                .ok_or_else(invalid)?;
            let length = u32::from_le_bytes(length.try_into().unwrap()) as usize;
            offset = offset
                .checked_add(4)
                .and_then(|n| n.checked_add(length))
                .ok_or_else(invalid)?;
            if offset > value.len() {
                return Err(invalid());
            }
            bytes = bytes.checked_add(length).ok_or_else(invalid)?;
        }
        if offset != value.len() || (count > 1 && value.len() > TARGET_BYTES) {
            return Err(invalid());
        }
        Ok(Self {
            value,
            paged,
            count,
            bytes,
        })
    }

    pub(super) fn len(&self) -> usize {
        self.count
    }
    pub(super) fn bytes(&self) -> usize {
        self.bytes
    }

    pub(super) fn iter(&self) -> impl Iterator<Item = &'a [u8]> + '_ {
        let mut offset = if self.paged { 9 } else { 0 };
        (0..self.count).map(move |_| {
            if !self.paged {
                return self.value;
            }
            let len =
                u32::from_le_bytes(self.value[offset..offset + 4].try_into().unwrap()) as usize;
            offset += 4;
            let row = &self.value[offset..offset + len];
            offset += len;
            row
        })
    }
}

fn invalid() -> DataFusionError {
    DataFusionError::Execution("invalid window join payload page framing".into())
}
