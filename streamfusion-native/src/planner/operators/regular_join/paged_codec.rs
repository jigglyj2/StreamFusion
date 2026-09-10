// Copyright 2026 StreamFusion Authors
// Licensed under the Apache License, Version 2.0

use super::*;

mod compact;
mod row_entries;
pub(super) use row_entries::{
    encode as encode_rows_manifest, encode_with_unloaded as encode_rows_with_unloaded, row_key,
};

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(super) enum Layout {
    Compact,
    Pages,
    Rows,
}
pub(super) use compact::{eligible as compact_eligible, encode as encode_compact};

pub(super) const PAGE_ROWS: u64 = 64;
const MANIFEST_MAGIC: &[u8] = b"SFJM\x01";
const PAGE_MAGIC: &[u8] = b"SFJP\x01";

pub(super) struct Manifest {
    pub(super) layout: Layout,
    pub(super) next_row_id: [u64; 2],
    pub(super) matchable: [Option<bool>; 2],
    pub(super) pages: [Vec<u64>; 2],
    pub(super) inline: Option<[Vec<StoredRow>; 2]>,
}

pub(super) fn manifest_key(key: &StateKey) -> StateKey {
    let mut bytes = Vec::with_capacity(key.key.len() + 1);
    bytes.push(0);
    bytes.extend_from_slice(&key.key);
    StateKey {
        key_group: key.key_group,
        key: bytes,
    }
}

pub(super) fn page_key(key: &StateKey, side: usize, page: u64) -> StateKey {
    let mut bytes = Vec::with_capacity(key.key.len() + 18);
    bytes.push(1);
    bytes.extend_from_slice(&(key.key.len() as u64).to_le_bytes());
    bytes.extend_from_slice(&key.key);
    bytes.push(side as u8);
    bytes.extend_from_slice(&page.to_le_bytes());
    StateKey {
        key_group: key.key_group,
        key: bytes,
    }
}

pub(super) fn pages(rows: &[StoredRow]) -> impl Iterator<Item = (u64, &[StoredRow])> {
    rows.chunk_by(|left, right| left.id / PAGE_ROWS == right.id / PAGE_ROWS)
        .map(|rows| (rows[0].id / PAGE_ROWS, rows))
}

pub(super) fn encode_manifest(state: &JoinState) -> Vec<u8> {
    let mut bytes = Vec::new();
    bytes.extend_from_slice(MANIFEST_MAGIC);
    for value in [state.left_matchable, state.right_matchable] {
        bytes.push(match value {
            None => 0,
            Some(false) => 1,
            Some(true) => 2,
        });
    }
    for id in state.next_row_id {
        bytes.extend_from_slice(&id.to_le_bytes());
    }
    for rows in [&state.left, &state.right] {
        let count = pages(rows).count();
        bytes.extend_from_slice(&(count as u64).to_le_bytes());
        for (page, _) in pages(rows) {
            bytes.extend_from_slice(&page.to_le_bytes());
        }
    }
    bytes
}

pub(super) fn decode_manifest(bytes: &[u8]) -> Result<Manifest> {
    if row_entries::is_manifest(bytes) {
        return row_entries::decode(bytes);
    }
    if compact::is_compact(bytes) {
        return compact::decode(bytes);
    }
    let mut reader = Reader::new(bytes, MANIFEST_MAGIC)?;
    let manifest = read_manifest(&mut reader)?;
    reader.finish()?;
    Ok(manifest)
}

fn read_manifest(reader: &mut Reader<'_>) -> Result<Manifest> {
    let mut matchable = [None; 2];
    for value in &mut matchable {
        *value = match reader.take(1)?[0] {
            0 => None,
            1 => Some(false),
            2 => Some(true),
            _ => return Err(invalid()),
        };
    }
    let next_row_id = [reader.u64()?, reader.u64()?];
    let mut pages = [Vec::new(), Vec::new()];
    for side in 0..2 {
        let count = reader.u64()?;
        if count > (reader.remaining() / 8) as u64 {
            return Err(invalid());
        }
        pages[side].reserve(count as usize);
        for _ in 0..count {
            let page = reader.u64()?;
            if page > u64::MAX / PAGE_ROWS
                || page * PAGE_ROWS >= next_row_id[side]
                || pages[side].last().is_some_and(|&previous| previous >= page)
            {
                return Err(invalid());
            }
            pages[side].push(page);
        }
    }
    Ok(Manifest {
        layout: Layout::Pages,
        next_row_id,
        matchable,
        pages,
        inline: None,
    })
}

pub(super) fn encode_page(rows: &[StoredRow]) -> Result<Vec<u8>> {
    if rows.is_empty()
        || rows.len() > PAGE_ROWS as usize
        || rows.iter().any(|row| row.row.len() > u32::MAX as usize)
    {
        return Err(invalid());
    }
    let mut bytes =
        Vec::with_capacity(9 + rows.iter().map(|row| 16 + row.row.len()).sum::<usize>());
    append_page(&mut bytes, rows);
    Ok(bytes)
}

// Callers validate page bounds before reserving output; compact entries also cap total bytes.
fn append_page(bytes: &mut Vec<u8>, rows: &[StoredRow]) {
    bytes.extend_from_slice(PAGE_MAGIC);
    bytes.extend_from_slice(&(rows.len() as u32).to_le_bytes());
    for row in rows {
        bytes.extend_from_slice(&row.id.to_le_bytes());
        bytes.extend_from_slice(&row.associations.to_le_bytes());
        bytes.extend_from_slice(&(row.row.len() as u32).to_le_bytes());
        bytes.extend_from_slice(&row.row);
    }
}

pub(super) fn decode_page(bytes: &[u8], page: u64, next_id: u64) -> Result<Vec<StoredRow>> {
    let mut reader = Reader::new(bytes, PAGE_MAGIC)?;
    let count = reader.u32()? as usize;
    if count == 0 || count > PAGE_ROWS as usize || count > reader.remaining() / 16 {
        return Err(invalid());
    }
    let mut rows: Vec<StoredRow> = Vec::with_capacity(count);
    for _ in 0..count {
        let id = reader.u64()?;
        if id / PAGE_ROWS != page || id >= next_id || rows.last().is_some_and(|row| row.id >= id) {
            return Err(invalid());
        }
        let associations = reader.u32()? as i32;
        let length = reader.u32()? as usize;
        rows.push(StoredRow {
            id,
            associations,
            row: Arc::from(reader.take(length)?),
        });
    }
    reader.finish()?;
    Ok(rows)
}

/// Batch admission from page headers: payload bytes plus coarse row-vector/Arc headroom.
/// Original and updated states share payload Arcs; they do not duplicate wide row bytes.
pub(super) fn decode_workspace(bytes: &[u8]) -> Result<usize> {
    let mut reader = Reader::new(bytes, PAGE_MAGIC)?;
    let count = reader.u32()? as usize;
    if count == 0 || count > PAGE_ROWS as usize || count > reader.remaining() / 16 {
        return Err(invalid());
    }
    Ok(bytes
        .len()
        .saturating_mul(2)
        .saturating_add(count.saturating_mul(128)))
}

/// Compact records contain payloads as well as directory metadata. Keep their decoded
/// payload/row-vector admission separate from the small external-page directory estimate.
pub(super) fn manifest_workspace(bytes: &[u8]) -> Result<usize> {
    if row_entries::is_manifest(bytes) {
        row_entries::workspace(bytes)
    } else if compact::is_compact(bytes) {
        compact::workspace(bytes)
    } else {
        Ok(bytes.len().saturating_mul(8))
    }
}

pub(super) fn is_manifest(bytes: &[u8]) -> bool {
    bytes.starts_with(MANIFEST_MAGIC)
        || compact::is_compact(bytes)
        || row_entries::is_manifest(bytes)
}

fn invalid() -> DataFusionError {
    DataFusionError::Execution("invalid paged regular join state".into())
}

struct Reader<'a> {
    bytes: &'a [u8],
    offset: usize,
}
impl<'a> Reader<'a> {
    fn new(bytes: &'a [u8], magic: &[u8]) -> Result<Self> {
        if !bytes.starts_with(magic) {
            return Err(invalid());
        }
        Ok(Self {
            bytes,
            offset: magic.len(),
        })
    }
    fn take(&mut self, length: usize) -> Result<&'a [u8]> {
        let end = self.offset.checked_add(length).ok_or_else(invalid)?;
        let bytes = self.bytes.get(self.offset..end).ok_or_else(invalid)?;
        self.offset = end;
        Ok(bytes)
    }
    fn u64(&mut self) -> Result<u64> {
        Ok(u64::from_le_bytes(self.take(8)?.try_into().unwrap()))
    }
    fn u32(&mut self) -> Result<u32> {
        Ok(u32::from_le_bytes(self.take(4)?.try_into().unwrap()))
    }
    fn remaining(&self) -> usize {
        self.bytes.len() - self.offset
    }
    fn finish(self) -> Result<()> {
        if self.remaining() == 0 {
            Ok(())
        } else {
            Err(invalid())
        }
    }
}

pub(super) fn entry_key(key: &StateKey, side: usize, id: u64, layout: Layout) -> StateKey {
    match layout {
        Layout::Rows => row_key(key, side, id),
        _ => page_key(key, side, id),
    }
}

pub(super) fn decode_entry(
    bytes: &[u8],
    id: u64,
    next: u64,
    layout: Layout,
) -> Result<Vec<StoredRow>> {
    let rows = decode_page(
        bytes,
        if layout == Layout::Rows {
            id / PAGE_ROWS
        } else {
            id
        },
        next,
    )?;
    if layout == Layout::Rows && (rows.len() != 1 || rows[0].id != id) {
        return Err(invalid());
    }
    Ok(rows)
}
