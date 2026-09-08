// Copyright 2026 StreamFusion Authors
// Licensed under the Apache License, Version 2.0.

use super::*;

const MAGIC: &[u8] = b"SFJC\x01";
const MAX_BYTES: usize = 8 * 1024;

/// Small equality keys share one backend entry for their directory and payloads. Hot/large
/// keys retain stable independently writable pages. The cap bounds rewrite amplification.
pub(in super::super) fn eligible(state: &JoinState) -> bool {
    encoded_size(state).is_some()
}

fn encoded_size(state: &JoinState) -> Option<usize> {
    if state.left.len() + state.right.len() > PAGE_ROWS as usize {
        return None;
    }
    let bytes = [&state.left, &state.right]
        .into_iter()
        .fold(39usize, |bytes, rows| {
            pages(rows).fold(bytes, |bytes, (_, page)| {
                page.iter().fold(bytes.saturating_add(21), |bytes, row| {
                    bytes.saturating_add(16).saturating_add(row.row.len())
                })
            })
        });
    (bytes <= MAX_BYTES).then_some(bytes)
}

pub(super) fn is_compact(bytes: &[u8]) -> bool {
    bytes.starts_with(MAGIC)
}

pub(in super::super) fn encode(state: &JoinState) -> Result<Vec<u8>> {
    let size = encoded_size(state).ok_or_else(invalid)?;
    let mut bytes = encode_manifest(state);
    bytes.reserve_exact(size - bytes.len());
    bytes[..MAGIC.len()].copy_from_slice(MAGIC);
    for rows in [&state.left, &state.right] {
        for (_, rows) in pages(rows) {
            let length = 9 + rows.iter().map(|row| 16 + row.row.len()).sum::<usize>();
            bytes.extend_from_slice(&(length as u32).to_le_bytes());
            append_page(&mut bytes, rows);
        }
    }
    Ok(bytes)
}

pub(super) fn decode(bytes: &[u8]) -> Result<Manifest> {
    if bytes.len() > MAX_BYTES {
        return Err(invalid());
    }
    let mut reader = Reader::new(bytes, MAGIC)?;
    let mut manifest = read_manifest(&mut reader)?;
    let mut inline: [Vec<StoredRow>; 2] = Default::default();
    let mut count = 0;
    for (side, rows) in inline.iter_mut().enumerate() {
        for &page in &manifest.pages[side] {
            let length = reader.u32()? as usize;
            let decoded = decode_page(reader.take(length)?, page, manifest.next_row_id[side])?;
            count += decoded.len();
            if count > PAGE_ROWS as usize {
                return Err(invalid());
            }
            rows.extend(decoded);
        }
    }
    reader.finish()?;
    manifest.inline = Some(inline);
    Ok(manifest)
}
