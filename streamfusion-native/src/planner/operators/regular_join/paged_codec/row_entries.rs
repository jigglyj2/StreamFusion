// Copyright 2026 StreamFusion Authors
// Licensed under the Apache License, Version 2.0.

use super::*;

const MAGIC: &[u8] = b"SFJI\x01";

/// Individual payloads use an unambiguous partition prefix and increasing stable row identity.
/// Flink partition hashing remains in the logical key; this suffix only orders stored rows.
pub(in super::super) fn row_key(key: &StateKey, side: usize, id: u64) -> StateKey {
    let mut bytes = Vec::with_capacity(key.key.len() + 18);
    bytes.push(2);
    bytes.extend_from_slice(&(key.key.len() as u64).to_be_bytes());
    bytes.extend_from_slice(&key.key);
    bytes.push(side as u8);
    bytes.extend_from_slice(&id.to_be_bytes());
    StateKey {
        key_group: key.key_group,
        key: bytes,
    }
}

pub(super) fn is_manifest(bytes: &[u8]) -> bool {
    bytes.starts_with(MAGIC)
}

/// One presence bitmap per 64 stable identities bounds directory growth independently of
/// payload width. Payload updates never rewrite another row's bytes.
pub(in super::super) fn encode(state: &JoinState) -> Vec<u8> {
    encode_with_unloaded(state, None)
}

pub(in super::super) fn encode_with_unloaded(
    state: &JoinState,
    unloaded: Option<&UnloadedRows>,
) -> Vec<u8> {
    let mut bytes = Vec::new();
    bytes.extend_from_slice(MAGIC);
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
    for (side, rows) in [&state.left, &state.right].into_iter().enumerate() {
        let retained = unloaded
            .filter(|u| u.side == side)
            .map_or(&[][..], |u| u.ids.as_slice());
        // Retained IDs precede the new accumulating rows. Opposite-side payloads are fully loaded.
        let ids = retained
            .iter()
            .copied()
            .chain(rows.iter().map(|row| row.id));
        append_bitmaps(&mut bytes, ids);
    }
    bytes
}

fn append_bitmaps(bytes: &mut Vec<u8>, ids: impl Iterator<Item = u64>) {
    let position = bytes.len();
    bytes.extend_from_slice(&0u64.to_le_bytes());
    let mut count = 0u64;
    let mut pending: Option<u64> = None;
    let mut bits = 0u64;
    for id in ids {
        let page = id / PAGE_ROWS;
        if pending.is_some_and(|previous| previous != page) {
            bytes.extend_from_slice(&pending.unwrap().to_le_bytes());
            bytes.extend_from_slice(&bits.to_le_bytes());
            count += 1;
            bits = 0;
        }
        pending = Some(page);
        bits |= 1 << (id % PAGE_ROWS);
    }
    if let Some(page) = pending {
        bytes.extend_from_slice(&page.to_le_bytes());
        bytes.extend_from_slice(&bits.to_le_bytes());
        count += 1;
    }
    bytes[position..position + 8].copy_from_slice(&count.to_le_bytes());
}

/// Validate before allocating expanded identity vectors, including hostile persisted bitmaps.
fn scan(
    bytes: &[u8],
    mut row: impl FnMut(usize, u64),
) -> Result<([Option<bool>; 2], [u64; 2], usize)> {
    let mut r = Reader::new(bytes, MAGIC)?;
    let mut matchable = [None; 2];
    for value in &mut matchable {
        *value = match r.take(1)?[0] {
            0 => None,
            1 => Some(false),
            2 => Some(true),
            _ => return Err(invalid()),
        };
    }
    let next = [r.u64()?, r.u64()?];
    let mut total = 0usize;
    for (side, &next) in next.iter().enumerate() {
        let count = r.u64()?;
        if count > (r.remaining() / 16) as u64 {
            return Err(invalid());
        }
        let mut previous = None;
        for _ in 0..count {
            let page = r.u64()?;
            let mut bits = r.u64()?;
            if page > u64::MAX / PAGE_ROWS || bits == 0 || previous.is_some_and(|p| p >= page) {
                return Err(invalid());
            }
            previous = Some(page);
            while bits != 0 {
                let id = page * PAGE_ROWS + u64::from(bits.trailing_zeros());
                if id >= next {
                    return Err(invalid());
                }
                total = total.checked_add(1).ok_or_else(invalid)?;
                row(side, id);
                bits &= bits - 1;
            }
        }
    }
    r.finish()?;
    Ok((matchable, next, total))
}

pub(super) fn workspace(bytes: &[u8]) -> Result<usize> {
    let (_, _, count) = scan(bytes, |_, _| {})?;
    Ok(bytes
        .len()
        .saturating_mul(8)
        .saturating_add(count.saturating_mul(16)))
}

pub(super) fn decode(bytes: &[u8]) -> Result<Manifest> {
    scan(bytes, |_, _| {})?;
    let mut ids: [Vec<u64>; 2] = Default::default();
    let (matchable, next_row_id, _) = scan(bytes, |side, id| ids[side].push(id))?;
    Ok(Manifest {
        layout: Layout::Rows,
        next_row_id,
        matchable,
        pages: ids,
        inline: None,
    })
}
