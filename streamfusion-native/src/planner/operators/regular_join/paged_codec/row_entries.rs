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
    let mut bytes = header(
        [state.left_matchable, state.right_matchable],
        state.next_row_id,
    );
    for (side, rows) in [&state.left, &state.right].into_iter().enumerate() {
        // Preserve old bitmaps directly. Only new rows are grouped, including an append into
        // the retained directory's final partial bitmap; historical IDs are never enumerated.
        let bitmaps = unloaded
            .filter(|u| u.side == side)
            .into_iter()
            .flat_map(|u| u.ids.bitmaps())
            .chain(
                rows.chunk_by(|a, b| a.id / PAGE_ROWS == b.id / PAGE_ROWS)
                    .map(|rows| {
                        (
                            rows[0].id / PAGE_ROWS,
                            rows.iter()
                                .fold(0u64, |bits, row| bits | (1 << (row.id % PAGE_ROWS))),
                        )
                    }),
            );
        append_bitmaps(&mut bytes, bitmaps);
    }
    bytes
}

fn header(matchable: [Option<bool>; 2], next: [u64; 2]) -> Vec<u8> {
    let mut bytes = Vec::new();
    bytes.extend_from_slice(MAGIC);
    for value in matchable {
        bytes.push(match value {
            None => 0,
            Some(false) => 1,
            Some(true) => 2,
        });
    }
    for id in next {
        bytes.extend_from_slice(&id.to_le_bytes());
    }
    bytes
}

/// Legacy SFRJ assigns dense identities while decoding. Emit the same presence bitmaps directly,
/// without allocating an ID per row or retaining the payloads used to derive them.
pub(in super::super) fn encode_dense(matchable: [Option<bool>; 2], counts: [u64; 2]) -> Vec<u8> {
    let mut bytes = header(matchable, counts);
    for count in counts {
        append_bitmaps(
            &mut bytes,
            (0..count.div_ceil(PAGE_ROWS)).map(|page| {
                let rows = (count - page * PAGE_ROWS).min(PAGE_ROWS);
                (
                    page,
                    if rows == 64 {
                        u64::MAX
                    } else {
                        (1u64 << rows) - 1
                    },
                )
            }),
        );
    }
    bytes
}

fn append_bitmaps(bytes: &mut Vec<u8>, pages: impl Iterator<Item = (u64, u64)>) {
    let position = bytes.len();
    bytes.extend_from_slice(&0u64.to_le_bytes());
    let mut count = 0u64;
    let mut pending: Option<u64> = None;
    let mut bits = 0u64;
    for (page, page_bits) in pages {
        if pending.is_some_and(|previous| previous != page) {
            bytes.extend_from_slice(&pending.unwrap().to_le_bytes());
            bytes.extend_from_slice(&bits.to_le_bytes());
            count += 1;
            bits = 0;
        }
        pending = Some(page);
        bits |= page_bits;
    }
    if let Some(page) = pending {
        bytes.extend_from_slice(&page.to_le_bytes());
        bytes.extend_from_slice(&bits.to_le_bytes());
        count += 1;
    }
    bytes[position..position + 8].copy_from_slice(&count.to_le_bytes());
}

/// Validate bitmap framing without expanding the historical identity directory.
fn scan_bitmaps(
    bytes: &[u8],
    mut page_visit: impl FnMut(usize, u64, u64, u64) -> Result<()>,
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
            let bits = r.u64()?;
            if page > u64::MAX / PAGE_ROWS || bits == 0 || previous.is_some_and(|p| p >= page) {
                return Err(invalid());
            }
            previous = Some(page);
            let highest = page * PAGE_ROWS + u64::from(63 - bits.leading_zeros());
            if highest >= next {
                return Err(invalid());
            }
            total = total
                .checked_add(bits.count_ones() as usize)
                .ok_or_else(invalid)?;
            page_visit(side, page, bits, next)?;
        }
    }
    r.finish()?;
    Ok((matchable, next, total))
}

/// Snapshot validators may visit each referenced payload, without retaining an expanded directory.
pub(super) fn scan(
    bytes: &[u8],
    mut row: impl FnMut(usize, u64, u64) -> Result<()>,
) -> Result<([Option<bool>; 2], [u64; 2], usize)> {
    scan_bitmaps(bytes, |side, page, mut bits, next| {
        while bits != 0 {
            row(
                side,
                page * PAGE_ROWS + u64::from(bits.trailing_zeros()),
                next,
            )?;
            bits &= bits - 1;
        }
        Ok(())
    })
}

pub(super) fn workspace(bytes: &[u8]) -> Result<usize> {
    scan_bitmaps(bytes, |_, _, _, _| Ok(()))?;
    // The decoded directory stores one pair per persisted bitmap, with vector growth headroom.
    // A dense bitmap must never be charged or allocated as 64 individual row identities.
    Ok(bytes.len().saturating_mul(4).saturating_add(512))
}

pub(super) fn decode(bytes: &[u8]) -> Result<Manifest> {
    scan_bitmaps(bytes, |_, _, _, _| Ok(()))?;
    let mut bitmaps: [Vec<(u64, u64)>; 2] = Default::default();
    let (matchable, next_row_id, _) = scan_bitmaps(bytes, |side, page, bits, _| {
        bitmaps[side].push((page, bits));
        Ok(())
    })?;
    Ok(Manifest {
        layout: Layout::Rows,
        next_row_id,
        matchable,
        pages: bitmaps.map(EntryIds::from_bitmaps),
        inline: None,
    })
}
