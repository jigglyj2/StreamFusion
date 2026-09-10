// Copyright 2026 StreamFusion Authors
// Licensed under the Apache License, Version 2.0.

use super::*;

// Bound transport keys, locations and backend-owned read buffers separately from retained rows.
// All groups are loaded before the first input transition; these are never per-row state calls.
const READ_ROWS: usize = 4096;

pub(super) fn load_entries(
    state: &dyn KeyedState,
    staged: &mut [StagedState],
    requests: Vec<(usize, usize, Layout, Vec<u64>)>,
    owner: &mut HostMemoryReservation,
) -> Result<u64> {
    let mut remaining = requests
        .iter()
        .map(|(_, _, _, ids)| ids.len())
        .sum::<usize>();
    let mut locations = requests.into_iter().flat_map(|(index, side, layout, ids)| {
        ids.into_iter().map(move |id| (index, side, id, layout))
    });
    let mut reads = 0;
    while remaining != 0 {
        let count = remaining.min(READ_ROWS);
        let mut workspace = owner.sibling("regular join payload read transport");
        workspace.resize(count.saturating_mul(256))?;
        let chunk = locations.by_ref().take(count).collect::<Vec<_>>();
        workspace.try_grow(chunk.iter().fold(0usize, |bytes, &(index, _, _, _)| {
            bytes.saturating_add(staged[index].key.key.len().saturating_add(18))
        }))?;
        let keys = chunk
            .iter()
            .map(|&(index, side, id, layout)| entry_key(&staged[index].key, side, id, layout))
            .collect::<Vec<_>>();
        let refs = refs(&keys);
        let values = state.get_batch(&refs, owner)?;
        // Retained decoded payloads, growing row vectors and their shallow original-state clone.
        // Backend read buffers and this chunk's transport workspace drop before the next chunk.
        owner.try_grow(values.iter().flatten().try_fold(0usize, |bytes, value| {
            Ok::<_, DataFusionError>(bytes.saturating_add(decode_workspace(value)?))
        })?)?;
        for ((index, side, id, layout), value) in chunk.into_iter().zip(values.iter()) {
            let bytes = value.as_ref().ok_or_else(|| {
                DataFusionError::Execution(
                    "regular join manifest references a missing payload".into(),
                )
            })?;
            let state = &mut staged[index].value;
            let rows = decode_entry(bytes, id, state.next_row_id[side], layout)?;
            if side == 0 {
                state.left.extend(rows);
            } else {
                state.right.extend(rows);
            }
        }
        reads += 1;
        remaining -= count;
    }
    Ok(reads)
}
