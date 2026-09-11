// Copyright 2026 StreamFusion Authors
// Licensed under the Apache License, Version 2.0.

use super::*;
use crate::state::StateBatchWriter;

/// Initialization only. Keep the old opaque value admitted, borrow its row frames, and write the
/// ordered index in bounded batches. Never construct the full decoded list or mutation vector.
pub(super) fn migrate_legacy_groups(
    state: &mut dyn KeyedState,
    group: u32,
    owner: &HostMemoryReservation,
    writes: &mut u64,
) -> Result<()> {
    let mut start = vec![ROWS_KEY_PREFIX];
    let end = [ROWS_KEY_PREFIX + 1];
    let mut metadata = owner.sibling("temporal legacy root page");
    metadata.resize(64 << 10)?;
    loop {
        let mut legacy = Vec::new();
        let mut after = None;
        let complete = state.visit_range_admitted(
            group,
            &start,
            Some(&end),
            128,
            64 << 10,
            owner,
            &mut |entries| {
                for (key, value) in entries {
                    if key.len() != 9 && *key != PROCESSING_TIME_ROWS_KEY {
                        return Err(row_state::invalid("timestamp group key"));
                    }
                    if row_state::next_arrival(value)?.is_none() {
                        legacy.push(key.to_vec());
                    }
                }
                after = entries.last().map(|(key, _)| {
                    let mut next = key.to_vec();
                    next.push(0);
                    next
                });
                Ok(false)
            },
        )?;
        for key in legacy {
            migrate_group(
                state,
                StateKey {
                    key_group: group,
                    key,
                },
                owner,
                writes,
            )?;
        }
        if complete {
            return Ok(());
        }
        start = after.ok_or_else(|| row_state::invalid("legacy root scan did not advance"))?;
    }
}

fn migrate_group(
    state: &mut dyn KeyedState,
    root: StateKey,
    owner: &HostMemoryReservation,
    writes: &mut u64,
) -> Result<()> {
    let mut memory = owner.sibling("temporal legacy encoded value and migration writes");
    let legacy = {
        let values = state.get_batch(
            &[StateKeyRef {
                key_group: root.key_group,
                key: &root.key,
            }],
            owner,
        )?;
        let value = values[0]
            .as_ref()
            .ok_or_else(|| row_state::invalid("legacy root disappeared"))?;
        // Memory state is borrowed and cannot stay borrowed while writing its replacement. RocksDB
        // also retains its admitted Arrow read through this copy; no decoded payload copies follow.
        memory.resize(value.len())?;
        value.to_vec()
    };
    let count = legacy_state::visit_rows(&legacy, &mut |_, _, _| Ok(()))?;
    let prefix = row_state::prefix(&root.key)?;
    let mut writer = StateBatchWriter::new(state, &mut memory, legacy.len(), writes)?;
    let mut ordinal = 0u64;
    legacy_state::visit_rows(&legacy, &mut |kind, sort_key, row| {
        let bytes = prefix
            .len()
            .saturating_add(sort_key.len())
            .saturating_add(8)
            .saturating_add(row_state::ROW_HEADER.len())
            .saturating_add(1)
            .saturating_add(row.len());
        writer.push(bytes, 0, || {
            let mut key = prefix.clone();
            key.extend_from_slice(sort_key);
            key.extend_from_slice(&ordinal.to_be_bytes());
            let mut value = row_state::ROW_HEADER.to_vec();
            value.push(kind as u8);
            value.extend_from_slice(row);
            Ok(StateMutation {
                key: StateKey {
                    key_group: root.key_group,
                    key,
                },
                value: Some(value),
            })
        })?;
        ordinal += 1;
        Ok(())
    })?;
    writer.push(root.key.len() + 13, 0, || {
        let value = if count == 0 {
            None
        } else {
            let mut value = row_state::ROOT_HEADER.to_vec();
            value.extend_from_slice(&(count as u64).to_be_bytes());
            Some(value)
        };
        Ok(StateMutation { key: root, value })
    })?;
    drop(legacy);
    writer.finish()
}

#[cfg(test)]
mod tests;
