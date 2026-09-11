// Copyright 2026 StreamFusion Authors
// Licensed under the Apache License, Version 2.0.

use super::*;
use crate::planner::operators::regular_join::state_codec::StateView;
use crate::state::StateBatchWriter as Writer;

pub(super) use crate::state::CheckpointSource as Source;

pub(super) fn restore(
    destination: &mut dyn KeyedState,
    source: Source<'_>,
    group: u32,
    owner: &HostMemoryReservation,
) -> Result<()> {
    source.validate_unique_keys(group, owner)?;
    source.visit(group, owner, &mut |_, value| {
        StateView::parse(value)?;
        Ok(())
    })?;
    crate::state::require_empty_key_group(destination, group, owner)?;
    let mut memory = owner.sibling("legacy join migration write page");
    let mut key_memory = owner.sibling("legacy join migration logical key and compact value");
    let mut writes = 0;
    let mut writer = Writer::new(destination, &mut memory, 0, &mut writes)?;
    source.visit(group, owner, &mut |key, value| {
        let required = key.len().saturating_mul(2).saturating_add(65536);
        if required > key_memory.size() {
            key_memory.resize(required)?;
        }
        let key = StateKey {
            key_group: group,
            key: key.to_vec(),
        };
        migrate_entry(&key, value, &mut writer)
    })?;
    drop(key_memory);
    writer.finish()
}

fn migrate_entry(key: &StateKey, value: &[u8], writer: &mut Writer<'_>) -> Result<()> {
    let view = StateView::parse(value)?;
    let counts = view.rows.each_ref().map(|rows| rows.len() as u64);
    let total = counts[0] + counts[1];
    if total == 0 {
        return Ok(());
    }
    // Only a bounded singleton may be decoded to preserve the historical compact-layout
    // selection exactly. Wide singletons and every larger history remain borrowed.
    if total == 1 && value.len() <= MAX_COMPACT_BYTES {
        let compact = view.to_owned();
        if compact_eligible(&compact) {
            return writer.push(key.key.len().saturating_add(MAX_COMPACT_BYTES), 0, || {
                Ok(StateMutation {
                    key: manifest_key(key),
                    value: Some(encode_compact(&compact)?),
                })
            });
        }
    }
    for (side, rows) in view.rows.iter().enumerate() {
        for (id, associations, row) in rows.iter() {
            writer.push(
                key.key.len().saturating_add(43).saturating_add(row.len()),
                0,
                || {
                    Ok(StateMutation {
                        key: row_key(key, side, id),
                        value: Some(encode_row(id, associations, row)?),
                    })
                },
            )?;
        }
    }
    let manifest_bytes = 39usize.saturating_add(
        counts
            .iter()
            .map(|count| count.div_ceil(PAGE_ROWS) as usize)
            .sum::<usize>()
            .saturating_mul(16),
    );
    writer.push(
        key.key
            .len()
            .saturating_add(1)
            .saturating_add(manifest_bytes),
        0,
        || {
            Ok(StateMutation {
                key: manifest_key(key),
                value: Some(encode_dense_manifest(view.matchable, counts)),
            })
        },
    )
}

#[cfg(test)]
mod tests;
