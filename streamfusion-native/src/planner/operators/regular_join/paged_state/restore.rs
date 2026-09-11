// Copyright 2026 StreamFusion Authors
// Licensed under the Apache License, Version 2.0.

use super::*;
use crate::state::RocksPluginKeyedState;

fn invalid(message: &str) -> DataFusionError {
    DataFusionError::Execution(format!("invalid regular join checkpoint: {message}"))
}

fn validate_page(
    bytes: &[u8],
    id: u64,
    next: u64,
    layout: Layout,
    memory: &mut HostMemoryReservation,
) -> Result<()> {
    let required = decode_workspace(bytes)?;
    if required > memory.size() {
        memory.resize(required)?;
    }
    decode_entry(bytes, id, next, layout)?;
    Ok(())
}

pub(super) fn validate_snapshot(
    group: u32,
    bytes: &[u8],
    owner: &HostMemoryReservation,
) -> Result<()> {
    let entries = streamfusion_state_abi::key_group_snapshot_entries(group, bytes)
        .map_err(|e| DataFusionError::Execution(e.to_string()))?;
    let mut memory = owner.sibling("join checkpoint borrowed index");
    // Borrow keys and values from the already admitted frame; never copy the payload into the
    // index or accumulate decoded histories just to verify page references.
    memory.resize(entries.len().saturating_mul(96))?;
    let count = entries.len();
    let index = entries.collect::<std::collections::BTreeMap<_, _>>();
    if index.len() != count {
        return Err(invalid("duplicate key"));
    }
    let mut consumed = 0usize;
    let mut payload_memory = owner.sibling("join checkpoint payload validation");
    let mut key_memory = owner.sibling("join checkpoint reference key");
    for (&key, &value) in &index {
        if key.first() != Some(&0) {
            continue;
        }
        let required = key.len().saturating_mul(4).saturating_add(128);
        if required > key_memory.size() {
            key_memory.resize(required)?;
        }
        let logical = StateKey {
            key_group: group,
            key: key[1..].to_vec(),
        };
        consumed += 1 + visit_manifest_entries(value, owner, |side, id, next, layout| {
            let key = entry_key(&logical, side, id, layout);
            let bytes = index
                .get(key.key.as_slice())
                .ok_or_else(|| invalid("missing page"))?;
            validate_page(bytes, id, next, layout, &mut payload_memory)
        })?;
    }
    if consumed != index.len() {
        return Err(invalid("orphan or unknown record"));
    }
    Ok(())
}

/// Validate a stable checkpoint before import, using bounded read batches for referenced pages.
/// No key-group snapshot, decoded partition, or expanded row-identity bitmap is materialized.
pub(in super::super) fn restore_from_checkpoint(
    state: &mut dyn KeyedState,
    source: &RocksPluginKeyedState,
    group: u32,
    owner: &HostMemoryReservation,
) -> Result<()> {
    let mut total = 0usize;
    let mut legacy = 0usize;
    source.visit_key_group_admitted(group, 1024, 256 << 10, owner, &mut |page| {
        total += page.len();
        legacy += page
            .iter()
            .filter(|(_, value)| value.starts_with(STATE_MAGIC))
            .count();
        Ok(())
    })?;
    if legacy > 0 {
        if legacy != total {
            return Err(invalid("mixed legacy and paged records"));
        }
        return legacy_restore::restore(
            state,
            legacy_restore::Source::Physical(source),
            group,
            owner,
        );
    }
    let mut consumed = 0usize;
    let mut references = owner.sibling("join checkpoint page references");
    source.visit_key_group_admitted(group, 256, 128 << 10, owner, &mut |page| {
        for &(key, value) in page {
            if key.first() != Some(&0) {
                continue;
            }
            // At most 256 framed keys plus their metadata are retained by the request queue.
            let limit = (65536 / key.len().saturating_add(128)).clamp(1, 256);
            let required = key.len().saturating_add(128).saturating_mul(limit + 2);
            if required > references.size() {
                references.resize(required)?;
            }
            let logical = StateKey {
                key_group: group,
                key: key[1..].to_vec(),
            };
            let mut pending = Vec::with_capacity(limit);
            consumed += 1 + visit_manifest_entries(value, owner, |side, id, next, layout| {
                pending.push((entry_key(&logical, side, id, layout), id, next, layout));
                if pending.len() == limit {
                    validate_pending(source, &mut pending, owner)?;
                }
                Ok(())
            })?;
            validate_pending(source, &mut pending, owner)?;
        }
        Ok(())
    })?;
    if consumed != total {
        return Err(invalid("orphan or unknown record"));
    }
    crate::state::import_key_group(state, source, group, owner, &mut |_, _| Ok(()))
}

fn validate_pending(
    source: &dyn KeyedState,
    pending: &mut Vec<(StateKey, u64, u64, Layout)>,
    owner: &HostMemoryReservation,
) -> Result<()> {
    if pending.is_empty() {
        return Ok(());
    }
    let keys = pending
        .iter()
        .map(|(key, ..)| StateKeyRef {
            key_group: key.key_group,
            key: &key.key,
        })
        .collect::<Vec<_>>();
    let values = source.get_batch(&keys, owner)?;
    let mut payload_memory = owner.sibling("join checkpoint payload validation");
    for ((_, id, next, layout), value) in pending.iter().zip(values.iter()) {
        let bytes = value.as_ref().ok_or_else(|| invalid("missing page"))?;
        validate_page(bytes, *id, *next, *layout, &mut payload_memory)?;
    }
    drop(values);
    drop(keys);
    pending.clear();
    Ok(())
}

#[cfg(test)]
mod tests;
