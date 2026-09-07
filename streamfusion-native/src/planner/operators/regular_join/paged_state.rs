// Copyright 2026 StreamFusion Authors
// Licensed under the Apache License, Version 2.0

use super::paged_codec::*;
use super::*;

/// Two batched reads at input admission: manifests first, then all required pages. No lookup is
/// performed while evaluating individual input rows or draining output. Stable row identities
/// keep deletes from shifting all later pages.
pub(super) fn load(
    state: &dyn KeyedState,
    keys: Vec<StateKey>,
    owner: &mut HostMemoryReservation,
) -> Result<(Vec<StagedState>, u64)> {
    let manifest_keys = keys.iter().map(manifest_key).collect::<Vec<_>>();
    let manifest_refs = refs(&manifest_keys);
    let values = state.get_batch(&manifest_refs, owner)?;
    owner.try_grow(
        values
            .iter()
            .flatten()
            .fold(0usize, |n, v| n.saturating_add(v.len()))
            .saturating_mul(8),
    )?;
    let manifests = values
        .iter()
        .map(|value| value.as_ref().map(|v| decode_manifest(v)).transpose())
        .collect::<Result<Vec<_>>>()?;
    let mut page_keys = Vec::new();
    let mut locations = Vec::new();
    for (index, manifest) in manifests.iter().enumerate() {
        if let Some(manifest) = manifest {
            for side in 0..2 {
                owner.try_grow(
                    manifest.pages[side]
                        .len()
                        .saturating_mul(keys[index].key.len().saturating_add(256)),
                )?;
                for &page in &manifest.pages[side] {
                    page_keys.push(page_key(&keys[index], side, page));
                    locations.push((index, side, page));
                }
            }
        }
    }
    let mut staged = keys
        .into_iter()
        .zip(manifests)
        .map(|(key, manifest)| {
            let value = manifest
                .map(|m| JoinState {
                    next_row_id: m.next_row_id,
                    left_matchable: m.matchable[0],
                    right_matchable: m.matchable[1],
                    ..Default::default()
                })
                .unwrap_or_default();
            StagedState {
                key,
                value,
                original: JoinState::default(),
                touched: false,
            }
        })
        .collect::<Vec<_>>();
    if !page_keys.is_empty() {
        let refs = refs(&page_keys);
        let values = state.get_batch(&refs, owner)?;
        owner.try_grow(
            values
                .iter()
                .flatten()
                .fold(0usize, |n, v| n.saturating_add(v.len()))
                .saturating_mul(8),
        )?;
        for ((index, side, page), value) in locations.into_iter().zip(values) {
            let value = value.ok_or_else(|| {
                DataFusionError::Execution("regular join manifest references a missing page".into())
            })?;
            let state = &mut staged[index].value;
            let rows = decode_page(&value, page, state.next_row_id[side])?;
            if side == 0 {
                state.left.extend(rows);
            } else {
                state.right.extend(rows);
            }
        }
    }
    for entry in &mut staged {
        entry.original = entry.value.clone();
    }
    Ok((staged, if page_keys.is_empty() { 1 } else { 2 }))
}

pub(super) fn mutations(entry: &StagedState) -> Result<Vec<StateMutation>> {
    let mut mutations = Vec::new();
    for (side, (before, after)) in [
        (&entry.original.left, &entry.value.left),
        (&entry.original.right, &entry.value.right),
    ]
    .into_iter()
    .enumerate()
    {
        let mut before = pages(before).peekable();
        let mut after = pages(after).peekable();
        while before.peek().is_some() || after.peek().is_some() {
            let page = match (before.peek(), after.peek()) {
                (Some((left, _)), Some((right, _))) => (*left).min(*right),
                (Some((page, _)), None) | (None, Some((page, _))) => *page,
                _ => unreachable!(),
            };
            let old = if before.peek().is_some_and(|(id, _)| *id == page) {
                before.next().unwrap().1
            } else {
                &[]
            };
            let new = if after.peek().is_some_and(|(id, _)| *id == page) {
                after.next().unwrap().1
            } else {
                &[]
            };
            let unchanged = old.len() == new.len()
                && old.iter().zip(new).all(|(a, b)| {
                    a.id == b.id
                        && a.associations == b.associations
                        && (Arc::ptr_eq(&a.row, &b.row) || a.row == b.row)
                });
            if !unchanged {
                mutations.push(StateMutation {
                    key: page_key(&entry.key, side, page),
                    value: if new.is_empty() {
                        None
                    } else {
                        Some(encode_page(new)?)
                    },
                });
            }
        }
    }
    let old = (!(entry.original.left.is_empty() && entry.original.right.is_empty()))
        .then(|| encode_manifest(&entry.original));
    let new = (!(entry.value.left.is_empty() && entry.value.right.is_empty()))
        .then(|| encode_manifest(&entry.value));
    if old != new {
        mutations.push(StateMutation {
            key: manifest_key(&entry.key),
            value: new,
        });
    }
    Ok(mutations)
}

pub(super) fn batch_mutations(
    entries: &[StagedState],
    owner: &mut HostMemoryReservation,
) -> Result<Vec<StateMutation>> {
    let mut changes = Vec::new();
    for entry in entries.iter().filter(|entry| entry.touched) {
        admit_mutations(entry, owner)?;
        changes.extend(mutations(entry)?);
    }
    Ok(changes)
}

fn admit_mutations(entry: &StagedState, owner: &mut HostMemoryReservation) -> Result<()> {
    // Derived page keys repeat the logical equality key. Its size need not be proportional to
    // the stored payload (a bounded projection may retain no payload at all).
    let count = [
        &entry.original.left,
        &entry.original.right,
        &entry.value.left,
        &entry.value.right,
    ]
    .into_iter()
    .fold(2usize, |n, rows| n.saturating_add(pages(rows).count()));
    owner.try_grow(count.saturating_mul(entry.key.key.len().saturating_add(512)))
}

fn refs(keys: &[StateKey]) -> Vec<StateKeyRef<'_>> {
    keys.iter()
        .map(|key| StateKeyRef {
            key_group: key.key_group,
            key: &key.key,
        })
        .collect()
}

/// SFS1 remains the canonical envelope. Old SFRJ v1/v2 entries migrate on restore; newly saved
/// envelopes contain versioned manifests/pages and are identical for native memory and RocksDB.
pub(super) fn restore(
    state: &mut dyn KeyedState,
    group: u32,
    bytes: &[u8],
    owner: &HostMemoryReservation,
) -> Result<()> {
    let count = streamfusion_state_abi::validate_key_group_snapshot(group, bytes)
        .map_err(|e| DataFusionError::Execution(e.to_string()))?;
    let mut memory = owner.sibling("regular join canonical page validation and migration");
    memory.resize(
        bytes
            .len()
            .saturating_mul(16)
            .saturating_add(count.saturating_mul(512)),
    )?;
    let entries = decode_key_group_snapshot(group, bytes)?;
    let legacy = !entries.is_empty()
        && entries
            .iter()
            .all(|(_, value)| value.starts_with(STATE_MAGIC));
    if !legacy {
        decode_entries(group, &entries)?;
        // Validation is finished. The backend consumes the original canonical bytes, not
        // these decoded keys/pages; do not retain both workspaces through restore.
        drop(entries);
        drop(memory);
        return state.restore_key_group(group, bytes, owner);
    }
    let mut migrated = Vec::new();
    for (key, value) in entries {
        let value = decode_state(&value)?;
        let entry = StagedState {
            key: StateKey {
                key_group: group,
                key,
            },
            value,
            original: JoinState::default(),
            touched: true,
        };
        admit_mutations(&entry, &mut memory)?;
        migrated.extend(mutations(&entry)?);
    }
    migrated.sort_by(|left, right| left.key.key.cmp(&right.key.key));
    let size = 16
        + migrated
            .iter()
            .map(|entry| 8 + entry.key.key.len() + entry.value.as_ref().unwrap().len())
            .sum::<usize>();
    let mut writer = streamfusion_state_abi::SnapshotWriter::new(group, migrated.len(), size)
        .map_err(|e| DataFusionError::Execution(e.to_string()))?;
    for entry in migrated {
        writer
            .append(&entry.key.key, entry.value.as_ref().unwrap())
            .map_err(|e| DataFusionError::Execution(e.to_string()))?;
    }
    let bytes = writer
        .finish()
        .map_err(|e| DataFusionError::Execution(e.to_string()))?;
    state.restore_key_group(group, &bytes, owner)
}

pub(super) fn decode_entries(
    group: u32,
    entries: &[(Vec<u8>, Vec<u8>)],
) -> Result<Vec<(Vec<u8>, JoinState)>> {
    let index = entries
        .iter()
        .map(|(key, value)| (key.as_slice(), value.as_slice()))
        .collect::<std::collections::BTreeMap<_, _>>();
    if index.len() != entries.len() {
        return Err(DataFusionError::Execution(
            "duplicate regular join snapshot key".into(),
        ));
    }
    let mut consumed = 0;
    let mut result = Vec::new();
    for (key, value) in entries {
        if key.first() != Some(&0) {
            continue;
        }
        if !is_manifest(value) {
            return Err(DataFusionError::Execution(
                "invalid regular join manifest record".into(),
            ));
        }
        let manifest = decode_manifest(value)?;
        let logical = StateKey {
            key_group: group,
            key: key[1..].to_vec(),
        };
        let mut state = JoinState {
            next_row_id: manifest.next_row_id,
            left_matchable: manifest.matchable[0],
            right_matchable: manifest.matchable[1],
            ..Default::default()
        };
        consumed += 1;
        for side in 0..2 {
            for &page in &manifest.pages[side] {
                let key = page_key(&logical, side, page);
                let bytes = index.get(key.key.as_slice()).ok_or_else(|| {
                    DataFusionError::Execution("missing regular join snapshot page".into())
                })?;
                let rows = decode_page(bytes, page, state.next_row_id[side])?;
                if side == 0 {
                    state.left.extend(rows);
                } else {
                    state.right.extend(rows);
                }
                consumed += 1;
            }
        }
        if state.left.is_empty() && state.right.is_empty() {
            return Err(DataFusionError::Execution(
                "empty regular join manifest".into(),
            ));
        }
        result.push((logical.key, state));
    }
    if consumed != entries.len() {
        return Err(DataFusionError::Execution(
            "orphan or unknown regular join snapshot record".into(),
        ));
    }
    Ok(result)
}
