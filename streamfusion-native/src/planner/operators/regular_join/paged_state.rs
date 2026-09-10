// Copyright 2026 StreamFusion Authors
// Licensed under the Apache License, Version 2.0

use super::paged_codec::*;
use super::*;

/// Batched reads at input admission: manifests first, then bounded groups of payloads. No lookup is
/// performed while evaluating individual input rows or draining output. Stable row identities
/// keep deletes from shifting later payload identities.
pub(super) fn load(
    state: &dyn KeyedState,
    keys: Vec<StateKey>,
    owner: &mut HostMemoryReservation,
) -> Result<(Vec<StagedState>, u64)> {
    load_impl(state, keys, owner, None)
}

pub(super) fn load_for_accumulation(
    state: &dyn KeyedState,
    keys: Vec<StateKey>,
    owner: &mut HostMemoryReservation,
    side: usize,
) -> Result<(Vec<StagedState>, u64)> {
    load_impl(state, keys, owner, Some(side))
}

fn load_impl(
    state: &dyn KeyedState,
    keys: Vec<StateKey>,
    owner: &mut HostMemoryReservation,
    accumulating_side: Option<usize>,
) -> Result<(Vec<StagedState>, u64)> {
    let manifest_keys = keys.iter().map(manifest_key).collect::<Vec<_>>();
    let manifest_refs = refs(&manifest_keys);
    let values = state.get_batch(&manifest_refs, owner)?;
    owner.try_grow(values.iter().flatten().try_fold(0usize, |bytes, value| {
        Ok::<_, DataFusionError>(bytes.saturating_add(manifest_workspace(value)?))
    })?)?;
    let manifests = values
        .iter()
        .map(|value| value.as_ref().map(|v| decode_manifest(v)).transpose())
        .collect::<Result<Vec<_>>>()?;
    drop(values);
    drop(manifest_refs);
    drop(manifest_keys);
    let mut requests = Vec::new();
    let mut staged = Vec::with_capacity(keys.len());
    for (index, (key, manifest)) in keys.into_iter().zip(manifests).enumerate() {
        let original_layout = manifest.as_ref().map_or(Layout::Compact, |m| m.layout);
        let mut unloaded = None;
        let value = if let Some(manifest) = manifest {
            let [left, right] = if let Some(inline) = manifest.inline {
                inline
            } else {
                for (side, ids) in manifest.pages.into_iter().enumerate() {
                    if accumulating_side == Some(side)
                        && manifest.layout == Layout::Rows
                        && !ids.is_empty()
                    {
                        unloaded = Some(UnloadedRows { side, ids });
                    } else {
                        requests.push((index, side, manifest.layout, ids));
                    }
                }
                Default::default()
            };
            JoinState {
                next_row_id: manifest.next_row_id,
                left_matchable: manifest.matchable[0],
                right_matchable: manifest.matchable[1],
                left,
                right,
            }
        } else {
            JoinState::default()
        };
        staged.push(StagedState {
            key,
            value,
            original: JoinState::default(),
            original_layout,
            unloaded,
            touched: false,
        });
    }
    let reads = loading::load_entries(state, &mut staged, requests, owner)?;
    for entry in &mut staged {
        entry.original = entry.value.clone();
    }
    Ok((staged, 1 + reads))
}

mod loading;
mod mutations;
pub(super) use mutations::mutations;

pub(super) fn batch_mutations(
    entries: &[StagedState],
    owner: &mut HostMemoryReservation,
) -> Result<Vec<StateMutation>> {
    // One batch admission covers all changed keys; no JNI call per state entry.
    owner
        .try_grow(
            entries
                .iter()
                .filter(|entry| entry.touched)
                .fold(0usize, |bytes, entry| {
                    bytes.saturating_add(mutation_workspace(entry))
                }),
        )
        .map_err(|error| match error {
            DataFusionError::ResourcesExhausted(message) => DataFusionError::ResourcesExhausted(
                format!("regular join state mutation encoding: {message}"),
            ),
            error => error,
        })?;
    let mut changes = Vec::new();
    for entry in entries.iter().filter(|entry| entry.touched) {
        changes.extend(mutations(entry)?);
    }
    Ok(changes)
}

fn mutation_workspace(entry: &StagedState) -> usize {
    mutations::workspace(entry)
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
            original_layout: Layout::Pages,
            unloaded: None,
            touched: true,
        };
        memory.try_grow(mutation_workspace(&entry))?;
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
        let compact = manifest.inline.is_some();
        let [left, right] = manifest.inline.unwrap_or_default();
        let mut state = JoinState {
            next_row_id: manifest.next_row_id,
            left_matchable: manifest.matchable[0],
            right_matchable: manifest.matchable[1],
            left,
            right,
        };
        consumed += 1;
        for side in 0..2 {
            if compact {
                break;
            }
            for &page in &manifest.pages[side] {
                let key = entry_key(&logical, side, page, manifest.layout);
                let bytes = index.get(key.key.as_slice()).ok_or_else(|| {
                    DataFusionError::Execution("missing regular join snapshot page".into())
                })?;
                let rows = decode_entry(bytes, page, state.next_row_id[side], manifest.layout)?;
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
