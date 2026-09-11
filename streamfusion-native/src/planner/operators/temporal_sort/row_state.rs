// Copyright 2026 StreamFusion Authors
// Licensed under the Apache License, Version 2.0.

//! Version 3 stores a small arrival counter at the legacy group key and independent ordered rows.
//! The secondary Arrow row key determines ordering, and the arrival ordinal preserves Flink's
//! stable ties. Payloads live in values, outside the ordering index. Version 2 list values remain
//! readable and migrate only when their group receives another input batch.

use super::*;

const ROOT_HEADER: &[u8] = b"SFTS\x03";
const ROW_HEADER: &[u8] = b"SFTR\x01";
const ROW_PREFIX: u8 = 9;
const PAGE_ROWS: usize = 1024;
const PAGE_BYTES: usize = 256 << 10;

pub(super) struct PendingRows {
    pub mutations: Vec<StateMutation>,
    pub was_empty: Vec<bool>,
    _memory: HostMemoryReservation,
    _legacy_memory: HostMemoryReservation,
}

pub(super) struct LoadedRows {
    pub groups: Vec<Vec<BufferedRow>>,
    pub mutations: Vec<StateMutation>,
    _memory: HostMemoryReservation,
}

fn invalid(message: &str) -> DataFusionError {
    DataFusionError::Execution(format!("invalid temporal sort row state: {message}"))
}

fn next_arrival(value: &[u8]) -> Result<Option<u64>> {
    if !value.starts_with(ROOT_HEADER) {
        return Ok(None);
    }
    let next = u64::from_be_bytes(
        value[ROOT_HEADER.len()..]
            .try_into()
            .map_err(|_| invalid("counter length"))?,
    );
    if next == 0 {
        return Err(invalid("empty indexed group"));
    }
    Ok(Some(next))
}

fn prefix(root: &[u8]) -> Result<Vec<u8>> {
    let length = u32::try_from(root.len()).map_err(|_| invalid("group key length"))?;
    let mut prefix = Vec::with_capacity(5 + root.len());
    prefix.push(ROW_PREFIX);
    prefix.extend_from_slice(&length.to_be_bytes());
    prefix.extend_from_slice(root);
    Ok(prefix)
}

fn append_entry(
    mutations: &mut Vec<StateMutation>,
    group: u32,
    prefix: &[u8],
    ordinal: u64,
    row: BufferedRow,
) {
    let mut key = Vec::with_capacity(prefix.len() + row.sort_key.len() + 8);
    key.extend_from_slice(prefix);
    key.extend_from_slice(&row.sort_key);
    key.extend_from_slice(&ordinal.to_be_bytes());
    let mut value = Vec::with_capacity(ROW_HEADER.len() + 1 + row.row.len());
    value.extend_from_slice(ROW_HEADER);
    value.push(row.kind as u8);
    value.extend_from_slice(&row.row);
    mutations.push(StateMutation {
        key: StateKey {
            key_group: group,
            key,
        },
        value: Some(value),
    });
}

pub(super) fn append(
    state: &dyn KeyedState,
    incoming: Vec<(StateKey, Vec<BufferedRow>)>,
    owner: &HostMemoryReservation,
) -> Result<PendingRows> {
    let mut memory = owner.sibling("temporal sort row writes");
    // Include descriptors, the Arrow ordering keys, and row values before constructing mutations.
    let incoming_bytes = incoming
        .iter()
        .map(|(key, rows)| {
            rows.iter()
                .map(|row| key.key.len() + row.sort_key.len() + row.row.len() + 256)
                .sum::<usize>()
                .saturating_add(key.key.len() + 256)
        })
        .sum::<usize>();
    memory.resize(incoming_bytes.saturating_mul(2))?;
    let refs = incoming
        .iter()
        .map(|(key, _)| StateKeyRef {
            key_group: key.key_group,
            key: &key.key,
        })
        .collect::<Vec<_>>();
    let existing = state.get_batch(&refs, owner)?;
    let mut legacy_memory = owner.sibling("temporal sort legacy list migration");
    // Admit the legacy decoder and changed entries from framed bytes and row descriptors;
    // large opaque payloads must not acquire a multiplier intended for many small records.
    let legacy_bytes = existing.iter().zip(&incoming).try_fold(
        0usize,
        |total, (value, (root, _))| match value {
            Some(value) if !value.as_ref().starts_with(ROOT_HEADER) => {
                Ok::<_, DataFusionError>(total.saturating_add(legacy_state::workspace_size(
                    value.as_ref(),
                    root.key.len(),
                )?))
            }
            _ => Ok(total),
        },
    )?;
    legacy_memory.resize(legacy_bytes)?;
    let mut mutations = Vec::new();
    let mut was_empty = Vec::with_capacity(incoming.len());
    for ((root, rows), existing) in incoming.into_iter().zip(existing) {
        if rows.is_empty() {
            return Err(invalid("append requires a nonempty group"));
        }
        let prefix = prefix(&root.key)?;
        let mut next = 0u64;
        if let Some(value) = existing {
            if let Some(counter) = next_arrival(value.as_ref())? {
                next = counter;
            } else {
                for row in legacy_state::decode_rows(value.as_ref())? {
                    append_entry(&mut mutations, root.key_group, &prefix, next, row);
                    next = next
                        .checked_add(1)
                        .ok_or_else(|| invalid("arrival overflow"))?;
                }
            }
        }
        was_empty.push(next == 0);
        for row in rows {
            let ordinal = next;
            next = next
                .checked_add(1)
                .ok_or_else(|| invalid("arrival overflow"))?;
            append_entry(&mut mutations, root.key_group, &prefix, ordinal, row);
        }
        let mut value = Vec::with_capacity(ROOT_HEADER.len() + 8);
        value.extend_from_slice(ROOT_HEADER);
        value.extend_from_slice(&next.to_be_bytes());
        mutations.push(StateMutation {
            key: root,
            value: Some(value),
        });
    }
    Ok(PendingRows {
        mutations,
        was_empty,
        _memory: memory,
        _legacy_memory: legacy_memory,
    })
}

pub(super) fn load(
    state: &dyn KeyedState,
    roots: &[StateKey],
    owner: &HostMemoryReservation,
) -> Result<LoadedRows> {
    let mut memory = owner.sibling("temporal sort fired rows and removals");
    memory.resize(roots.iter().map(|key| key.key.len() + 256).sum())?;
    let refs = roots
        .iter()
        .map(|key| StateKeyRef {
            key_group: key.key_group,
            key: &key.key,
        })
        .collect::<Vec<_>>();
    let values = state.get_batch(&refs, owner)?;
    let mut groups = Vec::with_capacity(roots.len());
    let mut mutations = Vec::new();
    for (root, value) in roots.iter().zip(values) {
        let Some(value) = value else {
            continue;
        };
        let rows = if let Some(next) = next_arrival(value.as_ref())? {
            let prefix = prefix(&root.key)?;
            let mut rows = Vec::new();
            state.visit_prefix_admitted(
                root.key_group,
                &prefix,
                PAGE_ROWS,
                PAGE_BYTES,
                owner,
                &mut |page| {
                    // The backend owns this page; admit retained copies and vector growth before copying.
                    let required = page
                        .iter()
                        .map(|(key, value)| key.len() + value.len() + 256)
                        .sum::<usize>();
                    memory.resize(memory.size().saturating_add(required.saturating_mul(2)))?;
                    for (key, value) in page {
                        if !key.starts_with(&prefix) || key.len() < prefix.len() + 8 {
                            return Err(invalid("row ordering key"));
                        }
                        let ordinal = u64::from_be_bytes(key[key.len() - 8..].try_into().unwrap());
                        if ordinal >= next
                            || !value.starts_with(ROW_HEADER)
                            || value.len() < ROW_HEADER.len() + 1
                        {
                            return Err(invalid("row entry"));
                        }
                        let kind = value[ROW_HEADER.len()] as i8;
                        if !matches!(kind, INSERT | UPDATE_BEFORE | UPDATE_AFTER | DELETE) {
                            return Err(invalid("RowKind"));
                        }
                        rows.push(BufferedRow {
                            kind,
                            sort_key: key[prefix.len()..key.len() - 8].to_vec(),
                            row: value[ROW_HEADER.len() + 1..].to_vec(),
                        });
                        mutations.push(StateMutation {
                            key: StateKey {
                                key_group: root.key_group,
                                key: key.to_vec(),
                            },
                            value: None,
                        });
                    }
                    Ok(())
                },
            )?;
            if rows.len() as u64 != next {
                return Err(invalid("row count does not match arrival counter"));
            }
            rows
        } else {
            memory.resize(memory.size().saturating_add(legacy_state::workspace_size(
                value.as_ref(),
                root.key.len(),
            )?))?;
            legacy_state::decode_rows(value.as_ref())?
        };
        if !rows.is_empty() {
            groups.push(rows);
        }
        mutations.push(StateMutation {
            key: root.clone(),
            value: None,
        });
    }
    Ok(LoadedRows {
        groups,
        mutations,
        _memory: memory,
    })
}
