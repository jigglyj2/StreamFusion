// Copyright 2026 StreamFusion Authors
// Licensed under the Apache License, Version 2.0.

//! A small per-key page directory and independently persisted row pages. Tombstones never
//! require rewriting surviving payload pages; empty pages leave the directory immediately.
use super::*;
use std::collections::BTreeSet;
pub(super) const PAGE_ROWS: u64 = 256;

pub(super) struct Directory {
    next: Vec<u64>,
    pages: Vec<BTreeSet<u64>>,
}
impl Directory {
    fn empty(inputs: usize) -> Self {
        Self {
            next: vec![0; inputs],
            pages: vec![BTreeSet::new(); inputs],
        }
    }
    pub(super) fn append(&mut self, input: usize) -> Result<u64> {
        let slot = self.next[input];
        self.next[input] = slot.checked_add(1).ok_or_else(|| {
            DataFusionError::ResourcesExhausted("multi-join row sequence overflow".into())
        })?;
        self.pages[input].insert(slot / PAGE_ROWS);
        Ok(slot)
    }
    fn encode(&self) -> Vec<u8> {
        let mut out = b"SFMD\x01".to_vec();
        out.extend_from_slice(&(self.next.len() as u32).to_le_bytes());
        for (next, pages) in self.next.iter().zip(&self.pages) {
            out.extend_from_slice(&next.to_le_bytes());
            out.extend_from_slice(&(pages.len() as u32).to_le_bytes());
            for page in pages {
                out.extend_from_slice(&page.to_le_bytes());
            }
        }
        out
    }
    fn decode(bytes: &[u8], inputs: usize) -> Result<Self> {
        if bytes.get(..5) != Some(b"SFMD\x01") {
            return Err(DataFusionError::Execution(
                "unsupported multi-join page directory".into(),
            ));
        }
        let mut offset = 5;
        if read_u32(bytes, &mut offset)? as usize != inputs {
            return Err(truncated());
        }
        let mut result = Self::empty(inputs);
        for input in 0..inputs {
            result.next[input] = read_u64(bytes, &mut offset)?;
            let count = read_u32(bytes, &mut offset)? as usize;
            if count > bytes.len().saturating_sub(offset) / 8 {
                return Err(truncated());
            }
            for _ in 0..count {
                let page = read_u64(bytes, &mut offset)?;
                if result.next[input] == 0
                    || page > (result.next[input] - 1) / PAGE_ROWS
                    || !result.pages[input].insert(page)
                {
                    return Err(truncated());
                }
            }
        }
        if offset != bytes.len() {
            return Err(truncated());
        }
        Ok(result)
    }
}

fn key(group: &StateKey, page: Option<(usize, u64)>) -> StateKey {
    let mut bytes = Vec::with_capacity(group.key.len() + 21);
    bytes.push(u8::from(page.is_some()));
    bytes.extend_from_slice(&(group.key.len() as u64).to_be_bytes());
    bytes.extend_from_slice(&group.key);
    if let Some((input, page)) = page {
        bytes.extend_from_slice(&(input as u32).to_be_bytes());
        bytes.extend_from_slice(&page.to_be_bytes());
    }
    StateKey {
        key_group: group.key_group,
        key: bytes,
    }
}

pub(super) fn load(
    backend: &dyn KeyedState,
    keys: Vec<StateKey>,
    inputs: usize,
    memory: &HostMemoryReservation,
) -> Result<(Vec<StagedState>, HostMemoryReservation)> {
    let headers = keys
        .iter()
        .map(|group| key(group, None))
        .collect::<Vec<_>>();
    let refs = headers
        .iter()
        .map(|key| StateKeyRef {
            key_group: key.key_group,
            key: &key.key,
        })
        .collect::<Vec<_>>();
    let values = backend.get_batch(&refs, memory)?;
    let mut workspace = crate::state::reserve_decoded_values(&values, memory)?;
    let mut staged = keys
        .into_iter()
        .zip(values)
        .map(|(key, bytes)| {
            let directory = bytes
                .map(|bytes| Directory::decode(bytes.as_ref(), inputs))
                .transpose()?
                .unwrap_or_else(|| Directory::empty(inputs));
            Ok(StagedState {
                key,
                value: MultiJoinState::empty(inputs),
                touched: false,
                directory,
                dirty: BTreeSet::new(),
            })
        })
        .collect::<Result<Vec<_>>>()?;
    let mut pages = Vec::new();
    for (group, entry) in staged.iter().enumerate() {
        for input in 0..inputs {
            for &page in &entry.directory.pages[input] {
                pages.push((group, input, page, key(&entry.key, Some((input, page)))));
            }
        }
    }
    if !pages.is_empty() {
        let refs = pages
            .iter()
            .map(|(_, _, _, key)| StateKeyRef {
                key_group: key.key_group,
                key: &key.key,
            })
            .collect::<Vec<_>>();
        let values = backend.get_batch(&refs, memory)?;
        let mut page_workspace = crate::state::reserve_decoded_values(&values, memory)?;
        let bytes = page_workspace.size();
        workspace.grow_from(&mut page_workspace, bytes)?;
        for ((group, input, page, _), value) in pages.into_iter().zip(values) {
            let value = value.ok_or_else(|| {
                DataFusionError::Execution("multi-join directory references a missing page".into())
            })?;
            let mut decoded = decode_state(value.as_ref(), 1)?;
            let rows = decoded.inputs.pop().unwrap();
            let entry = &mut staged[group];
            if rows.is_empty()
                || rows.len() > PAGE_ROWS as usize
                || rows.windows(2).any(|p| p[0].slot >= p[1].slot)
                || rows.iter().any(|row| {
                    row.slot / PAGE_ROWS != page || row.slot >= entry.directory.next[input]
                })
            {
                return Err(truncated());
            }
            entry.value.inputs[input].extend(rows);
        }
    }
    Ok((staged, workspace))
}

pub(super) fn mutations(
    staged: &mut [StagedState],
    memory: &HostMemoryReservation,
) -> Result<(Vec<StateMutation>, HostMemoryReservation)> {
    // Reserve one serialization workspace for all dirty pages before building the write batch.
    let mut allowance = 0usize;
    for entry in staged.iter().filter(|entry| entry.touched) {
        allowance = allowance.saturating_add(
            entry
                .directory
                .pages
                .iter()
                .map(|pages| pages.len() * 16)
                .sum::<usize>()
                + entry
                    .key
                    .key
                    .len()
                    .saturating_add(128)
                    .saturating_mul(entry.dirty.len().saturating_add(1))
                + 4096,
        );
        for &(input, page) in &entry.dirty {
            for row in entry.value.inputs[input]
                .iter()
                .filter(|row| row.slot / PAGE_ROWS == page)
            {
                allowance = allowance.saturating_add(
                    row.row.len().saturating_mul(2)
                        + row
                            .condition_values
                            .iter()
                            .map(|value| {
                                value.as_ref().map_or(8, |v| v.len().saturating_mul(2) + 8)
                            })
                            .sum::<usize>()
                        + 128,
                );
            }
        }
    }
    let mut workspace = memory.sibling("multi-join dirty-page serialization");
    workspace.resize(allowance)?;
    let mut changes = Vec::new();
    for entry in staged.iter_mut().filter(|entry| entry.touched) {
        for &(input, page) in &entry.dirty {
            let rows = entry.value.inputs[input]
                .iter()
                .filter(|row| row.slot / PAGE_ROWS == page)
                .cloned()
                .collect::<Vec<_>>();
            let value = if rows.is_empty() {
                entry.directory.pages[input].remove(&page);
                None
            } else {
                Some(encode_state(&MultiJoinState { inputs: vec![rows] }))
            };
            changes.push(StateMutation {
                key: key(&entry.key, Some((input, page))),
                value,
            });
        }
        changes.push(StateMutation {
            key: key(&entry.key, None),
            value: (!entry.value.is_empty()).then(|| entry.directory.encode()),
        });
    }
    Ok((changes, workspace))
}

pub(super) fn read_u64(bytes: &[u8], offset: &mut usize) -> Result<u64> {
    let end = offset.checked_add(8).ok_or_else(truncated)?;
    let value = bytes.get(*offset..end).ok_or_else(truncated)?;
    *offset = end;
    Ok(u64::from_le_bytes(value.try_into().unwrap()))
}
