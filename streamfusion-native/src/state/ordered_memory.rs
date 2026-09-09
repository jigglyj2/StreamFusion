// Copyright 2026 StreamFusion Authors
// Licensed under the Apache License, Version 2.0

//! Ordered state for range-oriented operators. Point-only operators retain hash-based state.
use std::collections::BTreeMap;
use std::ops::Bound;

use datafusion::error::{DataFusionError, Result};

use super::{KeyedState, SnapshotBytes, StateKeyRef, StateMutation, StateReadBatch, StateValue};
use crate::memory_pool::HostMemoryReservation;

type Group = BTreeMap<Box<[u8]>, Box<[u8]>>;
// Immutable boxed keys/values use two-word descriptors instead of growable Vecs.
// Keep conservative sparse-node/allocator headroom, plus the separate root allowance.
const ENTRY_OVERHEAD: usize = 128;

pub(crate) struct OrderedMemoryKeyedState {
    first: u32,
    groups: Vec<Group>,
    bytes: usize,
    reservation: HostMemoryReservation,
}

impl OrderedMemoryKeyedState {
    pub(crate) fn new(
        first: u32,
        last: u32,
        mut reservation: HostMemoryReservation,
    ) -> Result<Self> {
        if first > last {
            return Err(DataFusionError::Execution(
                "invalid ordered key-group range".into(),
            ));
        }
        let count = (last as usize - first as usize) + 1;
        let bytes = count
            .checked_mul(std::mem::size_of::<Group>())
            .ok_or_else(|| {
                DataFusionError::ResourcesExhausted("ordered key-group directory overflow".into())
            })?;
        reservation.resize(bytes)?;
        Ok(Self {
            first,
            groups: (0..count).map(|_| Group::new()).collect(),
            bytes,
            reservation,
        })
    }

    fn index(&self, group: u32) -> Result<usize> {
        group
            .checked_sub(self.first)
            .map(|i| i as usize)
            .filter(|&i| i < self.groups.len())
            .ok_or_else(|| {
                DataFusionError::Execution(format!(
                    "key group {group} is not owned by this subtask"
                ))
            })
    }
}

impl KeyedState for OrderedMemoryKeyedState {
    fn get_batch<'a>(
        &'a self,
        keys: &[StateKeyRef<'_>],
        owner: &HostMemoryReservation,
    ) -> Result<StateReadBatch<'a>> {
        let reservation = StateReadBatch::admit(keys.len(), owner)?;
        let values = keys
            .iter()
            .map(|key| {
                Ok(self.groups[self.index(key.key_group)?]
                    .get(key.key)
                    .map(|v| StateValue::Borrowed(v.as_ref())))
            })
            .collect::<Result<Vec<_>>>()?;
        Ok(StateReadBatch::new(values, reservation))
    }

    fn write_batch(&mut self, mutations: Vec<StateMutation>) -> Result<()> {
        // Admit the complete batch before mutation, including duplicate-key transitions.
        let growth = mutations.iter().try_fold(0usize, |n, m| {
            self.index(m.key.key_group)?;
            Ok::<_, DataFusionError>(n.saturating_add(m.value.as_ref().map_or(0, |v| {
                m.key
                    .key
                    .capacity()
                    .saturating_add(v.capacity())
                    .saturating_add(ENTRY_OVERHEAD)
            })))
        })?;
        let new_roots = mutations
            .iter()
            .filter(|m| m.value.is_some())
            .map(|m| m.key.key_group)
            .collect::<std::collections::BTreeSet<_>>()
            .into_iter()
            .filter(|&g| self.groups[self.index(g).expect("validated group")].is_empty())
            .count();
        self.reservation.resize(
            self.bytes
                .saturating_add(growth)
                .saturating_add(new_roots.saturating_mul(1024)),
        )?;
        for m in mutations {
            let index = self.index(m.key.key_group)?;
            let was_empty = self.groups[index].is_empty();
            if let Some(v) = m.value {
                let key = m.key.key.into_boxed_slice();
                let value = v.into_boxed_slice();
                let key_bytes = key.len();
                let value_bytes = value.len();
                // BTreeMap::insert keeps an existing key and its index position. Updating
                // a partial must not remove/reinsert that key or rebalance the tree twice.
                if let Some(previous) = self.groups[index].insert(key, value) {
                    self.bytes = self.bytes - previous.len() + value_bytes;
                } else {
                    self.bytes += key_bytes + value_bytes + ENTRY_OVERHEAD;
                }
            } else if let Some((key, value)) = self.groups[index].remove_entry(m.key.key.as_slice())
            {
                self.bytes -= key.len() + value.len() + ENTRY_OVERHEAD;
            }
            if was_empty && !self.groups[index].is_empty() {
                self.bytes += 1024;
            }
            if !was_empty && self.groups[index].is_empty() {
                self.bytes -= 1024;
            }
        }
        self.reservation.resize(self.bytes)
    }

    fn visit_key_group(
        &self,
        group: u32,
        rows: usize,
        bytes: usize,
        visitor: &mut dyn FnMut(&[(&[u8], &[u8])]) -> Result<()>,
    ) -> Result<()> {
        self.visit_range(group, &[], None, rows, bytes, &mut |page| {
            visitor(page)?;
            Ok(true)
        })
    }

    fn visit_prefix(
        &self,
        group: u32,
        prefix: &[u8],
        rows: usize,
        bytes: usize,
        visitor: &mut dyn FnMut(&[(&[u8], &[u8])]) -> Result<()>,
    ) -> Result<()> {
        let end = super::prefix_end(prefix);
        self.visit_range(group, prefix, end.as_deref(), rows, bytes, &mut |page| {
            visitor(page)?;
            Ok(true)
        })
    }

    fn visit_range(
        &self,
        group: u32,
        start: &[u8],
        end: Option<&[u8]>,
        max_rows: usize,
        max_bytes: usize,
        visitor: &mut dyn FnMut(&[(&[u8], &[u8])]) -> Result<bool>,
    ) -> Result<()> {
        let group = &self.groups[self.index(group)?];
        if max_rows == 0 || max_bytes == 0 {
            return Err(DataFusionError::Execution(
                "state scan bounds must be positive".into(),
            ));
        }
        if end.is_some_and(|end| start >= end) {
            return Ok(());
        }
        let upper = end.map(Bound::Excluded).unwrap_or(Bound::Unbounded);
        let mut page = Vec::with_capacity(max_rows.min(max_bytes / 96));
        let mut bytes = 0usize;
        for (k, v) in group.range::<[u8], _>((Bound::Included(start), upper)) {
            if page.len() == max_rows {
                if !visitor(&page)? {
                    return Ok(());
                }
                page.clear();
                bytes = 0;
            }
            let size = k.len().saturating_add(v.len()).saturating_add(100);
            if size > max_bytes {
                return Err(DataFusionError::ResourcesExhausted(
                    "state scan entry exceeds the admitted page budget".into(),
                ));
            }
            if bytes.saturating_add(size) > max_bytes {
                if !visitor(&page)? {
                    return Ok(());
                }
                page.clear();
                bytes = 0;
            }
            page.push((k.as_ref(), v.as_ref()));
            bytes += size;
        }
        if !page.is_empty() {
            visitor(&page)?;
        }
        Ok(())
    }

    fn snapshot_key_group(
        &self,
        group: u32,
        owner: &HostMemoryReservation,
    ) -> Result<SnapshotBytes> {
        let entries = &self.groups[self.index(group)?];
        let bytes = entries
            .iter()
            .fold(16usize, |n, (k, v)| n.saturating_add(8 + k.len() + v.len()));
        let mut reservation = owner.sibling("ordered state snapshot");
        reservation.resize(bytes)?;
        let mut writer = streamfusion_state_abi::SnapshotWriter::new(group, entries.len(), bytes)
            .map_err(|e| DataFusionError::Execution(e.to_string()))?;
        for (k, v) in entries {
            writer
                .append(k, v)
                .map_err(|e| DataFusionError::Execution(e.to_string()))?;
        }
        let encoded = writer
            .finish()
            .map_err(|e| DataFusionError::Execution(e.to_string()))?;
        Ok(SnapshotBytes::owned(encoded, reservation))
    }

    fn restore_key_group(
        &mut self,
        group: u32,
        bytes: &[u8],
        owner: &HostMemoryReservation,
    ) -> Result<()> {
        let index = self.index(group)?;
        if !self.groups[index].is_empty() {
            return Err(DataFusionError::Execution(format!(
                "key group {group} was restored more than once"
            )));
        }
        let count = streamfusion_state_abi::validate_key_group_snapshot(group, bytes)
            .map_err(|e| DataFusionError::Execution(e.to_string()))?;
        let mut scratch = owner.sibling("ordered state restore");
        scratch.resize(
            bytes
                .len()
                .saturating_mul(2)
                .saturating_add(count.saturating_mul(128)),
        )?;
        let entries = streamfusion_state_abi::decode_key_group_snapshot(group, bytes)
            .map_err(|e| DataFusionError::Execution(e.to_string()))?;
        self.write_batch(
            entries
                .into_iter()
                .map(|(key, value)| StateMutation {
                    key: super::StateKey {
                        key_group: group,
                        key,
                    },
                    value: Some(value),
                })
                .collect(),
        )
    }
}

#[cfg(test)]
mod tests;
