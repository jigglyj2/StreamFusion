// Copyright 2026 StreamFusion Authors
// Licensed under the Apache License, Version 2.0

//! Ordered state for range-oriented operators. Point-only operators retain hash-based state.
use std::collections::BTreeMap;
use std::ops::Bound;

use datafusion::error::{DataFusionError, Result};

use super::{KeyedState, SnapshotBytes, StateKeyRef, StateMutation, StateReadBatch, StateValue};
use crate::memory_pool::HostMemoryReservation;

type Group = BTreeMap<Vec<u8>, Vec<u8>>;
// Conservative amortized allowance for B-tree nodes (including sparse nodes).
const ENTRY_OVERHEAD: usize = 192;

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
                    .map(|v| StateValue::Borrowed(v.as_slice())))
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
            if let Some((k, v)) = self.groups[index].remove_entry(m.key.key.as_slice()) {
                self.bytes -= k.capacity() + v.capacity() + ENTRY_OVERHEAD;
            }
            if let Some(v) = m.value {
                self.bytes += m.key.key.capacity() + v.capacity() + ENTRY_OVERHEAD;
                self.groups[index].insert(m.key.key, v);
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
            page.push((k.as_slice(), v.as_slice()));
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
mod tests {
    use super::*;
    use crate::memory_pool::tests_support::TestBroker;
    use crate::state::{RocksPluginKeyedState, StateKey};
    use std::sync::Arc;

    #[test]
    fn ordered_ranges_page_stop_and_restore_identically_on_both_backends() {
        let broker = Arc::new(TestBroker::new(16 << 20));
        let owner = HostMemoryReservation::new(broker.clone(), "ordered range test");
        let memory = OrderedMemoryKeyedState::new(3, 4, owner.sibling("memory")).unwrap();
        let directory = tempfile::tempdir().unwrap();
        let mut backends: Vec<Box<dyn KeyedState>> = vec![Box::new(memory)];
        if let Ok(plugin) = std::env::var("STREAMFUSION_TEST_ROCKSDB_PLUGIN") {
            backends.push(Box::new(
                RocksPluginKeyedState::open(
                    std::path::Path::new(&plugin),
                    directory.path(),
                    3,
                    4,
                    8 << 20,
                )
                .unwrap(),
            ));
        }
        let mut snapshots = Vec::new();
        for mut state in backends {
            state
                .write_batch(
                    (0..100u32)
                        .rev()
                        .map(|i| StateMutation {
                            key: StateKey {
                                key_group: 3,
                                key: i.to_be_bytes().to_vec(),
                            },
                            value: Some(vec![i as u8; 40]),
                        })
                        .chain(std::iter::once(StateMutation {
                            key: StateKey {
                                key_group: 4,
                                key: 42u32.to_be_bytes().to_vec(),
                            },
                            value: Some(vec![255]),
                        }))
                        .collect(),
                )
                .unwrap();
            let mut seen = Vec::new();
            state
                .visit_range(
                    3,
                    &20u32.to_be_bytes(),
                    Some(&50u32.to_be_bytes()),
                    7,
                    500,
                    &mut |page| {
                        assert!(page.len() <= 3); // 4-byte key + value + 100 bytes overhead.
                        seen.extend(
                            page.iter()
                                .map(|(k, _)| u32::from_be_bytes((*k).try_into().unwrap())),
                        );
                        Ok(true)
                    },
                )
                .unwrap();
            assert_eq!(seen, (20..50).collect::<Vec<_>>());
            let mut pages = 0;
            state
                .visit_range(3, &[], None, 1, 500, &mut |_| {
                    pages += 1;
                    Ok(false)
                })
                .unwrap();
            assert_eq!(pages, 1);
            state
                .visit_range(
                    3,
                    &50u32.to_be_bytes(),
                    Some(&20u32.to_be_bytes()),
                    1,
                    500,
                    &mut |_| panic!("empty reversed range"),
                )
                .unwrap();
            assert!(state
                .visit_range(3, &[], None, 1, 1, &mut |_| panic!("oversize entry"))
                .is_err());
            assert!(state
                .visit_range(2, &[], None, 1, 500, &mut |_| Ok(true))
                .is_err());
            let snapshot = state.snapshot_key_group(3, &owner).unwrap();
            let mut restored =
                OrderedMemoryKeyedState::new(3, 3, owner.sibling("restored")).unwrap();
            restored.restore_key_group(3, &snapshot, &owner).unwrap();
            assert_eq!(restored.snapshot_key_group(3, &owner).unwrap(), snapshot);
            snapshots.push(snapshot.to_vec());
        }
        if snapshots.len() == 2 {
            assert_eq!(snapshots[0], snapshots[1]);
        }
        drop(owner);
        assert_eq!(broker.reserved(), 0);
    }

    #[test]
    fn rejected_write_is_atomic_and_releases_its_budget() {
        let broker = Arc::new(TestBroker::new(4096));
        let owner = HostMemoryReservation::new(broker.clone(), "ordered budget test");
        let mut state = OrderedMemoryKeyedState::new(0, 0, owner.sibling("state")).unwrap();
        assert!(state
            .write_batch(vec![StateMutation {
                key: StateKey {
                    key_group: 0,
                    key: vec![1]
                },
                value: Some(vec![0; 8192])
            }])
            .is_err());
        assert!(state.groups[0].is_empty());
        drop(state);
        drop(owner);
        assert_eq!(broker.reserved(), 0);
    }
}
