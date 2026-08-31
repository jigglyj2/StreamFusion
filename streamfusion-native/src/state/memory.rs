// Copyright 2026 StreamFusion Authors
// Licensed under the Apache License, Version 2.0

use std::borrow::Cow;
use std::mem::size_of;

use ahash::RandomState;
use datafusion::error::{DataFusionError, Result};
use hashbrown::hash_map::Entry;
use hashbrown::HashMap;

use crate::memory_pool::HostMemoryReservation;

use super::{snapshot, KeyedState, StateKeyRef, StateMutation};

type KeyGroupMap = HashMap<Vec<u8>, Vec<u8>, RandomState>;

/// In-memory state is independently owned by key group, making raw snapshots directly
/// redistributable when Flink changes parallelism.
pub(crate) struct MemoryKeyedState {
    first_key_group: u32,
    groups: Vec<KeyGroupMap>,
    reservation: HostMemoryReservation,
    entry_bytes: usize,
}

impl MemoryKeyedState {
    pub(crate) fn new(
        first_key_group: u32,
        last_key_group: u32,
        mut reservation: HostMemoryReservation,
    ) -> Result<Self> {
        if first_key_group > last_key_group {
            return Err(DataFusionError::Execution(format!(
                "invalid owned key-group range {first_key_group}..={last_key_group}"
            )));
        }
        let count = usize::try_from(last_key_group - first_key_group + 1).unwrap();
        reservation.resize(count.saturating_mul(size_of::<KeyGroupMap>()))?;
        let groups = (0..count)
            .map(|_| HashMap::with_hasher(RandomState::new()))
            .collect();
        Ok(Self {
            first_key_group,
            groups,
            reservation,
            entry_bytes: 0,
        })
    }

    fn group(&self, key_group: u32) -> Result<&KeyGroupMap> {
        let index = key_group.checked_sub(self.first_key_group).ok_or_else(|| {
            DataFusionError::Execution(format!(
                "key group {key_group} is not owned by this subtask"
            ))
        })? as usize;
        self.groups.get(index).ok_or_else(|| {
            DataFusionError::Execution(format!(
                "key group {key_group} is not owned by this subtask"
            ))
        })
    }

    fn group_mut(&mut self, key_group: u32) -> Result<&mut KeyGroupMap> {
        let index = key_group.checked_sub(self.first_key_group).ok_or_else(|| {
            DataFusionError::Execution(format!(
                "key group {key_group} is not owned by this subtask"
            ))
        })? as usize;
        self.groups.get_mut(index).ok_or_else(|| {
            DataFusionError::Execution(format!(
                "key group {key_group} is not owned by this subtask"
            ))
        })
    }

    fn estimated_heap_size(&self) -> usize {
        self.groups
            .capacity()
            .saturating_mul(size_of::<KeyGroupMap>())
            .saturating_add(self.entry_bytes)
            .saturating_add(
                self.groups
                    .iter()
                    .map(|group| table_heap_size(group.capacity()))
                    .sum(),
            )
    }
}

impl KeyedState for MemoryKeyedState {
    fn get_batch<'a>(&'a self, keys: &[StateKeyRef<'_>]) -> Result<Vec<Option<Cow<'a, [u8]>>>> {
        keys.iter()
            .map(|key| {
                Ok(self
                    .group(key.key_group)?
                    .get(key.key)
                    .map(|value| Cow::Borrowed(value.as_slice())))
            })
            .collect()
    }

    fn write_batch(&mut self, mutations: Vec<StateMutation>) -> Result<()> {
        // Admit a conservative upper bound before HashMap/Vec growth occurs. Shrink the
        // reservation to the observed capacities after the atomic operator batch is applied.
        let mutation_bytes = mutations.iter().fold(0usize, |total, mutation| {
            total
                .saturating_add(mutation.key.key.len())
                .saturating_add(mutation.value.as_ref().map_or(0, Vec::len))
                .saturating_add(bucket_bytes())
        });
        let current = self.estimated_heap_size();
        let capacities_before = self
            .groups
            .iter()
            .map(HashMap::capacity)
            .collect::<Vec<_>>();
        let table_growth_bound = capacities_before
            .iter()
            .map(|capacity| capacity.saturating_mul(bucket_bytes()))
            .sum::<usize>();
        self.reservation.resize(
            current
                .saturating_add(mutation_bytes)
                .saturating_add(table_growth_bound),
        )?;
        for mutation in mutations {
            let (added, removed) = {
                let group = self.group_mut(mutation.key.key_group)?;
                match mutation.value {
                    Some(value) => match group.entry(mutation.key.key) {
                        Entry::Occupied(mut entry) => {
                            let old_value_capacity = entry.get().capacity();
                            let new_value_capacity = value.capacity();
                            entry.insert(value);
                            (new_value_capacity, old_value_capacity)
                        }
                        Entry::Vacant(entry) => {
                            let added = entry.key().capacity().saturating_add(value.capacity());
                            entry.insert(value);
                            (added, 0)
                        }
                    },
                    None => group
                        .remove_entry(mutation.key.key.as_slice())
                        .map(|(key, value)| (0, key.capacity().saturating_add(value.capacity())))
                        .unwrap_or((0, 0)),
                }
            };
            self.entry_bytes = self
                .entry_bytes
                .saturating_sub(removed)
                .saturating_add(added);
        }
        debug_assert!(self
            .groups
            .iter()
            .zip(capacities_before)
            .all(|(group, before)| group.capacity() >= before));
        self.reservation.resize(self.estimated_heap_size())?;
        Ok(())
    }

    fn snapshot_key_group(&self, key_group: u32) -> Result<Vec<u8>> {
        let mut entries = self.group(key_group)?.iter().collect::<Vec<_>>();
        entries.sort_unstable_by(|left, right| left.0.cmp(right.0));
        snapshot::encode(
            key_group,
            entries
                .into_iter()
                .map(|(key, value)| (key.as_slice(), value.as_slice())),
        )
    }

    fn restore_key_group(&mut self, key_group: u32, bytes: &[u8]) -> Result<()> {
        let current = self.estimated_heap_size();
        self.reservation
            .resize(current.saturating_add(bytes.len().saturating_mul(3)))?;
        let entries = match snapshot::decode(key_group, bytes) {
            Ok(entries) => entries,
            Err(error) => {
                self.reservation.resize(current)?;
                return Err(error);
            }
        };
        if !self.group(key_group)?.is_empty() {
            self.reservation.resize(current)?;
            return Err(DataFusionError::Execution(format!(
                "key group {key_group} was restored more than once"
            )));
        }
        self.entry_bytes = self.entry_bytes.saturating_add(
            entries
                .iter()
                .map(|(key, value)| key.capacity().saturating_add(value.capacity()))
                .sum::<usize>(),
        );
        self.group_mut(key_group)?.extend(entries);
        self.reservation.resize(self.estimated_heap_size())?;
        Ok(())
    }
}

fn table_heap_size(capacity: usize) -> usize {
    if capacity == 0 {
        return 0;
    }
    capacity
        .saturating_mul(8)
        .saturating_div(7)
        .saturating_mul(bucket_bytes())
        .saturating_add(16)
}

const fn bucket_bytes() -> usize {
    size_of::<(Vec<u8>, Vec<u8>)>() + 1
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::sync::Arc;

    use crate::memory_pool::{tests_support::TestBroker, HostMemoryReservation};
    use crate::state::StateKey;

    fn new_state(first: u32, last: u32, broker: Arc<TestBroker>) -> MemoryKeyedState {
        MemoryKeyedState::new(
            first,
            last,
            HostMemoryReservation::new(broker, "test native keyed state"),
        )
        .unwrap()
    }

    #[test]
    fn snapshots_and_restores_one_key_group_without_exposing_rows() {
        let broker = Arc::new(TestBroker::new(1 << 20));
        let mut state = new_state(2, 3, broker.clone());
        state
            .write_batch(vec![StateMutation {
                key: StateKey {
                    key_group: 2,
                    key: vec![1],
                },
                value: Some(vec![9, 0, 0, 0, 0, 0, 0, 0, 4, 5]),
            }])
            .unwrap();

        let snapshot = state.snapshot_key_group(2).unwrap();
        let mut restored = new_state(2, 2, broker.clone());
        restored.restore_key_group(2, &snapshot).unwrap();

        assert_eq!(
            restored
                .get_batch(&[StateKeyRef {
                    key_group: 2,
                    key: &[1],
                }])
                .unwrap()[0]
                .as_deref(),
            Some(&[9, 0, 0, 0, 0, 0, 0, 0, 4, 5][..])
        );
        drop(restored);
        drop(state);
        assert_eq!(broker.reserved(), 0);
    }
}
