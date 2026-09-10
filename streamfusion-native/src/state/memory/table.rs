// Copyright 2026 StreamFusion Authors
// Licensed under the Apache License, Version 2.0.

use super::entry::PackedEntry;
use ahash::RandomState;
use datafusion::error::{DataFusionError, Result};
use hashbrown::HashTable;
use std::mem::size_of;

// Internal allocation granularity, independent of Flink's key groups and persisted keys.
// A hot key group must not require a second whole-group hash table during growth.
pub(super) const SHARDS: usize = 64;
struct Shard {
    table: HashTable<PackedEntry>,
    hasher: RandomState,
}

impl Shard {
    fn allocation_size(&self) -> usize {
        self.table.allocation_size()
    }
}

pub(super) struct KeyGroupMap {
    routing: RandomState,
    shards: Vec<Shard>,
}

impl KeyGroupMap {
    pub(super) fn with_hasher(routing: RandomState) -> Self {
        Self {
            routing,
            shards: Vec::new(),
        }
    }

    pub(super) fn shard_index(&self, key: &[u8]) -> usize {
        // Separate routing/table seeds preserve hashbrown's bucket and fingerprint entropy.
        self.routing.hash_one(key) as usize & (SHARDS - 1)
    }

    pub(super) fn directory_growth(&self) -> usize {
        if self.shards.is_empty() {
            SHARDS * size_of::<Shard>()
        } else {
            0
        }
    }

    pub(super) fn growth_for_shard(&self, index: usize, additions: usize) -> (usize, usize) {
        let (len, capacity) = self
            .shards
            .get(index)
            .map_or((0, 0), |s| (s.table.len(), s.table.capacity()));
        let required = len.saturating_add(additions);
        if required > capacity {
            let old = self.shards.get(index).map_or(0, Shard::allocation_size);
            // Deleted slots can make capacity() smaller than the backing table. hashbrown
            // may double that table even when the live entries would fit its old allocation.
            let replacement = super::table_size_for_entries(required).max(old.saturating_mul(2));
            (replacement.saturating_sub(old), old)
        } else {
            (0, 0)
        }
    }

    pub(super) fn reserve_shard(&mut self, index: usize, additions: usize) -> Result<()> {
        if self.shards.is_empty() {
            self.shards
                .try_reserve_exact(SHARDS)
                .map_err(|error| DataFusionError::ResourcesExhausted(error.to_string()))?;
            self.shards.extend((0..SHARDS).map(|_| Shard {
                table: HashTable::new(),
                hasher: RandomState::new(),
            }));
        }
        let shard = &mut self.shards[index];
        shard
            .table
            .try_reserve(additions, |entry| shard.hasher.hash_one(entry.key()))
            .map_err(|error| DataFusionError::ResourcesExhausted(error.to_string()))
    }

    pub(super) fn allocation_size(&self) -> usize {
        self.shards.capacity() * size_of::<Shard>()
            + self
                .shards
                .iter()
                .map(Shard::allocation_size)
                .sum::<usize>()
    }

    pub(super) fn len(&self) -> usize {
        self.shards.iter().map(|shard| shard.table.len()).sum()
    }
    pub(super) fn is_empty(&self) -> bool {
        self.shards.iter().all(|shard| shard.table.is_empty())
    }

    pub(super) fn get(&self, key: &[u8]) -> Option<&[u8]> {
        let shard = self.shards.get(self.shard_index(key))?;
        shard
            .table
            .find(shard.hasher.hash_one(key), |entry| entry.key() == key)
            .map(PackedEntry::value)
    }

    #[cfg(test)]
    pub(super) fn get_key_value(&self, key: &[u8]) -> Option<(&[u8], &[u8])> {
        let shard = self.shards.get(self.shard_index(key))?;
        shard
            .table
            .find(shard.hasher.hash_one(key), |entry| entry.key() == key)
            .map(|entry| (entry.key(), entry.value()))
    }

    pub(super) fn contains_key(&self, key: &[u8]) -> bool {
        self.get(key).is_some()
    }

    // The caller has admitted payload/replacement storage and reserved table capacity.
    pub(super) fn insert(&mut self, key: Vec<u8>, value: Vec<u8>) -> (usize, usize) {
        let index = self.shard_index(&key);
        let shard = &mut self.shards[index];
        let hash = shard.hasher.hash_one(key.as_slice());
        if let Some(entry) = shard.table.find_mut(hash, |entry| entry.key() == key) {
            let old = entry.value().len();
            let new = value.len();
            entry.update(key, &value);
            (new, old)
        } else {
            let added = key.len().saturating_add(value.len());
            shard
                .table
                .insert_unique(hash, PackedEntry::new(key, &value), |entry| {
                    shard.hasher.hash_one(entry.key())
                });
            (added, 0)
        }
    }

    pub(super) fn remove_bytes(&mut self, key: &[u8]) -> usize {
        let index = self.shard_index(key);
        let Some(shard) = self.shards.get_mut(index) else {
            return 0;
        };
        shard
            .table
            .find_entry(shard.hasher.hash_one(key), |entry| entry.key() == key)
            .ok()
            .map(|entry| {
                let (entry, _) = entry.remove();
                entry.key().len().saturating_add(entry.value().len())
            })
            .unwrap_or(0)
    }

    pub(super) fn iter(&self) -> impl Iterator<Item = (&[u8], &[u8])> {
        self.entries().map(|entry| (entry.key(), entry.value()))
    }

    pub(super) fn entries(&self) -> impl Iterator<Item = &PackedEntry> {
        self.shards.iter().flat_map(|shard| shard.table.iter())
    }

    pub(super) fn extend(
        &mut self,
        entries: impl Iterator<Item = (Vec<u8>, Vec<u8>)>,
    ) -> Result<()> {
        for (key, value) in entries {
            let index = self.shard_index(&key);
            self.reserve_shard(index, 1)?;
            self.insert(key, value);
        }
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn packed_wide_keys_fit_a_share_that_cannot_hold_the_unpacked_directory() {
        use super::super::MemoryKeyedState;
        use crate::memory_pool::{tests_support::TestBroker, HostMemoryReservation};
        use crate::state::{KeyedState, StateKey, StateKeyRef, StateMutation};
        use std::sync::Arc;

        let limit = 15 << 20;
        let broker = Arc::new(TestBroker::new(limit));
        let owner = HostMemoryReservation::new(broker.clone(), "packed capacity");
        let mut state = MemoryKeyedState::new(0, 0, owner.sibling("state")).unwrap();
        *state.group_mut(0).unwrap() =
            KeyGroupMap::with_hasher(RandomState::with_seeds(1, 2, 3, 4));
        let key = |n: u64| {
            let mut key = vec![5; 56];
            key[..8].copy_from_slice(&n.to_le_bytes());
            key
        };
        for start in (0..120_000u64).step_by(1024) {
            state
                .write_batch(
                    (start..(start + 1024).min(120_000))
                        .map(|n| StateMutation {
                            key: StateKey {
                                key_group: 0,
                                key: key(n),
                            },
                            value: Some(vec![n as u8; 10]),
                        })
                        .collect(),
                )
                .unwrap();
        }
        let group = state.group(0).unwrap();
        let unpacked_tables: usize = group
            .shards
            .iter()
            .map(|shard| {
                hashbrown::HashMap::<Box<[u8]>, Box<[u8]>>::with_capacity(shard.table.len())
                    .allocation_size()
            })
            .sum();
        assert!(unpacked_tables + state.entry_bytes > limit);
        assert!(state.estimated_heap_size() <= limit);
        assert_eq!(state.entry_bytes, 120_000 * 66);
        for start in (0..120_000u64).step_by(128) {
            let keys = (start..(start + 128).min(120_000))
                .map(key)
                .collect::<Vec<_>>();
            let refs = keys
                .iter()
                .map(|key| StateKeyRef { key_group: 0, key })
                .collect::<Vec<_>>();
            let values = state.get_batch(&refs, &owner).unwrap();
            for (offset, value) in values.iter().enumerate() {
                assert_eq!(
                    value.as_deref(),
                    Some(&[(start + offset as u64) as u8; 10][..])
                );
            }
        }
        drop(state);
        assert_eq!(broker.reserved(), 0);
    }

    #[test]
    fn tombstone_triggered_replacement_fits_the_admitted_table_bound() {
        let mut table = KeyGroupMap::with_hasher(RandomState::with_seeds(1, 2, 3, 4));
        table.reserve_shard(0, 224).unwrap();
        let shard = &mut table.shards[0];
        shard.hasher = RandomState::with_seeds(5, 6, 7, 8);
        let map = &mut shard.table;
        let hash = |entry: &PackedEntry| shard.hasher.hash_one(entry.key());
        let full_capacity = map.capacity();
        for key in 0..full_capacity as u64 {
            let entry = PackedEntry::new(key.to_le_bytes().to_vec(), &[1]);
            map.insert_unique(hash(&entry), entry, hash);
        }
        for key in 0..(full_capacity / 3) as u64 {
            let bytes = key.to_le_bytes();
            map.find_entry(shard.hasher.hash_one(bytes.as_slice()), |entry| {
                entry.key() == bytes
            })
            .ok()
            .unwrap()
            .remove();
        }
        assert!(
            map.capacity() < full_capacity,
            "fixture must leave tombstones"
        );
        let additions = map.capacity() - map.len() + 1;
        assert!(map.len() + additions <= full_capacity);
        let old = map.allocation_size();
        let (growth, overlap) = table.growth_for_shard(0, additions);
        table.reserve_shard(0, additions).unwrap();
        let new = table.shards[0].allocation_size();
        assert!(
            new > old,
            "hashbrown replaces the backing table despite spare live slots"
        );
        assert!(old + growth >= new);
        assert!(old + growth + overlap >= old + new);
    }
}
