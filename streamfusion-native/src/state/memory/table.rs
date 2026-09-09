// Copyright 2026 StreamFusion Authors
// Licensed under the Apache License, Version 2.0.

use super::StateBytes;
use ahash::RandomState;
use datafusion::error::{DataFusionError, Result};
use hashbrown::{hash_map::Entry, HashMap};
use std::mem::size_of;

// Internal allocation granularity, independent of Flink's key groups and persisted keys.
// A hot key group must not require a second whole-group hash table during growth.
pub(super) const SHARDS: usize = 64;
type Shard = HashMap<StateBytes, StateBytes, RandomState>;

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
            .map_or((0, 0), |s| (s.len(), s.capacity()));
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
            self.shards
                .extend((0..SHARDS).map(|_| HashMap::with_hasher(RandomState::new())));
        }
        self.shards[index]
            .try_reserve(additions)
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
        self.shards.iter().map(Shard::len).sum()
    }
    pub(super) fn is_empty(&self) -> bool {
        self.shards.iter().all(Shard::is_empty)
    }

    pub(super) fn get(&self, key: &[u8]) -> Option<&StateBytes> {
        self.shards.get(self.shard_index(key))?.get(key)
    }

    #[cfg(test)]
    pub(super) fn get_key_value(&self, key: &[u8]) -> Option<(&StateBytes, &StateBytes)> {
        self.shards.get(self.shard_index(key))?.get_key_value(key)
    }

    pub(super) fn contains_key(&self, key: &[u8]) -> bool {
        self.get(key).is_some()
    }

    pub(super) fn entry(
        &mut self,
        key: StateBytes,
    ) -> Entry<'_, StateBytes, StateBytes, RandomState> {
        let index = self.shard_index(&key);
        self.shards[index].entry(key)
    }

    pub(super) fn remove_entry(&mut self, key: &[u8]) -> Option<(StateBytes, StateBytes)> {
        let index = self.shard_index(key);
        self.shards.get_mut(index)?.remove_entry(key)
    }

    pub(super) fn iter(&self) -> impl Iterator<Item = (&StateBytes, &StateBytes)> {
        self.shards.iter().flat_map(Shard::iter)
    }

    pub(super) fn extend(
        &mut self,
        entries: impl Iterator<Item = (StateBytes, StateBytes)>,
    ) -> Result<()> {
        for (key, value) in entries {
            let index = self.shard_index(&key);
            self.reserve_shard(index, 1)?;
            self.shards[index].insert(key, value);
        }
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn tombstone_triggered_replacement_fits_the_admitted_table_bound() {
        let mut table = KeyGroupMap::with_hasher(RandomState::with_seeds(1, 2, 3, 4));
        table.reserve_shard(0, 224).unwrap();
        let map = &mut table.shards[0];
        *map = HashMap::with_capacity_and_hasher(224, RandomState::with_seeds(5, 6, 7, 8));
        let full_capacity = map.capacity();
        for key in 0..full_capacity as u64 {
            map.insert(
                key.to_le_bytes().to_vec().into_boxed_slice(),
                vec![1].into_boxed_slice(),
            );
        }
        for key in 0..(full_capacity / 3) as u64 {
            map.remove(key.to_le_bytes().as_slice());
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
