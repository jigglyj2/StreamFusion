// Copyright 2026 StreamFusion Authors
// Licensed under the Apache License, Version 2.0

use crate::state::StateValue;
use std::mem::size_of;

use ahash::RandomState;
use datafusion::error::{DataFusionError, Result};

use crate::memory_pool::HostMemoryReservation;

use super::{snapshot, KeyedState, StateKeyRef, StateMutation};

mod entry;
mod table;
use table::KeyGroupMap;

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
            .map(|_| KeyGroupMap::with_hasher(RandomState::new()))
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
            .saturating_add(self.groups.iter().map(KeyGroupMap::allocation_size).sum())
    }
}

impl KeyedState for MemoryKeyedState {
    fn get_batch<'a>(
        &'a self,
        keys: &[StateKeyRef<'_>],
        owner: &HostMemoryReservation,
    ) -> Result<super::StateReadBatch<'a>> {
        let reservation = super::StateReadBatch::admit(keys.len(), owner)?;
        let values = keys
            .iter()
            .map(|key| {
                Ok(self
                    .group(key.key_group)?
                    .get(key.key)
                    .map(|value| StateValue::Borrowed(value.as_ref())))
            })
            .collect::<Result<Vec<_>>>()?;
        Ok(super::StateReadBatch::new(values, reservation))
    }

    fn write_batch(&mut self, mutations: Vec<StateMutation>) -> Result<()> {
        let current = self.estimated_heap_size();
        let mut payload_overlap = 0usize;
        let grows = mutations.iter().try_fold(0usize, |growth, mutation| {
            let old = self
                .group(mutation.key.key_group)?
                .get(mutation.key.key.as_slice());
            Ok::<_, DataFusionError>(match (&mutation.value, old) {
                (Some(value), Some(old)) => {
                    if value.len() != old.len() {
                        payload_overlap =
                            payload_overlap.max(mutation.key.key.len().saturating_add(old.len()));
                    }
                    growth.saturating_add(value.capacity().saturating_sub(old.len()))
                }
                (Some(value), None) => growth
                    .saturating_add(mutation.key.key.capacity())
                    .saturating_add(value.capacity())
                    .saturating_add(bucket_bytes()),
                (None, _) => growth,
            })
        })?;
        // Only an insertion into a full, affected table can require a second table.
        // Value growth never grows a table, and spare capacity already belongs to the
        // persistent reservation. Count additions before admitting any table allocation.
        let mut workspace = self
            .reservation
            .sibling("native memory state write directory");
        let additions_bound = mutations
            .iter()
            .filter(|mutation| {
                mutation.value.is_some()
                    && !self
                        .group(mutation.key.key_group)
                        .expect("validated key group")
                        .contains_key(mutation.key.key.as_slice())
            })
            .count();
        workspace.try_grow(
            additions_bound
                .min(self.groups.len().saturating_mul(table::SHARDS))
                .saturating_mul(512),
        )?;
        let mut additions = std::collections::BTreeMap::<(u32, usize), usize>::new();
        for mutation in &mutations {
            if mutation.value.is_some()
                && !self
                    .group(mutation.key.key_group)?
                    .contains_key(mutation.key.key.as_slice())
            {
                let shard = self
                    .group(mutation.key.key_group)?
                    .shard_index(&mutation.key.key);
                *additions
                    .entry((mutation.key.key_group, shard))
                    .or_default() += 1;
            }
        }
        let mut table_growth_bound = 0usize;
        let mut previous_group = None;
        let mut overlap = 0usize;
        for (&(key_group, shard), &count) in &additions {
            let group = self.group(key_group)?;
            if previous_group != Some(key_group) {
                table_growth_bound = table_growth_bound.saturating_add(group.directory_growth());
                previous_group = Some(key_group);
            }
            let (retained_growth, old_table) = group.growth_for_shard(shard, count);
            table_growth_bound = table_growth_bound.saturating_add(retained_growth);
            overlap = overlap.max(old_table);
        }
        self.reservation.resize(
            current
                .saturating_add(grows)
                .saturating_add(table_growth_bound)
                .saturating_add(overlap.max(payload_overlap)),
        )?;
        // Tables grow sequentially: charge final table growth plus the largest old table
        // that can coexist with its replacement, rather than every replacement at once.
        // Variable-length value replacement likewise admits one old packed entry at a
        // time; equal-length updates reuse the existing allocation.
        // Reserve once per growing table instead of repeatedly reallocating while
        // applying a large batch. A failed allocation leaves logical state untouched.
        for ((key_group, shard), count) in additions {
            if let Err(error) = self.group_mut(key_group)?.reserve_shard(shard, count) {
                self.reservation.resize(self.estimated_heap_size())?;
                return Err(error);
            }
        }
        for mutation in mutations {
            let (added, removed) = {
                let group = self.group_mut(mutation.key.key_group)?;
                match mutation.value {
                    Some(value) => group.insert(mutation.key.key, value),
                    None => (0, group.remove_bytes(mutation.key.key.as_slice())),
                }
            };
            self.entry_bytes = self
                .entry_bytes
                .saturating_sub(removed)
                .saturating_add(added);
        }
        self.reservation.resize(self.estimated_heap_size())?;
        Ok(())
    }

    fn visit_key_group(
        &self,
        key_group: u32,
        max_rows: usize,
        max_bytes: usize,
        visitor: &mut dyn FnMut(&[(&[u8], &[u8])]) -> Result<()>,
    ) -> Result<()> {
        self.visit_prefix(key_group, &[], max_rows, max_bytes, visitor)
    }

    fn visit_prefix(
        &self,
        key_group: u32,
        prefix: &[u8],
        max_rows: usize,
        max_bytes: usize,
        visitor: &mut dyn FnMut(&[(&[u8], &[u8])]) -> Result<()>,
    ) -> Result<()> {
        if max_rows == 0 || max_bytes == 0 {
            return Err(DataFusionError::Execution(
                "state scan bounds must be positive".to_string(),
            ));
        }
        let mut page = Vec::with_capacity(max_rows.min(max_bytes / 96));
        let mut bytes = 0usize;
        for (key, value) in self.group(key_group)?.iter() {
            if !key.starts_with(prefix) {
                continue;
            }
            let size = key.len().saturating_add(value.len()).saturating_add(96);
            if size > max_bytes {
                return Err(DataFusionError::ResourcesExhausted(
                    "state scan entry exceeds the admitted page budget".to_string(),
                ));
            }
            if page.len() == max_rows || bytes.saturating_add(size) > max_bytes {
                visitor(&page)?;
                page.clear();
                bytes = 0;
            }
            page.push((key.as_ref(), value.as_ref()));
            bytes += size;
        }
        if !page.is_empty() {
            visitor(&page)?;
        }
        Ok(())
    }

    fn snapshot_key_group(
        &self,
        key_group: u32,
        owner: &HostMemoryReservation,
    ) -> Result<super::SnapshotBytes> {
        let group = self.group(key_group)?;
        let bytes = group.iter().try_fold(16usize, |bytes, (key, value)| {
            bytes
                .checked_add(8)
                .and_then(|bytes| bytes.checked_add(key.len()))
                .and_then(|bytes| bytes.checked_add(value.len()))
                .ok_or_else(|| {
                    DataFusionError::ResourcesExhausted("canonical snapshot size overflow".into())
                })
        })?;
        let mut reservation = owner.sibling("canonical snapshot bytes");
        reservation.resize(bytes)?;
        let mut sort_reservation = owner.sibling("canonical snapshot sorted references");
        sort_reservation.resize(group.len().saturating_mul(size_of::<&entry::PackedEntry>()))?;
        let mut entries = Vec::with_capacity(group.len());
        entries.extend(group.entries());
        entries.sort_unstable_by(|left, right| left.key().cmp(right.key()));
        let mut writer = streamfusion_state_abi::SnapshotWriter::new(key_group, group.len(), bytes)
            .map_err(|error| DataFusionError::Execution(error.to_string()))?;
        for entry in entries {
            writer
                .append(entry.key(), entry.value())
                .map_err(|error| DataFusionError::Execution(error.to_string()))?;
        }
        let bytes = writer
            .finish()
            .map_err(|error| DataFusionError::Execution(error.to_string()))?;
        Ok(super::SnapshotBytes::owned(bytes, reservation))
    }

    fn restore_key_group(
        &mut self,
        key_group: u32,
        bytes: &[u8],
        _owner: &HostMemoryReservation,
    ) -> Result<()> {
        if !self.group(key_group)?.is_empty() {
            return Err(DataFusionError::Execution(format!(
                "key group {key_group} was restored more than once"
            )));
        }
        let count = streamfusion_state_abi::validate_key_group_snapshot(key_group, bytes)
            .map_err(|error| DataFusionError::Execution(error.to_string()))?;
        let current = self.estimated_heap_size();
        self.reservation.resize(
            current
                .saturating_add(bytes.len().saturating_mul(3))
                .saturating_add(count.saturating_mul(192))
                .saturating_add(self.group(key_group)?.directory_growth()),
        )?;
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
                .map(|(key, value)| key.len().saturating_add(value.len()))
                .sum::<usize>(),
        );
        self.group_mut(key_group)?.extend(entries.into_iter())?;
        self.reservation.resize(self.estimated_heap_size())?;
        Ok(())
    }
}

fn table_heap_size(capacity: usize) -> usize {
    if capacity == 0 {
        return 0;
    }
    // hashbrown reports usable slots (3, 7, 14, 28, ...), not bucket count.
    capacity
        .saturating_add(1)
        .checked_next_power_of_two()
        .unwrap_or(usize::MAX)
        .saturating_mul(bucket_bytes())
        .saturating_add(16)
}

#[cfg(test)]
mod allocation_tests;
#[cfg(test)]
mod packed_tests;
#[cfg(test)]
mod sharded_growth;

fn table_size_for_entries(entries: usize) -> usize {
    if entries <= 3 {
        return table_heap_size(3);
    }
    if entries <= 7 {
        return table_heap_size(7);
    }
    entries
        .saturating_mul(8)
        .saturating_add(6)
        .saturating_div(7)
        .checked_next_power_of_two()
        .unwrap_or(usize::MAX)
        .saturating_mul(bucket_bytes())
        .saturating_add(16)
}

const fn bucket_bytes() -> usize {
    size_of::<entry::PackedEntry>() + 1
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
    fn snapshot_admission_is_released_on_denial_and_tracks_the_returned_bytes() {
        let broker = Arc::new(TestBroker::new(1 << 20));
        let mut state = new_state(0, 0, broker.clone());
        state
            .write_batch(vec![StateMutation {
                key: StateKey {
                    key_group: 0,
                    key: b"key".to_vec(),
                },
                value: Some(vec![9; 32_000]),
            }])
            .unwrap();
        let used = broker.reserved();
        let owner = HostMemoryReservation::new(broker.clone(), "snapshot caller");
        let mut pressure = owner.sibling("other operation");
        pressure.resize((1 << 20) - used - 32_027).unwrap();
        // The output fits exactly; the separate sorting workspace does not.
        assert!(state.snapshot_key_group(0, &owner).is_err());
        assert_eq!(broker.reserved(), (1 << 20) - 32_027);
        drop(pressure);
        let snapshot = state.snapshot_key_group(0, &owner).unwrap();
        assert_eq!(snapshot.len(), 32_027);
        assert_eq!(broker.reserved(), used + snapshot.len());
        drop(state);
        assert_eq!(broker.reserved(), snapshot.len());
        assert_eq!(
            snapshot::decode(0, &snapshot).unwrap()[0].1,
            vec![9; 32_000]
        );
        drop(snapshot);
        assert_eq!(broker.reserved(), 0);
    }

    #[test]
    fn value_growth_does_not_reserve_other_key_group_tables() {
        let broker = Arc::new(TestBroker::new(1 << 20));
        let mut state = new_state(0, 15, broker.clone());
        state
            .write_batch(
                (0..16)
                    .flat_map(|group| {
                        (0..64).map(move |key| StateMutation {
                            key: StateKey {
                                key_group: group,
                                key: vec![key],
                            },
                            value: Some(vec![0; 8]),
                        })
                    })
                    .collect(),
            )
            .unwrap();
        let used = broker.reserved();
        // Leave enough space for an existing value to grow, but not a duplicate
        // reservation for any of the unrelated key-group tables.
        let mut pressure = HostMemoryReservation::new(broker.clone(), "other operator");
        pressure.resize((1 << 20) - used - 64).unwrap();
        state
            .write_batch(vec![StateMutation {
                key: StateKey {
                    key_group: 0,
                    key: vec![0],
                },
                value: Some(vec![7; 32]),
            }])
            .unwrap();
        assert_eq!(
            state
                .get_batch(
                    &[StateKeyRef {
                        key_group: 0,
                        key: &[0]
                    }],
                    &state.reservation
                )
                .unwrap()[0]
                .as_deref(),
            Some(&[7; 32][..])
        );
        drop(pressure);
        drop(state);
        assert_eq!(broker.reserved(), 0);
    }

    #[test]
    fn denied_table_growth_leaves_all_batch_values_unchanged() {
        let broker = Arc::new(TestBroker::new(4096));
        let mut state = new_state(0, 0, broker.clone());
        let mut pressure = HostMemoryReservation::new(broker.clone(), "other operator");
        pressure.resize(4096 - broker.reserved() - 128).unwrap();
        assert!(state
            .write_batch(
                (0..64)
                    .map(|key| StateMutation {
                        key: StateKey {
                            key_group: 0,
                            key: vec![key]
                        },
                        value: Some(vec![1]),
                    })
                    .collect()
            )
            .is_err());
        assert!(state.group(0).unwrap().is_empty());
        drop(pressure);
        drop(state);
        assert_eq!(broker.reserved(), 0);
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

        let snapshot = state.snapshot_key_group(2, &state.reservation).unwrap();
        let mut restored = new_state(2, 2, broker.clone());
        restored
            .restore_key_group(2, &snapshot, &state.reservation)
            .unwrap();

        assert_eq!(
            restored
                .get_batch(
                    &[StateKeyRef {
                        key_group: 2,
                        key: &[1],
                    }],
                    &restored.reservation
                )
                .unwrap()[0]
                .as_deref(),
            Some(&[9, 0, 0, 0, 0, 0, 0, 0, 4, 5][..])
        );
        drop(restored);
        drop(state);
        drop(snapshot);
        assert_eq!(broker.reserved(), 0);
    }
}
