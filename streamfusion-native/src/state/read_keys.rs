// Copyright 2026 StreamFusion Authors
// Licensed under the Apache License, Version 2.0

use ahash::RandomState;
use datafusion::error::Result;
use hashbrown::{hash_map::Entry, HashMap};

use crate::memory_pool::HostMemoryReservation;

use super::StateKeyRef;

/// Borrowed unique requests plus the original ordering. Duplicate reads share the
/// backend's result owner; discovering them must not copy potentially large keys.
pub(super) struct CoalescedReadKeys<'a> {
    pub(super) unique: Vec<StateKeyRef<'a>>,
    pub(super) positions: Vec<usize>,
    _reservation: HostMemoryReservation,
}

impl<'a> CoalescedReadKeys<'a> {
    pub(super) fn new(keys: &[StateKeyRef<'a>], owner: &HostMemoryReservation) -> Result<Self> {
        let mut reservation = owner.sibling("RocksDB read key coalescing");
        // Include hashbrown's power-of-two bucket slack, control bytes, both
        // vectors, and allocator/control overhead. Admit before any allocation.
        let per_key = (size_of::<(StateKeyRef<'_>, usize)>() + 1) * 4
            + size_of::<StateKeyRef<'_>>()
            + size_of::<usize>();
        reservation.try_grow(keys.len().saturating_mul(per_key).saturating_add(4096))?;
        let mut lookup = HashMap::with_capacity_and_hasher(keys.len(), RandomState::new());
        let mut unique = Vec::with_capacity(keys.len());
        let mut positions = Vec::with_capacity(keys.len());
        for key in keys {
            let index = match lookup.entry(*key) {
                Entry::Occupied(entry) => *entry.get(),
                Entry::Vacant(entry) => {
                    let index = unique.len();
                    unique.push(*key);
                    entry.insert(index);
                    index
                }
            };
            positions.push(index);
        }
        Ok(Self {
            unique,
            positions,
            _reservation: reservation,
        })
    }
}

#[cfg(test)]
mod tests {
    use std::sync::Arc;

    use super::*;
    use crate::allocation_test_support::measure;
    use crate::memory_pool::tests_support::TestBroker;

    #[test]
    fn coalescing_preserves_order_groups_and_borrowed_key_bytes_with_admitted_allocations() {
        // Warm up ahash's test-thread initialization outside the observation.
        let _ = RandomState::new();
        for len in [0, 1, 2, 3, 7, 8, 15, 16, 255, 256, 2049] {
            for distinct in [1, 7, 2049] {
                let bytes = (0..distinct).map(usize::to_le_bytes).collect::<Vec<_>>();
                let keys = (0..len)
                    .map(|i| StateKeyRef {
                        key_group: (i % 2) as u32,
                        key: &bytes[i % distinct],
                    })
                    .collect::<Vec<_>>();
                let broker = Arc::new(TestBroker::new(1 << 20));
                let owner = HostMemoryReservation::new(broker.clone(), "coalescing test");
                let ((admitted, unique_count), observed) = measure(|| {
                    let coalesced = CoalescedReadKeys::new(&keys, &owner).unwrap();
                    for (key, index) in keys.iter().zip(&coalesced.positions) {
                        let unique = coalesced.unique[*index];
                        assert_eq!(*key, unique);
                        assert_eq!(key.key.as_ptr(), unique.key.as_ptr());
                    }
                    (broker.reserved(), coalesced.unique.len())
                });
                assert_eq!(unique_count, len.min(distinct * 2));
                assert!(
                    observed.peak <= admitted,
                    "{len}/{distinct}: {observed:?} > {admitted}"
                );
                assert_eq!(observed.live, 0);
                assert_eq!(broker.reserved(), 0);
            }
        }
    }

    #[test]
    fn denied_coalescing_releases_its_reservation() {
        let broker = Arc::new(TestBroker::new(1));
        let owner = HostMemoryReservation::new(broker.clone(), "denied coalescing");
        assert!(CoalescedReadKeys::new(
            &[StateKeyRef {
                key_group: 0,
                key: b"key"
            }],
            &owner
        )
        .is_err());
        assert_eq!(broker.reserved(), 0);
    }
}
