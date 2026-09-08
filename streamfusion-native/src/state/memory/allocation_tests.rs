// Copyright 2026 StreamFusion Authors
// Licensed under the Apache License, Version 2.0.

use super::*;
use crate::memory_pool::tests_support::TestBroker;
use crate::state::StateKey;
use std::sync::Arc;

#[test]
fn tombstone_removals_keep_the_entire_backing_table_admitted() {
    let broker = Arc::new(TestBroker::new(8 << 20));
    let mut state = MemoryKeyedState::new(
        0,
        0,
        HostMemoryReservation::new(broker.clone(), "tombstone state"),
    )
    .unwrap();
    *state.group_mut(0).unwrap() = HashMap::with_hasher(RandomState::with_seeds(1, 2, 3, 4));
    let baseline = broker.reserved();
    state
        .write_batch(
            (0..448u64)
                .map(|key| StateMutation {
                    key: StateKey {
                        key_group: 0,
                        key: key.to_le_bytes().to_vec(),
                    },
                    value: Some(vec![7; 128]),
                })
                .collect(),
        )
        .unwrap();
    let allocation = state.group(0).unwrap().allocation_size();
    assert!(allocation > 0);
    state
        .write_batch(
            (0..448u64)
                .map(|key| StateMutation {
                    key: StateKey {
                        key_group: 0,
                        key: key.to_le_bytes().to_vec(),
                    },
                    value: None,
                })
                .collect(),
        )
        .unwrap();
    assert!(state.group(0).unwrap().is_empty());
    assert_eq!(state.entry_bytes, 0);
    assert_eq!(state.group(0).unwrap().allocation_size(), allocation);
    assert_eq!(broker.reserved(), baseline + allocation);
    drop(state);
    assert_eq!(broker.reserved(), 0);
}

#[test]
fn immutable_state_entries_fit_a_bounded_hash_table_share_and_restore_identical_bytes() {
    let broker = Arc::new(TestBroker::new(64 << 20));
    let owner = HostMemoryReservation::new(broker.clone(), "immutable state test");
    let mut pressure = owner.sibling("other operators");
    pressure.resize(54 << 20).unwrap();
    let mut state = MemoryKeyedState::new(0, 0, owner.sibling("state")).unwrap();
    for start in (0..100_000u64).step_by(4096) {
        let end = (start + 4096).min(100_000);
        state
            .write_batch(
                (start..end)
                    .map(|key| StateMutation {
                        key: StateKey {
                            key_group: 0,
                            key: key.to_le_bytes().to_vec(),
                        },
                        value: Some(vec![key as u8; 32]),
                    })
                    .collect(),
            )
            .unwrap();
    }
    drop(pressure);
    let snapshot = state.snapshot_key_group(0, &owner).unwrap();
    let original_size = state.estimated_heap_size();
    drop(state);
    let mut restored = MemoryKeyedState::new(0, 0, owner.sibling("restored")).unwrap();
    restored.restore_key_group(0, &snapshot, &owner).unwrap();
    assert_eq!(restored.estimated_heap_size(), original_size);
    assert_eq!(restored.snapshot_key_group(0, &owner).unwrap(), snapshot);
    let decoded = snapshot::decode(0, &snapshot).unwrap();
    assert_eq!(decoded.len(), 100_000);
    for (key, value) in decoded {
        let key = u64::from_le_bytes(key.try_into().unwrap());
        assert_eq!(value, vec![key as u8; 32]);
    }
    drop(restored);
    drop(snapshot);
    assert_eq!(broker.reserved(), 0);
}

#[test]
fn stored_buffers_discard_spare_capacity_and_updates_keep_the_original_key() {
    let broker = Arc::new(TestBroker::new(1 << 20));
    let mut state = MemoryKeyedState::new(
        0,
        0,
        HostMemoryReservation::new(broker.clone(), "immutable buffers"),
    )
    .unwrap();
    let key = 7u64.to_le_bytes();
    for length in [32, 96, 0] {
        let previous = state
            .group(0)
            .unwrap()
            .get_key_value(key.as_slice())
            .map(|(key, _)| key.as_ptr());
        let mut oversized_key = Vec::with_capacity(1024);
        oversized_key.extend_from_slice(&key);
        let mut value = Vec::with_capacity(4096);
        value.resize(length, 3);
        state
            .write_batch(vec![StateMutation {
                key: StateKey {
                    key_group: 0,
                    key: oversized_key,
                },
                value: Some(value),
            }])
            .unwrap();
        assert_eq!(state.entry_bytes, key.len() + length);
        if let Some(previous) = previous {
            assert_eq!(
                state
                    .group(0)
                    .unwrap()
                    .get_key_value(key.as_slice())
                    .unwrap()
                    .0
                    .as_ptr(),
                previous
            );
        }
    }
    drop(state);
    assert_eq!(broker.reserved(), 0);
}
