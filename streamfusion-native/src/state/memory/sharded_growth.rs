// Copyright 2026 StreamFusion Authors
// Licensed under the Apache License, Version 2.0.

use super::*;
use crate::memory_pool::tests_support::TestBroker;
use crate::state::StateKey;
use std::sync::Arc;

#[test]
fn hot_key_group_grows_past_the_single_table_peak_under_the_same_budget() {
    let limit = 20 << 20;
    // Actual hashbrown allocations for the former whole-group directory cross this budget
    // when growing from 229,376 usable slots. Payloads alone are not the limiting factor.
    let old = hashbrown::HashMap::<StateBytes, StateBytes>::with_capacity(229_376);
    let next = hashbrown::HashMap::<StateBytes, StateBytes>::with_capacity(240_000);
    assert!(old.allocation_size() + next.allocation_size() + 240_000 * 9 > limit);
    drop((old, next));
    let broker = Arc::new(TestBroker::new(limit));
    let owner = HostMemoryReservation::new(broker.clone(), "sharded growth");
    let mut state = MemoryKeyedState::new(0, 0, owner.sibling("state")).unwrap();
    *state.group_mut(0).unwrap() = KeyGroupMap::with_hasher(RandomState::with_seeds(1, 2, 3, 4));
    for start in (0..240_000u64).step_by(1024) {
        state
            .write_batch(
                (start..(start + 1024).min(240_000))
                    .map(|key| StateMutation {
                        key: StateKey {
                            key_group: 0,
                            key: key.to_le_bytes().to_vec(),
                        },
                        value: Some(vec![key as u8]),
                    })
                    .collect(),
            )
            .unwrap();
    }
    assert_eq!(state.group(0).unwrap().len(), 240_000);
    assert_eq!(state.entry_bytes, 240_000 * 9);
    assert!(state.estimated_heap_size() <= limit);
    // Verify every value through the production batched accessor without constructing a
    // second whole-group snapshot. Canonical snapshot capacity is a separate contract.
    for start in (0..240_000u64).step_by(128) {
        let keys = (start..(start + 128).min(240_000))
            .map(u64::to_le_bytes)
            .collect::<Vec<_>>();
        let refs = keys
            .iter()
            .map(|key| StateKeyRef { key_group: 0, key })
            .collect::<Vec<_>>();
        let values = state.get_batch(&refs, &owner).unwrap();
        for (offset, value) in values.iter().enumerate() {
            assert_eq!(value.as_deref(), Some(&[(start + offset as u64) as u8][..]));
        }
    }
    drop(state);
    assert_eq!(broker.reserved(), 0);
}

#[test]
fn denied_multi_table_growth_keeps_existing_updates_deletions_and_insertions_atomic() {
    let limit = 16 << 20;
    let broker = Arc::new(TestBroker::new(limit));
    let owner = HostMemoryReservation::new(broker.clone(), "atomic directory growth");
    let mut state = MemoryKeyedState::new(0, 2, owner.sibling("state")).unwrap();
    state
        .write_batch(
            (0..3)
                .flat_map(|group| {
                    (0..320u64).map(move |key| StateMutation {
                        key: StateKey {
                            key_group: group,
                            key: key.to_le_bytes().to_vec(),
                        },
                        value: Some(vec![key as u8; 32]),
                    })
                })
                .collect(),
        )
        .unwrap();
    let before = (0..3)
        .map(|group| state.snapshot_key_group(group, &owner).unwrap())
        .collect::<Vec<_>>();
    let mut pressure = owner.sibling("other consumers");
    pressure
        .resize(limit - broker.reserved() - (128 << 10))
        .unwrap();
    let reserved = broker.reserved();
    let mut changes = vec![
        StateMutation {
            key: StateKey {
                key_group: 0,
                key: 0u64.to_le_bytes().to_vec(),
            },
            value: Some(vec![9; 64]),
        },
        StateMutation {
            key: StateKey {
                key_group: 1,
                key: 1u64.to_le_bytes().to_vec(),
            },
            value: None,
        },
    ];
    changes.extend((320..8512u64).map(|key| StateMutation {
        key: StateKey {
            key_group: 2,
            key: key.to_le_bytes().to_vec(),
        },
        value: Some(vec![key as u8; 8]),
    }));
    assert!(state
        .write_batch(changes)
        .unwrap_err()
        .to_string()
        .contains("Flink denied"));
    assert_eq!(
        broker.reserved(),
        reserved,
        "denied growth releases its directory workspace"
    );
    drop(pressure);
    for (group, snapshot) in before.iter().enumerate() {
        assert_eq!(
            &state.snapshot_key_group(group as u32, &owner).unwrap(),
            snapshot
        );
    }
    drop(before);
    drop(state);
    assert_eq!(broker.reserved(), 0);
}
