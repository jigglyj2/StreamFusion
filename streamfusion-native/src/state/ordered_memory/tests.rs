// Copyright 2026 StreamFusion Authors
// Licensed under the Apache License, Version 2.0.

use super::*;
use crate::memory_pool::tests_support::TestBroker;
use crate::state::{RocksPluginKeyedState, StateKey};
use std::sync::Arc;

#[test]
fn value_updates_keep_existing_order_key_storage_and_denial_is_atomic() {
    let broker = Arc::new(TestBroker::new(4096));
    let owner = HostMemoryReservation::new(broker.clone(), "ordered update test");
    let mut state = OrderedMemoryKeyedState::new(0, 0, owner.sibling("state")).unwrap();
    let put = |value| StateMutation {
        key: StateKey {
            key_group: 0,
            key: vec![1, 2, 3, 4],
        },
        value: Some(value),
    };
    state.write_batch(vec![put(vec![0; 16])]).unwrap();
    let key_address = state.groups[0].first_key_value().unwrap().0.as_ptr();
    for value in 0..20u8 {
        state
            .write_batch(vec![put(vec![value; 16 + usize::from(value)])])
            .unwrap();
        let (key, stored) = state.groups[0].first_key_value().unwrap();
        assert_eq!(key.as_ptr(), key_address);
        assert_eq!(stored.as_ref(), vec![value; 16 + usize::from(value)]);
    }
    assert!(state.write_batch(vec![put(vec![0; 8192])]).is_err());
    let (key, stored) = state.groups[0].first_key_value().unwrap();
    assert_eq!(key.as_ptr(), key_address);
    assert_eq!(stored.as_ref(), vec![19; 35]);
    drop(state);
    drop(owner);
    assert_eq!(broker.reserved(), 0);
}

#[test]
fn compact_immutable_entries_fit_budget_and_keep_range_and_snapshot_bytes() {
    let broker = Arc::new(TestBroker::new(21 << 20));
    let owner = HostMemoryReservation::new(broker.clone(), "compact ordered state test");
    let (mut state, allocation) = crate::allocation_test_support::measure(|| {
        let mut state = OrderedMemoryKeyedState::new(0, 0, owner.sibling("state")).unwrap();
        state
            .write_batch(
                (0..100_000u32)
                    .rev()
                    .map(|key| StateMutation {
                        key: StateKey {
                            key_group: 0,
                            key: key.to_be_bytes().to_vec(),
                        },
                        value: Some(vec![key as u8; 32]),
                    })
                    .collect(),
            )
            .unwrap();
        state
    });
    assert!(allocation.live.max(0) as usize <= broker.reserved());
    let mut count = 0u32;
    state
        .visit_range(0, &[], None, 128, 32 << 10, &mut |page| {
            for (key, value) in page {
                assert_eq!(*key, count.to_be_bytes());
                assert_eq!(*value, [count as u8; 32]);
                count += 1;
            }
            Ok(true)
        })
        .unwrap();
    assert_eq!(count, 100_000);
    let (_, removed) = crate::allocation_test_support::measure(|| {
        state
            .write_batch(
                (0..100_000u32)
                    .step_by(2)
                    .map(|key| StateMutation {
                        key: StateKey {
                            key_group: 0,
                            key: key.to_be_bytes().to_vec(),
                        },
                        value: None,
                    })
                    .collect(),
            )
            .unwrap();
    });
    // Sparse B-tree occupancy still fits the coarse retained-node allowance.
    assert!((allocation.live + removed.live).max(0) as usize <= broker.reserved());
    let snapshot = state.snapshot_key_group(0, &owner).unwrap();
    drop(state);
    let mut restored = OrderedMemoryKeyedState::new(0, 0, owner.sibling("restored")).unwrap();
    restored.restore_key_group(0, &snapshot, &owner).unwrap();
    assert_eq!(restored.snapshot_key_group(0, &owner).unwrap(), snapshot);
    drop(restored);
    drop(snapshot);
    drop(owner);
    assert_eq!(broker.reserved(), 0);
}

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
        let mut restored = OrderedMemoryKeyedState::new(3, 3, owner.sibling("restored")).unwrap();
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
