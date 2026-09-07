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
