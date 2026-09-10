// Copyright 2026 StreamFusion Authors
// Licensed under the Apache License, Version 2.0.

use super::*;
use crate::memory_pool::tests_support::TestBroker;
use crate::state::StateKey;
use std::sync::Arc;

#[test]
fn packed_entries_preserve_key_value_boundaries_and_canonical_restore() {
    let broker = Arc::new(TestBroker::new(8 << 20));
    let owner = HostMemoryReservation::new(broker.clone(), "packed boundaries");
    let mut state = MemoryKeyedState::new(0, 1, owner.sibling("state")).unwrap();
    let mut expected = std::collections::BTreeMap::new();
    // Identical concatenated payloads with different splits must remain different keys.
    for split in 0..=256 {
        let bytes = (0..256).map(|n| n as u8).collect::<Vec<_>>();
        expected.insert(bytes[..split].to_vec(), bytes[split..].to_vec());
    }
    for round in 0..4 {
        let mutations = expected
            .iter_mut()
            .map(|(key, value)| {
                if round > 0 {
                    *value = vec![round; (key.len() * round as usize) % 307];
                }
                StateMutation {
                    key: StateKey {
                        key_group: 0,
                        key: key.clone(),
                    },
                    value: Some(value.clone()),
                }
            })
            .collect();
        state.write_batch(mutations).unwrap();
        let snapshot = state.snapshot_key_group(0, &owner).unwrap();
        assert_eq!(
            snapshot::decode(0, &snapshot).unwrap(),
            expected.clone().into_iter().collect::<Vec<_>>()
        );
        let mut restored = MemoryKeyedState::new(0, 0, owner.sibling("restore")).unwrap();
        restored.restore_key_group(0, &snapshot, &owner).unwrap();
        assert_eq!(restored.snapshot_key_group(0, &owner).unwrap(), snapshot);
        let mut visited = std::collections::BTreeMap::new();
        restored
            .visit_prefix(0, &[0, 1], 17, 4096, &mut |page| {
                visited.extend(
                    page.iter()
                        .map(|(key, value)| (key.to_vec(), value.to_vec())),
                );
                Ok(())
            })
            .unwrap();
        assert_eq!(
            visited,
            expected
                .iter()
                .filter(|(key, _)| key.starts_with(&[0, 1]))
                .map(|(key, value)| (key.clone(), value.clone()))
                .collect()
        );
    }
    drop(state);
    assert_eq!(broker.reserved(), 0);
}

#[test]
fn variable_length_replacement_admits_packed_payload_overlap_before_mutation() {
    let limit = 1 << 20;
    let broker = Arc::new(TestBroker::new(limit));
    let owner = HostMemoryReservation::new(broker.clone(), "packed replacement");
    let mut state = MemoryKeyedState::new(0, 0, owner.sibling("state")).unwrap();
    let key = vec![4; 32_000];
    state
        .write_batch(vec![StateMutation {
            key: StateKey {
                key_group: 0,
                key: key.clone(),
            },
            value: Some(vec![1; 32_000]),
        }])
        .unwrap();
    let mut pressure = owner.sibling("other consumers");
    pressure.resize(limit - broker.reserved() - 4096).unwrap();
    let before = broker.reserved();
    let update = |length, byte| StateMutation {
        key: StateKey {
            key_group: 0,
            key: key.clone(),
        },
        value: Some(vec![byte; length]),
    };
    assert!(state.write_batch(vec![update(32_001, 2)]).is_err());
    assert_eq!(broker.reserved(), before);
    assert_eq!(state.group(0).unwrap().get(&key).unwrap(), &[1; 32_000]);
    // The same-size update needs no second packed allocation and remains admissible.
    state.write_batch(vec![update(32_000, 3)]).unwrap();
    assert_eq!(state.group(0).unwrap().get(&key).unwrap(), &[3; 32_000]);
    drop(pressure);
    state.write_batch(vec![update(32_001, 2)]).unwrap();
    assert_eq!(state.entry_bytes, 64_001);
    assert_eq!(state.group(0).unwrap().get(&key).unwrap(), &[2; 32_001]);
    drop(state);
    assert_eq!(broker.reserved(), 0);
}
