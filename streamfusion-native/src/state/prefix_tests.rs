// Copyright 2026 StreamFusion Authors
// Licensed under the Apache License, Version 2.0.

use super::*;
use crate::memory_pool::{tests_support::TestBroker, HostMemoryReservation};
use std::sync::Arc;

#[test]
fn prefix_pages_cover_empty_binary_and_unbounded_suffixes_on_every_backend() {
    let owner = HostMemoryReservation::new(Arc::new(TestBroker::new(32 << 20)), "prefix test");
    let directory = tempfile::tempdir().unwrap();
    let mut states: Vec<Box<dyn KeyedState>> = vec![
        Box::new(MemoryKeyedState::new(3, 4, owner.sibling("hash state")).unwrap()),
        Box::new(OrderedMemoryKeyedState::new(3, 4, owner.sibling("ordered state")).unwrap()),
    ];
    if let Ok(plugin) = std::env::var("STREAMFUSION_TEST_ROCKSDB_PLUGIN") {
        states.push(Box::new(
            RocksPluginKeyedState::open_for_owner(
                std::path::Path::new(&plugin),
                directory.path(),
                3,
                4,
                8 << 20,
                &owner,
            )
            .unwrap(),
        ));
    }
    let keys = vec![
        vec![],
        vec![0],
        vec![0, 0],
        vec![0, 1],
        vec![1],
        vec![254, 255],
        vec![255],
        vec![255, 0],
        vec![255, 255],
    ];
    for mut state in states {
        state
            .write_batch(
                [3, 4]
                    .into_iter()
                    .flat_map(|group| {
                        keys.iter().map(move |key| StateMutation {
                            key: StateKey {
                                key_group: group,
                                key: key.clone(),
                            },
                            value: Some(vec![group as u8; 8]),
                        })
                    })
                    .collect(),
            )
            .unwrap();
        for prefix in [
            vec![],
            vec![0],
            vec![0, 0],
            vec![42],
            vec![255],
            vec![255, 255],
        ] {
            let mut actual = Vec::new();
            state
                .visit_prefix(3, &prefix, 2, 256, &mut |page| {
                    assert!(page.len() <= 2);
                    for (key, value) in page {
                        assert_eq!(*value, &[3; 8]);
                        actual.push(key.to_vec());
                    }
                    Ok(())
                })
                .unwrap();
            actual.sort();
            assert_eq!(
                actual,
                keys.iter()
                    .filter(|key| key.starts_with(&prefix))
                    .cloned()
                    .collect::<Vec<_>>()
            );
        }
        // A large inline value from another logical group must not consume this prefix's
        // page budget. This matters while old and migrated DISTINCT groups coexist.
        state
            .write_batch(vec![StateMutation {
                key: StateKey {
                    key_group: 3,
                    key: vec![42],
                },
                value: Some(vec![0; 4096]),
            }])
            .unwrap();
        let mut selected = 0;
        state
            .visit_prefix(3, &[255], 2, 256, &mut |page| {
                selected += page.len();
                Ok(())
            })
            .unwrap();
        assert_eq!(selected, 3);
    }
}

#[test]
fn admitted_prefix_pages_keep_large_entries_and_release_every_scan_lease() {
    let broker = Arc::new(TestBroker::new(16 << 20));
    let owner = HostMemoryReservation::new(broker.clone(), "admitted prefix");
    let directory = tempfile::tempdir().unwrap();
    let plugin = std::env::var("STREAMFUSION_TEST_ROCKSDB_PLUGIN").unwrap();
    let mut states: Vec<Box<dyn KeyedState>> = vec![
        Box::new(OrderedMemoryKeyedState::new(3, 4, owner.sibling("ordered state")).unwrap()),
        Box::new(
            RocksPluginKeyedState::open_for_owner(
                std::path::Path::new(&plugin),
                directory.path(),
                3,
                4,
                1 << 20,
                &owner,
            )
            .unwrap(),
        ),
    ];
    for state in &mut states {
        state
            .write_batch(
                [3, 4]
                    .into_iter()
                    .flat_map(|group| {
                        [
                            vec![0],
                            vec![1],
                            vec![1, 0],
                            vec![1, 1],
                            vec![2],
                            vec![255],
                            vec![255, 0],
                        ]
                        .into_iter()
                        .map(move |key| StateMutation {
                            key: StateKey {
                                key_group: group,
                                key,
                            },
                            value: Some(vec![group as u8; 400_000]),
                        })
                    })
                    .collect(),
            )
            .unwrap();
        let baseline = broker.reserved();
        for (prefix, expected) in [
            (vec![1], vec![vec![1], vec![1, 0], vec![1, 1]]),
            (vec![255], vec![vec![255], vec![255, 0]]),
            (vec![42], vec![]),
        ] {
            let mut actual = Vec::new();
            state
                .visit_prefix_admitted(3, &prefix, 2, 256, &owner, &mut |page| {
                    assert!(page.len() <= 2);
                    for (key, value) in page {
                        assert_eq!(value.len(), 400_000);
                        assert!(value.iter().all(|&byte| byte == 3));
                        actual.push(key.to_vec());
                    }
                    Ok(())
                })
                .unwrap();
            assert_eq!(actual, expected);
            assert_eq!(broker.reserved(), baseline);
        }
        let error = state
            .visit_prefix_admitted(3, &[1], 2, 256, &owner, &mut |_| {
                Err(datafusion::error::DataFusionError::Execution(
                    "visitor stopped".into(),
                ))
            })
            .unwrap_err();
        assert!(error.to_string().contains("visitor stopped"));
        assert_eq!(broker.reserved(), baseline);
    }
    let small = Arc::new(TestBroker::new(64 << 10));
    let budget = HostMemoryReservation::new(small.clone(), "denied prefix");
    let mut visited = false;
    let error = states[1]
        .visit_prefix_admitted(3, &[1], 2, 256, &budget, &mut |_| {
            visited = true;
            Ok(())
        })
        .unwrap_err();
    assert!(error.to_string().contains("Flink denied"));
    assert!(!visited);
    assert_eq!(small.reserved(), 0);
    drop(states);
    assert_eq!(broker.reserved(), 0);
}
