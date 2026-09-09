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
