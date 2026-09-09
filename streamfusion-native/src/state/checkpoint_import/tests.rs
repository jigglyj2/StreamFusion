// Copyright 2026 StreamFusion Authors
// Licensed under the Apache License, Version 2.0.

use super::*;
use crate::memory_pool::{tests_support::TestBroker, HostMemoryReservation};
use std::sync::Arc;

fn plugin() -> std::path::PathBuf {
    std::env::var("STREAMFUSION_TEST_ROCKSDB_PLUGIN")
        .unwrap()
        .into()
}

#[test]
fn checkpoint_import_reads_a_group_larger_than_the_transfer_budget_in_bounded_pages() {
    let directory = tempfile::tempdir().unwrap();
    let plugin = plugin();
    let mut source =
        RocksPluginKeyedState::open(&plugin, &directory.path().join("source"), 2, 3, 1 << 20)
            .unwrap();
    for start in (0u32..8192).step_by(256) {
        source
            .write_batch(
                (start..start + 256)
                    .map(|key| StateMutation {
                        key: StateKey {
                            key_group: 2,
                            key: key.to_be_bytes().to_vec(),
                        },
                        value: Some(vec![key as u8; 1024]),
                    })
                    .collect(),
            )
            .unwrap();
    }
    let checkpoint = directory.path().join("checkpoint");
    source.checkpoint(&checkpoint).unwrap();
    drop(source);
    let broker = Arc::new(TestBroker::new(4 << 20));
    let owner = HostMemoryReservation::new(broker.clone(), "checkpoint test");
    // Charge the source reader and destination caches separately from transfer pages.
    let mut caches = owner.sibling("test source and destination caches");
    caches.resize(2 << 20).unwrap();
    let reader = RocksPluginKeyedState::open(&plugin, &checkpoint, 2, 3, 1 << 20).unwrap();
    let mut destination =
        RocksPluginKeyedState::open(&plugin, &directory.path().join("target"), 2, 3, 1 << 20)
            .unwrap();
    assert!(reader
        .snapshot_key_group(2, &owner)
        .unwrap_err()
        .to_string()
        .contains("Flink denied"));
    import_key_group(&mut destination, &reader, 2, &owner).unwrap();
    import_key_group(&mut destination, &reader, 3, &owner).unwrap();
    assert_eq!(
        broker.reserved(),
        2 << 20,
        "no retained page or cursor leases"
    );
    for start in (0u32..8192).step_by(128) {
        let keys = (start..start + 128)
            .map(|key| key.to_be_bytes())
            .collect::<Vec<_>>();
        let refs = keys
            .iter()
            .map(|key| StateKeyRef { key_group: 2, key })
            .collect::<Vec<_>>();
        let values = destination.get_batch(&refs, &owner).unwrap();
        for (offset, value) in values.iter().enumerate() {
            assert_eq!(
                value.as_ref().unwrap().as_ref(),
                vec![(start + offset as u32) as u8; 1024]
            );
        }
    }
    assert!(import_key_group(&mut destination, &reader, 2, &owner)
        .unwrap_err()
        .to_string()
        .contains("restored more than once"));
    drop(destination);
    drop(reader);
    drop(caches);
    assert_eq!(broker.reserved(), 0);
}

#[test]
fn paged_import_preserves_large_legacy_values_on_both_destinations_and_releases_denied_pages() {
    let directory = tempfile::tempdir().unwrap();
    let plugin = plugin();
    let mut source =
        RocksPluginKeyedState::open(&plugin, &directory.path().join("source"), 2, 3, 1 << 20)
            .unwrap();
    source
        .write_batch(vec![
            StateMutation {
                key: StateKey {
                    key_group: 2,
                    key: vec![0],
                },
                value: Some(vec![1; 400_000]),
            },
            StateMutation {
                key: StateKey {
                    key_group: 2,
                    key: vec![255; 300_000],
                },
                value: Some(vec![2]),
            },
            StateMutation {
                key: StateKey {
                    key_group: 2,
                    key: vec![255; 300_001],
                },
                value: Some(vec![3; 20]),
            },
        ])
        .unwrap();
    let broker = Arc::new(TestBroker::new(16 << 20));
    let owner = HostMemoryReservation::new(broker.clone(), "legacy import");
    let expected = source.snapshot_key_group(2, &owner).unwrap();
    let mut targets: Vec<Box<dyn KeyedState>> = vec![
        Box::new(MemoryKeyedState::new(2, 3, owner.sibling("memory")).unwrap()),
        Box::new(
            RocksPluginKeyedState::open(&plugin, &directory.path().join("target"), 2, 3, 1 << 20)
                .unwrap(),
        ),
    ];
    for target in &mut targets {
        import_key_group(target.as_mut(), &source, 2, &owner).unwrap();
        assert_eq!(&*target.snapshot_key_group(2, &owner).unwrap(), &*expected);
    }
    let small = Arc::new(TestBroker::new(64 << 10));
    let budget = HostMemoryReservation::new(small.clone(), "denied scan");
    let mut destination = MemoryKeyedState::new(2, 3, budget.sibling("memory")).unwrap();
    let baseline = small.reserved();
    assert!(import_key_group(&mut destination, &source, 2, &budget)
        .unwrap_err()
        .to_string()
        .contains("Flink denied"));
    assert_eq!(small.reserved(), baseline);
    let empty = destination.snapshot_key_group(2, &budget).unwrap();
    assert_eq!(decode_key_group_snapshot(2, &empty).unwrap().len(), 0);
}
