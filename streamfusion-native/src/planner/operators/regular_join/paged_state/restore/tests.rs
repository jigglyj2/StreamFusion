// Copyright 2026 StreamFusion Authors
// Licensed under the Apache License, Version 2.0.

use super::*;
use crate::memory_pool::tests_support::TestBroker;

fn entries(count: usize, width: usize) -> Vec<StateMutation> {
    let logical = StateKey {
        key_group: 2,
        key: vec![42],
    };
    let value = JoinState {
        next_row_id: [count as u64, 0],
        left_matchable: Some(true),
        left: (0..count)
            .map(|id| StoredRow {
                id: id as u64,
                associations: 0,
                row: Arc::from(vec![id as u8; width]),
            })
            .collect(),
        ..JoinState::default()
    };
    let mut entries = vec![StateMutation {
        key: manifest_key(&logical),
        value: Some(encode_rows_manifest(&value)),
    }];
    entries.extend(value.left.iter().map(|row| StateMutation {
        key: row_key(&logical, 0, row.id),
        value: Some(encode_page(std::slice::from_ref(row)).unwrap()),
    }));
    entries
}

fn snapshot(entries: &[StateMutation]) -> Vec<u8> {
    streamfusion_state_abi::encode_key_group_snapshot(
        2,
        entries.iter().map(|entry| {
            (
                entry.key.key.as_slice(),
                entry.value.as_ref().unwrap().as_slice(),
            )
        }),
    )
    .unwrap()
}

#[test]
fn current_snapshot_validation_borrows_payload_and_rejects_missing_or_orphan_pages() {
    let mut entries = entries(4096, 1024);
    let bytes = snapshot(&entries);
    let broker = Arc::new(TestBroker::new(1 << 20));
    let owner = HostMemoryReservation::new(broker.clone(), "snapshot validation");
    assert!(bytes.len() > 4 << 20);
    validate_snapshot(2, &bytes, &owner).unwrap();
    assert_eq!(broker.reserved(), 0);
    let last = entries.pop().unwrap();
    assert!(validate_snapshot(2, &snapshot(&entries), &owner)
        .unwrap_err()
        .to_string()
        .contains("missing page"));
    entries.push(last);
    entries.push(StateMutation {
        key: StateKey {
            key_group: 2,
            key: vec![255],
        },
        value: Some(vec![1]),
    });
    assert!(validate_snapshot(2, &snapshot(&entries), &owner)
        .unwrap_err()
        .to_string()
        .contains("orphan"));
    assert_eq!(broker.reserved(), 0);
}

#[test]
fn physical_join_restore_does_not_materialize_the_key_group_or_hot_key() {
    let plugin = std::env::var("STREAMFUSION_TEST_ROCKSDB_PLUGIN").unwrap();
    let directory = tempfile::tempdir().unwrap();
    let mut source = RocksPluginKeyedState::open(
        std::path::Path::new(&plugin),
        &directory.path().join("source"),
        2,
        3,
        1 << 20,
    )
    .unwrap();
    let mut entries = entries(8192, 1024).into_iter();
    loop {
        let page = entries.by_ref().take(256).collect::<Vec<_>>();
        if page.is_empty() {
            break;
        }
        source.write_batch(page).unwrap();
    }
    let checkpoint = directory.path().join("checkpoint");
    source.checkpoint(&checkpoint).unwrap();
    drop(source);
    let source =
        RocksPluginKeyedState::open(std::path::Path::new(&plugin), &checkpoint, 2, 3, 1 << 20)
            .unwrap();
    let mut destination = RocksPluginKeyedState::open(
        std::path::Path::new(&plugin),
        &directory.path().join("target"),
        2,
        3,
        1 << 20,
    )
    .unwrap();
    let broker = Arc::new(TestBroker::new(4 << 20));
    let owner = HostMemoryReservation::new(broker.clone(), "physical join restore");
    let mut caches = owner.sibling("source and destination caches");
    caches.resize(2 << 20).unwrap();
    assert!(source.snapshot_key_group(2, &owner).is_err());
    restore_from_checkpoint(&mut destination, &source, 2, &owner).unwrap();
    restore_from_checkpoint(&mut destination, &source, 3, &owner).unwrap();
    assert_eq!(broker.reserved(), 2 << 20);
    let expected = entries_for_comparison();
    for page in expected.chunks(128) {
        let keys = page
            .iter()
            .map(|entry| StateKeyRef {
                key_group: 2,
                key: &entry.key.key,
            })
            .collect::<Vec<_>>();
        let actual = destination.get_batch(&keys, &owner).unwrap();
        for (expected, actual) in page.iter().zip(actual.iter()) {
            assert_eq!(
                expected.value.as_ref().unwrap().as_slice(),
                actual.as_ref().unwrap().as_ref()
            );
        }
    }
    assert!(
        restore_from_checkpoint(&mut destination, &source, 2, &owner)
            .unwrap_err()
            .to_string()
            .contains("restored more than once")
    );
    assert_eq!(broker.reserved(), 2 << 20);
}

fn entries_for_comparison() -> Vec<StateMutation> {
    entries(8192, 1024)
}
