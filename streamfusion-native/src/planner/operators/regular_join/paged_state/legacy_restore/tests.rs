// Copyright 2026 StreamFusion Authors
// Licensed under the Apache License, Version 2.0.

use super::*;
use crate::memory_pool::tests_support::TestBroker;
use crate::state::SnapshotBytes;

fn legacy(counts: [usize; 2], width: usize, version: u8) -> Vec<u8> {
    let [left, right] = counts.map(|count| {
        (0..count)
            .map(|id| StoredRow {
                id: id as u64,
                associations: (id % 7) as i32,
                row: Arc::from(vec![id as u8; width]),
            })
            .collect()
    });
    let state = JoinState {
        left,
        right,
        left_matchable: Some(true),
        right_matchable: Some(false),
        ..Default::default()
    };
    let mut value = encode_state(&state);
    value[4] = version;
    if version == LEGACY_STATE_VERSION {
        value.drain(5..7);
    }
    value
}

fn frame(entries: &[(Vec<u8>, Vec<u8>)]) -> Vec<u8> {
    streamfusion_state_abi::encode_key_group_snapshot(
        0,
        entries
            .iter()
            .map(|(key, value)| (key.as_slice(), value.as_slice())),
    )
    .unwrap()
}

fn expected(entries: &[(Vec<u8>, Vec<u8>)], owner: &HostMemoryReservation) -> SnapshotBytes {
    let mut state = MemoryKeyedState::new(0, 0, owner.sibling("expected migration")).unwrap();
    for (key, value) in entries {
        let staged = StagedState {
            key: StateKey {
                key_group: 0,
                key: key.clone(),
            },
            value: decode_state(value).unwrap(),
            original: JoinState::default(),
            original_layout: Layout::Pages,
            unloaded: None,
            touched: true,
        };
        state.write_batch(mutations(&staged).unwrap()).unwrap();
    }
    state.snapshot_key_group(0, owner).unwrap()
}

#[test]
fn legacy_versions_preserve_exact_compact_and_dense_directory_bytes_on_both_backends() {
    let owner = HostMemoryReservation::new(
        Arc::new(TestBroker::new(128 << 20)),
        "legacy migration parity",
    );
    let plugin = std::env::var("STREAMFUSION_TEST_ROCKSDB_PLUGIN").ok();
    for version in [LEGACY_STATE_VERSION, STATE_VERSION] {
        let entries = [
            ([0, 0], 0),
            ([1, 0], 0),
            ([0, 1], 8116),
            ([1, 0], 8117),
            ([1, 1], 4),
            ([63, 64], 17),
            ([65, 4097], 4),
        ]
        .into_iter()
        .enumerate()
        .map(|(index, (counts, width))| (vec![index as u8], legacy(counts, width, version)))
        .rev()
        .collect::<Vec<_>>();
        let bytes = frame(&entries); // Unordered old canonical frames remain supported.
        let expected = expected(&entries, &owner);
        for rocks in [false, true] {
            if rocks && plugin.is_none() {
                continue;
            }
            let directory = tempfile::tempdir().unwrap();
            let mut target: Box<dyn KeyedState> = if rocks {
                Box::new(
                    RocksPluginKeyedState::open(
                        std::path::Path::new(plugin.as_ref().unwrap()),
                        directory.path(),
                        0,
                        0,
                        1 << 20,
                    )
                    .unwrap(),
                )
            } else {
                Box::new(MemoryKeyedState::new(0, 0, owner.sibling("target")).unwrap())
            };
            super::super::restore(target.as_mut(), 0, &bytes, &owner).unwrap();
            assert_eq!(target.snapshot_key_group(0, &owner).unwrap(), expected);
            assert!(super::super::restore(target.as_mut(), 0, &bytes, &owner)
                .unwrap_err()
                .to_string()
                .contains("restored more than once"));
        }
    }
}

#[test]
fn invalid_legacy_records_are_rejected_before_any_destination_write() {
    let owner =
        HostMemoryReservation::new(Arc::new(TestBroker::new(32 << 20)), "legacy corruption");
    let good = legacy([5000, 1], 64, STATE_VERSION);
    let mut huge = legacy([0, 0], 0, STATE_VERSION);
    huge[7..11].copy_from_slice(&u32::MAX.to_le_bytes());
    let mut short = legacy([1, 1], 3, STATE_VERSION);
    short.pop();
    let mut trailing = legacy([1, 0], 3, LEGACY_STATE_VERSION);
    trailing.push(99);
    let mut flags = legacy([1, 0], 3, STATE_VERSION);
    flags[5] = 3;
    for bad in [huge, short, trailing, flags, b"SFJI\x01".to_vec()] {
        let mut target = MemoryKeyedState::new(0, 0, owner.sibling("target")).unwrap();
        let before = target.snapshot_key_group(0, &owner).unwrap();
        let bytes = frame(&[(vec![1], good.clone()), (vec![2], bad)]);
        assert!(super::super::restore(&mut target, 0, &bytes, &owner).is_err());
        assert_eq!(target.snapshot_key_group(0, &owner).unwrap(), before);
    }
    let mut target = MemoryKeyedState::new(0, 0, owner.sibling("duplicates")).unwrap();
    assert!(super::super::restore(
        &mut target,
        0,
        &frame(&[(vec![1], good.clone()), (vec![1], good)]),
        &owner
    )
    .unwrap_err()
    .to_string()
    .contains("duplicate"));
}

#[test]
fn canonical_hot_key_migration_borrows_payloads_and_bounds_new_allocations() {
    let Ok(plugin) = std::env::var("STREAMFUSION_TEST_ROCKSDB_PLUGIN") else {
        return;
    };
    let entries = vec![(b"hot-key".to_vec(), legacy([4096, 1], 2048, STATE_VERSION))];
    let verification =
        HostMemoryReservation::new(Arc::new(TestBroker::new(128 << 20)), "verification");
    let expected = expected(&entries, &verification);
    let bytes = frame(&entries);
    drop(entries);
    assert!(bytes.len() > 8 << 20);
    let broker = Arc::new(TestBroker::new(bytes.capacity() + (4 << 20)));
    let owner = HostMemoryReservation::new(broker.clone(), "legacy hot-key migration");
    let mut frame_memory = owner.sibling("canonical input frame");
    frame_memory.resize(bytes.capacity()).unwrap();
    let mut cache = owner.sibling("destination cache");
    cache.resize(1 << 20).unwrap();
    let directory = tempfile::tempdir().unwrap();
    let mut target = RocksPluginKeyedState::open(
        std::path::Path::new(&plugin),
        directory.path(),
        0,
        0,
        1 << 20,
    )
    .unwrap();
    let ((), allocated) = crate::allocation_test_support::measure(|| {
        super::super::restore(&mut target, 0, &bytes, &owner).unwrap();
    });
    assert!(
        allocated.peak < 3 << 20,
        "migration must not duplicate the complete payload: {allocated:?}"
    );
    assert_eq!(broker.reserved(), bytes.capacity() + (1 << 20));
    assert_eq!(
        target.snapshot_key_group(0, &verification).unwrap(),
        expected
    );
    drop((target, cache, frame_memory));
    assert_eq!(broker.reserved(), 0);
}

#[test]
fn physical_legacy_migration_does_not_materialize_the_key_group() {
    let Ok(plugin) = std::env::var("STREAMFUSION_TEST_ROCKSDB_PLUGIN") else {
        return;
    };
    let directory = tempfile::tempdir().unwrap();
    let mut source = RocksPluginKeyedState::open(
        std::path::Path::new(&plugin),
        &directory.path().join("source"),
        0,
        0,
        1 << 20,
    )
    .unwrap();
    let entries = (0..64)
        .map(|key| (vec![key], legacy([128, 1], 1024, LEGACY_STATE_VERSION)))
        .collect::<Vec<_>>();
    for (key, value) in &entries {
        source
            .write_batch(vec![StateMutation {
                key: StateKey {
                    key_group: 0,
                    key: key.clone(),
                },
                value: Some(value.clone()),
            }])
            .unwrap();
    }
    let verification = HostMemoryReservation::new(
        Arc::new(TestBroker::new(128 << 20)),
        "physical verification",
    );
    let expected = expected(&entries, &verification);
    drop(entries);
    let checkpoint = directory.path().join("checkpoint");
    source.checkpoint(&checkpoint).unwrap();
    drop(source);
    let source = RocksPluginKeyedState::open_checkpoint(
        std::path::Path::new(&plugin),
        &checkpoint,
        0,
        0,
        1 << 20,
    )
    .unwrap();
    let mut target = RocksPluginKeyedState::open(
        std::path::Path::new(&plugin),
        &directory.path().join("target"),
        0,
        0,
        1 << 20,
    )
    .unwrap();
    let broker = Arc::new(TestBroker::new(4 << 20));
    let owner = HostMemoryReservation::new(broker.clone(), "physical legacy restore");
    let mut caches = owner.sibling("source and destination caches");
    caches.resize(2 << 20).unwrap();
    assert!(source.snapshot_key_group(0, &owner).is_err());
    super::super::restore_from_checkpoint(&mut target, &source, 0, &owner).unwrap();
    assert_eq!(broker.reserved(), 2 << 20);
    assert_eq!(
        target.snapshot_key_group(0, &verification).unwrap(),
        expected
    );
    drop((source, target, caches));
    assert_eq!(broker.reserved(), 0);
}

#[test]
fn corrupt_later_physical_value_cannot_leave_a_partially_migrated_destination() {
    let Ok(plugin) = std::env::var("STREAMFUSION_TEST_ROCKSDB_PLUGIN") else {
        return;
    };
    let directory = tempfile::tempdir().unwrap();
    let mut source = RocksPluginKeyedState::open(
        std::path::Path::new(&plugin),
        &directory.path().join("source"),
        0,
        0,
        1 << 20,
    )
    .unwrap();
    let mut bad = legacy([1, 1], 8, STATE_VERSION);
    bad.pop();
    for (key, value) in [
        (vec![1], legacy([5000, 1], 64, STATE_VERSION)),
        (vec![2], bad),
    ] {
        source
            .write_batch(vec![StateMutation {
                key: StateKey { key_group: 0, key },
                value: Some(value),
            }])
            .unwrap();
    }
    let checkpoint = directory.path().join("checkpoint");
    source.checkpoint(&checkpoint).unwrap();
    drop(source);
    let source = RocksPluginKeyedState::open_checkpoint(
        std::path::Path::new(&plugin),
        &checkpoint,
        0,
        0,
        1 << 20,
    )
    .unwrap();
    let owner = HostMemoryReservation::new(
        Arc::new(TestBroker::new(8 << 20)),
        "corrupt physical restore",
    );
    let mut target = MemoryKeyedState::new(0, 0, owner.sibling("target")).unwrap();
    let before = target.snapshot_key_group(0, &owner).unwrap();
    assert!(
        super::super::restore_from_checkpoint(&mut target, &source, 0, &owner)
            .unwrap_err()
            .to_string()
            .contains("truncated")
    );
    assert_eq!(target.snapshot_key_group(0, &owner).unwrap(), before);
}
