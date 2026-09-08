// Copyright 2026 StreamFusion Authors
// Licensed under the Apache License, Version 2.0

use super::*;
use crate::memory_pool::tests_support::TestBroker;

fn fixture() -> StagedState {
    let value = JoinState {
        left: (0..1024)
            .map(|id| StoredRow {
                id,
                row: Arc::from(vec![id as u8; 1024]),
                associations: 0,
            })
            .collect(),
        right: (0..1024)
            .map(|id| StoredRow {
                id,
                row: Arc::from(vec![id as u8; 1024]),
                associations: 0,
            })
            .collect(),
        next_row_id: [1024, 1024],
        ..Default::default()
    };
    StagedState {
        key: StateKey {
            key_group: 0,
            key: b"hot-key".to_vec(),
        },
        original: value.clone(),
        original_compact: false,
        value,
        touched: true,
    }
}

#[test]
fn paged_decode_reserves_payload_and_row_vectors_within_a_bounded_share() {
    for width in [0, 16, 1024] {
        let broker = Arc::new(TestBroker::new(10 << 20));
        let mut state = MemoryKeyedState::new(
            0,
            0,
            HostMemoryReservation::new(broker.clone(), "retained join pages"),
        )
        .unwrap();
        let mut entry = fixture();
        for row in entry.value.left.iter_mut().chain(&mut entry.value.right) {
            row.row = Arc::from(vec![row.id as u8; width]);
        }
        entry.original = JoinState::default();
        state
            .write_batch(paged_state::mutations(&entry).unwrap())
            .unwrap();
        let mut workspace = HostMemoryReservation::new(broker.clone(), "decoded join pages");
        let ((loaded, reads), observed) = crate::allocation_test_support::measure(|| {
            paged_state::load(&state, vec![entry.key.clone()], &mut workspace).unwrap()
        });
        assert_eq!(reads, 2);
        assert_eq!(loaded[0].value, entry.value);
        assert_eq!(loaded[0].original, entry.value);
        assert!(
            observed.peak <= workspace.size(),
            "width={width} {observed:?}"
        );
        for (before, after) in loaded[0].original.left.iter().zip(&loaded[0].value.left) {
            assert!(Arc::ptr_eq(&before.row, &after.row));
        }
        drop(loaded);
        drop(workspace);
        drop(state);
        assert_eq!(broker.reserved(), 0);
    }
}

#[test]
fn hot_key_changes_write_only_stable_dirty_pages() {
    let mut entry = fixture();
    assert!(paged_state::mutations(&entry).unwrap().is_empty());
    entry.value.left.push(StoredRow {
        id: 1024,
        row: Arc::from(&b"new"[..]),
        associations: 0,
    });
    entry.value.next_row_id[0] += 1;
    let mutations = paged_state::mutations(&entry).unwrap();
    assert_eq!(mutations.len(), 2); // one new page plus the small page directory
    assert!(
        mutations
            .iter()
            .filter_map(|change| change.value.as_ref())
            .map(Vec::len)
            .sum::<usize>()
            < 512
    );

    let mut entry = fixture();
    entry.value.left.remove(1);
    let mutations = paged_state::mutations(&entry).unwrap();
    assert_eq!(mutations.len(), 1);
    assert_eq!(mutations[0].key, paged_codec::page_key(&entry.key, 0, 0));
    assert!(mutations[0].value.as_ref().unwrap().len() < 64 * 1040);

    let mut entry = fixture();
    entry.value.left.drain(..64);
    let mutations = paged_state::mutations(&entry).unwrap();
    assert_eq!(mutations.len(), 2);
    assert_eq!(
        mutations[0],
        StateMutation {
            key: paged_codec::page_key(&entry.key, 0, 0),
            value: None
        }
    );

    let mut entry = fixture();
    entry.value.right[65].associations = 1;
    let mutations = paged_state::mutations(&entry).unwrap();
    assert_eq!(mutations.len(), 1);
    assert_eq!(mutations[0].key, paged_codec::page_key(&entry.key, 1, 1));
}

#[test]
fn paged_load_canonical_restore_and_dirty_write_match_on_memory_and_rocksdb() {
    let broker = Arc::new(TestBroker::new(128 << 20));
    let mut memory: Box<dyn KeyedState> = Box::new(
        MemoryKeyedState::new(
            0,
            0,
            HostMemoryReservation::new(broker.clone(), "paged memory"),
        )
        .unwrap(),
    );
    let mut entry = fixture();
    entry.original = JoinState::default();
    memory
        .write_batch(paged_state::mutations(&entry).unwrap())
        .unwrap();
    let mut workspace = HostMemoryReservation::new(broker.clone(), "paged IO");
    let (mut loaded, reads) =
        paged_state::load(memory.as_ref(), vec![entry.key.clone()], &mut workspace).unwrap();
    assert_eq!(reads, 2);
    assert_eq!(loaded[0].value, entry.value);
    loaded[0].value.left.remove(0);
    loaded[0].touched = true;
    memory
        .write_batch(paged_state::batch_mutations(&loaded, &mut workspace).unwrap())
        .unwrap();
    let snapshot = memory.snapshot_key_group(0, &workspace).unwrap();
    let expected = loaded.pop().unwrap().value;
    drop(loaded);
    drop(workspace);

    let Ok(plugin) = std::env::var("STREAMFUSION_TEST_ROCKSDB_PLUGIN") else {
        return;
    };
    let directory = tempfile::tempdir().unwrap();
    let mut rocks = RocksPluginKeyedState::open(
        std::path::Path::new(&plugin),
        directory.path(),
        0,
        0,
        16 << 20,
    )
    .unwrap();
    let mut workspace = HostMemoryReservation::new(broker.clone(), "paged restore");
    paged_state::restore(&mut rocks, 0, &snapshot, &workspace).unwrap();
    assert_eq!(rocks.snapshot_key_group(0, &workspace).unwrap(), snapshot);
    let (loaded, reads) = paged_state::load(&rocks, vec![entry.key], &mut workspace).unwrap();
    assert_eq!(reads, 2);
    assert_eq!(loaded[0].value, expected);
    drop(loaded);
    drop(snapshot);
    drop(workspace);
    drop(memory);
    drop(rocks);
    assert_eq!(broker.reserved(), 0);
}

#[test]
fn legacy_whole_key_snapshot_migrates_and_dangling_pages_are_rejected_before_restore() {
    let broker = Arc::new(TestBroker::new(128 << 20));
    let owner = HostMemoryReservation::new(broker.clone(), "legacy join restore");
    let mut state = MemoryKeyedState::new(0, 0, owner.sibling("state")).unwrap();
    let entry = fixture();
    for version in [LEGACY_STATE_VERSION, STATE_VERSION] {
        state = MemoryKeyedState::new(0, 0, owner.sibling("state")).unwrap();
        let mut legacy = encode_state(&entry.value);
        legacy[4] = version;
        if version == LEGACY_STATE_VERSION {
            legacy.drain(5..7); // v1 predates the two matchability flags
        }
        let bytes = streamfusion_state_abi::encode_key_group_snapshot(
            0,
            std::iter::once((entry.key.key.as_slice(), legacy.as_slice())),
        )
        .unwrap();
        paged_state::restore(&mut state, 0, &bytes, &owner).unwrap();
        let snapshot = state.snapshot_key_group(0, &owner).unwrap();
        let entries = decode_key_group_snapshot(0, &snapshot).unwrap();
        assert_eq!(
            paged_state::decode_entries(0, &entries).unwrap()[0].1,
            entry.value
        );
    }
    let snapshot = state.snapshot_key_group(0, &owner).unwrap();
    let entries = decode_key_group_snapshot(0, &snapshot).unwrap();
    let logical = paged_state::decode_entries(0, &entries).unwrap();
    assert_eq!(logical[0].1, entry.value);
    let mut corrupt = entries;
    corrupt.pop();
    let corrupt = streamfusion_state_abi::encode_key_group_snapshot(
        0,
        corrupt
            .iter()
            .map(|(key, value)| (key.as_slice(), value.as_slice())),
    )
    .unwrap();
    assert!(paged_state::restore(&mut state, 0, &corrupt, &owner).is_err());
    assert_eq!(state.snapshot_key_group(0, &owner).unwrap(), snapshot);
    drop(snapshot);
    drop(state);
    assert_eq!(broker.reserved(), 0);
}

#[test]
fn canonical_validation_releases_its_workspace_before_backend_restore() {
    let mut entry = fixture();
    entry.value.left.truncate(64);
    entry.value.right.clear();
    entry.original = JoinState::default();
    let mut changes = paged_state::mutations(&entry).unwrap();
    changes.sort_by(|left, right| left.key.key.cmp(&right.key.key));
    let bytes = streamfusion_state_abi::encode_key_group_snapshot(
        0,
        changes
            .iter()
            .map(|change| (change.key.key.as_slice(), change.value.as_deref().unwrap())),
    )
    .unwrap();
    let validation = bytes.len() * 16 + changes.len() * 512;
    // Each phase fits independently; retaining the validation lease while restoring cannot fit.
    for rocks in [false, true] {
        let broker = Arc::new(TestBroker::new(validation + 512));
        let owner = HostMemoryReservation::new(broker.clone(), "non-overlapping restore");
        let directory = tempfile::tempdir().unwrap();
        let mut state: Box<dyn KeyedState> = if rocks {
            let Ok(plugin) = std::env::var("STREAMFUSION_TEST_ROCKSDB_PLUGIN") else {
                continue;
            };
            Box::new(
                RocksPluginKeyedState::open(
                    std::path::Path::new(&plugin),
                    directory.path(),
                    0,
                    0,
                    16 << 20,
                )
                .unwrap(),
            )
        } else {
            Box::new(MemoryKeyedState::new(0, 0, owner.sibling("restored state")).unwrap())
        };
        paged_state::restore(state.as_mut(), 0, &bytes, &owner).unwrap();
        assert!(
            state.snapshot_key_group(0, &owner).unwrap().as_ref() == bytes.as_slice(),
            "canonical bytes changed on backend rocks={rocks}"
        );
        drop(state);
        drop(owner);
        assert_eq!(broker.reserved(), 0);
    }
}
