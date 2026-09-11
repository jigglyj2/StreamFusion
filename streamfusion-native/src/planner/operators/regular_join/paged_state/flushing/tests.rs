// Copyright 2026 StreamFusion Authors
// Licensed under the Apache License, Version 2.0.

use super::*;
use crate::memory_pool::tests_support::TestBroker;
use crate::state::observed_tests::{Io, Observed};
use std::sync::atomic::Ordering;

fn fixture(count: u64, width: usize) -> StagedState {
    let original = JoinState {
        next_row_id: [count, 0],
        left: (0..count)
            .map(|id| StoredRow {
                id,
                row: Arc::from(vec![id as u8; width]),
                associations: 0,
            })
            .collect(),
        ..Default::default()
    };
    let mut entry = StagedState {
        key: StateKey {
            key_group: 0,
            key: b"hot-key".to_vec(),
        },
        value: original.clone(),
        original,
        original_layout: Layout::Rows,
        unloaded: None,
        touched: true,
    };
    entry.original_layout = mutations::layout(&entry);
    for row in &mut entry.value.left {
        row.associations += 1;
    }
    if count > 1 {
        entry.value.left.remove(0);
    }
    entry.value.left.push(StoredRow {
        id: count,
        row: Arc::from(vec![99; width]),
        associations: 1,
    });
    entry.value.next_row_id[0] += 1;
    entry
}

fn initialize(state: &mut dyn KeyedState, entry: &StagedState) {
    let old = StagedState {
        key: entry.key.clone(),
        value: entry.original.clone(),
        original: JoinState::default(),
        original_layout: Layout::Compact,
        unloaded: None,
        touched: true,
    };
    state
        .write_batch(mutations::mutations(&old).unwrap())
        .unwrap();
}

#[test]
fn bounded_flush_preserves_exact_state_across_compact_and_row_layout_changes() {
    let broker = Arc::new(TestBroker::new(128 << 20));
    let owner = HostMemoryReservation::new(broker, "expected state");
    for (count, width) in [(0, 0), (1, 16), (65, 17), (1025, 1024), (2, 400_000)] {
        let mut expected = MemoryKeyedState::new(0, 0, owner.sibling("expected")).unwrap();
        let entry = fixture(count, width);
        initialize(&mut expected, &entry);
        expected
            .write_batch(mutations::mutations(&entry).unwrap())
            .unwrap();
        let snapshot = expected.snapshot_key_group(0, &owner).unwrap();
        for rocks in [false, true] {
            let plugin = std::env::var("STREAMFUSION_TEST_ROCKSDB_PLUGIN").ok();
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
            let entry = fixture(count, width);
            initialize(target.as_mut(), &entry);
            let mut memory = owner.sibling("flush");
            let mut writes = 0;
            flush(target.as_mut(), vec![entry], &mut memory, &mut writes).unwrap();
            assert!(writes > 0);
            assert_eq!(memory.size(), 0);
            assert_eq!(target.snapshot_key_group(0, &owner).unwrap(), snapshot);
        }
    }
}

#[test]
fn wide_hot_key_flush_does_not_build_a_second_complete_payload_collection() {
    let Ok(plugin) = std::env::var("STREAMFUSION_TEST_ROCKSDB_PLUGIN") else {
        return;
    };
    let directory = tempfile::tempdir().unwrap();
    let mut state = RocksPluginKeyedState::open(
        std::path::Path::new(&plugin),
        directory.path(),
        0,
        0,
        1 << 20,
    )
    .unwrap();
    let entry = fixture(2048, 4096);
    initialize(&mut state, &entry);
    let expected = entry.value.clone();
    let io = Arc::new(Io::default());
    let mut state = Observed {
        inner: Box::new(state),
        io: io.clone(),
    };
    let broker = Arc::new(TestBroker::new(14 << 20));
    let mut memory = HostMemoryReservation::new(broker.clone(), "hot-key flush");
    let mut cache = memory.sibling("RocksDB cache");
    cache.resize(1 << 20).unwrap();
    let mut writes = 0;
    let ((), allocated) = crate::allocation_test_support::measure(|| {
        flush(&mut state, vec![entry], &mut memory, &mut writes).unwrap()
    });
    assert!(writes > 1);
    assert_eq!(writes, io.write_batches.load(Ordering::Relaxed) as u64);
    assert_eq!(
        io.read_batches.load(Ordering::Relaxed),
        0,
        "flush never reloads RocksDB rows"
    );
    assert!(
        allocated.peak < 3 << 20,
        "{allocated:?}: encoded state must stay page-bounded"
    );
    assert_eq!(broker.reserved(), 1 << 20);
    let mut check = HostMemoryReservation::new(Arc::new(TestBroker::new(64 << 20)), "verification");
    let (actual, _) = super::super::load(
        &state,
        vec![StateKey {
            key_group: 0,
            key: b"hot-key".to_vec(),
        }],
        &mut check,
    )
    .unwrap();
    assert_eq!(actual[0].value, expected);
    drop(cache);
    assert_eq!(broker.reserved(), 0);
}
