// Copyright 2026 StreamFusion Authors
// Licensed under the Apache License, Version 2.0.

use super::*;

#[test]
fn current_slice_restore_reserves_entries_instead_of_eight_copies_of_the_group() {
    if backends().len() == 1 {
        return;
    }
    let source_broker = Arc::new(TestBroker::new(512 << 20));
    let source_dir = tempfile::tempdir().unwrap();
    let mut source = processor(source_broker.clone(), Some(source_dir.path()), 0, 127);
    for start in (0..10_000).step_by(128) {
        let rows = (start..(start + 128).min(10_000))
            .map(|index| (1, index + 1, (index + 1) * 2000))
            .collect::<Vec<_>>();
        source.process(&batch(&rows)).unwrap();
    }
    let group = source
        .kernel
        .timers
        .key_group_range()
        .find(|&group| {
            source
                .kernel
                .timers
                .snapshot_key_group(group)
                .unwrap()
                .len()
                > 100_000
        })
        .unwrap();
    let expected = source.snapshot(group).unwrap();
    let checkpoint_dir = tempfile::tempdir().unwrap();
    let checkpoint = checkpoint_dir.path().join("checkpoint");
    source.checkpoint(&checkpoint).unwrap();
    let plugin =
        std::path::PathBuf::from(std::env::var("STREAMFUSION_TEST_ROCKSDB_PLUGIN").unwrap());
    let reader =
        RocksPluginKeyedState::open_checkpoint(&plugin, &checkpoint, group, group, 1 << 20)
            .unwrap();
    for physical in [false, true] {
        let broker = Arc::new(TestBroker::new(16 << 20));
        let directory = tempfile::tempdir().unwrap();
        let mut restored = processor(broker.clone(), Some(directory.path()), group, group);
        let mut reader_cache = restored
            .kernel
            .scratch_reservation
            .sibling("reader and destination caches");
        reader_cache.resize(9 << 20).unwrap();
        // The previous reservation alone exceeded the available destination budget.
        let mut old_workspace = restored
            .kernel
            .scratch_reservation
            .sibling("old whole-group allowance");
        assert!(old_workspace
            .resize(expected.len().saturating_mul(8).saturating_add(4096))
            .is_err());
        if physical {
            restored.restore_physical(group, &reader, i64::MIN).unwrap();
        } else {
            restored.restore(group, &expected, i64::MIN).unwrap();
        }
        assert_eq!(
            restored.kernel.timers.snapshot_key_group(group).unwrap(),
            source.kernel.timers.snapshot_key_group(group).unwrap()
        );
        let mut offset = 0;
        restored
            .write_snapshot(group, &mut |chunk| {
                if offset == 0 {
                    assert_eq!(chunk, (expected.len() as i32).to_be_bytes());
                } else {
                    assert_eq!(chunk, &expected[offset - 4..offset - 4 + chunk.len()]);
                }
                offset += chunk.len();
                Ok(())
            })
            .unwrap();
        assert_eq!(offset, expected.len() + 4);
        drop(restored);
        drop(reader_cache);
        assert_eq!(broker.reserved(), 0);
    }
}

#[test]
fn physical_slice_validation_rejects_changed_markers_before_mutating_state() {
    if backends().len() == 1 {
        return;
    }
    let broker = Arc::new(TestBroker::new(64 << 20));
    let directory = tempfile::tempdir().unwrap();
    let mut source = processor(broker.clone(), Some(&directory.path().join("live")), 0, 127);
    source
        .checkpoint(&directory.path().join("checkpoint"))
        .unwrap();
    let plugin =
        std::path::PathBuf::from(std::env::var("STREAMFUSION_TEST_ROCKSDB_PLUGIN").unwrap());
    let reader = RocksPluginKeyedState::open_checkpoint(
        &plugin,
        &directory.path().join("checkpoint"),
        0,
        127,
        1 << 20,
    )
    .unwrap();
    let mut restored = processor(broker.clone(), None, 0, 127);
    restored.kernel.plan.offset_millis = 1;
    let baseline = broker.reserved();
    assert!(restored
        .restore_physical(0, &reader, i64::MIN)
        .unwrap_err()
        .to_string()
        .contains("matching versioned slice state"));
    assert_eq!(broker.reserved(), baseline);
    let snapshot = restored.kernel.snapshot_key_group(0).unwrap();
    assert!(crate::state::decode_key_group_snapshot(0, &snapshot)
        .unwrap()
        .is_empty());
}
