// Copyright 2026 StreamFusion Authors
// Licensed under the Apache License, Version 2.0.

use super::*;

fn plugin() -> std::path::PathBuf {
    std::env::var("STREAMFUSION_TEST_ROCKSDB_PLUGIN")
        .unwrap()
        .into()
}

#[test]
fn ordered_unordered_and_physical_restore_preserve_session_merges_and_flink_watermarks() {
    let broker = Arc::new(TestBroker::new(128 << 20));
    let directory = tempfile::tempdir().unwrap();
    let mut source = processor(broker.clone(), Some(&directory.path().join("source")));
    source
        .process(&batch(vec![10000, 40000, 80000, 80000]))
        .unwrap();
    assert!(advance(&mut source, 15000).is_empty());
    let snapshots = (0..128)
        .map(|group| source.snapshot(group).unwrap())
        .collect::<Vec<_>>();
    let checkpoint = directory.path().join("checkpoint");
    source.checkpoint(&checkpoint).unwrap();
    let reader =
        RocksPluginKeyedState::open_checkpoint(&plugin(), &checkpoint, 0, 127, 1 << 20).unwrap();
    source
        .process(&batch(vec![0, 20000, 30000, 120000]))
        .unwrap();
    let expected = advance(&mut source, i64::MAX);
    assert_eq!(
        expected,
        [(5, 0, 50000), (2, 80000, 90000), (1, 120000, 130000)]
    );
    for rocks in [false, true] {
        for mode in 0..3 {
            let target_dir = tempfile::tempdir().unwrap();
            let mut target = processor(broker.clone(), rocks.then_some(target_dir.path()));
            for group in 0..128 {
                match mode {
                    0 => target
                        .restore(group, &snapshots[group as usize], 15000)
                        .unwrap(),
                    1 => {
                        let mut entries = crate::state::decode_key_group_snapshot(
                            group,
                            &snapshots[group as usize],
                        )
                        .unwrap();
                        entries.reverse();
                        let unordered = streamfusion_state_abi::encode_key_group_snapshot(
                            group,
                            entries
                                .iter()
                                .map(|(key, value)| (key.as_slice(), value.as_slice())),
                        )
                        .unwrap();
                        target.restore(group, &unordered, 15000).unwrap();
                    }
                    _ => target.restore_physical(group, &reader, 15000).unwrap(),
                }
            }
            target
                .process(&batch(vec![0, 20000, 30000, 120000]))
                .unwrap();
            assert_eq!(advance(&mut target, i64::MAX), expected);
        }
    }
}

#[test]
fn invalid_current_session_state_is_rejected_before_target_mutation() {
    let broker = Arc::new(TestBroker::new(128 << 20));
    let mut source = processor(broker.clone(), None);
    source.process(&batch(vec![10000, 40000])).unwrap();
    let (group, snapshot) = (0..128)
        .map(|group| (group, source.snapshot(group).unwrap()))
        .find(|(_, bytes)| bytes.len() > 300)
        .unwrap();
    let original = crate::state::decode_key_group_snapshot(group, &snapshot).unwrap();
    for variant in 0..4 {
        let mut entries = original.clone();
        let reason = if variant == 0 {
            entries
                .iter_mut()
                .find(|(key, _)| key == super::super::checkpoint::MARKER_KEY)
                .unwrap()
                .1[0] ^= 1;
            "plan/version marker differs"
        } else {
            let value = &mut entries
                .iter_mut()
                .rfind(|(key, _)| codec::grouping(&key[..key.len().saturating_sub(9)]).is_ok())
                .unwrap()
                .1;
            match variant {
                1 => {
                    value[5..13].copy_from_slice(&20000i64.to_le_bytes());
                    "overlapping or invalid persisted sessions"
                }
                2 => {
                    value[13..21].copy_from_slice(&50001i64.to_le_bytes());
                    "session key or restored watermark"
                }
                _ => "session key or restored watermark",
            }
        };
        let watermark = if variant == 3 { 50000 } else { i64::MIN };
        let bytes = streamfusion_state_abi::encode_key_group_snapshot(
            group,
            entries
                .iter()
                .map(|(key, value)| (key.as_slice(), value.as_slice())),
        )
        .unwrap();
        let directory = tempfile::tempdir().unwrap();
        let mut database = RocksPluginKeyedState::open(
            &plugin(),
            &directory.path().join("source"),
            group,
            group,
            1 << 20,
        )
        .unwrap();
        database
            .restore_key_group(group, &bytes, &source.kernel.scratch_reservation)
            .unwrap();
        let checkpoint = directory.path().join("checkpoint");
        database.checkpoint(&checkpoint).unwrap();
        let reader =
            RocksPluginKeyedState::open_checkpoint(&plugin(), &checkpoint, group, group, 1 << 20)
                .unwrap();
        for physical in [false, true] {
            let mut target = processor(broker.clone(), None);
            let baseline = broker.reserved();
            let error = if physical {
                target.restore_physical(group, &reader, watermark)
            } else {
                target.restore(group, &bytes, watermark)
            }
            .unwrap_err()
            .to_string();
            assert!(error.contains(reason), "{error}");
            assert_eq!(broker.reserved(), baseline);
            let snapshot = target.kernel.snapshot_key_group(group).unwrap();
            assert!(crate::state::decode_key_group_snapshot(group, &snapshot)
                .unwrap()
                .is_empty());
        }
    }
}

#[test]
fn session_restore_pages_state_without_a_second_complete_interval_index() {
    let broker = Arc::new(TestBroker::new(512 << 20));
    let directory = tempfile::tempdir().unwrap();
    let mut source = processor(broker.clone(), Some(&directory.path().join("source")));
    for first in (0..10_000).step_by(128) {
        source
            .process(&batch(
                (first..(first + 128).min(10_000))
                    .map(|index| index * 20000 + 10000)
                    .collect(),
            ))
            .unwrap();
    }
    let (group, snapshot) = (0..128)
        .map(|group| (group, source.snapshot(group).unwrap()))
        .find(|(_, bytes)| bytes.len() > 100_000)
        .unwrap();
    let checkpoint = directory.path().join("checkpoint");
    source.checkpoint(&checkpoint).unwrap();
    let reader =
        RocksPluginKeyedState::open_checkpoint(&plugin(), &checkpoint, group, group, 1 << 20)
            .unwrap();
    for physical in [false, true] {
        let target_broker = Arc::new(TestBroker::new(16 << 20));
        let target_dir = tempfile::tempdir().unwrap();
        let mut target = processor(target_broker.clone(), Some(target_dir.path()));
        let mut caches = target
            .kernel
            .scratch_reservation
            .sibling("reader and destination caches");
        caches.resize(9 << 20).unwrap();
        let mut old_workspace = target
            .kernel
            .scratch_reservation
            .sibling("old whole-group reservation");
        assert!(old_workspace
            .resize(snapshot.len().saturating_mul(8).saturating_add(65536))
            .is_err());
        if physical {
            target.restore_physical(group, &reader, i64::MIN).unwrap();
        } else {
            target.restore(group, &snapshot, i64::MIN).unwrap();
        }
        let mut offset = 0;
        target
            .write_snapshot(group, &mut |chunk| {
                if offset == 0 {
                    assert_eq!(chunk, (snapshot.len() as i32).to_be_bytes());
                } else {
                    assert_eq!(chunk, &snapshot[offset - 4..offset - 4 + chunk.len()]);
                }
                offset += chunk.len();
                Ok(())
            })
            .unwrap();
        assert_eq!(offset, snapshot.len() + 4);
        assert_eq!(advance(&mut target, 19999), [(1, 10000, 20000)]);
        drop(target);
        drop(caches);
        assert_eq!(target_broker.reserved(), 0);
    }
}
