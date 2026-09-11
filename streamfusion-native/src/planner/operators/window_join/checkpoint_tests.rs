// Copyright 2026 StreamFusion Authors
// Licensed under the Apache License, Version 2.0.

use super::indexed_tests::{backends, processor};
use super::tests::{batch, plan};
use super::*;
use crate::memory_pool::tests_support::TestBroker;

#[test]
fn physical_restore_rejects_unknown_mixed_and_future_index_state_before_target_mutation() {
    if backends().len() == 1 {
        return;
    }
    for variant in 0..3 {
        let (mut source, _, _source_dir) = processor(true, 0, 127);
        source
            .process_arrow(0, batch(&[7], &[100], &[b"value"], &[INSERT]))
            .unwrap();
        let group = *source.dirty_timer_groups.iter().next().unwrap();
        let snapshot = source.snapshot_key_group(group).unwrap();
        let (key, value, reason) = match variant {
            0 => (vec![255], vec![], "unknown window join checkpoint key"),
            1 => (
                window_state_key(group, b"legacy", 100).key,
                encode_state(&JoinWindowState::default()),
                "mixes legacy and indexed",
            ),
            _ => {
                let (key, mut value) = crate::state::decode_key_group_snapshot(group, &snapshot)
                    .unwrap()
                    .into_iter()
                    .find(|(key, _)| key[0] == 0x91)
                    .unwrap();
                value[4] = 5;
                (key, value, "invalid window join index version")
            }
        };
        source
            .state
            .write_batch(vec![StateMutation {
                key: StateKey {
                    key_group: group,
                    key,
                },
                value: Some(value),
            }])
            .unwrap();
        let directory = tempfile::tempdir().unwrap();
        let checkpoint = directory.path().join("checkpoint");
        source.checkpoint(&checkpoint).unwrap();
        let plugin =
            std::path::PathBuf::from(std::env::var("STREAMFUSION_TEST_ROCKSDB_PLUGIN").unwrap());
        let reader =
            RocksPluginKeyedState::open_checkpoint(&plugin, &checkpoint, group, group, 1 << 20)
                .unwrap();
        for rocks in backends() {
            let (mut target, _, _target_dir) = processor(rocks, group, group);
            let error = target
                .restore_physical_key_group(group, &reader)
                .unwrap_err()
                .to_string();
            assert!(error.contains(reason), "{error}");
            assert_eq!(target.statistics()[5], 0);
            let bytes = target.snapshot_key_group(group).unwrap();
            assert!(crate::state::decode_key_group_snapshot(group, &bytes)
                .unwrap()
                .is_empty());
        }
    }
}

#[test]
fn indexed_restore_and_streamed_snapshot_do_not_require_a_whole_group_workspace() {
    if backends().len() == 1 {
        return;
    }
    let (mut source, _, _source_dir) = processor(true, 0, 127);
    let payload = vec![47; 8192];
    for _ in 0..32 {
        source
            .process_arrow(
                0,
                batch(
                    &[7; 32],
                    &[100; 32],
                    &[payload.as_slice(); 32],
                    &[INSERT; 32],
                ),
            )
            .unwrap();
    }
    let group = *source.dirty_timer_groups.iter().next().unwrap();
    let expected = source.snapshot_key_group(group).unwrap();
    assert!(expected.len() > 8 << 20);
    let directory = tempfile::tempdir().unwrap();
    let checkpoint = directory.path().join("checkpoint");
    source.checkpoint(&checkpoint).unwrap();
    drop(source);
    let plugin =
        std::path::PathBuf::from(std::env::var("STREAMFUSION_TEST_ROCKSDB_PLUGIN").unwrap());
    for physical in [false, true] {
        let broker = Arc::new(TestBroker::new(4 << 20));
        let owner = HostMemoryReservation::new(broker.clone(), "window join restore test");
        let target_dir = tempfile::tempdir().unwrap();
        let mut target = WindowJoinProcessor::new_rocksdb(
            &plan(),
            128,
            group,
            group,
            &plugin,
            target_dir.path(),
            1 << 20,
            owner.sibling("target"),
        )
        .unwrap();
        if physical {
            let mut source_cache = owner.sibling("checkpoint reader cache");
            source_cache.resize(1 << 20).unwrap();
            let reader =
                RocksPluginKeyedState::open_checkpoint(&plugin, &checkpoint, group, group, 1 << 20)
                    .unwrap();
            target.restore_physical_key_group(group, &reader).unwrap();
        } else {
            // The input frame is already held by the source's independent reservation. Only
            // target import and migration workspace consume this much smaller destination budget.
            target.restore_key_group(group, &expected).unwrap();
        }
        assert_eq!(target.statistics()[5], 1);
        assert!(target
            .snapshot_key_group(group)
            .unwrap_err()
            .to_string()
            .contains("Flink denied"));
        let mut offset = 0;
        let prefix = (expected.len() as i32).to_be_bytes();
        let size = target
            .write_snapshot(group, &mut |chunk| {
                if offset == 0 {
                    assert_eq!(chunk, prefix);
                } else {
                    assert_eq!(chunk, &expected[offset - 4..offset - 4 + chunk.len()]);
                }
                offset += chunk.len();
                Ok(())
            })
            .unwrap();
        assert_eq!(size, expected.len() + 4);
        assert_eq!(offset, size);
        drop(target);
        assert_eq!(broker.reserved(), 0);
    }
}
