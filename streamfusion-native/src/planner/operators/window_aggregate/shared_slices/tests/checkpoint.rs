// Copyright 2026 StreamFusion Authors
// Licensed under the Apache License, Version 2.0.

use super::*;

#[test]
fn shared_slice_restore_uses_the_flink_operator_watermark_before_replayed_input() {
    let broker = Arc::new(TestBroker::new(512 << 20));
    let mut source = processor(broker.clone(), None, 0, 127);
    source.process(&batch(&[(1, 2, 2000)])).unwrap();
    drop(source.advance(1999).unwrap());
    source.process(&batch(&[(2, 3, 2000)])).unwrap();
    let snapshots = (0..128)
        .map(|group| source.snapshot(group).unwrap())
        .collect::<Vec<_>>();
    drop(source);
    for rocks in backends() {
        let directory = tempfile::tempdir().unwrap();
        let mut restored = processor(broker.clone(), rocks.then_some(directory.path()), 0, 127);
        for (group, bytes) in snapshots.iter().enumerate() {
            restored.restore(group as u32, bytes, 1999).unwrap();
        }
        restored.process(&batch(&[(7, 13, -2000)])).unwrap();
        assert_eq!(restored.kernel.late_records_dropped, 1);
        assert_eq!(restored.advance(1999).unwrap().num_rows(), 0);
        let mut actual = Vec::new();
        while restored.next_timer().is_some() {
            actual.extend(output(&restored.advance(i64::MAX).unwrap()));
        }
        actual.sort();
        assert_eq!(
            actual,
            [
                (1, 2, -2000, 4000),
                (1, 2, 0, 6000),
                (2, 3, -2000, 4000),
                (2, 3, 0, 6000)
            ]
        );
    }
    drop(snapshots);
    assert_eq!(broker.reserved(), 0);
}

#[test]
fn shared_slices_rescale_one_to_two_to_one_with_a_single_flink_operator_clock() {
    let broker = Arc::new(TestBroker::new(512 << 20));
    let mut source = processor(broker.clone(), None, 0, 127);
    source
        .process(&batch(
            &(0..24).map(|key| (key, key + 1, 2000)).collect::<Vec<_>>(),
        ))
        .unwrap();
    drop(source.advance(1999).unwrap());
    let snapshots = (0..128)
        .map(|group| source.snapshot(group).unwrap())
        .collect::<Vec<_>>();
    drop(source);
    let mut second_snapshots = Vec::new();
    for (first, last) in [(0, 63), (64, 127)] {
        let mut partition = processor(broker.clone(), None, first, last);
        for group in first..=last {
            partition
                .restore(group, &snapshots[group as usize], 1999)
                .unwrap();
        }
        let emitted = partition.advance(3999).unwrap();
        assert!(output(&emitted)
            .iter()
            .all(|row| row.2 == -2000 && row.3 == 4000));
        drop(emitted);
        second_snapshots
            .extend((first..=last).map(|group| (group, partition.snapshot(group).unwrap())));
    }
    let mut restored = processor(broker.clone(), None, 0, 127);
    for (group, bytes) in &second_snapshots {
        restored.restore(*group, bytes, 3999).unwrap();
    }
    let mut actual = Vec::new();
    while restored.next_timer().is_some() {
        actual.extend(output(&restored.advance(i64::MAX).unwrap()));
    }
    actual.sort();
    assert_eq!(
        actual,
        (0..24)
            .map(|key| (key, key + 1, 0, 6000))
            .collect::<Vec<_>>()
    );
    drop(restored);
    drop(snapshots);
    drop(second_snapshots);
    assert_eq!(broker.reserved(), 0);
}

#[test]
fn rocksdb_checkpoint_materializes_current_timers_without_per_input_index_writes() {
    if std::env::var("STREAMFUSION_TEST_ROCKSDB_PLUGIN").is_err() {
        return;
    }
    let directory = tempfile::tempdir().unwrap();
    let checkpoint = directory.path().join("checkpoint");
    let broker = Arc::new(TestBroker::new(512 << 20));
    let mut source = processor(broker.clone(), Some(&directory.path().join("live")), 0, 127);
    source.process(&batch(&[(1, 2, 2000)])).unwrap();
    drop(source.advance(1999).unwrap());
    source.checkpoint(&checkpoint).unwrap();
    source.process(&batch(&[(1, 7, 2000)])).unwrap();
    let memory = HostMemoryReservation::new(broker.clone(), "checkpoint reader");
    let reader = RocksPluginKeyedState::open_for_owner(
        std::path::Path::new(&std::env::var("STREAMFUSION_TEST_ROCKSDB_PLUGIN").unwrap()),
        &checkpoint,
        0,
        127,
        8 << 20,
        &memory,
    )
    .unwrap();
    let mut restored = processor(broker.clone(), None, 0, 127);
    for group in 0..128 {
        restored
            .restore(
                group,
                &reader.snapshot_key_group(group, &memory).unwrap(),
                1999,
            )
            .unwrap();
    }
    let mut actual = Vec::new();
    while restored.next_timer().is_some() {
        actual.extend(output(&restored.advance(i64::MAX).unwrap()));
    }
    assert_eq!(actual, [(1, 2, -2000, 4000), (1, 2, 0, 6000)]);
    drop(restored);
    drop(reader);
    drop(source);
    assert_eq!(broker.reserved(), 0);
}

#[test]
fn restore_rejects_legacy_state_and_different_window_contracts() {
    let broker = Arc::new(TestBroker::new(64 << 20));
    let legacy =
        crate::planner::operators::window_aggregate::tests::processor(&plan(), broker.clone());
    let bytes = legacy.snapshot_key_group(0).unwrap();
    let mut window = processor(broker.clone(), None, 0, 127);
    assert!(window
        .restore(0, &bytes, i64::MIN)
        .unwrap_err()
        .to_string()
        .contains("expanded-window state cannot be reinterpreted"));
    drop(bytes);
    drop(legacy);
    let mut source = processor(broker.clone(), None, 0, 127);
    let bytes = source.snapshot(0).unwrap();
    window.kernel.plan.offset_millis = 1;
    assert!(window.restore(0, &bytes, i64::MIN).is_err());
    drop(bytes);
    drop(source);
    drop(window);
    assert_eq!(broker.reserved(), 0);
}
