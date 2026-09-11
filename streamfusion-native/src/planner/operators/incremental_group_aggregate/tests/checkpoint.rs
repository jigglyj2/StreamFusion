// Copyright 2026 StreamFusion Authors
// Licensed under the Apache License, Version 2.0.

use super::*;

#[test]
fn physical_restore_preserves_distinct_membership_and_rejects_pending_bundles() {
    let plugin = std::env::var("STREAMFUSION_TEST_ROCKSDB_PLUGIN").unwrap();
    let directory = tempfile::tempdir().unwrap();
    let mut source = IncrementalGroupAggregateProcessor::new_rocksdb(
        &plan(1),
        128,
        0,
        127,
        std::path::Path::new(&plugin),
        &directory.path().join("source"),
        1 << 20,
        HostMemoryReservation::new(
            Arc::new(TestBroker::new(64 << 20)),
            "incremental checkpoint source",
        ),
    )
    .unwrap();
    let input = batch(&[(5, 23, local_delta(99, true))]);
    source.process_arrow(input.clone()).unwrap();
    let path = directory.path().join("checkpoint");
    source.checkpoint(&path).unwrap();
    let checkpoint = crate::state::RocksPluginKeyedState::open(
        std::path::Path::new(&plugin),
        &path,
        0,
        127,
        1 << 20,
    )
    .unwrap();
    let broker = Arc::new(TestBroker::new(64 << 20));
    let mut restored = processor(1, broker.clone());
    let owner = restored.state_memory();
    for group in 0..128 {
        restored
            .restore_physical_key_group(group, &checkpoint, &owner)
            .unwrap();
        assert_eq!(
            restored.snapshot_key_group(group).unwrap(),
            source.snapshot_key_group(group).unwrap()
        );
    }
    assert_eq!(
        restored.process_arrow(input.clone()).unwrap(),
        source.process_arrow(input.clone()).unwrap()
    );
    drop(restored);
    drop(owner);
    assert_eq!(broker.reserved(), 0);

    let mut pending = processor(8, Arc::new(TestBroker::new(64 << 20)));
    pending.process_arrow(input).unwrap();
    let owner = pending.state_memory();
    assert!(pending
        .restore_physical_key_group(0, &checkpoint, &owner)
        .unwrap_err()
        .to_string()
        .contains("pending bundle"));
}
