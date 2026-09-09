// Copyright 2026 StreamFusion Authors
// Licensed under the Apache License, Version 2.0.

use super::*;
use crate::state::{KeyedState, RocksPluginKeyedState, StateKey, StateMutation};

#[test]
fn failed_paged_checkpoint_import_poisoning_discards_earlier_pages_and_all_reservations() {
    let plugin: std::path::PathBuf = std::env::var("STREAMFUSION_TEST_ROCKSDB_PLUGIN")
        .unwrap()
        .into();
    let directory = tempfile::tempdir().unwrap();
    let mut source =
        RocksPluginKeyedState::open(&plugin, &directory.path().join("source"), 0, 15, 1 << 20)
            .unwrap();
    source
        .write_batch(vec![
            StateMutation {
                key: StateKey {
                    key_group: 0,
                    key: vec![0],
                },
                value: Some(vec![1]),
            },
            StateMutation {
                key: StateKey {
                    key_group: 1,
                    key: vec![0],
                },
                value: Some(vec![2; 4 << 20]),
            },
        ])
        .unwrap();
    let checkpoint = directory.path().join("checkpoint");
    source.checkpoint(&checkpoint).unwrap();
    drop(source);
    let broker = Arc::new(TestBroker::new(2 << 20));
    let plan = plans().remove(1); // shared aggregate factory
    let mut context = context(&plan, &broker);
    install(&mut context, &broker, &resources().encode_to_vec()).unwrap();
    let error = context
        .import_state_checkpoint(2, &plugin, &checkpoint, 0, 15, 256 << 10)
        .unwrap_err();
    assert!(error.to_string().contains("Flink denied"), "{error}");
    assert!(context
        .snapshot_state(2, 0)
        .unwrap_err()
        .to_string()
        .contains("failed"));
    drop(context);
    assert_eq!(broker.reserved(), 0);
}
