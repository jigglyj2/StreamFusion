// Copyright 2026 StreamFusion Authors
// Licensed under the Apache License, Version 2.0

use super::*;
use crate::memory_pool::tests_support::TestBroker;
use crate::state::KeyedState;

fn resources(plugin: &str, path: &Path) -> crate::proto::NativeRocksDbState {
    crate::proto::NativeRocksDbState {
        plugin_path: plugin.into(),
        database_path: path.to_str().unwrap().into(),
        memory_limit: 4 << 20,
        statistics_tickers: vec![7],
        ..Default::default()
    }
}

#[test]
fn statistics_admission_precedes_creation_and_failed_open_releases_credit() {
    let Ok(plugin) = std::env::var("STREAMFUSION_TEST_ROCKSDB_PLUGIN") else {
        return;
    };
    let root = tempfile::tempdir().unwrap();
    let probe = RocksPluginKeyedState::open(
        Path::new(&plugin),
        &root.path().join("probe"),
        0,
        0,
        4 << 20,
    )
    .unwrap();
    let required = unsafe { (probe.api.statistics_memory_required)() };
    assert!(required > 64 << 10);
    let broker = Arc::new(TestBroker::new(required - 1));
    let memory = HostMemoryReservation::new(broker.clone(), "denied statistics");
    let config = resources(&plugin, &root.path().join("denied"));
    let result = RocksPluginKeyedState::open_configured(&config, 0, 0, Some(&memory));
    assert!(result.err().unwrap().to_string().contains("Flink denied"));
    assert!(!root.path().join("denied").exists());
    assert_eq!(broker.reserved(), 0);
    let result = RocksPluginKeyedState::open_configured(&config, 0, 0, None);
    assert!(result
        .err()
        .unwrap()
        .to_string()
        .contains("require Flink memory admission"));
    assert!(!root.path().join("denied").exists());
    let broker = Arc::new(TestBroker::new(required));
    let memory = HostMemoryReservation::new(broker.clone(), "failed statistics DB");
    let file = root.path().join("file");
    std::fs::write(&file, b"preserve").unwrap();
    let result = RocksPluginKeyedState::open_configured(
        &resources(&plugin, &file.join("db")),
        0,
        0,
        Some(&memory),
    );
    assert!(result.is_err());
    assert_eq!(broker.reserved(), 0);
    assert_eq!(std::fs::read(file).unwrap(), b"preserve");
}

#[test]
fn statistics_share_one_reservation_until_the_final_database_or_reader_closes() {
    let Ok(plugin) = std::env::var("STREAMFUSION_TEST_ROCKSDB_PLUGIN") else {
        return;
    };
    let root = tempfile::tempdir().unwrap();
    let broker = Arc::new(TestBroker::new(64 << 20));
    let memory = HostMemoryReservation::new(broker.clone(), "shared statistics");
    let state = RocksPluginKeyedState::open_configured(
        &resources(&plugin, &root.path().join("source")),
        0,
        0,
        Some(&memory),
    )
    .unwrap();
    let required = unsafe { (state.api.statistics_memory_required)() };
    assert_eq!(broker.reserved(), required);
    let first = state.statistics_reader().unwrap();
    let second = state.statistics_reader().unwrap();
    assert!(Arc::ptr_eq(&first.memory, &second.memory));
    let checkpoint = root.path().join("checkpoint");
    state.checkpoint(&checkpoint).unwrap();
    let reader = RocksPluginKeyedState::open_checkpoint_configured(
        &resources(&plugin, &checkpoint),
        0,
        0,
        None,
        Some(&first),
    )
    .unwrap();
    assert_eq!(
        broker.reserved(),
        required,
        "checkpoint readers must not reserve statistics twice"
    );
    drop(state);
    drop(first);
    drop(second);
    assert_eq!(
        broker.reserved(),
        required,
        "the checkpoint DB still owns statistics"
    );
    let final_reader = reader.statistics_reader().unwrap();
    drop(reader);
    assert_eq!(
        broker.reserved(),
        required,
        "the detached reader still owns statistics"
    );
    let mut values = [i64::MAX];
    final_reader.sample(&[7], &mut values).unwrap();
    assert_eq!(values, [0]);
    drop(final_reader);
    assert_eq!(broker.reserved(), 0);
}

#[test]
fn disabled_statistics_do_not_reserve_or_create_readers() {
    let Ok(plugin) = std::env::var("STREAMFUSION_TEST_ROCKSDB_PLUGIN") else {
        return;
    };
    let root = tempfile::tempdir().unwrap();
    let broker = Arc::new(TestBroker::new(0));
    let memory = HostMemoryReservation::new(broker.clone(), "disabled statistics");
    let mut config = resources(&plugin, root.path());
    config.statistics_tickers.clear();
    let state = RocksPluginKeyedState::open_configured(&config, 0, 0, Some(&memory)).unwrap();
    assert!(state.statistics_reader().is_err());
    assert_eq!(broker.reserved(), 0);
}
