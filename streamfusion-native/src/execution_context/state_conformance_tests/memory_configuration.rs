// Copyright 2026 StreamFusion Authors
// Licensed under the Apache License, Version 2.0

use super::*;

#[test]
fn invalid_memory_configuration_is_rejected_transactionally_before_plugin_loading() {
    let directory = tempfile::tempdir().unwrap();
    let broker = Arc::new(TestBroker::new(LIMIT));
    let mut context = context(&plans()[0], &broker);
    let baseline = broker.reserved();
    for (version, write, high, reason) in [
        (4, 0.7, 0.2, "require state-binding protocol 5"),
        (5, 0.8, 0.5, "invalid Flink RocksDB"),
        (5, f64::NAN, 0.1, "invalid Flink RocksDB"),
        (5, 0.5, 0.0, "invalid Flink RocksDB"),
    ] {
        let mut bindings = resources();
        bindings.protocol_version = version;
        bindings.spill_directories = vec![directory.path().to_str().unwrap().into()];
        bindings.bindings[0].backend = Some(proto::native_state_binding::Backend::Rocksdb(
            proto::NativeRocksDbState {
                statistics_tickers: Vec::new(),
                plugin_path: directory.path().join("missing.so").to_str().unwrap().into(),
                database_path: directory.path().join("db").to_str().unwrap().into(),
                memory_limit: 4 << 20,
                log_directory: None,
                write_buffer_ratio: Some(write),
                high_priority_pool_ratio: Some(high),
                database_options: None,
                partitioned_index_filters: None,
            },
        ));
        let error = install(&mut context, &broker, &bindings.encode_to_vec()).unwrap_err();
        assert!(error.to_string().contains(reason), "{error}");
        assert_eq!(broker.reserved(), baseline);
        assert!(context.state_resources.is_none());
        assert!(context.persistent.is_empty());
        assert!(!directory.path().join("db").exists());
    }
}

pub(super) fn database_options(seed: u64) -> proto::NativeRocksDbOptions {
    proto::NativeRocksDbOptions {
        max_background_jobs: 2 + seed as i32,
        max_open_files: 64 + seed as i32,
        max_log_file_size: 8 << 20,
        keep_log_file_num: 2 + seed,
        dynamic_level_bytes: seed % 2 == 0,
        target_file_size_base: (8 + seed) << 20,
        max_bytes_for_level_base: (32 + seed) << 20,
        write_buffer_size: (4 + seed) << 20,
        max_write_buffer_number: 3 + seed as i32,
        min_write_buffer_number_to_merge: 2,
        periodic_compaction_seconds: 7200,
        block_size: 4096 * (seed + 1),
        metadata_block_size: 1024 * (seed + 1),
        log_level: None,
        bloom_filter: None,
        compression: None,
        compaction_style: None,
        log_directory: None,
        write_batch_size: None,
    }
}

#[test]
fn invalid_database_configuration_is_rejected_before_plugin_loading_or_state_mutation() {
    let directory = tempfile::tempdir().unwrap();
    let broker = Arc::new(TestBroker::new(LIMIT));
    let mut context = context(&plans()[0], &broker);
    let baseline = broker.reserved();
    for (version, block_size, expected) in [
        (5, 4096, "require state-binding protocol 6"),
        (6, 0, "invalid Flink RocksDB"),
        (6, u64::MAX, "invalid Flink RocksDB"),
    ] {
        let mut bindings = resources();
        bindings.protocol_version = version;
        let mut config = database_options(1);
        config.block_size = block_size;
        bindings.bindings[0].backend = Some(proto::native_state_binding::Backend::Rocksdb(
            proto::NativeRocksDbState {
                statistics_tickers: Vec::new(),
                plugin_path: directory.path().join("missing.so").to_str().unwrap().into(),
                database_path: directory.path().join("db").to_str().unwrap().into(),
                memory_limit: 4 << 20,
                log_directory: None,
                write_buffer_ratio: None,
                high_priority_pool_ratio: None,
                database_options: Some(config),
                partitioned_index_filters: None,
            },
        ));
        let error = install(&mut context, &broker, &bindings.encode_to_vec()).unwrap_err();
        assert!(error.to_string().contains(expected), "{error}");
        assert_eq!(broker.reserved(), baseline);
        assert!(context.state_resources.is_none());
        assert!(context.persistent.is_empty());
        assert!(!directory.path().join("db").exists());
    }
}
