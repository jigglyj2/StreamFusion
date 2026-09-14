// Copyright 2026 StreamFusion Authors
// Licensed under the Apache License, Version 2.0
use super::*;

pub(super) fn database_options(seed: u64) -> proto::NativeRocksDbOptions {
    let mut options = super::memory_configuration::database_options(seed);
    options.log_level = Some(if seed == 1 { 0 } else { 2 });
    options.bloom_filter = Some(proto::NativeRocksDbBloomFilter {
        enabled: seed == 1,
        bits_per_key: 9.9,
        block_based_mode: true,
    });
    options.compression = Some(proto::NativeRocksDbCompression {
        per_level: if seed == 1 { vec![0, 1] } else { vec![1] },
    });
    options
}

#[test]
fn filter_configuration_rejects_older_protocols_unknown_codecs_and_log_levels_transactionally() {
    let directory = tempfile::tempdir().unwrap();
    let broker = Arc::new(TestBroker::new(LIMIT));
    let mut context = context(&plans()[0], &broker);
    let baseline = broker.reserved();
    for (version, codec, log, expected) in [
        (6, 0, 1, "require state-binding protocol 7"),
        (7, 6, 1, "unsupported Flink RocksDB per-level compression"),
        (7, -1, 1, "unsupported Flink RocksDB per-level compression"),
        (7, 1, 6, "invalid Flink RocksDB database option"),
    ] {
        let mut bindings = resources();
        bindings.protocol_version = version;
        let mut options = database_options(1);
        options.log_level = Some(log);
        options.compression.as_mut().unwrap().per_level = vec![codec];
        bindings.bindings[0].backend = Some(proto::native_state_binding::Backend::Rocksdb(
            proto::NativeRocksDbState {
                statistics_tickers: Vec::new(),
                plugin_path: directory.path().join("missing.so").to_str().unwrap().into(),
                database_path: directory.path().join("db").to_str().unwrap().into(),
                memory_limit: 4 << 20,
                log_directory: None,
                write_buffer_ratio: None,
                high_priority_pool_ratio: None,
                database_options: Some(options),
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

#[test]
fn previous_database_option_protocol_retains_filter_compression_and_log_defaults() {
    use crate::state::rocks_plugin::database_options::{compression, requires_protocol_7, resolve};
    let previous = super::memory_configuration::database_options(1);
    assert!(!requires_protocol_7(&previous));
    let options = resolve(Some(&previous)).unwrap();
    assert_eq!(options.bloom_enabled, 0);
    assert_eq!(options.bloom_bits_per_key, 10.0);
    assert_eq!(options.log_level, 1);
    assert_eq!(compression(Some(&previous)).unwrap(), &[1]);
    let mut empty = previous;
    empty.compression = Some(proto::NativeRocksDbCompression { per_level: vec![] });
    assert!(requires_protocol_7(&empty));
    assert!(compression(Some(&empty)).unwrap().is_empty());
}

#[test]
fn large_compression_lists_reserve_native_option_copies_before_open_and_release_on_denial() {
    let directory = tempfile::tempdir().unwrap();
    let broker = Arc::new(TestBroker::new(LIMIT));
    let mut context = context(&plans()[0], &broker);
    let baseline = broker.reserved();
    let mut bindings = resources();
    bindings.protocol_version = 7;
    let mut options = database_options(1);
    options.compression.as_mut().unwrap().per_level = vec![1; 2 << 20];
    bindings.bindings[0].backend = Some(proto::native_state_binding::Backend::Rocksdb(
        proto::NativeRocksDbState {
            statistics_tickers: Vec::new(),
            plugin_path: directory.path().join("missing.so").to_str().unwrap().into(),
            database_path: directory.path().join("db").to_str().unwrap().into(),
            memory_limit: 4 << 20,
            log_directory: None,
            write_buffer_ratio: None,
            high_priority_pool_ratio: None,
            database_options: Some(options),
            partitioned_index_filters: None,
        },
    ));
    let bytes = bindings.encode_to_vec();
    assert!(
        bytes.len() * 4 + 4096 < LIMIT - baseline,
        "decode reservation alone must fit"
    );
    assert!(matches!(
        install(&mut context, &broker, &bytes),
        Err(DataFusionError::ResourcesExhausted(_))
    ));
    assert_eq!(broker.reserved(), baseline);
    assert!(context.state_resources.is_none());
    assert!(context.persistent.is_empty());
    assert!(!directory.path().join("db").exists());
}
