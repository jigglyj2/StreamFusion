// Copyright 2026 StreamFusion Authors
// Licensed under the Apache License, Version 2.0
use super::*;

pub(super) fn database_options(seed: u64) -> proto::NativeRocksDbOptions {
    let mut options = super::filter_configuration::database_options(seed);
    options.compaction_style = Some(if seed == 1 { 1 } else { 3 });
    options
}

#[test]
fn partitioning_and_compaction_require_their_protocol_and_unknown_styles_fail_before_open() {
    let directory = tempfile::tempdir().unwrap();
    let broker = Arc::new(TestBroker::new(LIMIT));
    let mut context = context(&plans()[0], &broker);
    let baseline = broker.reserved();
    for (version, flag, style, reason) in [
        (7, Some(true), None, "require state-binding protocol 8"),
        (7, None, Some(0), "require state-binding protocol 8"),
        (
            8,
            Some(true),
            Some(2),
            "unsupported Flink RocksDB compaction style",
        ),
        (
            8,
            None,
            Some(u32::MAX),
            "unsupported Flink RocksDB compaction style",
        ),
    ] {
        let mut bindings = resources();
        bindings.protocol_version = version;
        let mut options = super::memory_configuration::database_options(1);
        options.compaction_style = style;
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
                partitioned_index_filters: flag,
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
