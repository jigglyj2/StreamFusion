// Copyright 2026 StreamFusion Authors
// Licensed under the Apache License, Version 2.0

use super::*;

#[test]
fn explicit_log_configuration_requires_its_protocol_and_rejects_invalid_paths_before_open() {
    let directory = tempfile::tempdir().unwrap();
    let broker = Arc::new(TestBroker::new(LIMIT));
    let mut context = context(&plans()[0], &broker);
    let baseline = broker.reserved();
    for (version, log, reason) in [
        (8, "/tmp/logs", "requires state-binding protocol 9"),
        (9, "relative", "must be an absolute path"),
        (9, "", "must be an absolute path"),
        (9, "/tmp/log\0s", "without NUL bytes"),
    ] {
        let mut bindings = resources();
        bindings.protocol_version = version;
        let mut options = super::memory_configuration::database_options(1);
        options.log_directory = Some(log.into());
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
        assert!(error.to_string().contains(reason), "{error}");
        assert_eq!(broker.reserved(), baseline);
        assert!(context.state_resources.is_none());
        assert!(context.persistent.is_empty());
        assert!(!directory.path().join("db").exists());
    }
}

#[test]
fn invalid_log_configuration_is_rejected_before_backend_construction() {
    let directory = tempfile::tempdir().unwrap();
    let plan = &plans()[0];
    let broker = Arc::new(TestBroker::new(LIMIT));
    let mut context = context(plan, &broker);
    let baseline = broker.reserved();
    for (version, log, reason) in [
        (1, "/tmp/logs", "requires state-binding protocol 2"),
        (2, "relative", "must be an absolute path"),
        (2, "/tmp/log\0s", "without NUL bytes"),
    ] {
        let mut bindings = resources();
        bindings.protocol_version = version;
        bindings.bindings[0].backend = Some(proto::native_state_binding::Backend::Rocksdb(
            proto::NativeRocksDbState {
                statistics_tickers: Vec::new(),
                plugin_path: directory.path().join("missing.so").to_str().unwrap().into(),
                database_path: directory.path().join("db").to_str().unwrap().into(),
                memory_limit: 4 << 20,
                log_directory: Some(log.into()),
                write_buffer_ratio: None,
                high_priority_pool_ratio: None,
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
