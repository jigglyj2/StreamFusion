// Copyright 2026 StreamFusion Authors
// Licensed under the Apache License, Version 2.0
use super::*;

#[test]
fn write_threshold_requires_its_protocol_and_rejects_invalid_sizes_transactionally() {
    let directory = tempfile::tempdir().unwrap();
    let broker = Arc::new(TestBroker::new(LIMIT));
    let mut context = context(&plans()[0], &broker);
    let baseline = broker.reserved();
    for (version, size, reason) in [
        (9, 0, "requires state-binding protocol 10"),
        (10, u64::MAX, "invalid Flink RocksDB write-batch-size"),
    ] {
        let mut bindings = resources();
        bindings.protocol_version = version;
        let mut options = super::memory_configuration::database_options(1);
        options.write_batch_size = Some(size);
        bindings.bindings[0].backend = Some(proto::native_state_binding::Backend::Rocksdb(
            proto::NativeRocksDbState {
                plugin_path: directory.path().join("missing.so").to_str().unwrap().into(),
                database_path: directory.path().join("db").to_str().unwrap().into(),
                memory_limit: 4 << 20,
                database_options: Some(options),
                ..Default::default()
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
fn absent_threshold_retains_flink_default_and_zero_remains_explicit() {
    let mut options = super::memory_configuration::database_options(1);
    let resolve = crate::state::rocks_plugin::database_options::resolve;
    assert_eq!(resolve(Some(&options)).unwrap().write_batch_size, 2 << 20);
    options.write_batch_size = Some(0);
    assert_eq!(resolve(Some(&options)).unwrap().write_batch_size, 0);
}
