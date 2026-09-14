// Copyright 2026 StreamFusion Authors
// Licensed under the Apache License, Version 2.0

use super::*;
use streamfusion_state_abi::RocksDbDatabaseOptions;

#[test]
fn shared_statistics_do_not_inherit_log_paths_or_keep_the_flink_memory_lease() {
    let root = tempfile::tempdir().unwrap();
    let logs = root.path().join("logs");
    std::fs::create_dir(&logs).unwrap();
    let config = RocksDbDatabaseOptions {
        statistics_enabled: 1,
        retain_log_files: 1,
        ..Default::default()
    };
    let source = RocksStateBackend::open_with_statistics(
        &root.path().join("source"),
        0,
        0,
        4 << 20,
        [829174, 1],
        Some(&logs),
        false,
        0.5,
        0.1,
        &config,
        &[1],
        None,
    )
    .unwrap();
    let stats = source.statistics_reader().unwrap();
    let source_memory = Arc::downgrade(&source._shared_memory);
    source
        .write_batch(vec![StateMutation {
            key: StateKey {
                key_group: 0,
                key: b"key".to_vec(),
            },
            value: Some(b"value".to_vec()),
        }])
        .unwrap();
    let checkpoint = root.path().join("checkpoint");
    source.checkpoint(&checkpoint).unwrap();
    let before = stats.get_ticker_count(rocksdb::statistics::Ticker::BytesRead);
    drop(source);
    assert!(source_memory.upgrade().is_none());
    let other_path = root.path().join("other");
    let other = RocksStateBackend::open_with_statistics(
        &other_path,
        0,
        0,
        2 << 20,
        [829174, 3],
        None,
        false,
        0.7,
        0.2,
        &config,
        &[4],
        Some(&stats),
    )
    .unwrap();
    assert!(
        other_path.join("LOG").exists(),
        "shared statistics inherited the old log directory"
    );
    drop(other);
    let restored = RocksStateBackend::open_with_statistics(
        &checkpoint,
        0,
        0,
        2 << 20,
        [829174, 2],
        None,
        true,
        0.7,
        0.2,
        &config,
        &[4],
        Some(&stats),
    )
    .unwrap();
    // Upstream read-only DB opening does not create a logger.
    assert!(!checkpoint.join("LOG").exists());
    assert_eq!(restored._shared_memory.limit, 2 << 20);
    assert_eq!(restored._shared_memory.write_buffer_ratio, 0.7);
    let restored_memory = Arc::downgrade(&restored._shared_memory);
    // An upstream single Get exercises BYTES_READ (MultiGet intentionally uses its own ticker).
    let key = [0, 0, 0, 0, b'k', b'e', b'y'];
    assert_eq!(restored.db.get(key).unwrap(), Some(b"value".to_vec()));
    assert_eq!(
        stats.get_ticker_count(rocksdb::statistics::Ticker::BytesRead),
        before + 5
    );
    let other_stats = restored.statistics_reader().unwrap();
    drop(restored);
    assert!(restored_memory.upgrade().is_none());
    assert_eq!(
        other_stats.get_ticker_count(rocksdb::statistics::Ticker::BytesRead),
        before + 5
    );
}
