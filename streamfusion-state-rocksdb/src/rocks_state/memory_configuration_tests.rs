// Copyright 2026 StreamFusion Authors
// Licensed under the Apache License, Version 2.0

use super::*;

#[test]
fn generated_geometry_reaches_live_databases_shared_pools_and_checkpoint_readers() {
    for write in [0.2_f64, 0.5, 0.7] {
        for high in [0.1_f64, 0.2] {
            let directory = tempfile::tempdir().unwrap();
            let budget = 3 << 20;
            let scope = [write.to_bits(), high.to_bits()];
            let first_path = directory.path().join("first");
            let first = RocksStateBackend::open_configured_mode(
                &first_path,
                0,
                0,
                budget,
                scope,
                None,
                false,
                write,
                high,
            )
            .unwrap();
            let second = RocksStateBackend::open_configured_mode(
                &directory.path().join("second"),
                0,
                0,
                budget,
                scope,
                None,
                false,
                write,
                high,
            )
            .unwrap();
            assert!(Arc::ptr_eq(&first._shared_memory, &second._shared_memory));
            // Same formulas and operation order as Flink RocksDBMemoryControllerUtils.
            let cache = ((3.0 - write) * budget as f64 / 3.0) as u64;
            let buffers = (2.0 * budget as f64 * write / 3.0) as usize;
            for state in [&first, &second] {
                assert_eq!(
                    state
                        .db
                        .property_int_value("rocksdb.block-cache-capacity")
                        .unwrap(),
                    Some(cache)
                );
                assert_eq!(
                    state._shared_memory.write_buffers.get_buffer_size(),
                    buffers
                );
            }
            let key = StateKey {
                key_group: 0,
                key: vec![1],
            };
            first
                .write_batch(vec![StateMutation {
                    key: key.clone(),
                    value: Some(vec![2, 3]),
                }])
                .unwrap();
            let checkpoint = directory.path().join("checkpoint");
            first.checkpoint(&checkpoint).unwrap();
            let reader = RocksStateBackend::open_configured_mode(
                &checkpoint,
                0,
                0,
                budget,
                [0, 0],
                None,
                true,
                write,
                high,
            )
            .unwrap();
            assert_eq!(reader.get_batch(&[key]).unwrap(), vec![Some(vec![2, 3])]);
            assert_eq!(
                reader
                    .db
                    .property_int_value("rocksdb.block-cache-capacity")
                    .unwrap(),
                Some(cache)
            );
            assert_eq!(
                reader._shared_memory.write_buffers.get_buffer_size(),
                buffers
            );
            drop(reader);
            drop(first);
            drop(second);
            // LOG is written by the real upstream cache, including our narrowly scoped setter.
            let log = std::fs::read_to_string(first_path.join("LOG")).unwrap();
            assert!(
                log.contains(&format!("high_pri_pool_ratio: {high:.3}")),
                "{log}"
            );
        }
    }
}

#[test]
fn invalid_or_conflicting_geometry_fails_before_database_creation() {
    let directory = tempfile::tempdir().unwrap();
    for (write, high) in [(0.8, 0.5), (0.0, 0.1), (0.5, 1.0), (f64::NAN, 0.1)] {
        let path = directory.path().join("invalid");
        assert!(RocksStateBackend::open_configured_mode(
            &path,
            0,
            0,
            1 << 20,
            [0, 0],
            None,
            false,
            write,
            high,
        )
        .is_err());
        assert!(!path.exists());
    }
    let first = shared_rocks_memory_configured(1 << 20, [441, 1], 0.7, 0.2).unwrap();
    assert!(shared_rocks_memory_configured(1 << 20, [441, 1], 0.5, 0.1).is_err());
    drop(first);
    assert!(shared_rocks_memory_configured(1 << 20, [441, 1], 0.5, 0.1).is_ok());
}
