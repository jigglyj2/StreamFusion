// Copyright 2026 StreamFusion Authors
// Licensed under the Apache License, Version 2.0
use super::*;
use streamfusion_state_abi::RocksDbDatabaseOptions;

fn configured(
    path: &Path,
    scope: [u64; 2],
    checkpoint: bool,
    options: &RocksDbDatabaseOptions,
) -> Result<RocksStateBackend> {
    RocksStateBackend::open_database_options(
        path,
        0,
        0,
        16 << 20,
        scope,
        None,
        checkpoint,
        0.5,
        0.1,
        options,
    )
}

pub(super) fn persisted(path: &Path) -> HashMap<String, String> {
    let file = std::fs::read_dir(path)
        .unwrap()
        .map(|entry| entry.unwrap().path())
        .filter(|path| {
            path.file_name()
                .unwrap()
                .to_str()
                .unwrap()
                .starts_with("OPTIONS-")
        })
        .max()
        .unwrap();
    std::fs::read_to_string(file)
        .unwrap()
        .lines()
        .filter_map(|line| {
            line.trim()
                .split_once('=')
                .map(|(key, value)| (key.trim().into(), value.trim().into()))
        })
        .collect()
}

#[test]
fn generated_database_options_reach_actual_rocksdb_and_preserve_checkpoint_state() {
    for seed in 1..=4_u64 {
        let directory = tempfile::tempdir().unwrap();
        let source = directory.path().join("source");
        let checkpoint = directory.path().join("checkpoint");
        let other = directory.path().join("other");
        let options = RocksDbDatabaseOptions {
            max_background_jobs: 2 + seed as i32,
            max_open_files: 64 + seed as i32,
            max_log_file_size: if seed % 2 == 0 { 0 } else { 8 << 20 },
            keep_log_file_num: 2 + seed,
            dynamic_level_bytes: (seed % 2 == 0) as u8,
            target_file_size_base: (8 + seed) << 20,
            max_bytes_for_level_base: (32 + seed) << 20,
            write_buffer_size: (4 + seed) << 20,
            max_write_buffer_number: 3 + seed as i32,
            min_write_buffer_number_to_merge: 2,
            periodic_compaction_seconds: if seed % 2 == 0 { 0 } else { 7200 },
            block_size: 4096 * (seed + 1),
            metadata_block_size: 1024 * (seed + 1),
            ..Default::default()
        };
        let state = configured(&source, [8721, seed], false, &options).unwrap();
        let values = persisted(&source);
        for (key, value) in [
            (
                "max_background_jobs",
                options.max_background_jobs.to_string(),
            ),
            ("max_open_files", options.max_open_files.to_string()),
            ("max_log_file_size", options.max_log_file_size.to_string()),
            ("keep_log_file_num", options.keep_log_file_num.to_string()),
            (
                "level_compaction_dynamic_level_bytes",
                (options.dynamic_level_bytes != 0).to_string(),
            ),
            (
                "target_file_size_base",
                options.target_file_size_base.to_string(),
            ),
            (
                "max_bytes_for_level_base",
                options.max_bytes_for_level_base.to_string(),
            ),
            ("write_buffer_size", options.write_buffer_size.to_string()),
            (
                "max_write_buffer_number",
                options.max_write_buffer_number.to_string(),
            ),
            (
                "min_write_buffer_number_to_merge",
                options.min_write_buffer_number_to_merge.to_string(),
            ),
            (
                "periodic_compaction_seconds",
                options.periodic_compaction_seconds.to_string(),
            ),
            ("block_size", options.block_size.to_string()),
            (
                "metadata_block_size",
                options.metadata_block_size.to_string(),
            ),
        ] {
            assert_eq!(values.get(key), Some(&value), "seed={seed}, {key}");
        }
        let key = StateKey {
            key_group: 0,
            key: b"key".to_vec(),
        };
        state
            .write_batch(vec![StateMutation {
                key: key.clone(),
                value: Some(vec![42; 8192]),
            }])
            .unwrap();
        state.checkpoint(&checkpoint).unwrap();
        let mut destination_options = options;
        destination_options.block_size *= 2;
        destination_options.write_buffer_size *= 2;
        let other_state = configured(&other, [8721, seed], false, &destination_options).unwrap();
        let reader = configured(&checkpoint, [8721, seed], true, &destination_options).unwrap();
        assert!(Arc::ptr_eq(&state._shared_memory, &reader._shared_memory));
        assert!(Arc::ptr_eq(
            &other_state._shared_memory,
            &reader._shared_memory
        ));
        assert_eq!(
            reader.get_batch(&[key.clone()]).unwrap(),
            state.get_batch(&[key]).unwrap()
        );
        assert!(reader
            .write_batch(vec![StateMutation {
                key: StateKey {
                    key_group: 0,
                    key: b"readonly".to_vec()
                },
                value: Some(vec![1])
            }])
            .is_err());
    }
}

#[test]
fn invalid_database_options_fail_before_creating_a_database_or_reserving_shared_memory() {
    let directory = tempfile::tempdir().unwrap();
    for options in [
        RocksDbDatabaseOptions {
            block_size: 0,
            ..Default::default()
        },
        RocksDbDatabaseOptions {
            max_background_jobs: 0,
            ..Default::default()
        },
        RocksDbDatabaseOptions {
            dynamic_level_bytes: 2,
            ..Default::default()
        },
        RocksDbDatabaseOptions {
            write_buffer_size: u64::MAX,
            ..Default::default()
        },
    ] {
        let path = directory.path().join("invalid");
        assert!(configured(&path, [7236, 1], false, &options).is_err());
        assert!(!path.exists());
        assert!(SHARED_ROCKS_MEMORY
            .lock()
            .unwrap()
            .get(&[7236, 1])
            .and_then(Weak::upgrade)
            .is_none());
    }
}
