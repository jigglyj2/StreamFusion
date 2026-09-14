// Copyright 2026 StreamFusion Authors
// Licensed under the Apache License, Version 2.0

use rocksdb::{DBCompactionStyle, DBCompressionType, LogLevel, Options};

/// Applies supported Flink 2.3 RocksDBResourceContainer / RocksDBConfigurableOptions settings.
/// Cache and write-buffer-manager pools are attached by the shared managed-memory owner.
/// Unimplemented configuration remains a planner fallback.
pub(crate) fn configured_options_with_statistics(
    config: &streamfusion_state_abi::RocksDbDatabaseOptions,
    compression: &[i32],
    statistics_owner: Option<&Options>,
) -> Result<Options, rocksdb::Error> {
    // The safe upstream binding has no dedicated shutdown-flush setter. Parse this before
    // attaching resources with Rust-managed lifetimes (cache, WBM, callbacks, etc.).
    let mut options = statistics_owner
        .cloned()
        .unwrap_or_default()
        .get_options_from_string(if config.compaction_style == 3 {
            "avoid_flush_during_shutdown=true;compaction_style=kCompactionStyleNone;"
        } else {
            "avoid_flush_during_shutdown=true;"
        })?;
    if config.statistics_enabled != 0 && statistics_owner.is_none() {
        options.enable_statistics();
    }
    options.create_if_missing(true);
    options.set_use_fsync(false);
    options.set_stats_dump_period_sec(0);
    options.set_max_background_jobs(config.max_background_jobs);
    options.set_max_open_files(config.max_open_files);
    options.set_log_level(match config.log_level {
        0 => LogLevel::Debug,
        1 => LogLevel::Info,
        2 => LogLevel::Warn,
        3 => LogLevel::Error,
        4 => LogLevel::Fatal,
        5 => LogLevel::Header,
        _ => unreachable!("database options validated before construction"),
    });
    options.set_max_log_file_size(config.max_log_file_size as usize);
    options.set_keep_log_file_num(config.keep_log_file_num as usize);
    match config.compaction_style {
        0 => options.set_compaction_style(DBCompactionStyle::Level),
        1 => options.set_compaction_style(DBCompactionStyle::Universal),
        3 => {} // Set through the upstream options parser before attaching resources above.
        _ => unreachable!("compaction style validated before construction"),
    }
    options.set_level_compaction_dynamic_level_bytes(config.dynamic_level_bytes != 0);
    options.set_target_file_size_base(config.target_file_size_base);
    options.set_max_bytes_for_level_base(config.max_bytes_for_level_base);
    options.set_min_write_buffer_number_to_merge(config.min_write_buffer_number_to_merge);
    options.set_write_buffer_size(config.write_buffer_size as usize);
    options.set_max_write_buffer_number(config.max_write_buffer_number);
    options.set_periodic_compaction_seconds(config.periodic_compaction_seconds);
    // Flink retains base Snappy when the per-level list is empty. All admitted codecs
    // are compiled into this component; checkpoint tests verify actual SSTs.
    options.set_compression_type(DBCompressionType::Snappy);
    options.set_compression_per_level(
        &compression
            .iter()
            .map(|level| match level {
                0 => DBCompressionType::None,
                1 => DBCompressionType::Snappy,
                2 => DBCompressionType::Zlib,
                3 => DBCompressionType::Bz2,
                4 => DBCompressionType::Lz4,
                5 => DBCompressionType::Lz4hc,
                7 => DBCompressionType::Zstd,
                _ => unreachable!("compression validated before construction"),
            })
            .collect::<Vec<_>>(),
    );
    Ok(options)
}

#[cfg(test)]
mod tests {
    use crate::{RocksStateBackend, StateKey, StateMutation};
    use std::collections::HashMap;
    use std::path::Path;

    #[test]
    fn opened_database_persists_flink_database_and_compaction_defaults() {
        let directory = tempfile::tempdir().unwrap();
        let state = RocksStateBackend::open(directory.path(), 0, 0).unwrap();
        let path = std::fs::read_dir(directory.path())
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
        let text = std::fs::read_to_string(path).unwrap();
        let options: HashMap<_, _> = text
            .lines()
            .filter_map(|line| line.trim().split_once('='))
            .map(|(key, value)| (key.trim(), value.trim()))
            .collect();
        for (key, expected) in [
            ("avoid_flush_during_shutdown", "true"),
            ("use_fsync", "false"),
            ("stats_dump_period_sec", "0"),
            ("max_background_jobs", "2"),
            ("max_open_files", "-1"),
            ("info_log_level", "INFO_LEVEL"),
            ("max_log_file_size", "26214400"),
            ("keep_log_file_num", "4"),
            ("compaction_style", "kCompactionStyleLevel"),
            ("level_compaction_dynamic_level_bytes", "false"),
            ("target_file_size_base", "67108864"),
            ("max_bytes_for_level_base", "268435456"),
            ("min_write_buffer_number_to_merge", "1"),
            ("write_buffer_size", "67108864"),
            ("max_write_buffer_number", "2"),
            ("periodic_compaction_seconds", "2592000"),
        ] {
            assert_eq!(options.get(key).copied(), Some(expected), "{key}");
        }
        // The cache description in LOG comes from the opened RocksDB instance, not a
        // Rust-side option mirror. The C API extension must reach the real cache.
        drop(state);
        let log = std::fs::read_to_string(directory.path().join("LOG")).unwrap();
        assert!(log.contains("high_pri_pool_ratio: 0.100"), "{log}");
    }

    #[test]
    fn close_skips_memtable_flush_but_checkpoint_establishes_durability() {
        let directory = tempfile::tempdir().unwrap();
        let snapshots = tempfile::tempdir().unwrap();
        let key = StateKey {
            key_group: 0,
            key: b"key".to_vec(),
        };
        {
            let state = RocksStateBackend::open(directory.path(), 0, 0).unwrap();
            state
                .write_batch(vec![StateMutation {
                    key: key.clone(),
                    value: Some(b"uncheckpointed".to_vec()),
                }])
                .unwrap();
        }
        assert!(
            !has_sst(directory.path()),
            "close must not create an unneeded SST"
        );
        let checkpoint = snapshots.path().join("checkpoint");
        {
            let state = RocksStateBackend::open(directory.path(), 0, 0).unwrap();
            state
                .write_batch(vec![StateMutation {
                    key: key.clone(),
                    value: Some(b"checkpointed".to_vec()),
                }])
                .unwrap();
            state.checkpoint(&checkpoint).unwrap();
        }
        assert!(has_sst(&checkpoint));
        let restored = RocksStateBackend::open(&checkpoint, 0, 0).unwrap();
        assert_eq!(
            restored.get_batch(&[key]).unwrap(),
            vec![Some(b"checkpointed".to_vec())]
        );
    }

    fn has_sst(path: &Path) -> bool {
        std::fs::read_dir(path).unwrap().any(|entry| {
            entry
                .unwrap()
                .path()
                .extension()
                .is_some_and(|extension| extension == "sst")
        })
    }
}
