// Copyright 2026 StreamFusion Authors
// Licensed under the Apache License, Version 2.0

use rocksdb::{DBCompactionStyle, DBCompressionType, LogLevel, Options};

/// Flink 2.3 RocksDBResourceContainer / RocksDBConfigurableOptions defaults. Cache and
/// memtable sizing are attached separately by the shared managed-memory owner; these defaults
/// alone do not establish full configuration parity or permit production admission.
pub(crate) fn base_options() -> Result<Options, rocksdb::Error> {
    // The safe upstream binding has no dedicated shutdown-flush setter. Parse this before
    // attaching resources with Rust-managed lifetimes (cache, WBM, callbacks, etc.).
    let mut options =
        Options::default().get_options_from_string("avoid_flush_during_shutdown=true;")?;
    options.create_if_missing(true);
    options.set_use_fsync(false);
    options.set_stats_dump_period_sec(0);
    options.set_max_background_jobs(2);
    options.set_max_open_files(-1);
    options.set_log_level(LogLevel::Info);
    options.set_max_log_file_size(25 << 20);
    options.set_keep_log_file_num(4);
    options.set_compaction_style(DBCompactionStyle::Level);
    options.set_level_compaction_dynamic_level_bytes(false);
    options.set_target_file_size_base(64 << 20);
    options.set_max_bytes_for_level_base(256 << 20);
    options.set_min_write_buffer_number_to_merge(1);
    options.set_periodic_compaction_seconds(30 * 24 * 60 * 60);
    // Snappy must also be compiled into RocksDB; the checkpoint test verifies actual SSTs.
    options.set_compression_type(DBCompressionType::Snappy);
    options.set_compression_per_level(&[DBCompressionType::Snappy]);
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
        let _state = RocksStateBackend::open(directory.path(), 0, 0).unwrap();
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
            ("periodic_compaction_seconds", "2592000"),
        ] {
            assert_eq!(options.get(key).copied(), Some(expected), "{key}");
        }
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
