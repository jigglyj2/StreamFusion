// Copyright 2026 StreamFusion Authors
// Licensed under the Apache License, Version 2.0

use std::path::{Path, PathBuf};

/// Owns only automatically relocated logs of one successfully opened native database.
/// Configuration is resolved by Flink in the TaskManager; this is native resource cleanup.
/// Explicit Flink log directories never enter this cleanup owner.
pub(crate) struct LogDirectory(Option<(PathBuf, String)>);

impl LogDirectory {
    pub(crate) fn new(database: &Path, directory: Option<&Path>) -> Self {
        Self(directory.map(|directory| {
            // RocksDB file/filename.cc flattens UTF-8 bytes, not Unicode code points.
            // Production bindings supply an absolute, normalized task-local database path.
            let mut prefix = String::new();
            for (index, byte) in database.to_string_lossy().bytes().enumerate() {
                // RocksDB's InfoLogPrefix has 260 bytes including the suffix and NUL.
                if prefix.len() == 255 {
                    break;
                }
                if byte.is_ascii_alphanumeric() || matches!(byte, b'-' | b'.' | b'_') {
                    prefix.push(byte as char);
                } else if index != 0 {
                    prefix.push('_');
                }
            }
            prefix.push_str("_LOG");
            (directory.to_path_buf(), prefix)
        }))
    }
}

impl Drop for LogDirectory {
    fn drop(&mut self) {
        let Some((directory, prefix)) = &self.0 else {
            return;
        };
        let Ok(entries) = std::fs::read_dir(directory) else {
            return;
        };
        for entry in entries.flatten() {
            let name = entry.file_name();
            let name = name.to_string_lossy();
            let owned = name == *prefix
                || name
                    .strip_prefix(prefix)
                    .and_then(|suffix| suffix.strip_prefix(".old."))
                    .is_some_and(|suffix| {
                        !suffix.is_empty() && suffix.bytes().all(|byte| byte.is_ascii_digit())
                    });
            if owned && !entry.path().is_dir() {
                // Match Flink's best-effort log cleanup without deleting another DB's logs
                // whose flattened name happens to start with this database's prefix.
                let _ = std::fs::remove_file(entry.path());
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use crate::{RocksStateBackend, StateKey, StateMutation};

    #[test]
    fn explicit_directories_retain_logs_and_checkpoint_reads_leave_them_untouched() {
        for seed in 0..4 {
            let root = tempfile::tempdir().unwrap();
            let base = if seed < 2 {
                root.path().to_path_buf()
            } else {
                let base = root.path().join("x".repeat(220));
                std::fs::create_dir(&base).unwrap();
                base
            };
            let database = base.join("db");
            let logs = root.path().join("日志");
            if seed % 2 == 0 {
                std::fs::create_dir(&logs).unwrap();
            }
            let options = streamfusion_state_abi::RocksDbDatabaseOptions {
                retain_log_files: 1,
                ..Default::default()
            };
            let saved = root.path().join("saved");
            let key = StateKey {
                key_group: 0,
                key: b"key".to_vec(),
            };
            {
                let state = RocksStateBackend::open_database_configuration(
                    &database,
                    0,
                    0,
                    16 << 20,
                    [0, 0],
                    Some(&logs),
                    false,
                    0.5,
                    0.1,
                    &options,
                    &[1],
                )
                .unwrap();
                state
                    .write_batch(vec![StateMutation {
                        key: key.clone(),
                        value: Some(b"value".to_vec()),
                    }])
                    .unwrap();
                state.checkpoint(&saved).unwrap();
                assert!(!database.join("LOG").exists());
            }
            let files = std::fs::read_dir(&logs)
                .unwrap()
                .map(|e| e.unwrap().path())
                .collect::<Vec<_>>();
            assert_eq!(files.len(), 1);
            let log = &files[0];
            assert!(log.file_name().unwrap().to_str().unwrap().ends_with("_LOG"));
            let original = std::fs::read(log).unwrap();
            let rotated = logs.join(format!(
                "{}.old.123",
                log.file_name().unwrap().to_str().unwrap()
            ));
            std::fs::write(&rotated, b"retained rotation").unwrap();
            {
                let reader = RocksStateBackend::open_database_configuration(
                    &saved,
                    0,
                    0,
                    16 << 20,
                    [0, 0],
                    Some(&logs),
                    true,
                    0.2,
                    0.2,
                    &options,
                    &[1],
                )
                .unwrap();
                assert_eq!(
                    reader.get_batch(&[key]).unwrap(),
                    vec![Some(b"value".to_vec())]
                );
            }
            assert_eq!(std::fs::read(log).unwrap(), original);
            assert_eq!(std::fs::read(rotated).unwrap(), b"retained rotation");
        }
    }

    #[test]
    fn unusable_explicit_directory_fails_without_removing_user_files() {
        let root = tempfile::tempdir().unwrap();
        let file = root.path().join("file");
        std::fs::write(&file, b"owned by user").unwrap();
        let options = streamfusion_state_abi::RocksDbDatabaseOptions {
            retain_log_files: 1,
            ..Default::default()
        };
        for (index, log) in [file.clone(), root.path().join("absent-parent/logs")]
            .iter()
            .enumerate()
        {
            assert!(RocksStateBackend::open_database_configuration(
                &root.path().join(format!("db-{index}")),
                0,
                0,
                16 << 20,
                [0, 0],
                Some(log),
                false,
                0.5,
                0.1,
                &options,
                &[1],
            )
            .is_err());
        }
        assert_eq!(std::fs::read(file).unwrap(), b"owned by user");
    }

    #[test]
    fn explicit_directory_preserves_upstream_failure_for_overlong_database_prefixes() {
        let root = tempfile::tempdir().unwrap();
        let database = root
            .path()
            .join("a".repeat(140))
            .join("b".repeat(140))
            .join("db");
        std::fs::create_dir_all(database.parent().unwrap()).unwrap();
        let logs = root.path().join("logs");
        let options = streamfusion_state_abi::RocksDbDatabaseOptions {
            retain_log_files: 1,
            ..Default::default()
        };
        let error = RocksStateBackend::open_database_configuration(
            &database,
            0,
            0,
            16 << 20,
            [0, 0],
            Some(&logs),
            false,
            0.5,
            0.1,
            &options,
            &[1],
        )
        .err()
        .unwrap();
        assert!(error.to_string().contains("File name too long"), "{error}");
        assert!(!database.join("LOG").exists());
    }

    #[test]
    fn relocation_and_close_preserve_checkpoint_state_and_neighbor_logs() {
        let root = tempfile::tempdir().unwrap();
        let database = root.path().join("état");
        let logs = root.path().join("logs");
        std::fs::create_dir(&logs).unwrap();
        let key = StateKey {
            key_group: 0,
            key: b"key".to_vec(),
        };
        let saved = root.path().join("saved");
        let neighbor;
        {
            let state =
                RocksStateBackend::open_configured(&database, 0, 0, 64 << 20, [0, 0], Some(&logs))
                    .unwrap();
            state
                .write_batch(vec![StateMutation {
                    key: key.clone(),
                    value: Some(b"value".to_vec()),
                }])
                .unwrap();
            state.checkpoint(&saved).unwrap();
            assert!(!database.join("LOG").exists());
            let files = std::fs::read_dir(&logs)
                .unwrap()
                .map(|entry| entry.unwrap().path())
                .collect::<Vec<_>>();
            assert_eq!(files.len(), 1);
            let prefix = files[0].file_name().unwrap().to_str().unwrap();
            assert!(prefix.ends_with("_LOG"));
            neighbor = logs.join(format!("{prefix}_neighbor_LOG"));
            std::fs::write(&neighbor, b"neighbor").unwrap();
            std::fs::write(logs.join(format!("{prefix}.old.123")), b"old log").unwrap();
        }
        let files = std::fs::read_dir(&logs)
            .unwrap()
            .map(|entry| entry.unwrap().path())
            .collect::<Vec<_>>();
        assert_eq!(files, vec![neighbor.clone()]);
        assert_eq!(std::fs::read(neighbor).unwrap(), b"neighbor");
        let restored = RocksStateBackend::open(&saved, 0, 0).unwrap();
        assert_eq!(
            restored.get_batch(&[key]).unwrap(),
            vec![Some(b"value".to_vec())]
        );
    }
}
