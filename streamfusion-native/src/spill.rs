// Copyright 2026 StreamFusion Authors
// Licensed under the Apache License, Version 2.0.

use std::path::PathBuf;

use datafusion::error::{DataFusionError, Result};
use datafusion::execution::disk_manager::{DiskManagerBuilder, DiskManagerMode};

/// Use Flink's assigned local directories and DataFusion's spill-file ownership/accounting.
/// Flink's IOManager imposes no separate byte quota: inheriting DataFusion's default 100 GiB
/// ceiling would reject an otherwise valid Flink job. Filesystem capacity and I/O errors still
/// apply. This is an internal resource translation, not a new deployment setting.
pub(crate) fn disk_manager(directories: Vec<PathBuf>) -> Result<DiskManagerBuilder> {
    if directories.is_empty()
        || directories
            .iter()
            .any(|directory| !directory.is_absolute() || !directory.is_dir())
    {
        return Err(DataFusionError::Execution(
            "native spilling requires existing absolute Flink IOManager directories".into(),
        ));
    }
    Ok(DiskManagerBuilder::default()
        .with_mode(DiskManagerMode::Directories(directories))
        .with_max_temp_directory_size(u64::MAX))
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::io::Write;
    use std::sync::Arc;

    #[test]
    fn tracks_spill_bytes_and_releases_files_with_the_last_owner() {
        let first = tempfile::tempdir().unwrap();
        let second = tempfile::tempdir().unwrap();
        let roots = vec![first.path().to_path_buf(), second.path().to_path_buf()];
        let manager = Arc::new(disk_manager(roots.clone()).unwrap().build().unwrap());
        assert_eq!(manager.max_temp_directory_size(), u64::MAX);
        let file = manager.create_tmp_file("Flink assigned spill").unwrap();
        let path = file.path().unwrap().to_path_buf();
        assert!(roots.iter().any(|root| path.starts_with(root)));
        let mut writer = file.open_writer().unwrap();
        writer.write_all(b"native state page").unwrap();
        writer.finish().unwrap();
        assert_eq!(manager.used_disk_space(), 17);
        assert_eq!(file.size(), Some(17));
        drop(writer);
        let retained = Arc::clone(&file);
        drop(file);
        assert!(path.exists());
        assert_eq!(manager.used_disk_space(), 17);
        drop(retained);
        assert!(!path.exists());
        assert_eq!(manager.used_disk_space(), 0);
        drop(manager);
        for root in roots {
            assert_eq!(std::fs::read_dir(root).unwrap().count(), 0);
        }
    }

    #[test]
    fn rejects_missing_assignments_without_falling_back_to_os_temporary_storage() {
        assert!(disk_manager(vec![]).is_err());
        assert!(disk_manager(vec![PathBuf::from("relative")]).is_err());
        let root = tempfile::tempdir().unwrap();
        assert!(disk_manager(vec![root.path().join("missing")]).is_err());
        let file = tempfile::NamedTempFile::new_in(root.path()).unwrap();
        assert!(disk_manager(vec![file.path().to_path_buf()]).is_err());
    }
}
