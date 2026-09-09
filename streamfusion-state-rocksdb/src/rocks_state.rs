// Copyright 2026 StreamFusion Authors
// Licensed under the Apache License, Version 2.0

use std::collections::HashMap;
use std::fs;
use std::io::{Error, ErrorKind, Result};
use std::path::{Path, PathBuf};
use std::sync::{Arc, LazyLock, Mutex, Weak};

use rocksdb::checkpoint::Checkpoint;
use rocksdb::{BlockBasedOptions, Cache, LruCacheOptions, WriteBatch, WriteBufferManager, DB};
use streamfusion_state_abi::decode_key_group_snapshot;

mod scan;
pub use scan::ScanPage;

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct StateKey {
    pub key_group: u32,
    pub key: Vec<u8>,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct StateMutation {
    pub key: StateKey,
    pub value: Option<Vec<u8>>,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct CheckpointFile {
    pub relative_path: PathBuf,
    pub size: u64,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct RocksCheckpoint {
    pub directory: PathBuf,
    pub files: Vec<CheckpointFile>,
}

/// Direct Rust RocksDB keyed state. Keys are prefixed by key group so rescaling and canonical
/// snapshots never require scanning unrelated groups.
pub struct RocksStateBackend {
    db: DB,
    _shared_memory: Arc<SharedRocksMemory>,
    first_key_group: u32,
    last_key_group: u32,
    // Drop after DB: RocksDB must finish logging before its owned log files are removed.
    _logs: crate::log_directory::LogDirectory,
}

struct SharedRocksMemory {
    limit: usize,
    cache: Cache,
    write_buffers: WriteBufferManager,
}

static SHARED_ROCKS_MEMORY: LazyLock<Mutex<HashMap<[u64; 2], Weak<SharedRocksMemory>>>> =
    LazyLock::new(|| Mutex::new(HashMap::new()));

impl RocksStateBackend {
    pub fn open(path: &Path, first_key_group: u32, last_key_group: u32) -> Result<Self> {
        Self::open_with_memory_limit(path, first_key_group, last_key_group, 64 << 20)
    }

    pub fn open_with_memory_limit(
        path: &Path,
        first_key_group: u32,
        last_key_group: u32,
        memory_limit: usize,
    ) -> Result<Self> {
        Self::open_with_memory_scope(path, first_key_group, last_key_group, memory_limit, [0, 0])
    }

    pub fn open_with_memory_scope(
        path: &Path,
        first_key_group: u32,
        last_key_group: u32,
        memory_limit: usize,
        scope: [u64; 2],
    ) -> Result<Self> {
        Self::open_configured(
            path,
            first_key_group,
            last_key_group,
            memory_limit,
            scope,
            None,
        )
    }

    pub(crate) fn open_configured(
        path: &Path,
        first_key_group: u32,
        last_key_group: u32,
        memory_limit: usize,
        scope: [u64; 2],
        log_directory: Option<&Path>,
    ) -> Result<Self> {
        if first_key_group > last_key_group {
            return Err(Error::new(
                ErrorKind::InvalidInput,
                "invalid key-group range",
            ));
        }
        if memory_limit < 256 * 1024 {
            return Err(Error::new(
                ErrorKind::InvalidInput,
                "RocksDB state memory limit must be at least 256 KiB",
            ));
        }
        let shared_memory = shared_rocks_memory(memory_limit, scope)?;
        let mut options = crate::flink_options::base_options().map_err(rocks_error)?;
        if let Some(directory) = log_directory {
            if !directory.is_absolute() || !path.is_absolute() {
                return Err(Error::new(
                    ErrorKind::InvalidInput,
                    "relocated RocksDB logs require absolute paths",
                ));
            }
            options.set_db_log_dir(directory);
        }
        let mut table_options = BlockBasedOptions::default();
        table_options.set_block_size(4096);
        table_options.set_metadata_block_size(4096);
        table_options.set_block_cache(&shared_memory.cache);
        // Keep index and filter blocks inside the same Flink-reserved cache instead of letting
        // RocksDB allocate an invisible second pool. Pinning L0 metadata avoids cache churn while
        // still charging those bytes to the shared cache.
        table_options.set_cache_index_and_filter_blocks(true);
        table_options.set_cache_index_and_filter_blocks_with_high_priority(true);
        table_options.set_pin_l0_filter_and_index_blocks_in_cache(true);
        options.set_block_based_table_factory(&table_options);
        options.set_write_buffer_manager(&shared_memory.write_buffers);
        // Open the default family explicitly so batched pinned reads can address it. `open_cf`
        // replaces column options with Options::default(), silently dropping this shared cache,
        // its charged index/filter blocks, compression, and the configured memtable limit.
        let db = DB::open_cf_with_opts(&options, path, [("default", options.clone())])
            .map_err(rocks_error)?;
        Ok(Self {
            db,
            _shared_memory: shared_memory,
            first_key_group,
            last_key_group,
            _logs: crate::log_directory::LogDirectory::new(path, log_directory),
        })
    }

    /// Performs one RocksDB multi-get for the entire incoming Arrow/operator batch.
    pub fn get_batch(&self, keys: &[StateKey]) -> Result<Vec<Option<Vec<u8>>>> {
        self.get_batch_refs(keys.iter().map(|key| (key.key_group, key.key.as_slice())))
    }

    /// Borrows Arrow-owned keys at the component boundary and only materializes RocksDB's
    /// key-group-prefixed representation.
    pub fn get_batch_refs<'a>(
        &self,
        keys: impl IntoIterator<Item = (u32, &'a [u8])>,
    ) -> Result<Vec<Option<Vec<u8>>>> {
        let database_keys = keys
            .into_iter()
            .map(|(key_group, key)| {
                self.check_owned(key_group)?;
                Ok(database_key_parts(key_group, key))
            })
            .collect::<Result<Vec<_>>>()?;
        self.db
            .multi_get(database_keys)
            .into_iter()
            .map(|result| result.map_err(rocks_error))
            .collect()
    }

    /// Pins one batched read in RocksDB's shared cache, admits the result payload once,
    /// then copies it into producer-owned Arrow storage. No per-key host callbacks occur.
    pub fn get_batch_refs_admitted<'a>(
        &self,
        keys: impl IntoIterator<Item = (u32, &'a [u8])>,
        mut admit: impl FnMut(usize) -> Result<()>,
    ) -> Result<Vec<Option<Vec<u8>>>> {
        let database_keys = keys
            .into_iter()
            .map(|(key_group, key)| {
                self.check_owned(key_group)?;
                Ok(database_key_parts(key_group, key))
            })
            .collect::<Result<Vec<_>>>()?;
        let cf = self
            .db
            .cf_handle("default")
            .ok_or_else(|| Error::other("RocksDB default column family is missing"))?;
        let pinned = self.db.batched_multi_get_cf(cf, &database_keys, false);
        let bytes = pinned.iter().try_fold(0usize, |bytes, value| {
            let value = value
                .as_ref()
                .map_err(|error| Error::other(error.to_string()))?;
            bytes
                .checked_add(value.as_ref().map_or(0, |value| value.len()))
                .ok_or_else(|| Error::other("RocksDB batch payload size overflow"))
        })?;
        admit(bytes)?;
        pinned
            .into_iter()
            .map(|value| {
                value
                    .map(|value| value.map(|value| value.to_vec()))
                    .map_err(rocks_error)
            })
            .collect()
    }

    /// Applies one RocksDB WriteBatch for the entire incoming Arrow/operator batch.
    pub fn write_batch(&self, mutations: Vec<StateMutation>) -> Result<()> {
        self.write_batch_refs(mutations.iter().map(|mutation| {
            (
                mutation.key.key_group,
                mutation.key.key.as_slice(),
                mutation.value.as_deref(),
            )
        }))
    }

    /// Borrows Arrow-owned mutations until RocksDB copies them into its native WriteBatch.
    pub fn write_batch_refs<'a>(
        &self,
        mutations: impl IntoIterator<Item = (u32, &'a [u8], Option<&'a [u8]>)>,
    ) -> Result<()> {
        let mut batch = WriteBatch::default();
        for (key_group, key, value) in mutations {
            self.check_owned(key_group)?;
            let key = database_key_parts(key_group, key);
            match value {
                Some(value) => batch.put(key, value),
                None => batch.delete(key),
            }
        }
        // Flink's RocksDB state backend disables WAL as well: checkpoint state and input replay,
        // rather than a task-local log, define recovery durability.
        self.db.write_without_wal(batch).map_err(rocks_error)
    }

    /// Emits the backend-neutral SFS1 key-group representation used by memory state as well.
    pub fn snapshot_key_group(&self, key_group: u32) -> Result<Vec<u8>> {
        self.snapshot_key_group_admitted(key_group, |_| Ok(()))
    }

    pub fn snapshot_key_group_admitted(
        &self,
        key_group: u32,
        mut admit: impl FnMut(usize) -> Result<()>,
    ) -> Result<Vec<u8>> {
        self.check_owned(key_group)?;
        let prefix = key_group.to_be_bytes();
        // Both passes read one RocksDB snapshot. The first measures borrowed slices;
        // the second writes directly into the admitted canonical buffer.
        let snapshot = self.db.snapshot();
        let mut iterator = snapshot.raw_iterator();
        iterator.seek(prefix);
        let mut count = 0usize;
        let mut bytes = 16usize;
        while iterator.valid() {
            let key = iterator.key().expect("valid iterator has key");
            if !key.starts_with(&prefix) {
                break;
            }
            let value = iterator.value().expect("valid iterator has value");
            bytes = bytes
                .checked_add(4)
                .and_then(|bytes| bytes.checked_add(key.len()))
                .and_then(|bytes| bytes.checked_add(value.len()))
                .ok_or_else(|| Error::other("canonical snapshot size overflow"))?;
            count += 1;
            iterator.next();
        }
        iterator.status().map_err(rocks_error)?;
        admit(bytes)?;
        let mut writer = streamfusion_state_abi::SnapshotWriter::new(key_group, count, bytes)
            .map_err(Error::other)?;
        iterator.seek(prefix);
        while iterator.valid() {
            let key = iterator.key().expect("valid iterator has key");
            if !key.starts_with(&prefix) {
                break;
            }
            writer
                .append(
                    &key[4..],
                    iterator.value().expect("valid iterator has value"),
                )
                .map_err(Error::other)?;
            iterator.next();
        }
        iterator.status().map_err(rocks_error)?;
        writer.finish().map_err(Error::other)
    }

    pub fn restore_key_group(&self, key_group: u32, bytes: &[u8]) -> Result<()> {
        self.check_owned(key_group)?;
        let entries = decode_key_group_snapshot(key_group, bytes).map_err(Error::other)?;
        let mutations = entries
            .into_iter()
            .map(|(key, value)| StateMutation {
                key: StateKey { key_group, key },
                value: Some(value),
            })
            .collect();
        self.write_batch(mutations)
    }

    /// Creates a native RocksDB checkpoint. RocksDB hard-links unchanged SSTs; callers can use
    /// this file manifest as Flink shared state and upload only file identities not seen before.
    pub fn checkpoint(&self, directory: &Path) -> Result<RocksCheckpoint> {
        // RocksDB's checkpoint call uses a zero log-size threshold, which flushes the WAL-disabled
        // memtables into immutable SSTs before it links the database files. Do not flush a second
        // time here: that adds another synchronous native call to every barrier without changing
        // the checkpoint boundary.
        Checkpoint::new(&self.db)
            .map_err(rocks_error)?
            .create_checkpoint(directory)
            .map_err(rocks_error)?;
        let mut files = Vec::new();
        collect_files(directory, directory, &mut files)?;
        files.sort_unstable_by(|left, right| left.relative_path.cmp(&right.relative_path));
        Ok(RocksCheckpoint {
            directory: directory.to_path_buf(),
            files,
        })
    }

    fn check_owned(&self, key_group: u32) -> Result<()> {
        if (self.first_key_group..=self.last_key_group).contains(&key_group) {
            Ok(())
        } else {
            Err(Error::new(
                ErrorKind::InvalidInput,
                format!("key group {key_group} is not owned by this backend"),
            ))
        }
    }
}

fn shared_rocks_memory(memory_limit: usize, scope: [u64; 2]) -> Result<Arc<SharedRocksMemory>> {
    let mut pools = SHARED_ROCKS_MEMORY
        .lock()
        .map_err(|_| Error::other("native RocksDB shared-memory registry is poisoned"))?;
    if let Some(existing) = pools.get(&scope).and_then(Weak::upgrade) {
        if existing.limit != memory_limit {
            return Err(Error::new(
                ErrorKind::InvalidInput,
                "RocksDB resource identity reused with a different memory budget",
            ));
        }
        return Ok(existing);
    }
    // Flink RocksDBMemoryControllerUtils reserves headroom for WBM's 50% over-capacity
    // threshold: cache=(3-write_ratio)*budget/3, WBM=2*budget*write_ratio/3.
    // The default write ratio is 0.5 and high-priority cache ratio is 0.1. These pools
    // share one Flink resource across DBs; WBM charges its entries to that same cache.
    let cache_capacity = (2.5 * memory_limit as f64 / 3.0) as usize;
    let mut cache_options = LruCacheOptions::default();
    cache_options.set_capacity(cache_capacity);
    cache_options.set_num_shard_bits(-1);
    cache_options.set_high_pri_pool_ratio(0.1);
    let cache = Cache::new_lru_cache_opts(&cache_options);
    let write_buffers = WriteBufferManager::new_write_buffer_manager_with_cache(
        (memory_limit as f64 / 3.0) as usize,
        false,
        cache.clone(),
    );
    let shared = Arc::new(SharedRocksMemory {
        limit: memory_limit,
        cache,
        write_buffers,
    });
    pools.retain(|_, pool| pool.strong_count() > 0);
    if scope != [0, 0] {
        pools.insert(scope, Arc::downgrade(&shared));
    }
    Ok(shared)
}

fn database_key_parts(key_group: u32, key: &[u8]) -> Vec<u8> {
    let mut encoded = Vec::with_capacity(4 + key.len());
    encoded.extend_from_slice(&key_group.to_be_bytes());
    encoded.extend_from_slice(key);
    encoded
}

fn collect_files(root: &Path, directory: &Path, files: &mut Vec<CheckpointFile>) -> Result<()> {
    for entry in fs::read_dir(directory)? {
        let entry = entry?;
        let path = entry.path();
        let metadata = entry.metadata()?;
        if metadata.is_dir() {
            collect_files(root, &path, files)?;
        } else {
            files.push(CheckpointFile {
                relative_path: path.strip_prefix(root).unwrap().to_path_buf(),
                size: metadata.len(),
            });
        }
    }
    Ok(())
}

fn rocks_error(error: rocksdb::Error) -> Error {
    Error::other(error)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn checkpoint_ssts_use_flinks_default_snappy_and_reopen_with_exact_values() {
        let directory = tempfile::tempdir().unwrap();
        let checkpoint = tempfile::tempdir().unwrap();
        let saved = checkpoint.path().join("state");
        let value = b"repeated-state-payload".repeat(2048);
        let keys = (0u32..128)
            .map(|index| StateKey {
                key_group: 0,
                key: index.to_be_bytes().to_vec(),
            })
            .collect::<Vec<_>>();
        let state = RocksStateBackend::open(directory.path(), 0, 0).unwrap();
        state
            .write_batch(
                keys.iter()
                    .cloned()
                    .map(|key| StateMutation {
                        key,
                        value: Some(value.clone()),
                    })
                    .collect(),
            )
            .unwrap();
        let snapshot = state.checkpoint(&saved).unwrap();
        let sst_bytes = snapshot
            .files
            .iter()
            .filter(|file| {
                file.relative_path
                    .extension()
                    .is_some_and(|extension| extension == "sst")
            })
            .map(|file| file.size)
            .sum::<u64>();
        assert!(sst_bytes > 0);
        assert!(
            sst_bytes < (keys.len() * value.len() / 4) as u64,
            "Snappy must be compiled in, not just named in OPTIONS: {sst_bytes} bytes"
        );
        let reopened = RocksStateBackend::open(&saved, 0, 0).unwrap();
        assert_eq!(
            reopened.get_batch(&keys).unwrap(),
            vec![Some(value); keys.len()]
        );
    }

    #[test]
    fn admitted_batch_reads_preserve_order_duplicates_and_missing_values() {
        let directory = tempfile::tempdir().unwrap();
        let state = RocksStateBackend::open(directory.path(), 2, 2).unwrap();
        state
            .write_batch(vec![mutation(2, b"a", Some(b"value"))])
            .unwrap();
        let keys = [
            (2, b"a".as_slice()),
            (2, b"missing".as_slice()),
            (2, b"a".as_slice()),
        ];
        let mut admissions = Vec::new();
        let values = state
            .get_batch_refs_admitted(keys, |bytes| {
                admissions.push(bytes);
                Ok(())
            })
            .unwrap();
        assert_eq!(admissions, vec![10]);
        assert_eq!(
            values,
            vec![Some(b"value".to_vec()), None, Some(b"value".to_vec())]
        );
        assert!(state
            .get_batch_refs_admitted(keys, |_| Err(Error::new(ErrorKind::OutOfMemory, "denied")))
            .is_err());
        assert_eq!(
            state.get_batch(&[key(2, b"a")]).unwrap(),
            vec![Some(b"value".to_vec())]
        );
    }

    #[test]
    fn batches_reads_and_writes_and_round_trips_the_canonical_memory_format() {
        let source_dir = tempfile::tempdir().unwrap();
        let source = RocksStateBackend::open(source_dir.path(), 2, 3).unwrap();
        source
            .write_batch(vec![
                mutation(2, b"a", Some(b"one")),
                mutation(2, b"b", Some(b"two")),
                mutation(3, b"c", Some(b"three")),
            ])
            .unwrap();
        assert_eq!(
            source
                .get_batch(&[key(2, b"b"), key(3, b"c"), key(2, b"missing")])
                .unwrap(),
            vec![Some(b"two".to_vec()), Some(b"three".to_vec()), None]
        );

        let canonical = source.snapshot_key_group(2).unwrap();
        let target_dir = tempfile::tempdir().unwrap();
        let target = RocksStateBackend::open(target_dir.path(), 2, 2).unwrap();
        target.restore_key_group(2, &canonical).unwrap();
        assert_eq!(
            target.get_batch(&[key(2, b"a"), key(2, b"b")]).unwrap(),
            vec![Some(b"one".to_vec()), Some(b"two".to_vec())]
        );
    }

    #[test]
    fn native_checkpoints_expose_reusable_sst_files() {
        let db_dir = tempfile::tempdir().unwrap();
        let backend = RocksStateBackend::open(db_dir.path(), 0, 0).unwrap();
        backend
            .write_batch(vec![mutation(0, b"a", Some(b"one"))])
            .unwrap();

        let checkpoints = tempfile::tempdir().unwrap();
        let first = backend.checkpoint(&checkpoints.path().join("1")).unwrap();
        backend
            .write_batch(vec![mutation(0, b"b", Some(b"two"))])
            .unwrap();
        let second = backend.checkpoint(&checkpoints.path().join("2")).unwrap();

        // Both writes were still in WAL-disabled memtables when checkpoint was called. Reopening
        // the physical checkpoint proves that the checkpoint API established the durable boundary.
        let restored = RocksStateBackend::open(&second.directory, 0, 0).unwrap();
        assert_eq!(
            restored.get_batch(&[key(0, b"a"), key(0, b"b")]).unwrap(),
            vec![Some(b"one".to_vec()), Some(b"two".to_vec())]
        );

        let first_ssts = first
            .files
            .iter()
            .filter(|file| {
                file.relative_path
                    .extension()
                    .is_some_and(|ext| ext == "sst")
            })
            .collect::<Vec<_>>();
        assert!(!first_ssts.is_empty());
        assert!(first_ssts.iter().all(|first_file| second.files.iter().any(
            |second_file| second_file.relative_path == first_file.relative_path
                && second_file.size == first_file.size
        )));
    }

    #[test]
    fn shares_one_cache_and_write_buffer_budget_across_databases() {
        let first_dir = tempfile::tempdir().unwrap();
        let second_dir = tempfile::tempdir().unwrap();
        let first =
            RocksStateBackend::open_with_memory_scope(first_dir.path(), 0, 0, 1 << 20, [1, 42])
                .unwrap();
        let second =
            RocksStateBackend::open_with_memory_scope(second_dir.path(), 0, 0, 1 << 20, [1, 42])
                .unwrap();

        assert!(Arc::ptr_eq(&first._shared_memory, &second._shared_memory));
        // Check the databases, not merely the Rust owner. An unconfigured column family opens
        // a separate default cache despite retaining the intended SharedRocksMemory wrapper.
        let expected_capacity = (2.5 * (1 << 20) as f64 / 3.0) as u64;
        for backend in [&first, &second] {
            assert_eq!(
                backend
                    .db
                    .property_int_value("rocksdb.block-cache-capacity")
                    .unwrap(),
                Some(expected_capacity)
            );
            assert_eq!(
                backend._shared_memory.write_buffers.get_buffer_size(),
                (1 << 20) / 3
            );
        }
    }

    #[test]
    fn equal_budgets_in_distinct_resources_do_not_share_and_scope_checks_size() {
        let first = shared_rocks_memory(1 << 20, [2, 41]).unwrap();
        let second = shared_rocks_memory(1 << 20, [2, 42]).unwrap();
        assert!(!Arc::ptr_eq(&first, &second));
        assert!(shared_rocks_memory(2 << 20, [2, 41]).is_err());
        let isolated = shared_rocks_memory(1 << 20, [0, 0]).unwrap();
        assert!(!Arc::ptr_eq(
            &isolated,
            &shared_rocks_memory(1 << 20, [0, 0]).unwrap()
        ));
        drop(first);
        assert!(shared_rocks_memory(2 << 20, [2, 41]).is_ok());
    }

    fn key(key_group: u32, key: &[u8]) -> StateKey {
        StateKey {
            key_group,
            key: key.to_vec(),
        }
    }

    fn mutation(key_group: u32, key_bytes: &[u8], value: Option<&[u8]>) -> StateMutation {
        StateMutation {
            key: key(key_group, key_bytes),
            value: value.map(<[u8]>::to_vec),
        }
    }
}
