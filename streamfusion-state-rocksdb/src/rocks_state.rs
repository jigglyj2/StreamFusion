// Copyright 2026 StreamFusion Authors
// Licensed under the Apache License, Version 2.0

use std::collections::HashMap;
use std::fs;
use std::io::{Error, ErrorKind, Result};
use std::path::{Path, PathBuf};
use std::sync::{Arc, LazyLock, Mutex, Weak};

use rocksdb::checkpoint::Checkpoint;
use rocksdb::{
    BlockBasedOptions, Cache, Direction, IteratorMode, Options, WriteBatch, WriteBufferManager, DB,
};
use streamfusion_state_abi::{decode_key_group_snapshot, encode_key_group_snapshot};

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
}

struct SharedRocksMemory {
    cache: Cache,
    write_buffers: WriteBufferManager,
}

static SHARED_ROCKS_MEMORY: LazyLock<Mutex<HashMap<usize, Weak<SharedRocksMemory>>>> =
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
        let shared_memory = shared_rocks_memory(memory_limit)?;
        let mut options = Options::default();
        options.create_if_missing(true);
        let mut table_options = BlockBasedOptions::default();
        table_options.set_block_cache(&shared_memory.cache);
        // Keep index and filter blocks inside the same Flink-reserved cache instead of letting
        // RocksDB allocate an invisible second pool. Pinning L0 metadata avoids cache churn while
        // still charging those bytes to the shared cache.
        table_options.set_cache_index_and_filter_blocks(true);
        table_options.set_pin_l0_filter_and_index_blocks_in_cache(true);
        options.set_block_based_table_factory(&table_options);
        options.set_write_buffer_manager(&shared_memory.write_buffers);
        options.set_write_buffer_size(memory_limit / 4);
        options.set_max_write_buffer_number(2);
        let db = DB::open(&options, path).map_err(rocks_error)?;
        Ok(Self {
            db,
            _shared_memory: shared_memory,
            first_key_group,
            last_key_group,
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
        self.check_owned(key_group)?;
        let prefix = key_group.to_be_bytes();
        let mut entries = Vec::new();
        for item in self
            .db
            .iterator(IteratorMode::From(&prefix, Direction::Forward))
        {
            let (key, value) = item.map_err(rocks_error)?;
            if !key.starts_with(&prefix) {
                break;
            }
            entries.push((key[4..].to_vec(), value.to_vec()));
        }
        encode_key_group_snapshot(
            key_group,
            entries
                .iter()
                .map(|(key, value)| (key.as_slice(), value.as_slice())),
        )
        .map_err(Error::other)
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

fn shared_rocks_memory(memory_limit: usize) -> Result<Arc<SharedRocksMemory>> {
    let mut pools = SHARED_ROCKS_MEMORY
        .lock()
        .map_err(|_| Error::other("native RocksDB shared-memory registry is poisoned"))?;
    if let Some(existing) = pools.get(&memory_limit).and_then(Weak::upgrade) {
        return Ok(existing);
    }
    // Match Flink's shared RocksDB design: one cache and one write-buffer manager cap memory
    // across all DB instances in the task manager instead of multiplying the budget per operator.
    // Charging memtables to the cache keeps their combined footprint within this single limit.
    // The Java lease reserves the full memory_limit from Flink. Keep one quarter as headroom for
    // RocksDB DB/iterator/table-reader metadata that is not cache- or memtable-owned.
    let cache_capacity = memory_limit - memory_limit / 4;
    let cache = Cache::new_lru_cache(cache_capacity);
    let write_buffers = WriteBufferManager::new_write_buffer_manager_with_cache(
        memory_limit / 4,
        false,
        cache.clone(),
    );
    let shared = Arc::new(SharedRocksMemory {
        cache,
        write_buffers,
    });
    pools.retain(|_, pool| pool.strong_count() > 0);
    pools.insert(memory_limit, Arc::downgrade(&shared));
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
            RocksStateBackend::open_with_memory_limit(first_dir.path(), 0, 0, 1 << 20).unwrap();
        let second =
            RocksStateBackend::open_with_memory_limit(second_dir.path(), 0, 0, 1 << 20).unwrap();

        assert!(Arc::ptr_eq(&first._shared_memory, &second._shared_memory));
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
