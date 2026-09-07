// Copyright 2026 StreamFusion Authors
// Licensed under the Apache License, Version 2.0

mod memory;
mod read_batch;
mod read_keys;
pub(crate) use read_batch::StateReadBatch;
mod rocks_plugin;
mod snapshot;
mod snapshot_bytes;
pub(crate) use snapshot_bytes::SnapshotBytes;
mod timer;
mod value;
mod workspace;
pub(crate) use value::StateValue;
pub(crate) use workspace::reserve_decoded_values;

use std::path::Path;

use datafusion::error::Result;

pub(crate) use memory::MemoryKeyedState;
pub(crate) use rocks_plugin::RocksPluginKeyedState;
pub(crate) use snapshot::decode as decode_key_group_snapshot;
pub(crate) use timer::{NativeTimerService, TimerDomain, TimerKey};

#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub(crate) struct StateKeyRef<'a> {
    pub(crate) key_group: u32,
    pub(crate) key: &'a [u8],
}

#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub(crate) struct StateKey {
    pub(crate) key_group: u32,
    pub(crate) key: Vec<u8>,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) struct StateMutation {
    pub(crate) key: StateKey,
    /// `None` deletes the key; `Some` stores the opaque value bytes.
    pub(crate) value: Option<Vec<u8>>,
}

/// Backend-neutral byte state used by native keyed operators.
///
/// Runtime keys and values are opaque to Java. Key-group snapshots deliberately use the same
/// versioned representation for every backend so a restore can change backend implementations.
pub(crate) trait KeyedState: Send {
    /// Fetches a whole operator batch in input order. An in-memory backend may borrow values;
    /// an external backend may own them (for example, RocksDB `multi_get`).
    fn get_batch<'a>(
        &'a self,
        keys: &[StateKeyRef<'_>],
        owner: &crate::memory_pool::HostMemoryReservation,
    ) -> Result<StateReadBatch<'a>>;

    /// Applies one atomic operator batch. Backends should use their native batch primitive.
    fn write_batch(&mut self, mutations: Vec<StateMutation>) -> Result<()>;

    /// Visits a stable key group in bounded pages, without constructing a canonical snapshot.
    /// The caller admits `max_bytes` plus processing workspace before invoking this method.
    fn visit_key_group(
        &self,
        key_group: u32,
        max_rows: usize,
        max_bytes: usize,
        visitor: &mut dyn FnMut(&[(&[u8], &[u8])]) -> Result<()>,
    ) -> Result<()>;

    fn snapshot_key_group(
        &self,
        key_group: u32,
        owner: &crate::memory_pool::HostMemoryReservation,
    ) -> Result<SnapshotBytes>;

    fn restore_key_group(
        &mut self,
        key_group: u32,
        bytes: &[u8],
        owner: &crate::memory_pool::HostMemoryReservation,
    ) -> Result<()>;

    fn checkpoint(&self, _directory: &Path) -> Result<()> {
        Err(datafusion::error::DataFusionError::Execution(
            "this native state backend does not support physical checkpoints".to_string(),
        ))
    }
}
