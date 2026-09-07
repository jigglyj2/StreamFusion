// Copyright 2026 StreamFusion Authors
// Licensed under the Apache License, Version 2.0

use crate::memory_pool::HostMemoryReservation;
use arrow::array::BinaryViewArray;
use std::ops::Deref;

/// Canonical bytes retain their host admission through JNI copying or native restore.
/// The bytes are dropped before the reservation on every terminal path.
pub(crate) struct SnapshotBytes {
    storage: Storage,
    _reservation: HostMemoryReservation,
    // Arrow release callbacks belong to the plugin. Unload it only after storage drops.
    _producer: Option<std::sync::Arc<libloading::Library>>,
}

enum Storage {
    Owned(Vec<u8>),
    Arrow(BinaryViewArray),
}

impl SnapshotBytes {
    pub(crate) fn owned(bytes: Vec<u8>, reservation: HostMemoryReservation) -> Self {
        Self {
            storage: Storage::Owned(bytes),
            _reservation: reservation,
            _producer: None,
        }
    }

    pub(crate) fn arrow(
        bytes: BinaryViewArray,
        reservation: HostMemoryReservation,
        producer: std::sync::Arc<libloading::Library>,
    ) -> Self {
        Self {
            storage: Storage::Arrow(bytes),
            _reservation: reservation,
            _producer: Some(producer),
        }
    }
}

impl Deref for SnapshotBytes {
    type Target = [u8];
    fn deref(&self) -> &[u8] {
        match &self.storage {
            Storage::Owned(bytes) => bytes,
            Storage::Arrow(bytes) => bytes.value(0),
        }
    }
}

impl std::fmt::Debug for SnapshotBytes {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_tuple("SnapshotBytes").field(&self.deref()).finish()
    }
}

impl PartialEq for SnapshotBytes {
    fn eq(&self, other: &Self) -> bool {
        self.deref() == other.deref()
    }
}
impl Eq for SnapshotBytes {}
