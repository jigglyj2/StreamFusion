// Copyright 2026 StreamFusion Authors
// Licensed under the Apache License, Version 2.0.

use super::BufferOwner;
use arrow::buffer::Buffer;
use std::collections::BTreeMap;
use std::sync::{Arc, Mutex, Weak};

/// Task-local index, never an ownership authority: a lookup must retain the returned
/// owner. Merely recognizing a pointer would not keep its allocation credit alive.
#[derive(Default)]
pub(crate) struct Registry {
    entries: Mutex<BTreeMap<(usize, usize), Weak<BufferOwner>>>,
}
impl std::fmt::Debug for Registry {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("ArrowLeaseRegistry").finish_non_exhaustive()
    }
}
impl Registry {
    pub(super) fn register(self: &Arc<Self>, owner: &Arc<BufferOwner>) -> Registration {
        let key = (owner._buffer.as_ptr() as usize, Arc::as_ptr(owner) as usize);
        self.entries
            .lock()
            .unwrap()
            .insert(key, Arc::downgrade(owner));
        Registration {
            registry: self.clone(),
            key,
        }
    }

    pub(super) fn find(&self, buffer: &Buffer) -> Option<Arc<BufferOwner>> {
        if buffer.is_empty() {
            return None;
        }
        let base = buffer.data_ptr().as_ptr() as usize;
        let end = buffer.ptr_offset().checked_add(buffer.len())?;
        let mut after = 0;
        loop {
            let next = self
                .entries
                .lock()
                .unwrap()
                .range((base, after)..=(base, usize::MAX))
                .next()
                .map(|(key, owner)| (*key, owner.clone()));
            let (key, weak) = next?;
            // Upgrade/drop outside the registry lock: dropping the last strong owner
            // unregisters itself and must never recursively acquire this lock.
            if let Some(owner) = weak.upgrade() {
                if end <= owner._buffer.len() {
                    return Some(owner);
                }
            }
            after = key.1.checked_add(1)?;
        }
    }
    #[cfg(test)]
    pub(super) fn len(&self) -> usize {
        self.entries.lock().unwrap().len()
    }
}

#[derive(Debug)]
pub(super) struct Registration {
    registry: Arc<Registry>,
    key: (usize, usize),
}
impl Drop for Registration {
    fn drop(&mut self) {
        let mut entries = self
            .registry
            .entries
            .lock()
            .unwrap_or_else(|e| e.into_inner());
        entries.remove(&self.key);
        // BTreeMap can retain its empty root. No uncharged task-global capacity survives
        // the last owner; bounded registry metadata needs no separate descriptor allowance.
        if entries.is_empty() {
            *entries = BTreeMap::new();
        }
    }
}
