// Copyright 2026 StreamFusion Authors
// Licensed under the Apache License, Version 2.0.

/// One exact-length allocation for a point-state key and its value. Only the key
/// participates in hashing; the split is private and never enters persisted state.
pub(super) struct PackedEntry {
    bytes: Box<[u8]>,
    key_len: usize,
}

impl PackedEntry {
    pub(super) fn new(mut key: Vec<u8>, value: &[u8]) -> Self {
        let key_len = key.len();
        key.reserve_exact(value.len());
        key.extend_from_slice(value);
        Self {
            bytes: key.into_boxed_slice(),
            key_len,
        }
    }

    pub(super) fn key(&self) -> &[u8] {
        &self.bytes[..self.key_len]
    }

    pub(super) fn value(&self) -> &[u8] {
        &self.bytes[self.key_len..]
    }

    pub(super) fn update(&mut self, key: Vec<u8>, value: &[u8]) {
        debug_assert_eq!(self.key(), key);
        if self.value().len() == value.len() {
            self.bytes[self.key_len..].copy_from_slice(value);
        } else {
            *self = Self::new(key, value);
        }
    }
}
