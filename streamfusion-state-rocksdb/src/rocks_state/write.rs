// Copyright 2026 StreamFusion Authors
// Licensed under the Apache License, Version 2.0

use super::database_key_parts;
use rocksdb::WriteBatch;
use std::io::Result;

// Flink RocksDBWriteBatchWrapper's default bulk capacity and allocation hint.
const CAPACITY: usize = 500;
const INITIAL_BYTES: usize = CAPACITY * 100;

/// One synchronous dirty-state flush. Chunking happens after operator computation and never
/// crosses Arrow/JNI boundaries. The upstream safe write API consumes each WriteBatch.
pub(super) fn flush_batches<'a>(
    mutations: impl Iterator<Item = (u32, &'a [u8], Option<&'a [u8]>)>,
    byte_limit: usize,
    mut flush: impl FnMut(WriteBatch) -> Result<()>,
) -> Result<()> {
    let initial = if byte_limit == 0 {
        INITIAL_BYTES
    } else {
        byte_limit.min(INITIAL_BYTES)
    };
    let mut pending: Option<WriteBatch> = None;
    for (group, key, value) in mutations {
        let batch = pending.get_or_insert_with(|| WriteBatch::with_capacity_bytes(initial));
        let key = database_key_parts(group, key);
        match value {
            Some(value) => batch.put(key, value),
            None => batch.delete(key),
        }
        // Check after adding, like Flink. One oversized entry is legal and flushes immediately.
        if batch.len() == CAPACITY || (byte_limit > 0 && batch.size_in_bytes() >= byte_limit) {
            flush(pending.take().unwrap())?;
        }
    }
    if let Some(batch) = pending {
        flush(batch)?;
    }
    Ok(())
}

#[cfg(test)]
mod tests;
