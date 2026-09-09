// Copyright 2026 StreamFusion Authors
// Licensed under the Apache License, Version 2.0.

use super::*;

impl RocksStateBackend {
    /// Select a bounded page before copying its payload, then admit that entire page once.
    /// A single value larger than the target is allowed if the host can admit it. Normal
    /// pages never grow with key-group cardinality; legacy large values remain readable.
    pub fn scan_range_page_admitted(
        &self,
        key_group: u32,
        start: &[u8],
        end: Option<&[u8]>,
        after: Option<&[u8]>,
        max_rows: usize,
        target_bytes: usize,
        mut admit: impl FnMut(usize) -> Result<()>,
    ) -> Result<ScanPage> {
        self.check_owned(key_group)?;
        if max_rows == 0 || target_bytes == 0 {
            return Err(Error::new(
                ErrorKind::InvalidInput,
                "state scan bounds must be positive",
            ));
        }
        if end.is_some_and(|end| start >= end) {
            return Ok(ScanPage {
                entries: Vec::new(),
                complete: true,
            });
        }
        let prefix = key_group.to_be_bytes();
        let lower = after.filter(|key| *key >= start).unwrap_or(start);
        let seek = database_key_parts(key_group, lower);
        let mut iterator = self.db.raw_iterator();
        let position = |iterator: &mut rocksdb::DBRawIterator<'_>| {
            iterator.seek(&seek);
            if after.is_some_and(|after| after >= start) && iterator.key() == Some(seek.as_slice())
            {
                iterator.next();
            }
        };
        position(&mut iterator);
        let in_range =
            |key: &[u8]| key.starts_with(&prefix) && end.is_none_or(|end| &key[4..] < end);
        let mut count = 0usize;
        let mut page_bytes = 0usize;
        let mut payload_bytes = 0usize;
        while iterator.valid() && count < max_rows {
            let key = iterator.key().expect("valid iterator has key");
            if !in_range(key) {
                break;
            }
            let value = iterator.value().expect("valid iterator has value");
            let payload = key.len().checked_add(value.len()).ok_or_else(|| {
                Error::new(ErrorKind::OutOfMemory, "state scan page size overflow")
            })?;
            let size = payload.checked_add(96).ok_or_else(|| {
                Error::new(ErrorKind::OutOfMemory, "state scan page size overflow")
            })?;
            if count > 0 && page_bytes.saturating_add(size) > target_bytes {
                break;
            }
            if key.len() - 4 >= u32::MAX as usize || value.len() >= u32::MAX as usize {
                return Err(Error::new(
                    ErrorKind::InvalidData,
                    "state scan entry exceeds Arrow BinaryView limit",
                ));
            }
            payload_bytes = payload_bytes.checked_add(payload).ok_or_else(|| {
                Error::new(ErrorKind::OutOfMemory, "state scan page size overflow")
            })?;
            page_bytes = page_bytes.saturating_add(size);
            count += 1;
            iterator.next();
            if page_bytes >= target_bytes {
                break;
            }
        }
        iterator.status().map_err(rocks_error)?;
        let complete = iterator.key().is_none_or(|key| !in_range(key));
        if count == 0 {
            return Ok(ScanPage {
                entries: Vec::new(),
                complete,
            });
        }
        // Payloads move into BinaryView blocks. Cover vector/Arrow descriptors and their
        // conversion overlap once per page; the caller retains this lease through release.
        let bytes = count
            .checked_mul(384)
            .and_then(|overhead| payload_bytes.checked_add(overhead))
            .and_then(|bytes| bytes.checked_add(4096))
            .ok_or_else(|| Error::new(ErrorKind::OutOfMemory, "state scan admission overflow"))?;
        admit(bytes)?;
        position(&mut iterator);
        let mut entries = Vec::with_capacity(count);
        for _ in 0..count {
            let key = iterator.key().ok_or_else(|| {
                Error::new(ErrorKind::InvalidData, "state changed during admitted scan")
            })?;
            let value = iterator.value().ok_or_else(|| {
                Error::new(ErrorKind::InvalidData, "state changed during admitted scan")
            })?;
            if !in_range(key) {
                return Err(Error::new(
                    ErrorKind::InvalidData,
                    "state changed during admitted scan",
                ));
            }
            entries.push((key[4..].to_vec(), value.to_vec()));
            iterator.next();
        }
        iterator.status().map_err(rocks_error)?;
        Ok(ScanPage { entries, complete })
    }
}
