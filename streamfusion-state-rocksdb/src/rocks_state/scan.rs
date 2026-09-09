// Copyright 2026 StreamFusion Authors
// Licensed under the Apache License, Version 2.0.

use super::*;

mod admitted;
#[cfg(test)]
mod admitted_tests;

/// A bounded ordered range page, including whether the iterator reached the requested boundary.
#[derive(Debug)]
pub struct ScanPage {
    pub entries: Vec<(Vec<u8>, Vec<u8>)>,
    pub complete: bool,
}

impl RocksStateBackend {
    /// Reads one bounded page within an owned Flink key group.
    pub fn scan_key_group(
        &self,
        key_group: u32,
        after: Option<&[u8]>,
        max_rows: usize,
        max_bytes: usize,
    ) -> Result<Vec<(Vec<u8>, Vec<u8>)>> {
        self.scan_range(key_group, &[], None, after, max_rows, max_bytes)
    }

    pub fn scan_range(
        &self,
        key_group: u32,
        start: &[u8],
        end: Option<&[u8]>,
        after: Option<&[u8]>,
        max_rows: usize,
        max_bytes: usize,
    ) -> Result<Vec<(Vec<u8>, Vec<u8>)>> {
        Ok(self
            .scan_range_page(key_group, start, end, after, max_rows, max_bytes)?
            .entries)
    }

    pub fn scan_range_page(
        &self,
        key_group: u32,
        start: &[u8],
        end: Option<&[u8]>,
        after: Option<&[u8]>,
        max_rows: usize,
        max_bytes: usize,
    ) -> Result<ScanPage> {
        self.check_owned(key_group)?;
        if max_rows == 0 || max_bytes == 0 {
            return Err(Error::new(
                ErrorKind::InvalidInput,
                "state scan bounds must be positive",
            ));
        }
        let prefix = key_group.to_be_bytes();
        if end.is_some_and(|end| start >= end) {
            return Ok(ScanPage {
                entries: Vec::new(),
                complete: true,
            });
        }
        let lower = after.filter(|key| *key >= start).unwrap_or(start);
        let seek = database_key_parts(key_group, lower);
        let mut iterator = self.db.raw_iterator();
        iterator.seek(&seek);
        if after.is_some_and(|after| after >= start) {
            if iterator.key() == Some(seek.as_slice()) {
                iterator.next();
            }
        }
        let mut entries = Vec::new();
        let mut bytes = 0usize;
        while iterator.valid() && entries.len() < max_rows {
            let key = iterator.key().expect("valid iterator has key");
            if !key.starts_with(&prefix) || end.is_some_and(|end| &key[4..] >= end) {
                break;
            }
            let value = iterator.value().expect("valid iterator has value");
            let size = key.len().saturating_add(value.len()).saturating_add(96);
            if size > max_bytes {
                return Err(Error::new(
                    ErrorKind::OutOfMemory,
                    "state scan entry exceeds the admitted page budget",
                ));
            }
            if bytes.saturating_add(size) > max_bytes {
                break;
            }
            entries.push((key[4..].to_vec(), value.to_vec()));
            bytes += size;
            iterator.next();
        }
        iterator.status().map_err(rocks_error)?;
        let complete = iterator
            .key()
            .is_none_or(|key| !key.starts_with(&prefix) || end.is_some_and(|end| &key[4..] >= end));
        Ok(ScanPage { entries, complete })
    }
}

#[cfg(test)]
mod tests;
