// Copyright 2026 StreamFusion Authors
// Licensed under the Apache License, Version 2.0
//! Test instrumentation for actual backend work, independent of operator diagnostic counters.
use super::*;
use crate::memory_pool::HostMemoryReservation;
use datafusion::error::Result;
use std::sync::{
    atomic::{AtomicUsize, Ordering},
    Arc,
};

#[derive(Default)]
pub(crate) struct Io {
    pub(crate) read_batches: AtomicUsize,
    pub(crate) write_batches: AtomicUsize,
    pub(crate) range_reads: AtomicUsize,
    pub(crate) read_bytes: AtomicUsize,
    pub(crate) scanned_rows: AtomicUsize,
    pub(crate) written_bytes: AtomicUsize,
}
impl Io {
    pub(crate) fn reset(&self) {
        self.read_batches.store(0, Ordering::Relaxed);
        self.write_batches.store(0, Ordering::Relaxed);
        self.range_reads.store(0, Ordering::Relaxed);
        self.read_bytes.store(0, Ordering::Relaxed);
        self.scanned_rows.store(0, Ordering::Relaxed);
        self.written_bytes.store(0, Ordering::Relaxed);
    }
}
pub(crate) struct Observed {
    pub(crate) inner: Box<dyn KeyedState>,
    pub(crate) io: Arc<Io>,
}
impl KeyedState for Observed {
    fn get_batch<'a>(
        &'a self,
        keys: &[StateKeyRef<'_>],
        owner: &HostMemoryReservation,
    ) -> Result<StateReadBatch<'a>> {
        self.io.read_batches.fetch_add(1, Ordering::Relaxed);
        let result = self.inner.get_batch(keys, owner)?;
        self.io.read_bytes.fetch_add(
            result.iter().flatten().map(|v| v.len()).sum::<usize>(),
            Ordering::Relaxed,
        );
        Ok(result)
    }
    fn write_batch(&mut self, mutations: Vec<StateMutation>) -> Result<()> {
        self.io.write_batches.fetch_add(1, Ordering::Relaxed);
        self.io.written_bytes.fetch_add(
            mutations
                .iter()
                .map(|m| m.key.key.len() + m.value.as_ref().map_or(0, Vec::len))
                .sum::<usize>(),
            Ordering::Relaxed,
        );
        self.inner.write_batch(mutations)
    }
    fn visit_key_group(
        &self,
        group: u32,
        rows: usize,
        bytes: usize,
        f: &mut dyn FnMut(&[(&[u8], &[u8])]) -> Result<()>,
    ) -> Result<()> {
        self.inner.visit_key_group(group, rows, bytes, f)
    }
    fn visit_prefix(
        &self,
        group: u32,
        prefix: &[u8],
        rows: usize,
        bytes: usize,
        f: &mut dyn FnMut(&[(&[u8], &[u8])]) -> Result<()>,
    ) -> Result<()> {
        self.io.range_reads.fetch_add(1, Ordering::Relaxed);
        self.inner
            .visit_prefix(group, prefix, rows, bytes, &mut |page| {
                self.io
                    .scanned_rows
                    .fetch_add(page.len(), Ordering::Relaxed);
                self.io.read_bytes.fetch_add(
                    page.iter().map(|(k, v)| k.len() + v.len()).sum::<usize>(),
                    Ordering::Relaxed,
                );
                f(page)
            })
    }

    fn visit_range(
        &self,
        group: u32,
        start: &[u8],
        end: Option<&[u8]>,
        rows: usize,
        bytes: usize,
        f: &mut dyn FnMut(&[(&[u8], &[u8])]) -> Result<bool>,
    ) -> Result<()> {
        self.io.range_reads.fetch_add(1, Ordering::Relaxed);
        self.inner
            .visit_range(group, start, end, rows, bytes, &mut |page| {
                self.io
                    .scanned_rows
                    .fetch_add(page.len(), Ordering::Relaxed);
                self.io.read_bytes.fetch_add(
                    page.iter().map(|(k, v)| k.len() + v.len()).sum::<usize>(),
                    Ordering::Relaxed,
                );
                f(page)
            })
    }
    fn snapshot_key_group(
        &self,
        group: u32,
        owner: &HostMemoryReservation,
    ) -> Result<SnapshotBytes> {
        self.inner.snapshot_key_group(group, owner)
    }
    fn restore_key_group(
        &mut self,
        group: u32,
        bytes: &[u8],
        owner: &HostMemoryReservation,
    ) -> Result<()> {
        self.inner.restore_key_group(group, bytes, owner)
    }
}
