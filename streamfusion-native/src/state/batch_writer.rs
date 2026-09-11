// Copyright 2026 StreamFusion Authors
// Licensed under the Apache License, Version 2.0.

use super::*;
use crate::memory_pool::HostMemoryReservation;
use datafusion::error::DataFusionError;
use std::mem::size_of;

// Bound write descriptors separately from encoded payload bytes.
const WRITE_ROWS: usize = 4096;
const WRITE_BYTES: usize = 256 << 10;

pub(crate) struct StateBatchWriter<'a> {
    state: &'a mut dyn KeyedState,
    memory: &'a mut HostMemoryReservation,
    retained: usize,
    pending: Vec<StateMutation>,
    bytes: usize,
    writes: &'a mut u64,
}

impl<'a> StateBatchWriter<'a> {
    pub(crate) fn new(
        state: &'a mut dyn KeyedState,
        memory: &'a mut HostMemoryReservation,
        retained: usize,
        writes: &'a mut u64,
    ) -> Result<Self> {
        memory.resize(retained)?;
        Ok(Self {
            state,
            memory,
            retained,
            pending: Vec::new(),
            bytes: 0,
            writes,
        })
    }

    pub(crate) fn release_retained(&mut self, bytes: usize) {
        self.retained = self.retained.saturating_sub(bytes);
    }

    pub(crate) fn finish(mut self) -> Result<()> {
        self.retained = 0;
        self.flush(0)?;
        self.memory.resize(0)
    }

    fn bound(&self, bytes: usize, rows: usize, extra: usize) -> usize {
        self.retained
            .saturating_add(bytes.saturating_mul(4))
            .saturating_add(rows.saturating_mul(512))
            .saturating_add(extra)
            .saturating_add(65536)
    }

    fn admit(&mut self, bytes: usize, rows: usize, extra: usize) -> Result<()> {
        let required = self.bound(bytes, rows, extra);
        if required > self.memory.size() {
            let rounded = required
                .checked_add(65535)
                .map_or(required, |bytes| bytes / 65536 * 65536);
            if let Err(error) = self.memory.resize(rounded) {
                if !matches!(error, DataFusionError::ResourcesExhausted(_))
                    || rounded == required
                    || !self.pending.is_empty()
                {
                    return Err(error);
                }
                self.memory.resize(required)?;
            }
        }
        Ok(())
    }

    pub(crate) fn admit_extra(&mut self, extra: usize) -> Result<()> {
        match self.admit(self.bytes, self.pending.len(), extra) {
            Err(DataFusionError::ResourcesExhausted(_)) if !self.pending.is_empty() => {
                self.flush(0)?;
                self.admit(0, 0, extra)
            }
            result => result,
        }
    }

    pub(crate) fn push(
        &mut self,
        bytes: usize,
        extra: usize,
        encode: impl FnOnce() -> Result<StateMutation>,
    ) -> Result<()> {
        if !self.pending.is_empty()
            && (self.pending.len() == WRITE_ROWS || self.bytes.saturating_add(bytes) > WRITE_BYTES)
        {
            self.flush(extra)?;
        }
        match self.admit(
            self.bytes.saturating_add(bytes),
            self.pending.len() + 1,
            extra,
        ) {
            Err(DataFusionError::ResourcesExhausted(_)) if !self.pending.is_empty() => {
                self.flush(extra)?;
                self.admit(bytes, 1, extra)?;
            }
            result => result?,
        }
        let mutation = encode()?;
        self.bytes = self.bytes.saturating_add(
            mutation
                .key
                .key
                .capacity()
                .saturating_add(mutation.value.as_ref().map_or(0, Vec::capacity)),
        );
        self.pending.push(mutation);
        Ok(())
    }

    fn flush(&mut self, extra: usize) -> Result<()> {
        if self.pending.is_empty() {
            return Ok(());
        }
        // Release credit for already-consumed staged groups before backend growth. Admission
        // and release happen per write page, never per individual dirty row. Encoding has
        // finished, so release its scratch allowance before the backend reserves its own
        // persistent growth and write workspace. Keep the pending buffers themselves charged.
        self.memory.resize(
            self.retained
                .saturating_add(self.bytes)
                .saturating_add(
                    self.pending
                        .capacity()
                        .saturating_mul(size_of::<StateMutation>()),
                )
                .saturating_add(extra)
                .saturating_add(65536),
        )?;
        self.state.write_batch(std::mem::take(&mut self.pending))?;
        self.bytes = 0;
        *self.writes = self.writes.saturating_add(1);
        Ok(())
    }
}
