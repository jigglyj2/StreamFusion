// Copyright 2026 StreamFusion Authors
// Licensed under the Apache License, Version 2.0.

//! Admit growing changelog descriptors and the selected Arrow payload before state is committed.
use super::*;
use crate::memory_pool::selection::{add, fixed_allowance, multiply, row_allowance};

const CHUNK: usize = 64 << 10;

pub(super) struct OutputBuffer {
    events: Vec<OutputEvent>,
    memory: Option<HostMemoryReservation>,
    row_cache_bytes: usize,
}

impl OutputBuffer {
    pub(super) fn new(
        sources: &CandidateSources,
        owner: &HostMemoryReservation,
        enabled: bool,
    ) -> Result<Self> {
        let rows = sources
            .iter()
            .try_fold(0, |n, batch| add(n, batch.num_rows()))?;
        let row_cache_bytes = multiply(rows, std::mem::size_of::<Option<usize>>())?;
        let mut result = Self {
            events: Vec::new(),
            memory: enabled
                .then(|| owner.sibling("append Top-N output selection and Arrow gather")),
            row_cache_bytes,
        };
        if result.memory.is_some() {
            result.reserve(add(CHUNK, row_cache_bytes)?)?;
        }
        Ok(result)
    }

    pub(super) fn len(&self) -> usize {
        self.events.len()
    }

    pub(super) fn push(&mut self, event: OutputEvent) -> Result<()> {
        if self.memory.is_some() {
            // Covers geometric Vec growth, gather indices, rank/kind columns and envelopes.
            // Broker calls occur only at coarse chunk boundaries, never for every allocation.
            self.reserve(add(
                self.row_cache_bytes,
                multiply(add(self.len(), 1)?, 256)?,
            )?)?;
        }
        self.events.push(event);
        Ok(())
    }

    pub(super) fn reserve_payload(&mut self, sources: &CandidateSources) -> Result<()> {
        if self.memory.is_none() {
            return Ok(());
        }
        let mut bytes = add(self.row_cache_bytes, multiply(self.len(), 256)?)?;
        for source in sources.iter() {
            bytes = add(bytes, fixed_allowance(source.columns())?)?;
        }
        let mut rows = sources
            .iter()
            .map(|batch| vec![None; batch.num_rows()])
            .collect::<Vec<_>>();
        for event in &self.events {
            let candidate = event.candidate;
            let size = match rows[candidate.source][candidate.row] {
                Some(size) => size,
                None => {
                    let size = row_allowance(sources[candidate.source].columns(), candidate.row)?;
                    rows[candidate.source][candidate.row] = Some(size);
                    size
                }
            };
            // Repeated historical winners require repeated output bytes even with small input.
            bytes = add(bytes, size)?;
        }
        self.reserve(bytes)
    }

    fn reserve(&mut self, bytes: usize) -> Result<()> {
        let memory = self.memory.as_mut().expect("accounted output");
        if bytes > memory.size() {
            memory.resize(multiply(add(bytes, CHUNK - 1)? / CHUNK, CHUNK)?)?;
        }
        Ok(())
    }

    pub(super) fn into_parts(self) -> (Vec<OutputEvent>, Option<HostMemoryReservation>) {
        (self.events, self.memory)
    }
}

#[cfg(test)]
mod tests;
