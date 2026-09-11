// Copyright 2026 StreamFusion Authors
// Licensed under the Apache License, Version 2.0.

use super::*;

pub(super) struct TimerBatch {
    entries: Vec<(u32, TimerDomain, TimerKey)>,
    bytes: usize,
    memory: HostMemoryReservation,
}

impl TimerBatch {
    pub(super) fn new(owner: &HostMemoryReservation) -> Self {
        Self {
            entries: Vec::new(),
            bytes: 0,
            memory: owner.sibling("session migration timer batch"),
        }
    }

    pub(super) fn push(
        &mut self,
        group: u32,
        timestamp: i64,
        key: &[u8],
        namespace: &[u8],
        service: &mut NativeTimerService,
    ) -> Result<()> {
        let bytes = key
            .len()
            .saturating_add(namespace.len())
            .saturating_add(512);
        if !self.entries.is_empty()
            && (self.entries.len() == 256 || self.bytes.saturating_add(bytes) > 128 << 10)
        {
            self.flush(service)?;
        }
        admit_workspace(
            &mut self.memory,
            self.bytes.saturating_add(bytes).saturating_mul(2),
        )?;
        self.entries.push((
            group,
            TimerDomain::EventTime,
            TimerKey {
                timestamp,
                key: key.to_vec(),
                namespace: namespace.to_vec(),
            },
        ));
        self.bytes = self.bytes.saturating_add(bytes);
        Ok(())
    }

    pub(super) fn flush(&mut self, service: &mut NativeTimerService) -> Result<()> {
        if !self.entries.is_empty() {
            service.register_batch(std::mem::take(&mut self.entries))?;
        }
        self.bytes = 0;
        self.memory.resize(0)
    }
}
