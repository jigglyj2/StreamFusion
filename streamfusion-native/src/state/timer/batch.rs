// Copyright 2026 StreamFusion Authors
// Licensed under the Apache License, Version 2.0.

use super::*;

/// Borrow timer entries while handling a callback. Keeping the owner prevents returning their
/// memory credit while keys/namespaces or the descriptor vector are still live.
pub(crate) struct FiredTimerBatch {
    entries: Vec<FiredTimer>,
    _memory: HostMemoryReservation,
}

impl std::ops::Deref for FiredTimerBatch {
    type Target = [FiredTimer];
    fn deref(&self) -> &Self::Target {
        &self.entries
    }
}

impl NativeTimerService {
    #[cfg(test)]
    pub(crate) fn key_group_range(&self) -> std::ops::RangeInclusive<u32> {
        self.first_key_group..=self.first_key_group + self.groups.len() as u32 - 1
    }

    /// Serialized size is available without constructing the checkpoint buffer.
    #[cfg(test)]
    pub(crate) fn snapshot_size(&self, key_group: u32) -> Result<usize> {
        let group = self.group(key_group)?;
        Ok(group
            .event_time
            .iter()
            .chain(&group.processing_time)
            .fold(13usize, |size, timer| {
                size.saturating_add(17)
                    .saturating_add(timer.key.len())
                    .saturating_add(timer.namespace.len())
            }))
    }

    /// Register the distinct new timers with one host admission. The caller admits the input
    /// vector and deduplication workspace at its Arrow/control batch boundary. Returned key groups
    /// identify successful registrations, in input order, for timer counters and checkpoint dirtiness.
    pub(crate) fn register_batch(
        &mut self,
        timers: Vec<(u32, TimerDomain, TimerKey)>,
    ) -> Result<Vec<u32>> {
        let mut selected = Vec::with_capacity(timers.len());
        let mut seen =
            hashbrown::HashSet::with_capacity_and_hasher(timers.len(), ahash::RandomState::new());
        let mut growth = 0usize;
        for (key_group, domain, timer) in &timers {
            let existing = timer_set_ref(self.group(*key_group)?, *domain);
            let insert = !existing.contains(timer) && seen.insert((*key_group, *domain, timer));
            selected.push(insert);
            if insert {
                growth = growth.saturating_add(timer_heap_size(timer));
            }
        }
        drop(seen);
        let new_size = self.accounted_bytes.saturating_add(growth);
        self.reservation.resize(new_size)?;
        let mut inserted = Vec::with_capacity(selected.iter().filter(|&&insert| insert).count());
        for ((key_group, domain, timer), insert) in timers.into_iter().zip(selected) {
            if insert {
                let did_insert = self.timer_set_mut(key_group, domain)?.insert(timer);
                debug_assert!(did_insert, "timer batch was deduplicated before admission");
                inserted.push(key_group);
            }
        }
        self.accounted_bytes = new_size;
        Ok(inserted)
    }

    /// Move a bounded due prefix into an owned callback batch. Large keys retain the credit
    /// originally admitted for the timer; only the new descriptor vector requires extra credit.
    pub(crate) fn advance_owned_limited(
        &mut self,
        domain: TimerDomain,
        progress: i64,
        limit: usize,
    ) -> Result<FiredTimerBatch> {
        let count = self
            .groups
            .iter()
            .flat_map(|group| {
                timer_set_ref(group, domain)
                    .iter()
                    .take_while(|timer| timer.timestamp <= progress)
            })
            .take(limit)
            .count();
        let mut memory = self.reservation.sibling("native due timer batch");
        memory.resize(count.saturating_mul(size_of::<FiredTimer>()))?;
        let mut entries = Vec::with_capacity(count);
        let mut moved_bytes = 0usize;
        for (offset, group) in self.groups.iter_mut().enumerate() {
            let timers = timer_set(group, domain);
            while entries.len() < count
                && timers
                    .first()
                    .is_some_and(|timer| timer.timestamp <= progress)
            {
                let timer = timers.pop_first().expect("due timer exists");
                moved_bytes = moved_bytes.saturating_add(timer_heap_size(&timer));
                entries.push(FiredTimer {
                    key_group: self.first_key_group + offset as u32,
                    domain,
                    timer,
                });
            }
            if entries.len() == count {
                break;
            }
        }
        memory.grow_from(&mut self.reservation, moved_bytes)?;
        self.accounted_bytes -= moved_bytes;
        Ok(FiredTimerBatch {
            entries,
            _memory: memory,
        })
    }
}

#[cfg(test)]
mod tests;
