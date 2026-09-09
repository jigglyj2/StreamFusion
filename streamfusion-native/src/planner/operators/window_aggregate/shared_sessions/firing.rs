// Copyright 2026 StreamFusion Authors
// Licensed under the Apache License, Version 2.0.

use super::*;

impl SharedSessions {
    pub(super) fn advance_inner(&mut self, watermark: i64) -> Result<RecordBatch> {
        if watermark < self.kernel.current_event_time {
            return self.empty_owned();
        }
        self.kernel.current_event_time = watermark;
        let frontier = self.next_timer().unwrap_or(i64::MAX).min(watermark);
        let fired = self.kernel.timers.advance_owned_limited(
            TimerDomain::EventTime,
            frontier,
            OUTPUT_ROWS,
        )?;
        if fired.is_empty() {
            return self.empty_owned();
        }
        let key_bytes = fired
            .iter()
            .map(|timer| timer.timer.key.len())
            .sum::<usize>();
        let base = key_bytes
            .saturating_mul(8)
            .saturating_add(64 * 1024)
            .saturating_add(
                fired
                    .len()
                    .saturating_mul(512 + self.kernel.calls.len() * 512),
            );
        self.admit(base)?;
        let refs = fired
            .iter()
            .map(|timer| StateKeyRef {
                key_group: timer.key_group,
                key: &timer.timer.key,
            })
            .collect::<Vec<_>>();
        let values = self
            .kernel
            .state
            .get_batch(&refs, &self.kernel.scratch_reservation)?;
        let decoded = values
            .iter()
            .flatten()
            .map(|value| value.len())
            .sum::<usize>()
            .saturating_mul(8);
        self.kernel
            .scratch_reservation
            .resize(base.saturating_add(decoded))?;
        let mut keys = Vec::new();
        let mut starts = Vec::new();
        let mut ends = Vec::new();
        let mut columns = self
            .kernel
            .calls
            .iter()
            .map(|_| Vec::new())
            .collect::<Vec<_>>();
        let mut mutations = Vec::new();
        for (timer, value) in fired.iter().zip(values.iter()) {
            let value = value.as_ref().ok_or_else(|| {
                DataFusionError::Execution("session timer has no accumulator".into())
            })?;
            let (start, end, state) = codec::decode(value.as_ref(), &self.kernel.calls)?;
            if timer.timer.timestamp != end - 1 || timer.timer.namespace != end.to_le_bytes() {
                return Err(DataFusionError::Execution(
                    "session timer differs from its namespace".into(),
                ));
            }
            keys.push(codec::grouping(&timer.timer.key[..timer.timer.key.len() - 9])?.to_vec());
            starts.push(start);
            ends.push(end);
            for (column, value) in columns.iter_mut().zip(state.values(&self.kernel.calls)) {
                column.push(value);
            }
            mutations.push(StateMutation {
                key: StateKey {
                    key_group: timer.key_group,
                    key: timer.timer.key.clone(),
                },
                value: None,
            });
        }
        drop(values);
        self.kernel.state_read_batches += 1;
        self.kernel.timers_fired += fired.len() as u64;
        self.kernel.state.write_batch(mutations)?;
        self.kernel.state_write_batches += 1;
        let output = self.kernel.output_batch(keys, columns, starts, ends)?;
        let credit = self
            .kernel
            .scratch_reservation
            .split(output.get_array_memory_size(), "shared session output")?;
        crate::memory_pool::arrow_lease::host_batch(output, credit)
    }

    fn empty_owned(&mut self) -> Result<RecordBatch> {
        self.admit(4096 + self.kernel.calls.len() * 4096)?;
        let output = self.kernel.empty_output()?;
        let credit = self
            .kernel
            .scratch_reservation
            .split(output.get_array_memory_size(), "empty session output")?;
        crate::memory_pool::arrow_lease::host_batch(output, credit)
    }
}
