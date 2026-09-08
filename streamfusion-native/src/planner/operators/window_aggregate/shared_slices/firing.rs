// Copyright 2026 StreamFusion Authors
// Licensed under the Apache License, Version 2.0.

use super::*;

impl SharedSlices {
    pub(super) fn advance_inner(&mut self, watermark: i64) -> Result<RecordBatch> {
        if watermark < self.kernel.current_event_time {
            return self.empty_owned();
        }
        self.kernel.current_event_time = watermark;
        // A callback can create an earlier timer than another already-queued window.
        // Drain one timestamp frontier at a time so newly registered windows fire before
        // later windows expire the slice state they still need.
        let frontier = self
            .kernel
            .next_event_timer()
            .unwrap_or(i64::MAX)
            .min(watermark);
        let fired = self.kernel.timers.advance_owned_limited(
            TimerDomain::EventTime,
            frontier,
            OUTPUT_ROWS,
        )?;
        if fired.is_empty() {
            return self.empty_owned();
        }
        self.kernel.timers_fired = self.kernel.timers_fired.saturating_add(fired.len() as u64);
        let ends = fired
            .iter()
            .map(|timer| {
                codec::grouping_row(&timer.timer.key)?;
                let bytes: [u8; 8] = timer.timer.namespace.as_slice().try_into().map_err(|_| {
                    DataFusionError::Execution("invalid shared-slice timer namespace".into())
                })?;
                let end = i64::from_le_bytes(bytes);
                if timer.timer.timestamp != end.wrapping_sub(1) {
                    return Err(DataFusionError::Execution(
                        "shared-slice timer timestamp differs from its namespace".into(),
                    ));
                }
                Ok(end)
            })
            .collect::<Result<Vec<_>>>()?;
        let interval = self.kernel.plan.slide_or_step_millis;
        let shared = self.kernel.plan.partial_windows_are_slices;
        let slices = if shared {
            usize::try_from(self.kernel.plan.size_millis / interval).map_err(|_| {
                DataFusionError::Execution("shared-slice window cardinality exceeds usize".into())
            })?
        } else {
            // Flink WindowedSliceAssigner treats an attached window as an unshared slice.
            1
        };
        let requests = fired.len().checked_mul(slices).ok_or_else(|| {
            DataFusionError::ResourcesExhausted("shared-slice read cardinality overflow".into())
        })?;
        let max_key = fired
            .iter()
            .map(|timer| timer.timer.key.len())
            .max()
            .unwrap_or(0);
        let base = requests
            .min(READ_ROWS)
            .saturating_mul(
                max_key
                    .saturating_mul(8)
                    .saturating_add(1024 + self.kernel.calls.len().saturating_mul(512)),
            )
            .saturating_add(
                fired
                    .len()
                    .saturating_mul(512 + self.kernel.calls.len().saturating_mul(512)),
            );
        self.admit(base)?;
        let neutral = AccumulatorState::new(&self.kernel.calls);
        self.compute.merge(
            &self.kernel.calls,
            &vec![&neutral; fired.len()],
            &(0..fired.len()).collect::<Vec<_>>(),
            fired.len(),
        )?;
        // Bound each MultiGet and its decoded columns. No window callback performs per-row
        // RocksDB access, and very wide HOP windows do not require an unbounded read buffer.
        for offset in (0..requests).step_by(READ_ROWS) {
            let end = requests.min(offset + READ_ROWS);
            let slice_ends = (offset..end)
                .map(|index| {
                    ends[index / slices]
                        .wrapping_sub(((index % slices) as i64).wrapping_mul(interval))
                })
                .collect::<Vec<_>>();
            let rows = self
                .end_codec
                .convert_columns(&[Arc::new(Int64Array::from(slice_ends)) as ArrayRef])?;
            let mut unique =
                HashMap::<StateKey, usize, RandomState>::with_hasher(RandomState::new());
            let mut keys = Vec::new();
            let mut assignments = Vec::new();
            for (row, index) in (offset..end).enumerate() {
                let timer = &fired[index / slices];
                let key = codec::key(timer.key_group, &timer.timer.key, rows.row(row).as_ref());
                let next = keys.len();
                let slot = *unique.entry(key.clone()).or_insert_with(|| {
                    keys.push(key);
                    next
                });
                assignments.push((index / slices, slot));
            }
            let refs = keys
                .iter()
                .map(|key| StateKeyRef {
                    key_group: key.key_group,
                    key: &key.key,
                })
                .collect::<Vec<_>>();
            let values = self
                .kernel
                .state
                .get_batch(&refs, &self.kernel.scratch_reservation)?;
            self.kernel.state_read_batches = self.kernel.state_read_batches.saturating_add(1);
            let decoded_bytes = values
                .iter()
                .flatten()
                .map(|value| value.len())
                .sum::<usize>()
                .saturating_mul(8);
            let needed = base
                .saturating_add(decoded_bytes)
                .saturating_add(self.compute.size().saturating_mul(2));
            if needed > self.kernel.scratch_reservation.size() {
                self.kernel.scratch_reservation.resize(needed)?;
            }
            let states = values
                .iter()
                .map(|value| {
                    value
                        .as_ref()
                        .map(|value| codec::decode(value.as_ref(), &self.kernel.calls))
                        .transpose()
                })
                .collect::<Result<Vec<_>>>()?;
            let mut selected = Vec::new();
            let mut groups = Vec::new();
            for (group, slot) in assignments {
                if let Some(state) = &states[slot] {
                    selected.push(state);
                    groups.push(group);
                }
            }
            self.compute
                .merge(&self.kernel.calls, &selected, &groups, fired.len())?;
        }
        let merged = self.compute.finish()?;
        self.admit(base.saturating_add(merged.size().saturating_mul(8)))?;
        let expired = ends
            .iter()
            .map(|end| {
                if shared {
                    end.wrapping_sub(self.kernel.plan.size_millis)
                        .wrapping_add(interval)
                } else {
                    *end
                }
            })
            .collect::<Vec<_>>();
        let expired_rows = self
            .end_codec
            .convert_columns(&[Arc::new(Int64Array::from(expired)) as ArrayRef])?;
        let mut registrations = Vec::new();
        let mut mutations = Vec::new();
        let mut output_keys = Vec::new();
        let mut output_starts = Vec::new();
        let mut output_ends = Vec::new();
        let mut output_values = (0..self.kernel.calls.len())
            .map(|_| Vec::new())
            .collect::<Vec<_>>();
        for (index, timer) in fired.iter().enumerate() {
            let state = merged.state(&self.kernel.calls, index)?;
            if state.row_count != 0 {
                output_keys.push(codec::grouping_row(&timer.timer.key)?.to_vec());
                output_starts.push(ends[index].wrapping_sub(self.kernel.plan.size_millis));
                output_ends.push(ends[index]);
                for (column, value) in output_values
                    .iter_mut()
                    .zip(state.values(&self.kernel.calls))
                {
                    column.push(value);
                }
                if shared {
                    let next = ends[index].wrapping_add(interval);
                    registrations.push((
                        timer.key_group,
                        TimerDomain::EventTime,
                        TimerKey {
                            timestamp: next.wrapping_sub(1),
                            key: timer.timer.key.clone(),
                            namespace: next.to_le_bytes().to_vec(),
                        },
                    ));
                }
            }
            mutations.push(StateMutation {
                key: codec::key(
                    timer.key_group,
                    &timer.timer.key,
                    expired_rows.row(index).as_ref(),
                ),
                value: None,
            });
        }
        let inserted = self.kernel.timers.register_batch(registrations)?;
        self.kernel.timer_registrations = self
            .kernel
            .timer_registrations
            .saturating_add(inserted.len() as u64);
        self.kernel.state.write_batch(mutations)?;
        self.kernel.state_write_batches = self.kernel.state_write_batches.saturating_add(1);
        let output =
            self.kernel
                .output_batch(output_keys, output_values, output_starts, output_ends)?;
        let credit = self
            .kernel
            .scratch_reservation
            .split(output.get_array_memory_size(), "shared window output")?;
        let output = crate::memory_pool::arrow_lease::host_batch(output, credit)?;
        // Locals still hold admitted workspace until this function returns. The caller trims
        // it only after these owners have dropped.
        Ok(output)
    }

    fn empty_owned(&mut self) -> Result<RecordBatch> {
        self.admit(4096 + self.kernel.calls.len() * 4096)?;
        let output = self.kernel.empty_output()?;
        let credit = self
            .kernel
            .scratch_reservation
            .split(output.get_array_memory_size(), "empty shared window output")?;
        crate::memory_pool::arrow_lease::host_batch(output, credit)
    }
}
