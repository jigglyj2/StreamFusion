// Copyright 2026 StreamFusion Authors
// Licensed under the Apache License, Version 2.0.

use super::*;

mod grouped;

impl WindowAggregateProcessor {
    pub(super) fn process_partial_batch(&mut self, batch: &RecordBatch) -> Result<RecordBatch> {
        let accumulator_index =
            self.plan
                .partial_accumulator_index
                .expect("partial window indices were validated") as usize;
        let slice_end_index = self
            .plan
            .partial_slice_end_index
            .expect("partial window indices were validated") as usize;
        let window_start_index =
            self.plan
                .partial_window_start_index
                .expect("partial window indices were validated") as usize;
        let partials = batch
            .column(accumulator_index)
            .as_any()
            .downcast_ref::<BinaryArray>()
            .ok_or_else(|| {
                DataFusionError::Execution(
                    "global window partial accumulator must be Arrow Binary".to_string(),
                )
            })?;
        let slice_ends = batch
            .column(slice_end_index)
            .as_any()
            .downcast_ref::<Int64Array>()
            .ok_or_else(|| {
                DataFusionError::Execution(
                    "global window partial slice end must be Arrow Int64".to_string(),
                )
            })?;
        let window_starts = batch
            .column(window_start_index)
            .as_any()
            .downcast_ref::<Int64Array>()
            .ok_or_else(|| {
                DataFusionError::Execution(
                    "global window partial start must be Arrow Int64".to_string(),
                )
            })?;
        let grouping_rows = self.encode_grouping_rows(batch)?;
        let mut unique = HashMap::<StateKey, usize, RandomState>::with_capacity_and_hasher(
            batch.num_rows(),
            RandomState::new(),
        );
        let mut row_windows = Vec::new();
        let mut group_key = Vec::new();
        let mut assigned_windows = Vec::new();
        for row in 0..batch.num_rows() {
            if partials.is_null(row) || window_starts.is_null(row) || slice_ends.is_null(row) {
                return Err(DataFusionError::Execution(
                    "global window partial accumulator and slice end may not be null".to_string(),
                ));
            }
            self.group_key_into(batch, row, &mut group_key)?;
            let key_group = assign_key_group(&group_key, self.max_parallelism);
            if self.plan.partial_windows_are_slices {
                assign_windows_into(
                    &self.window,
                    slice_ends.value(row).wrapping_sub(1),
                    &mut assigned_windows,
                );
            } else {
                assigned_windows.clear();
                assigned_windows.push((window_starts.value(row), slice_ends.value(row)));
            }
            let first_window = row_windows.len();
            for &(start, end) in &assigned_windows {
                let deadline = self.timer_timestamp(end.saturating_sub(1))?;
                if deadline <= self.current_event_time {
                    continue;
                }
                let state_key = window_state_key(key_group, &group_key, start, end);
                let next = unique.len();
                let index = *unique.entry(state_key).or_insert(next);
                row_windows.push((row, index, start, end));
            }
            // Flink drops one input partial only after its last overlapping window fired.
            // An already-fired base slice may still contribute to later shared windows.
            if row_windows.len() == first_window && !assigned_windows.is_empty() {
                self.late_records_dropped = self.late_records_dropped.saturating_add(1);
            }
        }
        let mut ordered_keys = (0..unique.len()).map(|_| None).collect::<Vec<_>>();
        for (key, index) in unique.drain() {
            ordered_keys[index] = Some(key);
        }
        let keys = ordered_keys
            .into_iter()
            .map(|key| key.expect("every partial window state index is populated"))
            .collect::<Vec<_>>();
        let refs = keys
            .iter()
            .map(|key| StateKeyRef {
                key_group: key.key_group,
                key: &key.key,
            })
            .collect::<Vec<_>>();
        let existing = self.state.get_batch(&refs, &self.scratch_reservation)?;
        let _loaded_state_workspace =
            crate::state::reserve_decoded_values(&existing, &self.scratch_reservation)?;
        if !refs.is_empty() {
            self.state_read_batches = self.state_read_batches.saturating_add(1);
        }
        let mut staged = keys
            .into_iter()
            .zip(existing)
            .map(|(key, value)| {
                let (grouping_row, accumulator) = match value {
                    Some(value) => decode_window_state(value.as_ref(), &self.calls)?,
                    None => (Vec::new(), AccumulatorState::new(&self.calls)),
                };
                Ok(StagedWindow {
                    key,
                    grouping_row,
                    accumulator,
                    touched: false,
                })
            })
            .collect::<Result<Vec<_>>>()?;
        // Decode each accepted input once, even when it contributes to several windows.
        let mut decoded = (0..batch.num_rows()).map(|_| None).collect::<Vec<_>>();
        for &(row, _, _, _) in &row_windows {
            if decoded[row].is_none() {
                decoded[row] = Some(decode_state(partials.value(row), &self.calls)?);
            }
        }
        let merged = grouped::merge_append_partials(&self.calls, &staged, &decoded, &row_windows)?;
        let mut dirty_timer_groups = BTreeSet::new();
        for (row, index, start, end) in row_windows {
            let partial = decoded[row].as_ref().unwrap();
            let entry = &mut staged[index];
            let was_empty = entry.accumulator.row_count == 0;
            if was_empty && partial.row_count <= 0 {
                continue;
            }
            if was_empty {
                entry.grouping_row = grouping_rows[row].clone();
            }
            if merged.is_some() {
                // Flink timer transitions use input order; aggregate computation is vectorized.
                entry.accumulator.row_count += partial.row_count;
            } else {
                entry.accumulator.merge(&self.calls, partial)?;
            }
            let timer = TimerKey {
                timestamp: self.timer_timestamp(end.saturating_sub(1))?,
                key: entry.key.key.clone(),
                namespace: window_namespace(start, end),
            };
            let domain = self.timer_domain();
            if was_empty && entry.accumulator.row_count != 0 {
                if self.timers.register(entry.key.key_group, domain, timer)? {
                    self.timer_registrations = self.timer_registrations.saturating_add(1);
                    dirty_timer_groups.insert(entry.key.key_group);
                }
            } else if entry.accumulator.row_count == 0
                && self.timers.delete(entry.key.key_group, domain, &timer)?
            {
                self.timer_deletions = self.timer_deletions.saturating_add(1);
                dirty_timer_groups.insert(entry.key.key_group);
            }
            entry.touched = true;
        }
        if let Some(merged) = merged {
            for (index, entry) in staged.iter_mut().enumerate() {
                entry.accumulator = merged.state(&self.calls, index)?;
            }
        }
        let mut mutations = staged
            .into_iter()
            .filter(|entry| entry.touched)
            .map(|entry| StateMutation {
                key: entry.key,
                value: (entry.accumulator.row_count != 0)
                    .then(|| encode_window_state(&entry.grouping_row, &entry.accumulator)),
            })
            .collect::<Vec<_>>();
        self.append_timer_mutations(&mut mutations, dirty_timer_groups)?;
        if !mutations.is_empty() {
            self.state.write_batch(mutations)?;
            self.state_write_batches = self.state_write_batches.saturating_add(1);
        }
        self.empty_output()
    }
}

#[cfg(test)]
mod tests;
