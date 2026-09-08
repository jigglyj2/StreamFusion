// Copyright 2026 StreamFusion Authors
// Licensed under the Apache License, Version 2.0.

use super::*;

impl SharedSlices {
    pub(super) fn process_inner(&mut self, batch: &RecordBatch) -> Result<()> {
        self.kernel.prepare_schema(batch.schema())?;
        if let Some(index) = self.kernel.input_kind_index {
            let kinds = batch
                .column(index)
                .as_any()
                .downcast_ref::<Int8Array>()
                .ok_or_else(|| {
                    DataFusionError::Execution("shared slice RowKind must be Int8".into())
                })?;
            if kinds.null_count() != 0 || kinds.values().iter().any(|&kind| kind != INSERT) {
                return Err(DataFusionError::Execution(
                    "shared slice partial input must be INSERT-only".into(),
                ));
            }
        }
        let plan = &self.kernel.plan;
        let partials = batch
            .column(plan.partial_accumulator_index.unwrap() as usize)
            .as_any()
            .downcast_ref::<BinaryArray>()
            .ok_or_else(|| DataFusionError::Execution("slice partial must be Binary".into()))?;
        let ends = batch
            .column(plan.partial_slice_end_index.unwrap() as usize)
            .as_any()
            .downcast_ref::<Int64Array>()
            .ok_or_else(|| DataFusionError::Execution("slice end must be BIGINT".into()))?;
        let starts = batch
            .column(plan.partial_window_start_index.unwrap() as usize)
            .as_any()
            .downcast_ref::<Int64Array>()
            .ok_or_else(|| DataFusionError::Execution("slice start must be BIGINT".into()))?;
        if partials.null_count() != 0 || ends.null_count() != 0 || starts.null_count() != 0 {
            return Err(DataFusionError::Execution(
                "shared slice partial and bounds must not be null".into(),
            ));
        }
        // The producer already owns the Arrow buffers. A slice can retain a much larger
        // parent allocation; only its logical partial bytes are decoded here. Reserve
        // that decoding plus per-row keys, indexes, mutations and DataFusion workspace.
        let offsets = partials.value_offsets();
        let decoded_input = (offsets[offsets.len() - 1] - offsets[0]) as usize;
        let base = decoded_input
            .saturating_mul(8)
            .saturating_add(64 * 1024)
            .saturating_add(
                batch
                    .num_rows()
                    .saturating_mul(512 + self.kernel.calls.len().saturating_mul(1024)),
            );
        self.admit(base)?;
        let plan = &self.kernel.plan;
        let end_rows = self.end_codec.convert_columns(&[batch
            .column(plan.partial_slice_end_index.unwrap() as usize)
            .clone()])?;
        let grouping_rows = self.kernel.encode_grouping_rows(batch)?;
        let mut unique = HashMap::<StateKey, usize, RandomState>::with_hasher(RandomState::new());
        let mut keys = Vec::new();
        let mut groups = Vec::new();
        let mut decoded = Vec::new();
        let mut timers = Vec::new();
        let mut partition = Vec::new();
        for row in 0..batch.num_rows() {
            let end = ends.value(row);
            let width = if plan.partial_windows_are_slices {
                plan.slide_or_step_millis
            } else {
                plan.size_millis
            };
            if starts.value(row) != end.wrapping_sub(width) {
                return Err(DataFusionError::Execution(
                    "HOP partial bounds must match the planned base slice or attached window"
                        .into(),
                ));
            }
            if self.last_window_end(end).wrapping_sub(1) <= self.kernel.current_event_time {
                self.kernel.late_records_dropped = self.kernel.late_records_dropped.wrapping_add(1);
                continue;
            }
            self.kernel.group_key_into(batch, row, &mut partition)?;
            let key_group = assign_key_group(&partition, self.kernel.max_parallelism);
            let prefix = codec::prefix(&grouping_rows[row])?;
            let key = codec::key(key_group, &prefix, end_rows.row(row).as_ref());
            let next = keys.len();
            let group = *unique.entry(key.clone()).or_insert_with(|| {
                keys.push(key);
                let first = self.first_unfired(end);
                timers.push((
                    key_group,
                    TimerDomain::EventTime,
                    TimerKey {
                        timestamp: first.wrapping_sub(1),
                        key: prefix,
                        namespace: first.to_le_bytes().to_vec(),
                    },
                ));
                next
            });
            groups.push(group);
            decoded.push(decode_state(partials.value(row), &self.kernel.calls)?);
        }
        if keys.is_empty() {
            return Ok(());
        }
        let refs = keys
            .iter()
            .map(|key| StateKeyRef {
                key_group: key.key_group,
                key: &key.key,
            })
            .collect::<Vec<_>>();
        let existing = self
            .kernel
            .state
            .get_batch(&refs, &self.kernel.scratch_reservation)?;
        let decoded_bytes = existing
            .iter()
            .flatten()
            .map(|value| value.len())
            .sum::<usize>()
            .saturating_mul(8);
        let needed = base
            .saturating_add(decoded_bytes)
            .saturating_add(self.compute.size());
        if needed > self.kernel.scratch_reservation.size() {
            self.kernel.scratch_reservation.resize(needed)?;
        }
        self.kernel.state_read_batches = self.kernel.state_read_batches.saturating_add(1);
        let states = existing
            .iter()
            .map(|value| match value {
                Some(value) => codec::decode(value.as_ref(), &self.kernel.calls),
                None => Ok(AccumulatorState::new(&self.kernel.calls)),
            })
            .collect::<Result<Vec<_>>>()?;
        for (chunk, states) in states.chunks(READ_ROWS).enumerate() {
            let indices = (chunk * READ_ROWS..chunk * READ_ROWS + states.len()).collect::<Vec<_>>();
            self.compute.merge(
                &self.kernel.calls,
                &states.iter().collect::<Vec<_>>(),
                &indices,
                keys.len(),
            )?;
        }
        for (states, groups) in decoded.chunks(READ_ROWS).zip(groups.chunks(READ_ROWS)) {
            self.compute.merge(
                &self.kernel.calls,
                &states.iter().collect::<Vec<_>>(),
                groups,
                keys.len(),
            )?;
        }
        let merged = self.compute.finish()?;
        drop(existing);
        let mutations = keys
            .into_iter()
            .enumerate()
            .map(|(index, key)| {
                Ok(StateMutation {
                    key,
                    value: Some(codec::encode(&merged.state(&self.kernel.calls, index)?)),
                })
            })
            .collect::<Result<Vec<_>>>()?;
        let inserted = self.kernel.timers.register_batch(timers)?;
        self.kernel.timer_registrations = self
            .kernel
            .timer_registrations
            .saturating_add(inserted.len() as u64);
        self.kernel.state.write_batch(mutations)?;
        self.kernel.state_write_batches = self.kernel.state_write_batches.saturating_add(1);
        Ok(())
    }
}
