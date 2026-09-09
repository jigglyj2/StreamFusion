// Copyright 2026 StreamFusion Authors
// Licensed under the Apache License, Version 2.0.

use super::*;
use assignments::{Assignments, Interval};
use std::collections::{BTreeMap, BTreeSet};

struct Existing {
    key: StateKey,
    slot: usize,
    end: i64,
    state: AccumulatorState,
}
struct Partition {
    group: u32,
    prefix: Vec<u8>,
    minimum: i64,
    maximum_end: i64,
    assignments: Assignments,
    existing: Vec<Existing>,
}

impl SharedSessions {
    pub(super) fn process_inner(&mut self, batch: &RecordBatch) -> Result<()> {
        self.kernel.prepare_schema(batch.schema())?;
        if let Some(index) = self.kernel.input_kind_index {
            let kinds = batch
                .column(index)
                .as_any()
                .downcast_ref::<Int8Array>()
                .ok_or_else(|| {
                    DataFusionError::Execution("shared session RowKind must be Int8".into())
                })?;
            if kinds.null_count() != 0 || kinds.values().iter().any(|&kind| kind != INSERT) {
                return Err(DataFusionError::Execution(
                    "shared sessions require INSERT-only input".into(),
                ));
            }
        }
        let times = batch
            .column(self.kernel.plan.time_attribute_index as usize)
            .as_any()
            .downcast_ref::<TimestampMillisecondArray>()
            .ok_or_else(|| {
                DataFusionError::Execution("shared session time must be TIMESTAMP(3)".into())
            })?;
        if times.null_count() != 0 {
            return Err(DataFusionError::Execution(
                "RowTime field should not be null".into(),
            ));
        }
        let gap = self.kernel.plan.size_millis;
        if times
            .values()
            .iter()
            .any(|&time| time.checked_add(gap).is_none())
        {
            return Err(DataFusionError::Execution(
                "session window end overflows TIMESTAMP(3)".into(),
            ));
        }
        if batch.num_rows() == 0 {
            return Ok(());
        }
        let mut used = self
            .kernel
            .plan
            .grouping_indices
            .iter()
            .map(|&i| i as usize)
            .collect::<BTreeSet<_>>();
        for call in &self.kernel.calls {
            used.extend(call.input_index);
            used.extend(call.filter_index);
        }
        let payload = used.into_iter().try_fold(0usize, |sum, index| {
            Ok::<_, DataFusionError>(
                sum.saturating_add(batch.column(index).to_data().get_slice_memory_size()?),
            )
        })?;
        let base = payload
            .saturating_mul(8)
            .saturating_add(64 * 1024)
            .saturating_add(
                batch
                    .num_rows()
                    .saturating_mul(1024 + self.kernel.calls.len() * 512),
            );
        self.admit(base.saturating_add(sortable_state::PAGE_BYTES))?;
        let columns = self
            .kernel
            .plan
            .grouping_indices
            .iter()
            .map(|&i| batch.column(i as usize).clone())
            .collect::<Vec<_>>();
        let grouping = if columns.is_empty() {
            None
        } else {
            Some(
                self.kernel
                    .grouping_converter
                    .as_ref()
                    .unwrap()
                    .convert_columns(&columns)?,
            )
        };
        let mut partitions = Vec::<Partition>::new();
        let mut unique = hashbrown::HashTable::<usize>::new();
        let hasher = RandomState::new();
        let mut row_partitions = Vec::with_capacity(batch.num_rows());
        let mut flink_key = Vec::new();
        for row in 0..batch.num_rows() {
            let group_row = grouping.as_ref().map(|rows| rows.row(row));
            let bytes = group_row.as_ref().map_or(&[][..], |row| row.as_ref());
            let hash = hasher.hash_one(bytes);
            let index = match unique
                .find(hash, |&i| &partitions[i].prefix[11..] == bytes)
                .copied()
            {
                Some(index) => index,
                None => {
                    self.kernel.group_key_into(batch, row, &mut flink_key)?;
                    let index = partitions.len();
                    partitions.push(Partition {
                        group: assign_key_group(&flink_key, self.kernel.max_parallelism),
                        prefix: codec::prefix(bytes)?,
                        minimum: times.value(row),
                        maximum_end: times.value(row) + gap,
                        assignments: Assignments::default(),
                        existing: Vec::new(),
                    });
                    unique.insert_unique(hash, index, |&i| {
                        hasher.hash_one(&partitions[i].prefix[11..])
                    });
                    index
                }
            };
            partitions[index].minimum = partitions[index].minimum.min(times.value(row));
            partitions[index].maximum_end =
                partitions[index].maximum_end.max(times.value(row) + gap);
            row_partitions.push(index);
        }
        // One bounded range per distinct touched partition, all before computation. The end index
        // skips earlier sessions; disjoint ordering lets the scan stop past the batch's last start.
        let lower_rows =
            self.end_codec
                .convert_columns(&[Arc::new(Int64Array::from_iter_values(
                    partitions.iter().map(|partition| partition.minimum),
                )) as ArrayRef])?;
        let mut retained = base;
        for (index, partition) in partitions.iter_mut().enumerate() {
            let start = codec::key(
                partition.group,
                &partition.prefix,
                lower_rows.row(index).as_ref(),
            );
            let end = sortable_state::prefix_end(&partition.prefix);
            let scratch = &mut self.kernel.scratch_reservation;
            let calls = &self.kernel.calls;
            self.kernel.state.visit_range(
                partition.group,
                &start.key,
                end.as_deref(),
                sortable_state::PAGE_ROWS,
                sortable_state::PAGE_BYTES,
                &mut |entries| {
                    let bytes = entries
                        .iter()
                        .map(|(key, value)| key.len().saturating_add(value.len()))
                        .sum::<usize>();
                    retained = retained
                        .saturating_add(bytes.saturating_mul(8))
                        .saturating_add(entries.len() * (512 + calls.len() * 512));
                    scratch.resize(retained.saturating_add(sortable_state::PAGE_BYTES))?;
                    for &(key, value) in entries {
                        let (start, end) = codec::bounds(value)?;
                        if start > partition.maximum_end {
                            return Ok(false);
                        }
                        let (_, _, state) = codec::decode(value, calls)?;
                        let slot = partition.assignments.existing(start, end)?;
                        partition.existing.push(Existing {
                            key: StateKey {
                                key_group: partition.group,
                                key: key.to_vec(),
                            },
                            slot,
                            end,
                            state,
                        });
                    }
                    Ok(true)
                },
            )?;
            self.kernel.state_read_batches = self.kernel.state_read_batches.saturating_add(1);
        }
        let mut rows = Vec::with_capacity(batch.num_rows());
        let mut dropped = 0u64;
        for (row, &partition) in row_partitions.iter().enumerate() {
            let slot = partitions[partition].assignments.assign(
                times.value(row),
                gap,
                self.kernel.current_event_time,
            )?;
            if slot.is_none() {
                dropped += 1;
            }
            rows.push(slot);
        }
        // Resolve all contributing arrivals to final namespaces after preserving lateness order.
        // DataFusion consumes the original Arrow columns in bounded slices, once per batch.
        let mut dirty = BTreeMap::new();
        for (row, slot) in rows.iter_mut().enumerate() {
            if let Some(slot) = slot {
                *slot = partitions[row_partitions[row]].assignments.resolve(*slot);
                let next = dirty.len();
                dirty.entry((row_partitions[row], *slot)).or_insert(next);
            }
        }
        self.kernel.late_records_dropped = self.kernel.late_records_dropped.saturating_add(dropped);
        if dirty.is_empty() {
            return Ok(());
        }
        let groups = rows
            .iter()
            .enumerate()
            .map(|(row, slot)| slot.map_or(0, |slot| dirty[&(row_partitions[row], slot)]))
            .collect::<Vec<_>>();
        let valid = BooleanArray::from(rows.iter().map(Option::is_some).collect::<Vec<_>>());
        for offset in (0..batch.num_rows()).step_by(COMPUTE_ROWS) {
            let len = COMPUTE_ROWS.min(batch.num_rows() - offset);
            self.compute.update(
                &self.kernel.calls,
                &batch.slice(offset, len),
                &groups[offset..offset + len],
                Some(&valid.slice(offset, len)),
                dirty.len(),
            )?;
        }
        let partial = self.compute.finish()?;
        let partial_states = (0..dirty.len())
            .map(|i| partial.state(&self.kernel.calls, i))
            .collect::<Result<Vec<_>>>()?;
        for (chunk, states) in partial_states.chunks(COMPUTE_ROWS).enumerate() {
            self.merge.merge(
                &self.kernel.calls,
                &states.iter().collect::<Vec<_>>(),
                &(chunk * COMPUTE_ROWS..chunk * COMPUTE_ROWS + states.len()).collect::<Vec<_>>(),
                dirty.len(),
            )?;
        }
        let mut old = Vec::new();
        let mut old_groups = Vec::new();
        let mut old_keys = Vec::new();
        let mut output = Vec::<(usize, usize, Interval)>::new();
        for partition in &mut partitions {
            for existing in &mut partition.existing {
                existing.slot = partition.assignments.resolve(existing.slot);
            }
        }
        for (index, partition) in partitions.iter().enumerate() {
            for existing in &partition.existing {
                let root = existing.slot;
                if let Some(&group) = dirty.get(&(index, root)) {
                    old.push(&existing.state);
                    old_groups.push(group);
                    old_keys.push((&existing.key, existing.end));
                }
            }
            for interval in partition.assignments.intervals() {
                if let Some(&group) = dirty.get(&(index, interval.slot)) {
                    output.push((index, group, interval));
                }
            }
        }
        for (states, groups) in old
            .chunks(COMPUTE_ROWS)
            .zip(old_groups.chunks(COMPUTE_ROWS))
        {
            self.merge
                .merge(&self.kernel.calls, states, groups, dirty.len())?;
        }
        let computed = self.merge.finish()?;
        let ends = self
            .end_codec
            .convert_columns(&[Arc::new(Int64Array::from_iter_values(
                output.iter().map(|(_, _, interval)| interval.end),
            )) as ArrayRef])?;
        let mut mutations = Vec::new();
        let mut registrations = Vec::new();
        let mut final_keys = BTreeSet::new();
        for (row, &(partition, group, interval)) in output.iter().enumerate() {
            let partition = &partitions[partition];
            let key = codec::key(partition.group, &partition.prefix, ends.row(row).as_ref());
            registrations.push((
                partition.group,
                TimerDomain::EventTime,
                TimerKey {
                    timestamp: interval.end - 1,
                    key: key.key.clone(),
                    namespace: interval.end.to_le_bytes().to_vec(),
                },
            ));
            final_keys.insert((key.key_group, key.key.clone()));
            mutations.push(StateMutation {
                key,
                value: Some(codec::encode(
                    interval.start,
                    interval.end,
                    &computed.state(&self.kernel.calls, group)?,
                )),
            });
        }
        for (key, end) in old_keys {
            if !final_keys.contains(&(key.key_group, key.key.clone())) {
                if self.kernel.timers.delete(
                    key.key_group,
                    TimerDomain::EventTime,
                    &TimerKey {
                        timestamp: end - 1,
                        key: key.key.clone(),
                        namespace: end.to_le_bytes().to_vec(),
                    },
                )? {
                    self.kernel.timer_deletions += 1;
                }
                mutations.push(StateMutation {
                    key: key.clone(),
                    value: None,
                });
            }
        }
        let added = self.kernel.timers.register_batch(registrations)?;
        self.kernel.timer_registrations += added.len() as u64;
        self.kernel.state.write_batch(mutations)?;
        self.kernel.state_write_batches += 1;
        Ok(())
    }
}
