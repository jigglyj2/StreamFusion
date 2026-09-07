// Copyright 2026 StreamFusion Authors
// Licensed under the Apache License, Version 2.0

//! Raw and partial mini-batches preserve every Flink bundle boundary, but backend I/O belongs to the
//! incoming Arrow batch. A decoded working set carries the latest flushed state between
//! bundles. Unflushed deltas stay in `pending`; only flushed states are committed.

use super::*;
use hashbrown::HashSet;

impl GroupAggregateProcessor {
    pub(super) fn process_raw_mini_batch(
        &mut self,
        batch: RecordBatch,
        base: usize,
    ) -> Result<RecordBatch> {
        self.process_mini_input(batch, base, None)
    }

    pub(super) fn process_mini_input(
        &mut self,
        batch: RecordBatch,
        base: usize,
        partials: Option<&[AccumulatorState]>,
    ) -> Result<RecordBatch> {
        self.prepare_schema(batch.schema(), batch.num_columns())?;
        let input_credit = self.input_admission(&batch)?;
        let pending_credit = self.estimated_pending_bytes();
        self.scratch_reservation.resize(
            base.saturating_add(input_credit.saturating_mul(2))
                .saturating_add(pending_credit.saturating_mul(4)),
        )?;

        let grouping_rows = self.encode_grouping_rows(&batch)?;
        let mut unique = HashMap::<StateKey, usize, RandomState>::with_capacity_and_hasher(
            batch.num_rows(),
            RandomState::new(),
        );
        let mut keys = Vec::new();
        let mut row_keys = Vec::with_capacity(batch.num_rows());
        for row in 0..batch.num_rows() {
            let key = self.state_key(&batch, row)?;
            let next = keys.len();
            let index = *unique.entry(key.clone()).or_insert_with(|| {
                keys.push(key);
                next
            });
            row_keys.push(index);
        }
        drop(unique);
        let accumulator_credit = self.accumulator_admission(
            keys.len().saturating_add(self.pending.len()),
            batch.num_rows(),
        )?;
        self.scratch_reservation
            .try_grow(accumulator_credit.saturating_mul(3))?;
        let missing = keys
            .iter()
            .filter(|key| !self.pending.contains_key(*key))
            .collect::<Vec<_>>();
        let refs = missing
            .iter()
            .map(|key| StateKeyRef {
                key_group: key.key_group,
                key: &key.key,
            })
            .collect::<Vec<_>>();
        let existing = if refs.is_empty() {
            crate::state::StateReadBatch::empty(&self.scratch_reservation)
        } else {
            let existing = self.state.get_batch(&refs, &self.scratch_reservation)?;
            self.state_read_batches = self.state_read_batches.saturating_add(1);
            existing
        };
        // Keep the read/decode allowance alive through decoding, cloning, and final output.
        // Sparse node/vector credit above supplements the packed historical byte allowance.
        let _loaded = crate::state::reserve_decoded_values(&existing, &self.scratch_reservation)?;
        let historical = existing
            .iter()
            .flatten()
            .fold(0usize, |n, value| n.saturating_add(value.len()));
        self.scratch_reservation
            .try_grow(historical.saturating_mul(24))?;
        let mut working =
            HashMap::<StateKey, Option<AccumulatorState>, RandomState>::with_capacity_and_hasher(
                keys.len().saturating_add(self.pending.len()),
                RandomState::new(),
            );
        for (key, value) in missing.into_iter().zip(existing) {
            working.insert(
                key.clone(),
                value
                    .as_deref()
                    .map(|bytes| decode_state(bytes, &self.calls))
                    .transpose()?,
            );
        }
        let states = keys.iter().map(|key| {
            self.pending
                .get(key)
                .and_then(|group| group.current.as_ref())
                .or_else(|| working.get(key).and_then(Option::as_ref))
        });
        let event_credit = match partials {
            Some(partials) => self.partial_event_admission(states, &row_keys, partials)?,
            None => self.event_admission_iter(&batch, states, &row_keys)?,
        };
        let rows = batch
            .num_rows()
            .saturating_add(self.pending.len())
            .saturating_mul(2);
        let columns = self
            .plan
            .grouping_indices
            .len()
            .saturating_add(self.calls.len())
            .saturating_add(3);
        self.scratch_reservation.try_grow(
            event_credit
                .saturating_mul(3)
                .saturating_add(rows.saturating_mul(columns).saturating_mul(64))
                .saturating_add(columns.saturating_mul(4096)),
        )?;

        let trigger = usize::try_from(self.plan.mini_batch_size)
            .map_err(|_| DataFusionError::Plan("mini-batch trigger exceeds usize".into()))?;
        if trigger == 0 {
            return Err(DataFusionError::Plan(
                "aggregate mini-batch trigger must be positive".into(),
            ));
        }
        let mut dirty = HashSet::<StateKey, RandomState>::with_capacity_and_hasher(
            keys.len(),
            RandomState::new(),
        );
        let mut events = BundleOutputEvents::new(self.calls.len());
        for (row, index) in row_keys.into_iter().enumerate() {
            let key = &keys[index];
            if !self.pending.contains_key(key) {
                let state = working
                    .get(key)
                    .expect("every incoming key was prefetched or flushed");
                self.pending_order.push(key.clone());
                self.pending.insert(
                    key.clone(),
                    PendingGroup {
                        grouping_row: grouping_rows[row].clone(),
                        original: state.clone(),
                        current: state.clone(),
                    },
                );
            }
            let accumulate = if partials.is_some() {
                true
            } else {
                self.accumulates(&batch, row)?
            };
            let group = self.pending.get_mut(key).expect("staged group");
            if let Some(partials) = partials {
                group
                    .current
                    .get_or_insert_with(|| AccumulatorState::new(&self.calls))
                    .merge(&self.calls, &partials[row])?;
            } else if self.calls.is_empty() {
                match apply_count_change(
                    group.current.as_ref().map(|state| state.row_count),
                    accumulate,
                ) {
                    CountChange::Ignored => {}
                    CountChange::Present(count, _) => {
                        group.current = Some(AccumulatorState {
                            row_count: count,
                            accumulators: Vec::new(),
                        })
                    }
                    CountChange::Removed => {
                        group.current = Some(AccumulatorState {
                            row_count: 0,
                            accumulators: Vec::new(),
                        })
                    }
                }
            } else if accumulate
                || group
                    .current
                    .as_ref()
                    .is_some_and(|state| state.row_count != 0)
            {
                group
                    .current
                    .get_or_insert_with(|| AccumulatorState::new(&self.calls))
                    .apply(&self.calls, &batch, row, accumulate)?;
            }
            self.pending_elements += 1;
            if self.pending_elements == trigger {
                self.flush_to_working(&mut working, &mut dirty, &mut events);
            }
        }
        // Pending owns its retained credit before temporary historical/cache credit is released.
        self.bundle_reservation
            .resize(self.estimated_pending_bytes())?;
        let mutations = dirty
            .into_iter()
            .map(|key| {
                let value = working
                    .get(&key)
                    .expect("dirty state was flushed")
                    .as_ref()
                    .map(encode_state);
                StateMutation { key, value }
            })
            .collect::<Vec<_>>();
        let output = self.bundle_output_batch(events)?;
        if !mutations.is_empty() {
            self.state.write_batch(mutations)?;
            self.state_write_batches = self.state_write_batches.saturating_add(1);
        }
        Ok(output)
    }

    fn flush_to_working(
        &mut self,
        working: &mut HashMap<StateKey, Option<AccumulatorState>, RandomState>,
        dirty: &mut HashSet<StateKey, RandomState>,
        events: &mut BundleOutputEvents,
    ) {
        let mut order = std::mem::take(&mut self.pending_order);
        sort_flink_hashmap_keys(&mut order, |key| &key.key);
        for key in order {
            let group = self
                .pending
                .remove(&key)
                .expect("pending order matches map");
            let first = group
                .original
                .as_ref()
                .is_none_or(|state| state.row_count == 0);
            let Some(current) = group.current else {
                working.entry(key).or_insert(None);
                continue;
            };
            let previous = group
                .original
                .as_ref()
                .map(|state| state.values(&self.calls))
                .unwrap_or_else(|| vec![None; self.calls.len()]);
            if current.row_count == 0 {
                if !first {
                    events.push(group.grouping_row, DELETE, previous);
                    dirty.insert(key.clone());
                }
                working.insert(key, None);
            } else {
                let values = current.values(&self.calls);
                if first {
                    events.push(group.grouping_row, INSERT, values);
                } else if previous != values {
                    if self.plan.generate_update_before {
                        events.push(group.grouping_row.clone(), UPDATE_BEFORE, previous);
                    }
                    events.push(group.grouping_row, UPDATE_AFTER, values);
                }
                dirty.insert(key.clone());
                working.insert(key, Some(current));
            }
        }
        self.pending_elements = 0;
    }
}
