// Copyright 2026 StreamFusion Authors
// Licensed under the Apache License, Version 2.0.

use super::*;

pub(in crate::planner::operators::group_aggregate) struct MembershipBatch {
    members: HashMap<StateKey, Member, RandomState>,
    cleanup: HashMap<StateKey, (), RandomState>,
    reset: Vec<bool>,
    workspace: HostMemoryReservation,
    mutation_bytes: usize,
    pub(in crate::planner::operators::group_aggregate) read_batches: u64,
}

impl MembershipLayout {
    pub(in crate::planner::operators::group_aggregate) fn load(
        &self,
        calls: &[Call],
        state: &dyn KeyedState,
        batch: &RecordBatch,
        keys: &[StateKey],
        row_groups: &[usize],
        accumulates: &[bool],
        staged: &mut [Option<AccumulatorState>],
        external: &[bool],
        modes: &[Mode],
        owner: &HostMemoryReservation,
    ) -> Result<MembershipBatch> {
        let mut workspace = owner.sibling("DISTINCT batch membership keys, counts and mutations");
        workspace.resize(self.batch_admission(batch, keys, staged, external))?;
        let prefixes = keys.iter().map(prefix).collect::<Result<Vec<_>>>()?;
        let mut members =
            self.collect_members(calls, batch, keys, row_groups, staged, external, &prefixes)?;
        let refs = members
            .iter()
            .filter(|(_, member)| external[member.group])
            .map(|(key, _)| StateKeyRef {
                key_group: key.key_group,
                key: &key.key,
            })
            .collect::<Vec<_>>();
        let mut read_batches = 0;
        if !refs.is_empty() {
            let stored = state.get_batch(&refs, &workspace)?;
            for ((_, member), stored) in members
                .iter()
                .filter(|(_, member)| external[member.group])
                .zip(stored.iter())
            {
                let column = &self.columns[member.column];
                if let Some(stored) = stored {
                    let counts = codec::decode_members(
                        stored.as_ref(),
                        column.calls.len(),
                        modes[member.group],
                    )?;
                    for (&call, count) in column.calls.iter().zip(counts) {
                        let Accumulator::DistinctCount { values, .. } =
                            &mut staged[member.group].as_mut().unwrap().accumulators[call]
                        else {
                            unreachable!()
                        };
                        if count != 0 {
                            values.insert(member.value.clone(), count);
                        }
                    }
                }
            }
            read_batches += 1;
        }
        drop(refs);
        for member in members.values_mut().filter(|member| external[member.group]) {
            member.original = counts(
                staged[member.group].as_ref(),
                &self.columns[member.column],
                &member.value,
            );
        }
        // Predict group cleanup from row counts before running any aggregate kernel. This
        // loads deletion keys up front, even if deletion/recreation occurs mid-batch.
        let mut row_counts = staged
            .iter()
            .map(|state| state.as_ref().map_or(0, |state| state.row_count))
            .collect::<Vec<_>>();
        let mut reset = vec![false; keys.len()];
        for (&group, &accumulate) in row_groups.iter().zip(accumulates) {
            if row_counts[group] == 0 && !accumulate {
                continue;
            }
            row_counts[group] = row_counts[group].wrapping_add(if accumulate { 1 } else { -1 });
            reset[group] |= row_counts[group] == 0;
        }
        let mut cleanup = HashMap::with_hasher(RandomState::new());
        for group in (0..keys.len()).filter(|&group| external[group] && reset[group]) {
            let mut page_credit = owner.sibling("DISTINCT group cleanup scan page");
            page_credit.resize(sortable_state::PAGE_BYTES + sortable_state::PAGE_ROWS * 64)?;
            state.visit_prefix(
                keys[group].key_group,
                &prefixes[group],
                sortable_state::PAGE_ROWS,
                sortable_state::PAGE_BYTES,
                &mut |page| {
                    workspace.try_grow(
                        page.iter()
                            .map(|(key, _)| key.len().saturating_add(256))
                            .sum(),
                    )?;
                    for (key, _) in page {
                        cleanup.insert(
                            StateKey {
                                key_group: keys[group].key_group,
                                key: key.to_vec(),
                            },
                            (),
                        );
                    }
                    Ok(())
                },
            )?;
            read_batches += 1;
        }
        Ok(MembershipBatch {
            members,
            cleanup,
            reset,
            workspace: workspace,
            mutation_bytes: 0,
            read_batches,
        })
    }
}

impl MembershipBatch {
    pub(in crate::planner::operators::group_aggregate) fn mutations(
        &mut self,
        layout: &MembershipLayout,
        staged: &[Option<AccumulatorState>],
        touched: &[bool],
        modes: &[Mode],
    ) -> Vec<StateMutation> {
        let mut mutations = Vec::with_capacity(self.members.len() + self.cleanup.len());
        for (key, member) in self.members.drain() {
            if !touched[member.group] {
                continue;
            }
            let mut current = counts(
                staged[member.group].as_ref(),
                &layout.columns[member.column],
                &member.value,
            );
            if modes[member.group] == Mode::Presence {
                // Only INSERTs reach this mode. Within-batch duplicate counts are temporary;
                // the persisted fact is presence, exactly as in Flink's append-only data view.
                for count in &mut current {
                    *count = i64::from(*count != 0);
                }
            }
            // Coalesce cleanup and later reinsertion into one mutation per key. Never discard
            // a negative count merely because DataFusion's visible distinct count is zero.
            let removed = self.cleanup.remove(&key).is_some();
            if removed || self.reset[member.group] || current != member.original {
                mutations.push(StateMutation {
                    key,
                    value: current.iter().any(|&count| count != 0).then(|| {
                        if modes[member.group] == Mode::Presence {
                            codec::encode_presence(&current)
                        } else {
                            codec::encode_counts(&current)
                        }
                    }),
                });
            }
        }
        mutations.extend(
            self.cleanup
                .drain()
                .map(|(key, ())| StateMutation { key, value: None }),
        );
        // The caller appends these entries to the header-mutation vector. Two descriptors
        // per entry cover that vector's geometric capacity; key/value buffers move intact.
        self.mutation_bytes = mutations.iter().fold(
            mutations
                .len()
                .saturating_mul(2 * std::mem::size_of::<StateMutation>()),
            |bytes, mutation| {
                bytes
                    .saturating_add(mutation.key.key.capacity())
                    .saturating_add(mutation.value.as_ref().map_or(0, Vec::capacity))
            },
        );
        mutations
    }

    /// Call only after the staged accumulator maps have been consumed into dirty state.
    /// Drop the input-sized lookup tables before admitting Arrow output. Only the moved
    /// mutation buffers remain live under this batch's ownership until the backend flush.
    pub(in crate::planner::operators::group_aggregate) fn finish_computation(
        &mut self,
    ) -> Result<()> {
        if !self.members.is_empty() || !self.cleanup.is_empty() {
            return Err(DataFusionError::Internal(
                "DISTINCT computation must drain mutations before retiring workspace".into(),
            ));
        }
        if self.mutation_bytes > self.workspace.size() {
            return Err(DataFusionError::Internal(
                "DISTINCT mutations exceeded admitted batch workspace".into(),
            ));
        }
        self.members.shrink_to_fit();
        self.cleanup.shrink_to_fit();
        self.reset = Vec::new();
        self.workspace.resize(self.mutation_bytes)
    }
}
