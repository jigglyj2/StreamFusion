// Copyright 2026 StreamFusion Authors
// Licensed under the Apache License, Version 2.0.

use super::*;

pub(in crate::planner::operators::group_aggregate) struct MembershipBatch {
    members: HashMap<StateKey, Member, RandomState>,
    cleanup: HashMap<StateKey, (), RandomState>,
    reset: Vec<bool>,
    _workspace: HostMemoryReservation,
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
                    let counts = codec::decode_counts(stored.as_ref(), column.calls.len())?;
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
            _workspace: workspace,
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
    ) -> Vec<StateMutation> {
        let mut mutations = Vec::with_capacity(self.members.len() + self.cleanup.len());
        for (key, member) in self.members.drain() {
            if !touched[member.group] {
                continue;
            }
            let current = counts(
                staged[member.group].as_ref(),
                &layout.columns[member.column],
                &member.value,
            );
            // Coalesce cleanup and later reinsertion into one mutation per key. Never discard
            // a negative count merely because DataFusion's visible distinct count is zero.
            let removed = self.cleanup.remove(&key).is_some();
            if removed || self.reset[member.group] || current != member.original {
                mutations.push(StateMutation {
                    key,
                    value: current
                        .iter()
                        .any(|&count| count != 0)
                        .then(|| codec::encode_counts(&current)),
                });
            }
        }
        mutations.extend(
            self.cleanup
                .drain()
                .map(|(key, ())| StateMutation { key, value: None }),
        );
        mutations
    }
}
