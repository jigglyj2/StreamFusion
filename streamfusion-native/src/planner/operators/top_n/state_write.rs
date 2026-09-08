// Copyright 2026 StreamFusion Authors
// Licensed under the Apache License, Version 2.0.

use super::*;
use crate::planner::operators::ordered_partition::Entries;

impl TopNProcessor {
    pub(super) fn state_mutations(
        &self,
        sources: &CandidateSources,
        groups: Vec<GroupWork>,
        indexed: &mut [Entries],
        index_credit: &mut HostMemoryReservation,
        now_millis: i64,
    ) -> Result<(Vec<StateMutation>, usize)> {
        if self.native_schema.is_some() {
            return self.top_one_mutations(sources, groups, indexed);
        }
        let touched_group_count = groups.len();
        let converter = self.row_converter.as_ref().expect("input schema prepared");
        let encoded_sources = sources
            .iter()
            .map(|source| converter.convert_columns(source.columns()))
            .collect::<std::result::Result<Vec<Rows>, _>>()?;
        let mut mutations = Vec::with_capacity(groups.len());
        for (group_index, group) in groups.into_iter().enumerate() {
            let sequences = group
                .candidates
                .iter()
                .map(|candidate| candidate.sequence)
                .collect::<Vec<_>>();
            let row_kinds = self.plan.physical_input_semantics.then(|| {
                group
                    .candidates
                    .iter()
                    .map(|candidate| candidate.kind)
                    .collect::<Vec<_>>()
            });
            let preserve_empty = group.rank_end.is_some();
            if let Some(orders) = &sources.orders {
                let prefix =
                    crate::planner::operators::sortable_state::prefix(0xf0, &group.state_key.key)?;
                let mut entries = crate::planner::operators::ordered_partition::Entries::new();
                index_credit.try_grow(
                    group
                        .candidates
                        .iter()
                        .map(|c| {
                            prefix.len()
                                + orders[c.source].row(c.row).data().len()
                                + encoded_sources[c.source].row(c.row).data().len()
                                + 201
                        })
                        .sum(),
                )?;
                for c in &group.candidates {
                    let key = crate::planner::operators::sortable_state::row_key(
                        &prefix,
                        orders[c.source].row(c.row).data(),
                        c.sequence,
                    );
                    let mut value = vec![c.kind as u8];
                    value.extend_from_slice(encoded_sources[c.source].row(c.row).data());
                    entries.insert(key, value);
                }
                crate::planner::operators::ordered_partition::write_delta(
                    &group.state_key,
                    std::mem::take(&mut indexed[group_index]),
                    entries,
                    &mut mutations,
                );
                let value = if !sequences.is_empty() || preserve_empty {
                    let mut bytes = crate::planner::operators::sortable_state::FORMAT.to_vec();
                    bytes.extend_from_slice(&encode_state_rows_with_kinds(
                        group.next_sequence,
                        group.rank_end,
                        now_millis,
                        &[],
                        None,
                        std::iter::empty::<arrow_row::Row<'_>>(),
                    )?);
                    Some(bytes)
                } else {
                    None
                };
                mutations.push(StateMutation {
                    key: group.state_key,
                    value,
                });
                continue;
            }
            mutations.push(StateMutation {
                key: group.state_key,
                value: (!sequences.is_empty() || preserve_empty)
                    .then(|| {
                        encode_state_rows_with_kinds(
                            group.next_sequence,
                            group.rank_end,
                            now_millis,
                            &sequences,
                            row_kinds.as_deref(),
                            group.candidates.iter().map(|candidate| {
                                encoded_sources[candidate.source].row(candidate.row)
                            }),
                        )
                    })
                    .transpose()?,
            });
        }
        Ok((mutations, touched_group_count))
    }

    fn top_one_mutations(
        &self,
        sources: &CandidateSources,
        groups: Vec<GroupWork>,
        indexed: &mut [Entries],
    ) -> Result<(Vec<StateMutation>, usize)> {
        // The shared factory verifies a constant Top-1 with disabled TTL. Like Flink's
        // FastTop1 ValueState, one point value is sufficient; no range index is needed.
        // An old index is read only during migration and removed in this same write batch.
        let mut changed = Vec::new();
        for (index, group) in groups.into_iter().enumerate() {
            if group.candidates.len() != 1 {
                return Err(DataFusionError::Execution(
                    "shared Top-1 must retain exactly one winner".into(),
                ));
            }
            if group.candidates[0].source == 0 || !indexed[index].is_empty() {
                changed.push((index, group));
            }
        }
        if changed.is_empty() {
            return Ok((Vec::new(), 0));
        }
        let positions = changed
            .iter()
            .map(|(_, group)| {
                let winner = &group.candidates[0];
                (winner.source, winner.row)
            })
            .collect::<Vec<_>>();
        // Encode only final changed winners. Intermediate changelog results still reference
        // the original input/restored Arrow batches and retain every arrival transition.
        let batches = sources.iter().map(Arc::as_ref).collect::<Vec<_>>();
        let winners = interleave_record_batch(&batches, &positions)?;
        let encoded = self
            .row_converter
            .as_ref()
            .expect("prepared")
            .convert_columns(winners.columns())?;
        drop(winners);
        let changed_count = changed.len();
        let mut mutations = Vec::with_capacity(changed_count * 2);
        for (row, (index, group)) in changed.into_iter().enumerate() {
            for (key, _) in std::mem::take(&mut indexed[index]) {
                mutations.push(StateMutation {
                    key: StateKey {
                        key_group: group.state_key.key_group,
                        key,
                    },
                    value: None,
                });
            }
            // SFTN v5 already represents a schema-aware Arrow row, range and tie sequence.
            // Reusing it preserves v4/v5 snapshot compatibility without inventing another codec.
            let winner = &group.candidates[0];
            let value = encode_state_rows_with_kinds(
                group.next_sequence,
                Some(1),
                0,
                &[winner.sequence],
                None,
                [encoded.row(row)],
            )?;
            mutations.push(StateMutation {
                key: group.state_key,
                value: Some(value),
            });
        }
        Ok((mutations, changed_count))
    }
}

#[cfg(test)]
mod tests;
