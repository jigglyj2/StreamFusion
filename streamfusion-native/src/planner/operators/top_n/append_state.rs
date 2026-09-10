// Copyright 2026 StreamFusion Authors
// Licensed under the Apache License, Version 2.0.

//! Ordered append-state deltas encode only final new candidates and preserve old payload buffers.
use super::*;
use crate::planner::operators::{ordered_partition::Entries, sortable_state};

struct Pending {
    group: GroupWork,
    index: usize,
    keys: Vec<Vec<u8>>,
    encoded_rows: Vec<Option<usize>>,
}

impl TopNProcessor {
    pub(super) fn append_mutations(
        &self,
        sources: &CandidateSources,
        groups: Vec<GroupWork>,
        indexed: &mut [Entries],
        credit: &mut HostMemoryReservation,
    ) -> Result<(Vec<StateMutation>, usize)> {
        let orders = sources
            .orders
            .as_ref()
            .expect("DataFusion-compatible append ordering");
        let mut positions = Vec::new();
        let mut pending = Vec::new();
        for (index, group) in groups.into_iter().enumerate() {
            if !indexed[index].is_empty() && group.candidates.iter().all(|c| c.source != 0) {
                // Discarded arrival identities are unobservable. The persisted next sequence
                // already exceeds every retained candidate's sequence, preserving future ties.
                continue;
            }
            let prefix = sortable_state::prefix(0xf0, &group.state_key.key)?;
            credit.try_grow(
                group
                    .candidates
                    .iter()
                    .map(|c| {
                        prefix
                            .len()
                            .saturating_add(orders[c.source].row(c.row).data().len())
                            .saturating_add(256)
                    })
                    .sum(),
            )?;
            let mut keys = Vec::with_capacity(group.candidates.len());
            let mut encoded_rows = Vec::with_capacity(group.candidates.len());
            for c in &group.candidates {
                let key = sortable_state::row_key(
                    &prefix,
                    orders[c.source].row(c.row).data(),
                    c.sequence,
                );
                encoded_rows.push(if indexed[index].contains_key(&key) {
                    None
                } else {
                    let row = positions.len();
                    positions.push((c.source, c.row));
                    Some(row)
                });
                keys.push(key);
            }
            pending.push(Pending {
                group,
                index,
                keys,
                encoded_rows,
            });
        }
        if pending.is_empty() {
            return Ok((Vec::new(), 0));
        }
        // Batch/input and decoded-history allowances cover this gather/row-encoding workspace.
        // Unchanged candidates and losing input payloads never enter it.
        let refs = sources.iter().map(Arc::as_ref).collect::<Vec<_>>();
        let selected = interleave_record_batch(&refs, &positions)?;
        let encoded = self
            .row_converter
            .as_ref()
            .expect("prepared")
            .convert_columns(selected.columns())?;
        drop(selected);
        credit.try_grow(
            encoded
                .iter()
                .map(|row| row.data().len().saturating_add(128))
                .sum(),
        )?;
        let changed_count = pending.len();
        let mut mutations = Vec::new();
        for Pending {
            group,
            index,
            keys,
            encoded_rows,
        } in pending
        {
            let mut old = std::mem::take(&mut indexed[index]);
            for ((key, row), candidate) in keys.into_iter().zip(encoded_rows).zip(&group.candidates)
            {
                if let Some(row) = row {
                    let mut value = Vec::with_capacity(encoded.row(row).data().len() + 1);
                    value.push(candidate.kind as u8);
                    value.extend_from_slice(encoded.row(row).data());
                    mutations.push(StateMutation {
                        key: StateKey {
                            key_group: group.state_key.key_group,
                            key,
                        },
                        value: Some(value),
                    });
                } else {
                    // Remove retained keys from the deletion set without copying their payloads.
                    old.remove(&key);
                }
            }
            for (key, _) in old {
                mutations.push(StateMutation {
                    key: StateKey {
                        key_group: group.state_key.key_group,
                        key,
                    },
                    value: None,
                });
            }
            let mut metadata = sortable_state::FORMAT.to_vec();
            metadata.extend_from_slice(&encode_state_rows_with_kinds(
                group.next_sequence,
                group.rank_end,
                0,
                &[],
                None,
                std::iter::empty::<arrow_row::Row<'_>>(),
            )?);
            mutations.push(StateMutation {
                key: group.state_key,
                value: Some(metadata),
            });
        }
        Ok((mutations, changed_count))
    }
}

#[cfg(test)]
mod tests;
