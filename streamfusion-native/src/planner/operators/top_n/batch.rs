// Copyright 2026 StreamFusion Authors
// Licensed under the Apache License, Version 2.0.

use super::*;

impl TopNProcessor {
    pub(super) fn process_arrow_accounted(
        &mut self,
        batch: RecordBatch,
        now_millis: i64,
        base_reservation: usize,
    ) -> Result<RecordBatch> {
        let visible = Arc::new(RecordBatch::try_new(
            Arc::clone(&self.input_schema),
            batch.columns()[..self.input_schema.fields().len()].to_vec(),
        )?);
        let kinds = batch
            .column(self.input_kind_index.expect("input schema prepared"))
            .as_any()
            .downcast_ref::<Int8Array>()
            .ok_or_else(|| {
                DataFusionError::Execution("top-n RowKind metadata is not Arrow Int8".to_string())
            })?;
        let mut unique = HashMap::<Vec<u8>, usize, RandomState>::with_capacity_and_hasher(
            batch.num_rows(),
            RandomState::new(),
        );
        let mut row_groups = Vec::with_capacity(batch.num_rows());
        for row in 0..batch.num_rows() {
            let key = self.group_key(&batch, row)?;
            let next = unique.len();
            row_groups.push(*unique.entry(key).or_insert(next));
        }
        let mut ordered_keys = (0..unique.len()).map(|_| None).collect::<Vec<_>>();
        for (key, index) in unique {
            ordered_keys[index] = Some(key);
        }
        let ordered_keys = ordered_keys
            .into_iter()
            .map(|key| key.expect("every top-n group index is populated"))
            .collect::<Vec<_>>();
        let state_keys = ordered_keys
            .iter()
            .map(|key| top_n_state_key(assign_key_group(key, self.max_parallelism), key))
            .collect::<Vec<_>>();
        let state_refs = state_keys
            .iter()
            .map(|key| StateKeyRef {
                key_group: key.key_group,
                key: &key.key,
            })
            .collect::<Vec<_>>();
        let values = self
            .state
            .get_batch(&state_refs, &self.scratch_reservation)?;
        let _loaded_state_workspace =
            crate::state::reserve_decoded_values(&values, &self.scratch_reservation)?;
        self.state_read_batches = self.state_read_batches.saturating_add(1);
        self.groups_read = self.groups_read.saturating_add(values.len() as u64);
        let state_bytes = values
            .iter()
            .flatten()
            .map(|value| value.len())
            .sum::<usize>();
        self.scratch_reservation
            .resize(base_reservation.saturating_add(state_bytes.saturating_mul(4)))?;

        let mut index_credit = self
            .scratch_reservation
            .sibling("top-n ordered partition working set");
        let mut indexed = state_keys
            .iter()
            .zip(values.iter())
            .map(|(key, value)| {
                if value.as_ref().is_some_and(|v| {
                    v.starts_with(crate::planner::operators::sortable_state::FORMAT)
                }) {
                    crate::planner::operators::ordered_partition::load(
                        self.state.as_ref(),
                        key,
                        &mut index_credit,
                    )
                } else {
                    Ok(crate::planner::operators::ordered_partition::Entries::new())
                }
            })
            .collect::<Result<Vec<_>>>()?;
        index_credit.try_grow(
            indexed
                .iter()
                .flat_map(|entries| entries.iter())
                .map(|(k, v)| {
                    k.len()
                        .saturating_add(v.len().saturating_mul(4))
                        .saturating_add(256)
                })
                .sum(),
        )?;
        let mut restored_rows = Vec::new();
        let mut decoded_groups = Vec::with_capacity(values.len());
        for (group_index, value) in values.iter().enumerate() {
            let mut decoded = value
                .as_ref()
                .map(|bytes| {
                    decode_state_rows(
                        bytes
                            .strip_prefix(crate::planner::operators::sortable_state::FORMAT)
                            .unwrap_or(bytes.as_ref()),
                    )
                })
                .transpose()?;
            if value
                .as_ref()
                .is_some_and(|v| v.starts_with(crate::planner::operators::sortable_state::FORMAT))
            {
                if indexed[group_index].values().any(Vec::is_empty) {
                    return Err(DataFusionError::Execution(
                        "truncated Top-N indexed candidate".into(),
                    ));
                }
                let d = decoded.as_mut().expect("indexed group has metadata");
                d.sequences = indexed[group_index]
                    .keys()
                    .map(|k| u64::from_be_bytes(k[k.len() - 8..].try_into().unwrap()))
                    .collect();
                d.row_kinds = Some(indexed[group_index].values().map(|v| v[0] as i8).collect());
                d.rows = indexed[group_index].values().map(|v| &v[1..]).collect();
            }
            if let Some(decoded) = decoded {
                if self.is_expired(decoded.last_access_millis, now_millis) {
                    self.expired_groups = self.expired_groups.saturating_add(1);
                    decoded_groups.push(DecodedGroup {
                        next_sequence: 0,
                        rank_end: None,
                        sequences: Vec::new(),
                        row_kinds: Vec::new(),
                        restored_row_offset: restored_rows.len(),
                    });
                } else {
                    let restored_row_offset = restored_rows.len();
                    restored_rows.extend(decoded.rows);
                    decoded_groups.push(DecodedGroup {
                        next_sequence: decoded.next_sequence,
                        rank_end: decoded.rank_end,
                        row_kinds: decoded
                            .row_kinds
                            .unwrap_or_else(|| vec![INSERT; decoded.sequences.len()]),
                        sequences: decoded.sequences,
                        restored_row_offset,
                    });
                }
            } else {
                decoded_groups.push(DecodedGroup {
                    next_sequence: 0,
                    rank_end: None,
                    sequences: Vec::new(),
                    row_kinds: Vec::new(),
                    restored_row_offset: restored_rows.len(),
                });
            }
        }
        let restored = if restored_rows.is_empty() {
            RecordBatch::new_empty(Arc::clone(&self.input_schema))
        } else {
            let converter = self.row_converter.as_ref().expect("input schema prepared");
            let parser = converter.parser();
            let columns =
                converter.convert_rows(restored_rows.into_iter().map(|row| parser.parse(row)))?;
            RecordBatch::try_new(Arc::clone(&self.input_schema), columns)?
        };
        let batches = vec![visible, Arc::new(restored)];
        let orders = self
            .sort_keys
            .as_ref()
            .map(|keys| {
                batches
                    .iter()
                    .map(|b| keys.encode(b.columns()))
                    .collect::<Result<Vec<_>>>()
            })
            .transpose()?;
        let sources = CandidateSources { batches, orders };
        let mut groups = state_keys
            .into_iter()
            .zip(decoded_groups)
            .map(|(state_key, decoded)| GroupWork {
                state_key,
                next_sequence: decoded.next_sequence,
                rank_end: decoded.rank_end,
                candidates: decoded
                    .sequences
                    .into_iter()
                    .enumerate()
                    .map(|(row, sequence)| CandidateRef {
                        source: 1,
                        row: decoded.restored_row_offset + row,
                        sequence,
                        kind: decoded.row_kinds[row],
                    })
                    .collect(),
            })
            .collect::<Vec<_>>();
        if let Some(orders) = &sources.orders {
            for (i, group) in groups.iter_mut().enumerate() {
                if values[i].as_ref().is_some_and(|v| {
                    !v.starts_with(crate::planner::operators::sortable_state::FORMAT)
                }) {
                    // Old snapshots predate independent descending null placement.
                    group.candidates.sort_by(|a, b| {
                        orders[a.source]
                            .row(a.row)
                            .cmp(&orders[b.source].row(b.row))
                            .then_with(|| a.sequence.cmp(&b.sequence))
                    });
                }
            }
        }
        if is_append_limit(&self.plan) {
            for group in &mut groups {
                group.candidates.clear();
                group.rank_end = Some(0);
            }
        }
        let top_one_winners = if datafusion_top_one::compatible(&self.plan, &sources) {
            Some(datafusion_top_one::winners(
                &sources,
                &groups,
                &row_groups,
                &self.scratch_reservation,
            )?)
        } else {
            None
        };
        let mut output = Vec::new();
        let append_limit_end = is_append_limit(&self.plan)
            .then(|| usize::try_from(self.plan.rank_end.unwrap()).unwrap_or(usize::MAX));
        for row in 0..batch.num_rows() {
            let group = &mut groups[row_groups[row]];
            if let Some(limit_end) = append_limit_end {
                require_insert(kinds.value(row), "append-fast")?;
                // Flink's append-only Limit stores only the number already observed. Payload
                // rows can never re-enter the output, so retaining them would turn a constant
                // counter into O(limit) state and make OFFSET-only queries unbounded.
                group.rank_end = Some(0);
                if group.next_sequence < limit_end as u64 {
                    let candidate = CandidateRef {
                        source: 0,
                        row,
                        sequence: group.next_sequence,
                        kind: INSERT,
                    };
                    group.next_sequence = next_sequence(group.next_sequence)?;
                    let rank = group.next_sequence;
                    if rank >= self.plan.rank_start {
                        output.push(OutputEvent {
                            candidate,
                            rank: rank as i64,
                            kind: INSERT,
                        });
                    }
                }
                continue;
            }
            let rank_end = rank_end(
                &self.plan,
                visible_source(&sources),
                row,
                group,
                &mut self.invalid_top_sizes,
            )?;
            let candidate = CandidateRef {
                source: 0,
                row,
                sequence: group.next_sequence,
                kind: if self.plan.physical_input_semantics {
                    let kind = kinds.value(row);
                    if !matches!(kind, INSERT | UPDATE_BEFORE | UPDATE_AFTER | DELETE) {
                        return Err(unknown_row_kind(kind));
                    }
                    kind
                } else {
                    INSERT
                },
            };
            let before = if self.plan.bounded_final_output {
                Vec::new()
            } else {
                selected(group, self.plan.rank_start, rank_end).to_vec()
            };
            match proto::TopNStrategy::try_from(self.plan.strategy)
                .map_err(|_| DataFusionError::Plan("top-n strategy is unknown".to_string()))?
            {
                proto::TopNStrategy::AppendFast => {
                    if !self.plan.physical_input_semantics {
                        require_insert(kinds.value(row), "append-fast")?;
                    }
                    group.next_sequence = next_sequence(group.next_sequence)?;
                    if let Some(winners) = &top_one_winners {
                        if winners[row] {
                            group.candidates.clear();
                            group.candidates.push(candidate);
                        }
                    } else {
                        insert_sorted(
                            &self.plan,
                            &sources,
                            &mut group.candidates,
                            candidate,
                            &mut self.comparator_calls,
                        )?;
                    }
                }
                proto::TopNStrategy::UpdateFast => {
                    if !matches!(kinds.value(row), INSERT | UPDATE_AFTER) {
                        return Err(DataFusionError::Execution(format!(
                            "update-fast Top-N received RowKind {}",
                            kinds.value(row)
                        )));
                    }
                    let previous = find_equal(
                        &sources,
                        &group.candidates,
                        candidate,
                        self.plan
                            .primary_key_indices
                            .iter()
                            .map(|&value| value as usize),
                    )?;
                    let candidate = if let Some(index) = previous {
                        let sequence = group.candidates.remove(index).sequence;
                        CandidateRef {
                            sequence,
                            ..candidate
                        }
                    } else {
                        group.next_sequence = next_sequence(group.next_sequence)?;
                        candidate
                    };
                    insert_sorted(
                        &self.plan,
                        &sources,
                        &mut group.candidates,
                        candidate,
                        &mut self.comparator_calls,
                    )?;
                }
                proto::TopNStrategy::Retract => match kinds.value(row) {
                    INSERT | UPDATE_AFTER => {
                        group.next_sequence = next_sequence(group.next_sequence)?;
                        insert_sorted(
                            &self.plan,
                            &sources,
                            &mut group.candidates,
                            candidate,
                            &mut self.comparator_calls,
                        )?;
                    }
                    UPDATE_BEFORE | DELETE => {
                        let previous = find_equal(
                            &sources,
                            &group.candidates,
                            candidate,
                            0..self.input_schema.fields().len(),
                        )?;
                        if let Some(index) = previous {
                            group.candidates.remove(index);
                        } else {
                            self.invalid_retractions = self.invalid_retractions.saturating_add(1);
                        }
                    }
                    other => return Err(unknown_row_kind(other)),
                },
                proto::TopNStrategy::Unspecified => unreachable!("validated Top-N strategy"),
            }
            if self.plan.bounded_final_output && rank_type(&self.plan) == proto::TopNRankType::Rank
            {
                truncate_rank(
                    &self.plan,
                    &sources,
                    &mut group.candidates,
                    rank_end,
                    &mut self.comparator_calls,
                )?;
            } else if self.plan.strategy != proto::TopNStrategy::Retract as i32 {
                let retained = usize::try_from(rank_end.max(0)).unwrap_or(usize::MAX);
                group.candidates.truncate(retained);
            }
            if !self.plan.bounded_final_output {
                let after = selected(group, self.plan.rank_start, rank_end);
                emit_difference(&self.plan, &sources, &before, after, &mut output)?;
            }
        }

        let saturates_append_limit = is_non_expiring_append_limit(&self.plan)
            && groups
                .first()
                .is_some_and(|group| group.next_sequence >= self.plan.rank_end.unwrap());

        let converter = self.row_converter.as_ref().expect("input schema prepared");
        let encoded_sources = sources
            .iter()
            .map(|source| converter.convert_columns(source.columns()))
            .collect::<std::result::Result<Vec<Rows>, _>>()?;
        let touched_group_count = groups.len();
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
        self.groups_written = self
            .groups_written
            .saturating_add(touched_group_count as u64);
        if !mutations.is_empty() {
            self.state.write_batch(mutations)?;
            self.state_write_batches = self.state_write_batches.saturating_add(1);
        }
        self.saturated_append_limit = saturates_append_limit;
        output_batch(&self.plan, &self.output_schema, &sources, output)
    }
}
