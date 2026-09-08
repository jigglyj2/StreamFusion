// Copyright 2026 StreamFusion Authors
// Licensed under the Apache License, Version 2.0.

use super::*;

pub(super) fn rank_end(
    plan: &proto::TopN,
    input: &RecordBatch,
    row: usize,
    group: &mut GroupWork,
    invalid_top_sizes: &mut u64,
) -> Result<i64> {
    if let Some(value) = plan.rank_end {
        return i64::try_from(value)
            .map_err(|_| DataFusionError::Execution("top-n rank end exceeds i64".to_string()));
    }
    let index = plan.variable_rank_end_index.expect("validated") as usize;
    let value = if input.column(index).is_null(row) {
        0
    } else {
        match input.column(index).data_type() {
            DataType::Int16 => input
                .column(index)
                .as_any()
                .downcast_ref::<Int16Array>()
                .unwrap()
                .value(row) as i64,
            DataType::Int32 => input
                .column(index)
                .as_any()
                .downcast_ref::<Int32Array>()
                .unwrap()
                .value(row) as i64,
            DataType::Int64 => input
                .column(index)
                .as_any()
                .downcast_ref::<Int64Array>()
                .unwrap()
                .value(row),
            other => {
                return Err(DataFusionError::Execution(format!(
                    "top-n variable rank end has Arrow type {other}"
                )))
            }
        }
    };
    match group.rank_end {
        None => group.rank_end = Some(value),
        Some(existing) if existing != value => {
            *invalid_top_sizes = invalid_top_sizes.saturating_add(1);
        }
        Some(_) => {}
    }
    Ok(group.rank_end.expect("rank end initialized"))
}

pub(super) fn selected(group: &GroupWork, rank_start: u64, rank_end: i64) -> &[CandidateRef] {
    if rank_end < rank_start as i64 {
        return &group.candidates[0..0];
    }
    let from = usize::try_from(rank_start - 1)
        .unwrap_or(usize::MAX)
        .min(group.candidates.len());
    let to = usize::try_from(rank_end)
        .unwrap_or(usize::MAX)
        .min(group.candidates.len());
    &group.candidates[from..to]
}

pub(super) fn insert_sorted(
    plan: &proto::TopN,
    sources: &CandidateSources,
    candidates: &mut Vec<CandidateRef>,
    candidate: CandidateRef,
    comparator_calls: &mut u64,
) -> Result<()> {
    // Flink's LIMIT specialization has no ordering: arrival sequence is its stable order.
    // Avoid a logarithmic binary search whose comparisons can only ever fall through to that
    // same sequence ordering.
    if plan.sort_key_indices.is_empty() {
        candidates.push(candidate);
        return Ok(());
    }
    // Flink's generated floating comparator uses `>`/`<`, so NaN compares equal to every
    // value. Its TopNBuffer (a TreeMap of sort-key buckets) therefore preserves the insertion
    // order of that comparator-equivalent bucket. Keep the same stable bucket behavior without
    // imposing Rust's total float order; the normal path remains logarithmic.
    let can_have_nan = plan.sort_key_indices.iter().any(|&index| {
        self::compare::data_type_can_have_nan(
            sources[candidate.source]
                .schema()
                .field(index as usize)
                .data_type(),
        )
    });
    let mut has_nan = false;
    if can_have_nan {
        has_nan = sort_key_has_nan(plan, sources, candidate)?;
        if !has_nan {
            for &existing in candidates.iter() {
                if sort_key_has_nan(plan, sources, existing)? {
                    has_nan = true;
                    break;
                }
            }
        }
    }
    if has_nan {
        for start in 0..candidates.len() {
            *comparator_calls = comparator_calls.saturating_add(1);
            if sort_key_order(plan, sources, candidates[start], candidate)? == Ordering::Equal {
                let representative = candidates[start];
                let mut end = start + 1;
                while end < candidates.len() {
                    *comparator_calls = comparator_calls.saturating_add(1);
                    if sort_key_order(plan, sources, representative, candidates[end])?
                        != Ordering::Equal
                    {
                        break;
                    }
                    end += 1;
                }
                let position = (start..end)
                    .find(|&index| candidates[index].sequence > candidate.sequence)
                    .unwrap_or(end);
                candidates.insert(position, candidate);
                return Ok(());
            }
        }
    }
    let mut low = 0;
    let mut high = candidates.len();
    while low < high {
        let middle = low + (high - low) / 2;
        *comparator_calls = comparator_calls.saturating_add(1);
        if candidate_order(plan, sources, candidates[middle], candidate)? == Ordering::Less {
            low = middle + 1;
        } else {
            high = middle;
        }
    }
    candidates.insert(low, candidate);
    Ok(())
}

pub(super) fn rank_type(plan: &proto::TopN) -> proto::TopNRankType {
    match proto::TopNRankType::try_from(plan.rank_type) {
        Ok(proto::TopNRankType::Rank) => proto::TopNRankType::Rank,
        _ => proto::TopNRankType::RowNumber,
    }
}

pub(super) fn truncate_rank(
    plan: &proto::TopN,
    sources: &CandidateSources,
    candidates: &mut Vec<CandidateRef>,
    rank_end: i64,
    comparator_calls: &mut u64,
) -> Result<()> {
    let end = usize::try_from(rank_end.max(0)).unwrap_or(usize::MAX);
    if candidates.len() <= end || end == 0 {
        candidates.truncate(end);
        return Ok(());
    }
    let cutoff = candidates[end - 1];
    let mut retained = end;
    while retained < candidates.len() {
        *comparator_calls = comparator_calls.saturating_add(1);
        if sort_key_order(plan, sources, cutoff, candidates[retained])? != Ordering::Equal {
            break;
        }
        retained += 1;
    }
    candidates.truncate(retained);
    Ok(())
}

pub(super) fn candidate_order(
    plan: &proto::TopN,
    sources: &CandidateSources,
    left: CandidateRef,
    right: CandidateRef,
) -> Result<Ordering> {
    let ordering = sort_key_order(plan, sources, left, right)?;
    Ok(ordering.then_with(|| left.sequence.cmp(&right.sequence)))
}

pub(super) fn sort_key_order(
    plan: &proto::TopN,
    sources: &CandidateSources,
    left: CandidateRef,
    right: CandidateRef,
) -> Result<Ordering> {
    if let Some(orders) = &sources.orders {
        return Ok(orders[left.source]
            .row(left.row)
            .cmp(&orders[right.source].row(right.row)));
    }
    compare_rows(
        sources[left.source].as_ref(),
        left.row,
        sources[right.source].as_ref(),
        right.row,
        &plan.sort_key_indices,
        &plan.sort_ascending,
        &plan.sort_nulls_last,
    )
}

pub(super) fn sort_key_has_nan(
    plan: &proto::TopN,
    sources: &[Arc<RecordBatch>],
    candidate: CandidateRef,
) -> Result<bool> {
    self::compare::row_has_nan(
        sources[candidate.source].as_ref(),
        candidate.row,
        &plan.sort_key_indices,
    )
}

pub(super) fn find_equal(
    sources: &[Arc<RecordBatch>],
    candidates: &[CandidateRef],
    candidate: CandidateRef,
    indices: impl IntoIterator<Item = usize> + Clone,
) -> Result<Option<usize>> {
    for (index, existing) in candidates.iter().enumerate() {
        if equal_rows(
            sources[existing.source].as_ref(),
            existing.row,
            sources[candidate.source].as_ref(),
            candidate.row,
            indices.clone(),
        )? {
            return Ok(Some(index));
        }
    }
    Ok(None)
}

pub(super) fn emit_difference(
    plan: &proto::TopN,
    sources: &[Arc<RecordBatch>],
    before: &[CandidateRef],
    after: &[CandidateRef],
    output: &mut Vec<OutputEvent>,
) -> Result<()> {
    if plan.output_rank_number || plan.rank_start > 1 {
        for index in 0..before.len().max(after.len()) {
            let old = before.get(index).copied();
            let next = after.get(index).copied();
            if same_output(sources, old, next)? {
                continue;
            }
            let rank = i64::try_from(plan.rank_start)
                .unwrap_or(i64::MAX)
                .saturating_add(index as i64);
            match (old, next) {
                (Some(old), Some(next)) => {
                    if plan.generate_update_before {
                        output.push(OutputEvent {
                            candidate: old,
                            rank,
                            kind: UPDATE_BEFORE,
                        });
                    }
                    output.push(OutputEvent {
                        candidate: next,
                        rank,
                        kind: UPDATE_AFTER,
                    });
                }
                (Some(old), None) => output.push(OutputEvent {
                    candidate: old,
                    rank,
                    kind: DELETE,
                }),
                (None, Some(next)) => output.push(OutputEvent {
                    candidate: next,
                    rank,
                    kind: INSERT,
                }),
                (None, None) => unreachable!(),
            }
        }
        return Ok(());
    }
    // Flink selects FastTop1Function for both append-fast and update-fast Top-1. Unlike the
    // general no-rank-number Top-N helper, replacement of the current winner is represented as
    // an optional UPDATE_BEFORE followed by UPDATE_AFTER. This distinction is observable by
    // upsert-aware downstream operators and sinks even though DELETE + INSERT has the same
    // materialized multiset.
    if plan.rank_start == 1
        && plan.rank_end == Some(1)
        && plan.strategy != proto::TopNStrategy::Retract as i32
    {
        let old = before.first().copied();
        let next = after.first().copied();
        if same_output(sources, old, next)? {
            return Ok(());
        }
        match (old, next) {
            (Some(old), Some(next)) => {
                if plan.generate_update_before {
                    output.push(OutputEvent {
                        candidate: old,
                        rank: 0,
                        kind: UPDATE_BEFORE,
                    });
                }
                output.push(OutputEvent {
                    candidate: next,
                    rank: 0,
                    kind: UPDATE_AFTER,
                });
            }
            (None, Some(next)) => output.push(OutputEvent {
                candidate: next,
                rank: 0,
                kind: INSERT,
            }),
            (Some(old), None) => output.push(OutputEvent {
                candidate: old,
                rank: 0,
                kind: DELETE,
            }),
            (None, None) => {}
        }
        return Ok(());
    }
    for old in before {
        match after.iter().find(|next| next.sequence == old.sequence) {
            None => output.push(OutputEvent {
                candidate: *old,
                rank: 0,
                kind: DELETE,
            }),
            // Append/retract strategies keep the exact candidate reference for an unchanged
            // row. Short-circuit that identity before value equality: SQL floating equality
            // deliberately treats NaN differently from the sort comparator, but an untouched
            // NaN row must not turn into a spurious UPDATE_BEFORE/UPDATE_AFTER pair.
            Some(next) if old == next => {}
            Some(next)
                if !equal_rows(
                    sources[old.source].as_ref(),
                    old.row,
                    sources[next.source].as_ref(),
                    next.row,
                    0..sources[old.source].num_columns(),
                )? =>
            {
                if plan.generate_update_before {
                    output.push(OutputEvent {
                        candidate: *old,
                        rank: 0,
                        kind: UPDATE_BEFORE,
                    });
                }
                output.push(OutputEvent {
                    candidate: *next,
                    rank: 0,
                    kind: UPDATE_AFTER,
                });
            }
            Some(_) => {}
        }
    }
    for next in after {
        if !before.iter().any(|old| old.sequence == next.sequence) {
            output.push(OutputEvent {
                candidate: *next,
                rank: 0,
                kind: INSERT,
            });
        }
    }
    Ok(())
}

pub(super) fn same_output(
    sources: &[Arc<RecordBatch>],
    left: Option<CandidateRef>,
    right: Option<CandidateRef>,
) -> Result<bool> {
    match (left, right) {
        (None, None) => Ok(true),
        (Some(left), Some(right)) if left == right => Ok(true),
        (Some(left), Some(right)) if left.sequence == right.sequence => equal_rows(
            sources[left.source].as_ref(),
            left.row,
            sources[right.source].as_ref(),
            right.row,
            0..sources[left.source].num_columns(),
        ),
        _ => Ok(false),
    }
}
