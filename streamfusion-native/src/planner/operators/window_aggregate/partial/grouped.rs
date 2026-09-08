// Copyright 2026 StreamFusion Authors
// Licensed under the Apache License, Version 2.0.

use super::*;
use crate::planner::operators::group_aggregate::grouped_compute::{GroupedMerge, GroupedOutput};

pub(super) fn merge_append_partials(
    calls: &[Call],
    staged: &[StagedWindow],
    partials: &[Option<AccumulatorState>],
    assignments: &[(usize, usize, i64, i64)],
) -> Result<Option<GroupedOutput>> {
    if assignments.is_empty() {
        return Ok(None);
    }
    // Deleting/recreating an empty group is an ordered Flink transition. Use the existing
    // transition path for deltas or cardinality overflow instead of aggregating across it.
    let mut counts = staged
        .iter()
        .map(|entry| entry.accumulator.row_count)
        .collect::<Vec<_>>();
    if counts.iter().any(|&count| count < 0) {
        return Ok(None);
    }
    for &(row, group, _, _) in assignments {
        let count = partials[row].as_ref().unwrap().row_count;
        let Some(next) = counts[group].checked_add(count).filter(|_| count > 0) else {
            return Ok(None);
        };
        counts[group] = next;
    }
    let Some(mut compute) = GroupedMerge::new(calls)? else {
        return Ok(None);
    };
    // Bound the temporary column construction independently of the number of shared windows.
    const CHUNK: usize = 2048;
    for (chunk, entries) in staged.chunks(CHUNK).enumerate() {
        let states = entries
            .iter()
            .map(|entry| &entry.accumulator)
            .collect::<Vec<_>>();
        let groups = (chunk * CHUNK..chunk * CHUNK + entries.len()).collect::<Vec<_>>();
        compute.merge(calls, &states, &groups, staged.len())?;
    }
    for entries in assignments.chunks(CHUNK) {
        let states = entries
            .iter()
            .map(|&(row, _, _, _)| partials[row].as_ref().unwrap())
            .collect::<Vec<_>>();
        let groups = entries
            .iter()
            .map(|&(_, group, _, _)| group)
            .collect::<Vec<_>>();
        compute.merge(calls, &states, &groups, staged.len())?;
    }
    compute.finish().map(Some)
}
