// Copyright 2026 StreamFusion Authors
// Licensed under the Apache License, Version 2.0.

// Bounded fixtures retain the former recursive enumeration as an independent order oracle.
use super::*;
pub(super) fn enumerate_join(
    plan: &proto::MultiJoin,
    state: &MultiJoinState,
    null_rows: &[Vec<u8>],
    active_input: usize,
    active: &StoredRow,
    kind: i8,
    ordinal: i32,
    output: &mut Vec<OutputRow>,
) {
    fn recurse<'a>(
        plan: &proto::MultiJoin,
        state: &'a MultiJoinState,
        null_rows: &[Vec<u8>],
        active_input: usize,
        active: &'a StoredRow,
        kind: i8,
        ordinal: i32,
        depth: usize,
        joined: &mut Vec<Option<&'a StoredRow>>,
        is_active: bool,
        output: &mut Vec<OutputRow>,
    ) {
        if depth == state.inputs.len() {
            if is_active {
                output.push(OutputRow {
                    inputs: joined
                        .iter()
                        .enumerate()
                        .map(|(input, row)| {
                            row.as_ref()
                                .map(|row| row.row.clone())
                                .unwrap_or_else(|| null_rows[input].clone())
                        })
                        .collect(),
                    kind,
                    input_ordinal: ordinal,
                });
            }
            return;
        }

        let is_left = depth > 0 && plan.join_types[depth] == proto::RegularJoinType::Left as i32;
        let accumulate = matches!(kind, INSERT | UPDATE_AFTER);
        let mut any_match = false;
        let mut associations = 0_i32;

        // Flink only scans the active level's existing state for a LEFT join, where the
        // association count determines null-padded transitions. INNER joins can activate the
        // incoming row directly.
        if is_left || depth != active_input {
            for candidate in &state.inputs[depth] {
                if !conditions_match(plan, depth, joined, candidate) {
                    continue;
                }
                any_match = true;
                if is_left {
                    associations += if !is_active || accumulate { 1 } else { -1 };
                    if depth == active_input
                        && ((accumulate && associations > 0) || (!accumulate && associations > 1))
                    {
                        break;
                    }
                }
                if !is_active && depth == active_input {
                    continue;
                }
                joined.push(Some(candidate));
                recurse(
                    plan,
                    state,
                    null_rows,
                    active_input,
                    active,
                    kind,
                    ordinal,
                    depth + 1,
                    joined,
                    is_active,
                    output,
                );
                joined.pop();
            }
        }

        if depth == active_input {
            if conditions_match(plan, depth, joined, active) {
                if is_left {
                    associations += if accumulate { 1 } else { -1 };
                }
                if accumulate && is_left && !any_match {
                    joined.push(None);
                    recurse(
                        plan,
                        state,
                        null_rows,
                        active_input,
                        active,
                        DELETE,
                        ordinal,
                        depth + 1,
                        joined,
                        true,
                        output,
                    );
                    joined.pop();
                }
                joined.push(Some(active));
                recurse(
                    plan,
                    state,
                    null_rows,
                    active_input,
                    active,
                    kind,
                    ordinal,
                    depth + 1,
                    joined,
                    true,
                    output,
                );
                joined.pop();
                if !accumulate && is_left && associations == 0 {
                    joined.push(None);
                    recurse(
                        plan,
                        state,
                        null_rows,
                        active_input,
                        active,
                        INSERT,
                        ordinal,
                        depth + 1,
                        joined,
                        true,
                        output,
                    );
                    joined.pop();
                }
            }
        } else if is_left && !any_match && associations == 0 {
            joined.push(None);
            recurse(
                plan,
                state,
                null_rows,
                active_input,
                active,
                kind,
                ordinal,
                depth + 1,
                joined,
                is_active,
                output,
            );
            joined.pop();
        }
    }

    recurse(
        plan,
        state,
        null_rows,
        active_input,
        active,
        kind,
        ordinal,
        0,
        &mut Vec::with_capacity(state.inputs.len()),
        false,
        output,
    );
}
