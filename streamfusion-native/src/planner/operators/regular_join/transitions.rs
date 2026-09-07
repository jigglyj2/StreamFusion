// Copyright 2026 StreamFusion Authors
// Licensed under the Apache License, Version 2.0

use super::*;

#[cfg(test)]
pub(super) fn process_change(
    join_type: proto::RegularJoinType,
    side: usize,
    input_kind: i8,
    accumulate: bool,
    candidate_matches: &CandidateMatches,
    input: Arc<[u8]>,
    ordinal: i32,
    state: &mut JoinState,
    output: &mut Vec<OutputRow>,
) -> Result<()> {
    let mut cursor = super::change_cursor::ChangeCursor::new(
        join_type, side, input_kind, accumulate, input, ordinal,
    );
    if !cursor.drain(state, candidate_matches, output, usize::MAX)? {
        return Err(DataFusionError::Internal(
            "unbounded join transition did not finish".into(),
        ));
    }
    Ok(())
}

// Retain the original transition as a test oracle while the runtime switches to bounded pulls.
#[cfg(test)]
pub(super) fn reference_process_change(
    join_type: proto::RegularJoinType,
    side: usize,
    input_kind: i8,
    accumulate: bool,
    candidate_matches: &CandidateMatches,
    input: Arc<[u8]>,
    ordinal: i32,
    state: &mut JoinState,
    output: &mut Vec<OutputRow>,
) -> Result<()> {
    let inserted_id = state.next_row_id[side];
    if accumulate {
        state.next_row_id[side] += 1;
    }
    if matches!(
        join_type,
        proto::RegularJoinType::Semi | proto::RegularJoinType::Anti
    ) {
        return process_semi_anti(
            inserted_id,
            join_type,
            side,
            input_kind,
            accumulate,
            candidate_matches,
            input,
            ordinal,
            state,
            output,
        );
    }
    let input_outer = is_outer(join_type, side);
    let other_outer = is_outer(join_type, 1 - side);
    let (input_rows, other) = if side == 0 {
        (&mut state.left, &mut state.right)
    } else {
        (&mut state.right, &mut state.left)
    };
    debug_assert_eq!(candidate_matches.len(), other.len());
    let matches = candidate_matches.iter().filter(|&matched| matched).count();
    if accumulate {
        if matches == 0 {
            if input_outer {
                push_pair(output, side, Some(input.clone()), None, INSERT, ordinal);
            }
        } else {
            for (candidate, _) in other
                .iter_mut()
                .zip(candidate_matches.iter())
                .filter(|(_, matched)| *matched)
            {
                if other_outer {
                    if candidate.associations == 0 {
                        push_pair(
                            output,
                            1 - side,
                            Some(candidate.row.clone()),
                            None,
                            DELETE,
                            ordinal,
                        );
                    }
                    candidate.associations = candidate.associations.wrapping_add(1);
                }
                let kind = if input_outer || other_outer {
                    INSERT
                } else {
                    input_kind
                };
                push_pair(
                    output,
                    side,
                    Some(input.clone()),
                    Some(candidate.row.clone()),
                    kind,
                    ordinal,
                );
            }
        }
        let stored = StoredRow {
            id: inserted_id,
            row: input,
            associations: if input_outer { matches as i32 } else { 0 },
        };
        input_rows.push(stored);
    } else {
        // Flink's no-unique-key state view ignores a missing record (for
        // example after TTL expiry) but still executes the join transition.
        if let Some(position) = input_rows
            .iter()
            .position(|candidate| candidate.row == input)
        {
            input_rows.remove(position);
        }
        if matches == 0 {
            if input_outer {
                push_pair(output, side, Some(input), None, DELETE, ordinal);
            }
        } else {
            for (candidate, _) in other
                .iter_mut()
                .zip(candidate_matches.iter())
                .filter(|(_, matched)| *matched)
            {
                let kind = if input_outer { DELETE } else { input_kind };
                push_pair(
                    output,
                    side,
                    Some(input.clone()),
                    Some(candidate.row.clone()),
                    kind,
                    ordinal,
                );
                if other_outer {
                    if candidate.associations == 1 {
                        push_pair(
                            output,
                            1 - side,
                            Some(candidate.row.clone()),
                            None,
                            INSERT,
                            ordinal,
                        );
                    }
                    candidate.associations = candidate.associations.wrapping_sub(1);
                }
            }
        }
    }
    Ok(())
}

#[allow(clippy::too_many_arguments)]
#[cfg(test)]
fn process_semi_anti(
    inserted_id: u64,
    join_type: proto::RegularJoinType,
    side: usize,
    input_kind: i8,
    accumulate: bool,
    candidate_matches: &CandidateMatches,
    input: Arc<[u8]>,
    ordinal: i32,
    state: &mut JoinState,
    output: &mut Vec<OutputRow>,
) -> Result<()> {
    let anti = matches!(join_type, proto::RegularJoinType::Anti);
    if side == 0 {
        debug_assert_eq!(candidate_matches.len(), state.right.len());
        let matches = candidate_matches.iter().filter(|&matched| matched).count();
        if (anti && matches == 0) || (!anti && matches > 0) {
            output.push(OutputRow {
                left: Some(input.clone()),
                right: None,
                kind: input_kind,
                input_ordinal: ordinal,
            });
        }
        if accumulate {
            state.left.push(StoredRow {
                id: inserted_id,
                row: input,
                associations: matches as i32,
            });
        } else {
            if let Some(position) = state
                .left
                .iter()
                .position(|candidate| candidate.row == input)
            {
                state.left.remove(position);
            }
        }
        return Ok(());
    }
    if accumulate {
        state.right.push(StoredRow {
            id: inserted_id,
            row: input,
            associations: 0,
        });
        for (left, _) in state
            .left
            .iter_mut()
            .zip(candidate_matches.iter())
            .filter(|(_, matched)| *matched)
        {
            if left.associations == 0 {
                output.push(OutputRow {
                    left: Some(left.row.clone()),
                    right: None,
                    kind: if anti { DELETE } else { input_kind },
                    input_ordinal: ordinal,
                });
            }
            left.associations = left.associations.wrapping_add(1);
        }
    } else {
        if let Some(position) = state
            .right
            .iter()
            .position(|candidate| candidate.row == input)
        {
            state.right.remove(position);
        }
        for (left, _) in state
            .left
            .iter_mut()
            .zip(candidate_matches.iter())
            .filter(|(_, matched)| *matched)
        {
            if left.associations == 1 {
                output.push(OutputRow {
                    left: Some(left.row.clone()),
                    right: None,
                    kind: if anti { INSERT } else { input_kind },
                    input_ordinal: ordinal,
                });
            }
            left.associations = left.associations.wrapping_sub(1);
        }
    }
    Ok(())
}

pub(super) fn is_outer(join_type: proto::RegularJoinType, side: usize) -> bool {
    matches!(join_type, proto::RegularJoinType::Full)
        || (side == 0 && matches!(join_type, proto::RegularJoinType::Left))
        || (side == 1 && matches!(join_type, proto::RegularJoinType::Right))
}

#[cfg(test)]
fn push_pair(
    output: &mut Vec<OutputRow>,
    input_side: usize,
    input: Option<Arc<[u8]>>,
    other: Option<Arc<[u8]>>,
    kind: i8,
    ordinal: i32,
) {
    let (left, right) = if input_side == 0 {
        (input, other)
    } else {
        (other, input)
    };
    output.push(OutputRow {
        left,
        right,
        kind,
        input_ordinal: ordinal,
    });
}
