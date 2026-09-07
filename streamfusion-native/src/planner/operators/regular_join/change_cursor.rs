// Copyright 2026 StreamFusion Authors
// Licensed under the Apache License, Version 2.0

use super::*;

/// Resumable Flink join transition. A candidate emits at most two changelog records; it is
/// advanced only when both fit, preserving null-padding retractions next to their joined row.
pub(super) struct ChangeCursor {
    join_type: proto::RegularJoinType,
    side: usize,
    kind: i8,
    accumulate: bool,
    input: Arc<[u8]>,
    ordinal: i32,
    candidate: usize,
    initialized: bool,
    finished: bool,
}

impl ChangeCursor {
    pub(super) fn new(
        join_type: proto::RegularJoinType,
        side: usize,
        kind: i8,
        accumulate: bool,
        input: Arc<[u8]>,
        ordinal: i32,
    ) -> Self {
        Self {
            join_type,
            side,
            kind,
            accumulate,
            input,
            ordinal,
            candidate: 0,
            initialized: false,
            finished: false,
        }
    }

    /// Returns true once the transition is complete. `limit` includes rows already in output.
    /// The caller admits descriptor/payload workspace before calling and retains staged state
    /// until the complete input batch has drained; no checkpoint may observe a partial change.
    pub(super) fn drain(
        &mut self,
        state: &mut JoinState,
        matches: &CandidateMatches,
        output: &mut Vec<OutputRow>,
        limit: usize,
    ) -> Result<bool> {
        if limit < 2 {
            return Err(DataFusionError::Internal(
                "regular join output limit must be at least two".into(),
            ));
        }
        if self.finished {
            return Ok(true);
        }
        let next_id = &mut state.next_row_id[self.side];
        let (own, other) = if self.side == 0 {
            (&mut state.left, &mut state.right)
        } else {
            (&mut state.right, &mut state.left)
        };
        if matches.len() != other.len() {
            return Err(DataFusionError::Internal(
                "regular join candidates changed during a transition".into(),
            ));
        }
        if !self.initialized {
            if !self.accumulate {
                // Flink ignores absent records but still emits the matching retract transition.
                if let Some(position) = own.iter().position(|row| row.row == self.input) {
                    own.remove(position);
                }
            }
            self.initialized = true;
        }
        let semi = matches!(
            self.join_type,
            proto::RegularJoinType::Semi | proto::RegularJoinType::Anti
        );
        let anti = self.join_type == proto::RegularJoinType::Anti;
        let own_outer = is_outer(self.join_type, self.side);
        let other_outer = is_outer(self.join_type, 1 - self.side);
        if !(semi && self.side == 0) {
            while self.candidate < other.len() {
                if !matches.get(self.candidate) {
                    self.candidate += 1;
                    continue;
                }
                if output.len().saturating_add(2) > limit {
                    return Ok(false);
                }
                let row = &mut other[self.candidate];
                if semi {
                    if row.associations == if self.accumulate { 0 } else { 1 } {
                        self.push(
                            output,
                            0,
                            Some(row.row.clone()),
                            None,
                            if anti {
                                if self.accumulate {
                                    DELETE
                                } else {
                                    INSERT
                                }
                            } else {
                                self.kind
                            },
                        );
                    }
                    row.associations = if self.accumulate {
                        row.associations.wrapping_add(1)
                    } else {
                        row.associations.wrapping_sub(1)
                    };
                } else if self.accumulate {
                    if other_outer {
                        if row.associations == 0 {
                            self.push(output, 1 - self.side, Some(row.row.clone()), None, DELETE);
                        }
                        row.associations = row.associations.wrapping_add(1);
                    }
                    self.push(
                        output,
                        self.side,
                        Some(self.input.clone()),
                        Some(row.row.clone()),
                        if own_outer || other_outer {
                            INSERT
                        } else {
                            self.kind
                        },
                    );
                } else {
                    self.push(
                        output,
                        self.side,
                        Some(self.input.clone()),
                        Some(row.row.clone()),
                        if own_outer { DELETE } else { self.kind },
                    );
                    if other_outer {
                        if row.associations == 1 {
                            self.push(output, 1 - self.side, Some(row.row.clone()), None, INSERT);
                        }
                        row.associations = row.associations.wrapping_sub(1);
                    }
                }
                self.candidate += 1;
            }
        }
        let count = matches.count();
        let emit_own = if semi && self.side == 0 {
            (count == 0) == anti
        } else {
            !semi && own_outer && count == 0
        };
        if emit_own {
            if output.len() >= limit {
                return Ok(false);
            }
            self.push(
                output,
                self.side,
                Some(self.input.clone()),
                None,
                if semi {
                    self.kind
                } else if self.accumulate {
                    INSERT
                } else {
                    DELETE
                },
            );
        }
        if self.accumulate {
            let id = *next_id;
            *next_id = id.checked_add(1).ok_or_else(|| {
                DataFusionError::Execution("regular join row identity exhausted".into())
            })?;
            own.push(StoredRow {
                id,
                row: self.input.clone(),
                associations: if own_outer || (semi && self.side == 0) {
                    count as i32
                } else {
                    0
                },
            });
        }
        self.finished = true;
        Ok(true)
    }

    fn push(
        &self,
        output: &mut Vec<OutputRow>,
        side: usize,
        own: Option<Arc<[u8]>>,
        other: Option<Arc<[u8]>>,
        kind: i8,
    ) {
        let (left, right) = if side == 0 {
            (own, other)
        } else {
            (other, own)
        };
        output.push(OutputRow {
            left,
            right,
            kind,
            input_ordinal: self.ordinal,
        });
    }
}
