// Copyright 2026 StreamFusion Authors
// Licensed under the Apache License, Version 2.0.

//! Depth-first continuation of Flink's multi-join enumeration. Storage is proportional to
//! join depth, not Cartesian fan-out; each pull returns only positions in already loaded state.
use super::*;
pub(super) const ACTIVE: usize = usize::MAX;
struct Frame {
    index: usize,
    stage: u8,
    any_match: bool,
    associations: i64,
    active: bool,
    kind: i8,
}
impl Frame {
    fn new(active: bool, kind: i8) -> Self {
        Self {
            index: 0,
            stage: 0,
            any_match: false,
            associations: 0,
            active,
            kind,
        }
    }
}
pub(super) struct Cursor {
    frames: Vec<Frame>,
    joined: Vec<Option<usize>>,
    active_input: usize,
    ordinal: i32,
}
pub(super) struct Selection {
    pub(super) rows: Vec<Option<usize>>,
    pub(super) kind: i8,
    pub(super) ordinal: i32,
}
impl Cursor {
    pub(super) fn new(input: usize, kind: i8, ordinal: i32) -> Self {
        Self {
            frames: vec![Frame::new(false, kind)],
            joined: Vec::new(),
            active_input: input,
            ordinal,
        }
    }
    fn descend(&mut self, position: Option<usize>, active: bool, kind: i8) {
        self.joined.push(position);
        self.frames.push(Frame::new(active, kind));
    }
    fn finish(&mut self) {
        self.frames.pop();
        if !self.frames.is_empty() {
            self.joined.pop();
        }
    }
    pub(super) fn next(
        &mut self,
        plan: &proto::MultiJoin,
        state: &MultiJoinState,
        incoming: &StoredRow,
    ) -> Option<Selection> {
        while !self.frames.is_empty() {
            let depth = self.frames.len() - 1;
            if depth == state.inputs.len() {
                let frame = self.frames.last().unwrap();
                let result = frame.active.then(|| Selection {
                    rows: self.joined.clone(),
                    kind: frame.kind,
                    ordinal: self.ordinal,
                });
                self.finish();
                if result.is_some() {
                    return result;
                } else {
                    continue;
                }
            }
            let left = depth > 0 && plan.join_types[depth] == proto::RegularJoinType::Left as i32;
            let joined = self
                .joined
                .iter()
                .enumerate()
                .map(|(input, position)| {
                    position.map(|position| {
                        if position == ACTIVE {
                            incoming
                        } else {
                            &state.inputs[input][position]
                        }
                    })
                })
                .collect::<Vec<_>>();
            let frame = self.frames.last_mut().unwrap();
            let accumulate = matches!(frame.kind, INSERT | UPDATE_AFTER);
            let kind = frame.kind;
            let active = frame.active;
            match frame.stage {
                0 => {
                    if (!left && depth == self.active_input)
                        || frame.index == state.inputs[depth].len()
                    {
                        frame.stage = 1;
                        continue;
                    }
                    let position = frame.index;
                    frame.index += 1;
                    if !conditions_match(plan, depth, &joined, &state.inputs[depth][position]) {
                        continue;
                    }
                    frame.any_match = true;
                    if left {
                        frame.associations += if !active || accumulate { 1 } else { -1 };
                        if depth == self.active_input
                            && ((accumulate && frame.associations > 0)
                                || (!accumulate && frame.associations > 1))
                        {
                            frame.stage = 1;
                            continue;
                        }
                    }
                    if !active && depth == self.active_input {
                        continue;
                    }
                    self.descend(Some(position), active, kind);
                }
                1 => {
                    if depth == self.active_input {
                        if !conditions_match(plan, depth, &joined, incoming) {
                            frame.stage = 5;
                            continue;
                        }
                        if left {
                            frame.associations += if accumulate { 1 } else { -1 };
                        }
                        frame.stage = 2;
                        if accumulate && left && !frame.any_match {
                            self.descend(None, true, DELETE);
                        }
                    } else {
                        frame.stage = 5;
                        if left && !frame.any_match && frame.associations == 0 {
                            self.descend(None, active, kind);
                        }
                    }
                }
                2 => {
                    frame.stage = 3;
                    self.descend(Some(ACTIVE), true, kind);
                }
                3 => {
                    frame.stage = 5;
                    if !accumulate && left && frame.associations == 0 {
                        self.descend(None, true, INSERT);
                    }
                }
                _ => self.finish(),
            }
        }
        None
    }
}

#[cfg(test)]
mod tests;
