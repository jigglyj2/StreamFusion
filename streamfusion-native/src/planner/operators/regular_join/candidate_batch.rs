// Copyright 2026 StreamFusion Authors
// Licensed under the Apache License, Version 2.0

use super::candidates::SharedMatchMask;
use super::*;
use std::collections::VecDeque;

const MAX_PAIRS: usize = 4096;
// A row-count limit alone permits very large materializations for wide payloads. This is an
// internal vectorization quantum, not a memory-budget override. One oversized pair still has
// to acquire its full reservation in the single-row path.
pub(super) const MAX_PREDICATE_BYTES: usize = 8 << 20;
pub(super) const PREDICATE_BASE_BYTES: usize = 4 * 4096;
pub(super) fn pair_workspace(input: usize, candidate: usize) -> Result<usize> {
    input
        .checked_add(candidate)
        .and_then(|n| n.checked_add(256))
        .and_then(|n| n.checked_mul(4))
        .ok_or_else(|| {
            DataFusionError::ResourcesExhausted("regular join predicate workspace overflow".into())
        })
}

#[derive(Default)]
pub(super) struct CandidateBatch {
    rows: VecDeque<CandidateMatches>,
    // Also retains descriptor admission when every cached row has a constant/null mask.
    _mask: Option<Arc<SharedMatchMask>>,
}

impl CandidateBatch {
    pub(super) fn is_empty(&self) -> bool {
        self.rows.is_empty()
    }
    pub(super) fn pop(&mut self) -> Option<CandidateMatches> {
        self.rows.pop_front()
    }
}

fn candidates(state: &StagedState, side: usize) -> &[StoredRow] {
    if side == 0 {
        &state.value.right
    } else {
        &state.value.left
    }
}

impl RegularJoinProcessor {
    /// One incoming batch belongs to one input port. Its transitions may update opposite-side
    /// association counts, but never insert/delete/reorder opposite payloads. Predicate results
    /// can therefore be prepared across input rows, then consumed in the original changelog order.
    /// Cap both input descriptors and candidate pairs; a single larger fan-out uses the existing
    /// row mask with bounded expression chunks instead of materializing the full join product.
    pub(super) fn condition_matches_batch(
        &self,
        side: usize,
        batch: &RecordBatch,
        encoded: &Rows,
        staged: &[StagedState],
        indices: &[usize],
        start: usize,
    ) -> Result<CandidateBatch> {
        let condition = self
            .residual_condition
            .as_ref()
            .expect("residual cache only");
        let mut row_limit = MAX_PAIRS;
        let (end, count, retained, workspace) = loop {
            let mut end = start;
            let mut count = 0usize;
            let mut workspace_bytes = PREDICATE_BASE_BYTES;
            while end < batch.num_rows() && end - start < row_limit {
                let len = if self.row_is_matchable(side, batch, end) {
                    candidates(&staged[indices[end]], side).len()
                } else {
                    0
                };
                if len > MAX_PAIRS - count {
                    break;
                }
                let row_bytes = if len == 0 {
                    0
                } else {
                    candidates(&staged[indices[end]], side).iter().try_fold(
                        0usize,
                        |bytes, candidate| {
                            bytes
                                .checked_add(pair_workspace(
                                    encoded.row(end).data().len(),
                                    candidate.row.len(),
                                )?)
                                .ok_or_else(|| {
                                    DataFusionError::ResourcesExhausted(
                                        "regular join predicate workspace overflow".into(),
                                    )
                                })
                        },
                    )?
                };
                let next_bytes = workspace_bytes.checked_add(row_bytes).ok_or_else(|| {
                    DataFusionError::ResourcesExhausted(
                        "regular join predicate workspace overflow".into(),
                    )
                })?;
                if next_bytes > MAX_PREDICATE_BYTES {
                    break;
                }
                workspace_bytes = next_bytes;
                count += len;
                end += 1;
            }
            if end == start {
                return Ok(CandidateBatch::default());
            }
            let overflow = || {
                DataFusionError::ResourcesExhausted("regular join candidate batch overflow".into())
            };
            let mut retained = self
                .scratch_reservation
                .sibling("regular join batch predicate masks");
            let mask_bytes = (end - start)
                .checked_mul(std::mem::size_of::<CandidateMatches>())
                .and_then(|bytes| bytes.checked_add(count + 4096))
                .ok_or_else(overflow)?;
            let mut workspace = self
                .scratch_reservation
                .sibling("regular join batch predicate Arrow workspace");
            match retained
                .resize(mask_bytes)
                .and_then(|_| workspace.resize(if count == 0 { 0 } else { workspace_bytes }))
            {
                Ok(()) => break (end, count, retained, workspace),
                Err(DataFusionError::ResourcesExhausted(_)) => {
                    // No arrays or state transitions have been materialized yet. Reduce actual
                    // vectorized work under memory pressure, keeping every pair's full charge.
                    // A single input's larger fan-out uses the row path's adaptive chunks.
                    if end - start == 1 {
                        return Ok(CandidateBatch::default());
                    }
                    row_limit = (end - start) / 2;
                }
                Err(error) => return Err(error),
            }
        };
        let mut values = Vec::with_capacity(count);
        if count != 0 {
            let mut pairs = Vec::with_capacity(count);
            for row in start..end {
                if self.row_is_matchable(side, batch, row) {
                    pairs.extend(
                        (0..candidates(&staged[indices[row]], side).len())
                            .map(|candidate| (row, candidate)),
                    );
                }
            }
            let contiguous = pairs.windows(2).all(|pair| pair[1].0 == pair[0].0 + 1);
            let input_indices = if contiguous {
                None
            } else {
                Some(UInt32Array::from(
                    pairs
                        .iter()
                        .map(|(row, _)| {
                            u32::try_from(*row).map_err(|_| {
                                DataFusionError::Execution(
                                    "regular join input exceeds UInt32 ordinals".into(),
                                )
                            })
                        })
                        .collect::<Result<Vec<_>>>()?,
                ))
            };
            let mut columns = Vec::with_capacity(self.condition_schema.fields().len());
            for pair_side in 0..2 {
                if pair_side == side {
                    // Reuse the incoming Arrow columns; only repeated/noncontiguous pair indices
                    // require a gather. Do not serialize and decode the input payload again.
                    columns.extend(
                        batch.columns()[..self.visible_schemas[side].fields().len()]
                            .iter()
                            .map(|column| match &input_indices {
                                Some(indices) => take(column.as_ref(), indices, None),
                                None => Ok(column.slice(pairs[0].0, count)),
                            })
                            .collect::<Result<Vec<_>, _>>()?,
                    );
                } else {
                    let converter = &self.row_converters[pair_side];
                    let parser = converter.parser();
                    columns.extend(converter.convert_rows(pairs.iter().map(
                        |(row, candidate)| {
                            parser.parse(&candidates(&staged[indices[*row]], side)[*candidate].row)
                        },
                    ))?);
                }
            }
            let pairs = RecordBatch::try_new(Arc::clone(&self.condition_schema), columns)?;
            let evaluated = condition.evaluate(&pairs)?.into_array(count)?;
            let evaluated = evaluated
                .as_any()
                .downcast_ref::<BooleanArray>()
                .ok_or_else(|| {
                    DataFusionError::Execution(
                        "regular join residual condition did not evaluate to BooleanArray".into(),
                    )
                })?;
            values.extend(
                (0..count).map(|index| !evaluated.is_null(index) && evaluated.value(index)),
            );
        }
        drop(workspace);
        let mask = Arc::new(SharedMatchMask {
            values,
            _memory: retained,
        });
        let mut rows = VecDeque::with_capacity(end - start);
        let mut offset = 0;
        for row in start..end {
            let len = candidates(&staged[indices[row]], side).len();
            if self.row_is_matchable(side, batch, row) && len != 0 {
                rows.push_back(CandidateMatches::shared(len, offset, Arc::clone(&mask)));
                offset += len;
            } else {
                rows.push_back(CandidateMatches::constant(len, false));
            }
        }
        Ok(CandidateBatch {
            rows,
            _mask: Some(mask),
        })
    }
}
