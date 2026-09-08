// Copyright 2026 StreamFusion Authors
// Licensed under the Apache License, Version 2.0

use super::candidate_batch::{pair_workspace, MAX_PREDICATE_BYTES, PREDICATE_BASE_BYTES};
use super::*;

const CONDITION_BATCH_ROWS: usize = 4096;

/// Equality and null-rejected keys do not need a fan-out-sized bitmap. A residual keeps only
/// one input row's mask; its decoded Arrow pairs are evaluated in bounded vectorized chunks.
pub(super) struct CandidateMatches {
    len: usize,
    constant: bool,
    values: Option<Vec<bool>>,
    shared: Option<(Arc<SharedMatchMask>, usize)>,
    count: usize,
    _memory: Option<HostMemoryReservation>,
}

impl CandidateMatches {
    #[cfg(test)]
    pub(super) fn from_values(values: Vec<bool>, owner: &HostMemoryReservation) -> Self {
        let mut result = Self::constant(values.len(), false);
        let mut memory = owner.sibling("regular join candidate mask");
        memory.try_grow(values.capacity()).unwrap();
        result._memory = Some(memory);
        result.values = Some(values);
        result.count = result
            .values
            .as_ref()
            .unwrap()
            .iter()
            .filter(|&&v| v)
            .count();
        result
    }

    pub(super) fn constant(len: usize, value: bool) -> Self {
        Self {
            len,
            constant: value,
            values: None,
            shared: None,
            count: if value { len } else { 0 },
            _memory: None,
        }
    }

    pub(super) fn shared(len: usize, offset: usize, mask: Arc<SharedMatchMask>) -> Self {
        let mut result = Self::constant(len, false);
        result.count = mask.values[offset..offset + len]
            .iter()
            .filter(|&&value| value)
            .count();
        result.shared = Some((mask, offset));
        result
    }

    pub(super) fn len(&self) -> usize {
        self.len
    }

    #[cfg(test)]
    pub(super) fn iter(&self) -> impl Iterator<Item = bool> + '_ {
        (0..self.len).map(|index| self.get(index))
    }

    pub(super) fn get(&self, index: usize) -> bool {
        if let Some((mask, offset)) = &self.shared {
            mask.values[offset + index]
        } else {
            self.values
                .as_ref()
                .map_or(self.constant, |values| values[index])
        }
    }

    pub(super) fn count(&self) -> usize {
        self.count
    }
}

/// The mask and its batch descriptors share one coarse owner, including while a row's
/// transition spans several output pulls. No per-input-row reservation crosses JNI.
pub(super) struct SharedMatchMask {
    pub(super) values: Vec<bool>,
    pub(super) _memory: HostMemoryReservation,
}

impl RegularJoinProcessor {
    pub(super) fn condition_matches_row(
        &self,
        side: usize,
        batch: &RecordBatch,
        row: usize,
        input: &[u8],
        candidates: &[StoredRow],
    ) -> Result<CandidateMatches> {
        let matchable = self.row_is_matchable(side, batch, row);
        let mut result = CandidateMatches::constant(candidates.len(), matchable);
        let Some(condition) = &self.residual_condition else {
            return Ok(result);
        };
        if !matchable || candidates.is_empty() {
            return Ok(result);
        }
        let mut mask_memory = self
            .scratch_reservation
            .sibling("regular join candidate mask");
        mask_memory.try_grow(candidates.len())?;
        result._memory = Some(mask_memory);
        let values = result.values.insert(vec![false; candidates.len()]);
        result.count = 0;
        let mut workspace = self
            .scratch_reservation
            .sibling("regular join residual Arrow chunk");
        let mut offset = 0;
        while offset < candidates.len() {
            let mut end = offset;
            let mut bytes = PREDICATE_BASE_BYTES;
            while end < candidates.len() && end - offset < CONDITION_BATCH_ROWS {
                let next = bytes
                    .checked_add(pair_workspace(input.len(), candidates[end].row.len())?)
                    .ok_or_else(|| {
                        DataFusionError::ResourcesExhausted(
                            "regular join residual workspace overflow".into(),
                        )
                    })?;
                if end > offset && next > MAX_PREDICATE_BYTES {
                    break;
                }
                bytes = next;
                end += 1;
            }
            let chunk = &candidates[offset..end];
            // Bound both rows and bytes, preserving the same per-pair admission. A single
            // oversized pair is attempted once and returns a recoverable error if it cannot fit.
            workspace.resize(bytes)?;
            let mut columns = Vec::with_capacity(self.condition_schema.fields().len());
            for pair_side in 0..2 {
                let converter = &self.row_converters[pair_side];
                let parser = converter.parser();
                columns.extend(converter.convert_rows(chunk.iter().map(|candidate| {
                    parser.parse(if pair_side == side {
                        input
                    } else {
                        &candidate.row
                    })
                }))?);
            }
            let pairs = RecordBatch::try_new(Arc::clone(&self.condition_schema), columns)?;
            let evaluated = condition.evaluate(&pairs)?.into_array(chunk.len())?;
            let evaluated = evaluated
                .as_any()
                .downcast_ref::<BooleanArray>()
                .ok_or_else(|| {
                    DataFusionError::Execution(
                        "regular join residual condition did not evaluate to BooleanArray".into(),
                    )
                })?;
            for index in 0..chunk.len() {
                values[offset + index] = !evaluated.is_null(index) && evaluated.value(index);
                result.count += usize::from(values[offset + index]);
            }
            offset = end;
        }
        Ok(result)
    }
}
