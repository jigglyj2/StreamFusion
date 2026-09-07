// Copyright 2026 StreamFusion Authors
// Licensed under the Apache License, Version 2.0

use super::*;

const CONDITION_BATCH_ROWS: usize = 4096;

/// Equality and null-rejected keys do not need a fan-out-sized bitmap. A residual keeps only
/// one input row's mask; its decoded Arrow pairs are evaluated in bounded vectorized chunks.
pub(super) struct CandidateMatches {
    len: usize,
    constant: bool,
    values: Option<Vec<bool>>,
    count: usize,
    _memory: HostMemoryReservation,
}

impl CandidateMatches {
    #[cfg(test)]
    pub(super) fn from_values(values: Vec<bool>, owner: &HostMemoryReservation) -> Self {
        let mut result = Self::constant(values.len(), false, owner);
        result._memory.try_grow(values.capacity()).unwrap();
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

    pub(super) fn constant(len: usize, value: bool, owner: &HostMemoryReservation) -> Self {
        Self {
            len,
            constant: value,
            values: None,
            count: if value { len } else { 0 },
            _memory: owner.sibling("regular join candidate mask"),
        }
    }

    pub(super) fn len(&self) -> usize {
        self.len
    }

    #[cfg(test)]
    pub(super) fn iter(&self) -> impl Iterator<Item = bool> + '_ {
        (0..self.len).map(|index| self.get(index))
    }

    pub(super) fn get(&self, index: usize) -> bool {
        self.values
            .as_ref()
            .map_or(self.constant, |values| values[index])
    }

    pub(super) fn count(&self) -> usize {
        self.count
    }
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
        let mut result =
            CandidateMatches::constant(candidates.len(), matchable, &self.scratch_reservation);
        let Some(condition) = &self.residual_condition else {
            return Ok(result);
        };
        if !matchable || candidates.is_empty() {
            return Ok(result);
        }
        result._memory.try_grow(candidates.len())?;
        let values = result.values.insert(vec![false; candidates.len()]);
        result.count = 0;
        let mut workspace = self
            .scratch_reservation
            .sibling("regular join residual Arrow chunk");
        for (chunk_index, chunk) in candidates.chunks(CONDITION_BATCH_ROWS).enumerate() {
            // Decoded columns, expression temporaries and array headers coexist. Admit before
            // converting Arrow rows, and retain the previous chunk's charge until it is dropped.
            let bytes = chunk.iter().try_fold(4096usize, |bytes, candidate| {
                bytes
                    .checked_add(input.len())
                    .and_then(|bytes| bytes.checked_add(candidate.row.len()))
                    .and_then(|bytes| bytes.checked_add(256))
                    .ok_or_else(|| {
                        DataFusionError::ResourcesExhausted(
                            "regular join residual workspace overflow".to_string(),
                        )
                    })
            })?;
            workspace.resize(bytes.saturating_mul(4))?;
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
            let offset = chunk_index * CONDITION_BATCH_ROWS;
            for index in 0..chunk.len() {
                values[offset + index] = !evaluated.is_null(index) && evaluated.value(index);
                result.count += usize::from(values[offset + index]);
            }
        }
        Ok(result)
    }
}
