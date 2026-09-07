// Copyright 2026 StreamFusion Authors
// Licensed under the Apache License, Version 2.0

use super::*;

impl GroupAggregateProcessor {
    /// Pending accumulators remain charged to the bundle while it is drained. Only their
    /// visible scalar payload and serialized dirty mutations need additional workspace;
    /// multiplying the retained B-tree node capacity counts memory that is never copied.
    pub(super) fn control_output_admission(&self, selected: &[StateKey]) -> Result<usize> {
        let overflow = || {
            DataFusionError::ResourcesExhausted("group aggregate control admission overflow".into())
        };
        let columns = self
            .plan
            .grouping_indices
            .len()
            .checked_add(self.calls.len())
            .and_then(|n| n.checked_add(3))
            .ok_or_else(overflow)?;
        let base = selected
            .len()
            .checked_mul(columns)
            .and_then(|n| n.checked_mul(256))
            .and_then(|n| columns.checked_mul(4096)?.checked_add(n))
            .ok_or_else(overflow)?;
        selected.iter().try_fold(base, |bytes, key| {
            let group = &self.pending[key];
            let visible = group
                .original
                .as_ref()
                .map_or(0, |state| state.visible_payload_bytes(&self.calls))
                .checked_add(
                    group
                        .current
                        .as_ref()
                        .map_or(0, |state| state.visible_payload_bytes(&self.calls)),
                )
                .and_then(|n| n.checked_add(group.grouping_row.len()))
                .and_then(|n| n.checked_mul(8))
                .ok_or_else(overflow)?;
            let serialized = group
                .current
                .as_ref()
                .filter(|state| state.row_count != 0)
                .map_or(0, state_codec::encoded_state_size);
            // Vec's geometric growth may retain nearly twice the serialized length, with
            // the previous buffer also alive during realloc. The codec sizing pass allocates nothing.
            serialized
                .checked_mul(3)
                .and_then(|n| n.checked_add(visible))
                .and_then(|n| bytes.checked_add(n))
                .ok_or_else(overflow)
        })
    }

    /// Admit accumulator vectors/serialized mutations and sparse B-tree nodes before decoding
    /// or inserting. Only distinct keys need initial-node credit; each row can add at most one
    /// counted value per call. Variable payloads are covered by input/historical allowances.
    pub(super) fn accumulator_admission(&self, keys: usize, rows: usize) -> Result<usize> {
        let maps = self
            .calls
            .iter()
            .filter(|call| {
                call.distinct
                    || (call.retractable
                        && matches!(
                            call.function,
                            proto::AggregateFunction::Min | proto::AggregateFunction::Max
                        ))
            })
            .count();
        let overflow = || {
            DataFusionError::ResourcesExhausted(
                "group aggregate accumulator admission overflow".into(),
            )
        };
        let vectors = keys
            .checked_mul(self.calls.len())
            .and_then(|n| n.checked_mul(std::mem::size_of::<Accumulator>() + 64))
            .ok_or_else(overflow)?;
        let trees = keys
            .checked_mul(accumulator::COUNTED_MAP_BASE_BYTES)
            .and_then(|n| {
                rows.checked_mul(accumulator::counted_map_entry_bytes())?
                    .checked_add(n)
            })
            .and_then(|n| n.checked_mul(maps))
            .ok_or_else(overflow)?;
        vectors.checked_add(trees).ok_or_else(overflow)
    }

    /// Borrowed Arrow input is not charged again. This additional allowance covers native
    /// BinaryRow key encodings and accumulator/mutation copies derived from those buffers.
    pub(super) fn input_admission(&self, input: &RecordBatch) -> Result<usize> {
        self.plan
            .grouping_indices
            .iter()
            .map(|index| *index as usize)
            .chain(
                self.calls
                    .iter()
                    .filter(|call| {
                        !self.partial_input
                            && (call.function != proto::AggregateFunction::Count || call.distinct)
                    })
                    .filter_map(|call| call.input_index),
            )
            .try_fold(0usize, |total, index| {
                input
                    .column(index)
                    .get_array_memory_size()
                    .checked_mul(4)
                    .and_then(|bytes| total.checked_add(bytes))
                    .ok_or_else(|| {
                        DataFusionError::ResourcesExhausted(
                            "group aggregate input admission overflow".into(),
                        )
                    })
            })
    }
    /// Synchronous aggregation emits at most two events per input ordinal. Four times each
    /// grouping array's retained capacity covers that gather plus builder growth/alignment,
    /// including nested child buffers and dictionary values. Aggregate values may come from
    /// historical state, so their byte payload must be counted independently of the input.
    /// This credit is additional to the live key/state/event workspace, never a replacement.
    pub(super) fn output_admission(
        &self,
        input: &RecordBatch,
        events: &OutputEvents,
    ) -> Result<usize> {
        let overflow = || {
            DataFusionError::ResourcesExhausted("group aggregate output admission overflow".into())
        };
        let rows = events.input_rows.len();
        let columns = self
            .plan
            .grouping_indices
            .len()
            .checked_add(self.calls.len())
            .and_then(|n| n.checked_add(2))
            .ok_or_else(overflow)?;
        // Array descriptors, validity/offset buffer alignment, index/kind/ordinal buffers,
        // and temporary scalar option vectors used by the aggregate Arrow builders.
        let mut bytes = columns
            .checked_mul(4096)
            .and_then(|n| rows.checked_mul(columns)?.checked_mul(64)?.checked_add(n))
            .ok_or_else(overflow)?;
        for &index in &self.plan.grouping_indices {
            bytes = input
                .column(index as usize)
                .get_array_memory_size()
                .checked_mul(4)
                .and_then(|n| bytes.checked_add(n))
                .ok_or_else(overflow)?;
        }
        for values in &events.values {
            for value in values {
                if let Some(AggregateValue::Bytes(value)) = value {
                    bytes = value
                        .len()
                        .checked_mul(2)
                        .and_then(|n| bytes.checked_add(n))
                        .ok_or_else(overflow)?;
                }
            }
        }
        Ok(bytes)
    }
}

impl GroupAggregateProcessor {
    /// One batch admission, not a JNI memory-budget call per row. Any retained extremum
    /// can become visible after retractions. Bound both event copies by the largest old or
    /// incoming payload for that key/call, weighted by the batch's actual key occurrences.
    /// Numeric/count/average results have no heap payload and need no scan or extra credit.
    pub(super) fn event_admission(
        &self,
        input: &RecordBatch,
        states: &[Option<AccumulatorState>],
        row_keys: &[usize],
    ) -> Result<usize> {
        self.event_admission_iter(input, states.iter().map(Option::as_ref), row_keys)
    }

    pub(super) fn event_admission_iter<'a>(
        &self,
        input: &RecordBatch,
        states: impl Iterator<Item = Option<&'a AccumulatorState>> + Clone,
        row_keys: &[usize],
    ) -> Result<usize> {
        let mut bytes = 0usize;
        for (index, call) in self.calls.iter().enumerate() {
            if call.output_type != DataType::Utf8 {
                continue;
            }
            let mut maximum = states
                .clone()
                .map(|state| {
                    state.map_or(0, |state| match &state.accumulators[index] {
                        Accumulator::AppendExtremum(value) => {
                            value.as_ref().map_or(0, AggregateValue::dynamic_bytes)
                        }
                        Accumulator::Extremum(values) => values
                            .keys()
                            .map(AggregateValue::dynamic_bytes)
                            .max()
                            .unwrap_or(0),
                        _ => 0,
                    })
                })
                .collect::<Vec<_>>();
            let values = input
                .column(call.input_index.expect("string extremum input"))
                .as_any()
                .downcast_ref::<StringArray>()
                .ok_or_else(|| {
                    DataFusionError::Execution("string extremum requires Utf8 input".into())
                })?;
            for (row, key) in row_keys.iter().enumerate() {
                if !values.is_null(row) {
                    maximum[*key] = maximum[*key].max(values.value(row).len());
                }
            }
            for key in row_keys {
                bytes = maximum[*key]
                    .checked_mul(2)
                    .and_then(|n| bytes.checked_add(n))
                    .ok_or_else(|| {
                        DataFusionError::ResourcesExhausted(
                            "group aggregate historical event admission overflow".into(),
                        )
                    })?;
            }
        }
        Ok(bytes)
    }
}

#[cfg(test)]
mod tests;
