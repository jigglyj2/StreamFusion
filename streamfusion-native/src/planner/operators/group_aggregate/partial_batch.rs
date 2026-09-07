// Copyright 2026 StreamFusion Authors
// Licensed under the Apache License, Version 2.0.

//! Opaque local accumulators enter the same batch-scoped working set as raw records.
//! Decode and validate the entire input before any pending group is mutated.

use super::*;

impl GroupAggregateProcessor {
    pub(super) fn process_partial_batch(
        &mut self,
        batch: RecordBatch,
        base: usize,
    ) -> Result<RecordBatch> {
        self.prepare_schema(batch.schema(), batch.num_columns())?;
        let accumulators = batch
            .column(self.visible_count.expect("prepared partial schema") - 1)
            .as_any()
            .downcast_ref::<BinaryArray>()
            .expect("validated partial accumulator type");
        if accumulators.null_count() != 0 {
            return Err(DataFusionError::Execution(
                "local aggregate accumulator cannot be null".into(),
            ));
        }
        let bytes = accumulators
            .iter()
            .try_fold(0usize, |bytes, value| {
                bytes.checked_add(value.unwrap().len())
            })
            .and_then(|bytes| bytes.checked_mul(80))
            .and_then(|bytes| {
                batch
                    .num_rows()
                    .checked_mul(
                        self.calls
                            .len()
                            .checked_mul(std::mem::size_of::<Accumulator>())?
                            .checked_mul(4)?
                            .checked_add(std::mem::size_of::<AccumulatorState>() + 128)?,
                    )?
                    .checked_add(bytes)
            })
            .and_then(|bytes| bytes.checked_add(64 * 1024))
            .ok_or_else(|| {
                DataFusionError::ResourcesExhausted(
                    "partial aggregate decode admission overflow".into(),
                )
            })?;
        // Packed values expand to sparse B-trees and vectors. Keep this credit alive
        // through merging, output construction and transfer to the retained bundle.
        let mut decoded = self
            .scratch_reservation
            .sibling("global aggregate decoded partials");
        decoded.resize(bytes)?;
        let partials = accumulators
            .iter()
            .map(|value| decode_state(value.unwrap(), &self.calls))
            .collect::<Result<Vec<_>>>()?;
        self.process_mini_input(batch, base, Some(&partials))
    }

    pub(super) fn partial_event_admission<'a>(
        &self,
        states: impl Iterator<Item = Option<&'a AccumulatorState>> + Clone,
        row_keys: &[usize],
        partials: &[AccumulatorState],
    ) -> Result<usize> {
        fn maximum(state: &AccumulatorState, index: usize) -> usize {
            match &state.accumulators[index] {
                Accumulator::AppendExtremum(value) => {
                    value.as_ref().map_or(0, AggregateValue::dynamic_bytes)
                }
                Accumulator::Extremum(values) => values
                    .keys()
                    .map(AggregateValue::dynamic_bytes)
                    .max()
                    .unwrap_or(0),
                _ => 0,
            }
        }
        let mut bytes = 0usize;
        for (index, call) in self.calls.iter().enumerate() {
            if call.output_type != DataType::Utf8 {
                continue;
            }
            let mut limits = states
                .clone()
                .map(|state| state.map_or(0, |state| maximum(state, index)))
                .collect::<Vec<_>>();
            for (row, key) in row_keys.iter().enumerate() {
                limits[*key] = limits[*key].max(maximum(&partials[row], index));
            }
            for key in row_keys {
                bytes = limits[*key]
                    .checked_mul(2)
                    .and_then(|n| bytes.checked_add(n))
                    .ok_or_else(|| {
                        DataFusionError::ResourcesExhausted(
                            "partial aggregate output admission overflow".into(),
                        )
                    })?;
            }
        }
        Ok(bytes)
    }
}
