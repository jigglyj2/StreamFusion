// Copyright 2026 StreamFusion Authors
// Licensed under the Apache License, Version 2.0.

use super::*;

impl LocalWindowAggregateProcessor {
    /// Buffered execution retains compact DataFusion group vectors and an
    /// ordered key index. It borrows input arrays and emits partials separately at a
    /// Flink buffer/control boundary, so the eager path's input-copy and serialized-
    /// output allowance is not needed here. Reserve growth and batch scratch together;
    /// the buffered owner adds variable key payload credit from logical Arrow spans.
    pub(super) fn buffered_batch_admission(&self, rows: usize) -> Result<usize> {
        let per_row = self
            .calls
            .len()
            .checked_mul(64)
            .and_then(|bytes| {
                self.plan
                    .grouping_indices
                    .len()
                    .checked_mul(32)?
                    .checked_add(bytes)
            })
            // Hash-table/order-vector growth, selection indices, row offsets and counts.
            // The key term includes owned index keys and the Arrow row-encoding buffer.
            .and_then(|bytes| bytes.checked_add(256))
            .ok_or_else(overflow)?;
        rows.checked_mul(per_row)
            .and_then(|bytes| bytes.checked_add(64 * 1024))
            .ok_or_else(overflow)
    }

    /// Admit derived Arrow selections, row encodings, hash entries, accumulator deltas and
    /// serialized partials together. Count variable-width input once per use: several calls
    /// may retain the same column independently. The borrowed input remains producer-owned.
    pub(super) fn batch_admission(&self, batch: &RecordBatch) -> Result<usize> {
        // Grouped DataFusion vectors and canonical partial bytes have bounded per-call
        // storage. The ordered fallback also allows counted extrema and richer states.
        let per_call = if self.grouped_compute.is_some() {
            256
        } else {
            2048
        };
        let per_row = self
            .calls
            .len()
            .checked_mul(per_call)
            .and_then(|bytes| {
                self.plan
                    .grouping_indices
                    .len()
                    .checked_mul(256)?
                    .checked_add(bytes)
            })
            .and_then(|bytes| bytes.checked_add(512))
            .ok_or_else(overflow)?;
        let mut bytes = batch
            .get_array_memory_size()
            .checked_mul(4)
            .and_then(|bytes| batch.num_rows().checked_mul(per_row)?.checked_add(bytes))
            .and_then(|bytes| bytes.checked_add(64 * 1024))
            .ok_or_else(overflow)?;
        for index in self
            .plan
            .grouping_indices
            .iter()
            .map(|&i| i as usize)
            .chain(self.calls.iter().filter_map(|call| call.input_index))
        {
            let column = batch.columns().get(index).ok_or_else(|| {
                DataFusionError::Plan("local window referenced column is outside its input".into())
            })?;
            bytes = column
                .get_array_memory_size()
                .checked_mul(8)
                .and_then(|size| bytes.checked_add(size))
                .ok_or_else(overflow)?;
        }
        Ok(bytes)
    }

    pub(super) fn finish_legacy_output(
        &mut self,
        result: Result<RecordBatch>,
    ) -> Result<RecordBatch> {
        let result = result.and_then(|output| {
            let bytes = output.get_array_memory_size();
            // Move pre-admitted output credit to the compatibility C Data edge. No new
            // budget request after allocating or serializing the partials.
            self.output_reservation = self
                .reservation
                .split(bytes, "local window partial output")?;
            self.output_reservation.transfer_to_arrow(bytes)?;
            Ok(output)
        });
        if result.is_err() {
            // A failed grouped update may retain scratch vectors. Drop those buffers before
            // returning workspace credit. The enclosing Flink task will recover/replay.
            self.grouped_compute = None;
        }
        self.output_reservation.resize(0)?;
        self.reservation.resize(0)?;
        result
    }
}

fn overflow() -> DataFusionError {
    DataFusionError::ResourcesExhausted("local window workspace admission overflow".into())
}
