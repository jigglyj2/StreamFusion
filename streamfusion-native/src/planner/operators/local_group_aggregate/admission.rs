// Copyright 2026 StreamFusion Authors
// Licensed under the Apache License, Version 2.0.

//! Admit borrowed-input derivatives before grouping, accumulator updates and output encoding.
use super::*;

impl LocalGroupAggregateProcessor {
    pub(super) fn batch_admission(&self, batch: &RecordBatch) -> Result<usize> {
        // Every arriving row may create a group and a sparse counted-map node per call.
        // Variable/nested keys and values may overlap as BinaryRows, Arrow rows, accumulator
        // values and serialized output. Pending input is flushed at most once: unlike the
        // global consumer, a local bundle never reloads historical state after its flush.
        let per_row = self
            .calls
            .len()
            .checked_mul(2048)
            .and_then(|bytes| {
                self.plan
                    .grouping_indices
                    .len()
                    .checked_mul(256)?
                    .checked_add(bytes)
            })
            .and_then(|bytes| bytes.checked_add(512))
            .ok_or_else(overflow)?;
        batch
            .get_array_memory_size()
            .checked_mul(16)
            .and_then(|bytes| batch.num_rows().checked_mul(per_row)?.checked_add(bytes))
            .and_then(|bytes| self.pending_bytes().checked_mul(8)?.checked_add(bytes))
            .and_then(|bytes| bytes.checked_add(64 * 1024))
            .ok_or_else(overflow)
    }

    pub(super) fn flush_admission(&self) -> Result<usize> {
        self.pending_bytes()
            .checked_mul(8)
            .and_then(|bytes| {
                self.output_schema
                    .fields()
                    .len()
                    .checked_mul(4096)?
                    .checked_add(bytes)
            })
            .and_then(|bytes| bytes.checked_add(64 * 1024))
            .ok_or_else(overflow)
    }

    pub(super) fn finish_legacy_output(
        &mut self,
        result: Result<RecordBatch>,
    ) -> Result<RecordBatch> {
        let result = result.and_then(|output| {
            // The payload already exists, under workspace admission. Move its credit without
            // a post-allocation reservation attempt, then use the existing compatibility edge.
            self.output_reservation = self.workspace.split(
                output.get_array_memory_size(),
                "native local group aggregate output",
            )?;
            self.output_reservation
                .transfer_to_arrow(output.get_array_memory_size())?;
            Ok(output)
        });
        if result.is_err() {
            // The task must replay input after a computation/transfer failure. Drop all
            // mutated payloads before releasing their workspace or retained-state credit.
            self.drop_failed_bundle()?;
        }
        self.workspace.resize(0)?;
        result
    }

    pub(super) fn drop_failed_bundle(&mut self) -> Result<()> {
        self.invocation = InvocationState::Failed;
        self.pending = HashMap::with_hasher(RandomState::new());
        self.pending_order = Vec::new();
        self.pending_elements = 0;
        self.control_flushing = false;
        self.pending_reservation.resize(0)?;
        self.output_reservation.resize(0)
    }
}

fn overflow() -> DataFusionError {
    DataFusionError::ResourcesExhausted("local aggregate workspace admission overflow".into())
}
