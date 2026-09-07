// Copyright 2026 StreamFusion Authors
// Licensed under the Apache License, Version 2.0.

use super::*;

const CONTROL_MAX_GROUPS: usize = 2048;

impl LocalGroupAggregateProcessor {
    pub(super) fn drain_native_bundle(&mut self) -> Result<Option<RecordBatch>> {
        if self.pending_order.is_empty() {
            self.pending_elements = 0;
            self.control_flushing = false;
            return Ok(None);
        }
        if !self.control_flushing {
            self.workspace.resize(
                self.pending_order
                    .len()
                    .saturating_mul(std::mem::size_of::<Vec<u8>>())
                    .saturating_add(4096),
            )?;
            self.pending_elements = 0;
            sort_flink_hashmap_keys(&mut self.pending_order, Vec::as_slice);
            self.pending_order.reverse();
            self.control_flushing = true;
        }
        let count = CONTROL_MAX_GROUPS.min(self.pending_order.len());
        let mut bytes = self
            .output_schema
            .fields()
            .len()
            .saturating_mul(4096)
            .saturating_add(64 * 1024);
        for key in &self.pending_order[self.pending_order.len() - count..] {
            let pending = self.pending.get(key).expect("local bundle key exists");
            bytes = bytes.saturating_add(
                pending
                    .grouping_row
                    .len()
                    .saturating_add(pending.accumulator.estimated_dynamic_bytes())
                    .saturating_add(self.calls.len().saturating_mul(2048))
                    .saturating_add(1024)
                    .saturating_mul(8),
            );
        }
        self.workspace.resize(bytes)?;
        let mut keys = Vec::with_capacity(count);
        let mut accumulators = Vec::with_capacity(count);
        for _ in 0..count {
            let key = self.pending_order.pop().expect("admitted local group");
            let pending = self.pending.remove(&key).expect("local order matches map");
            keys.push(pending.grouping_row);
            accumulators.push(encode_state(&pending.accumulator));
        }
        let output = self.output_batch(keys, accumulators)?;
        self.resize_reservation()?;
        Ok(Some(output))
    }
}
