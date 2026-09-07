// Copyright 2026 StreamFusion Authors
// Licensed under the Apache License, Version 2.0

//! Explicit Flink bundle drains, pulled through the common UnaryExec control interface.
//! No Java handoff or arrival ordinal is needed for a buffered result.

use super::*;

const CONTROL_MAX_GROUPS: usize = 2048;

impl GroupAggregateProcessor {
    pub(super) fn require_native_state_boundary(&self) -> Result<()> {
        self.invocation.require_idle("group aggregate")?;
        if self.pending_elements != 0 || self.control_flushing {
            return Err(DataFusionError::Execution(
                "native aggregate state boundary requires a completed bundle control drain".into(),
            ));
        }
        Ok(())
    }

    pub(super) fn drain_native_bundle(&mut self) -> Result<Option<RecordBatch>> {
        if self.pending_order.is_empty() {
            self.pending_elements = 0;
            self.control_flushing = false;
            return Ok(None);
        }
        if !self.control_flushing {
            // Flink resets numOfElements before invoking its bundle function, including on
            // a failing output path. Pending state itself remains guarded until drain EOF.
            self.pending_elements = 0;
            // Stable sorting preserves Flink bucket insertion order. Its temporary key vector
            // is separate from the retained pending map/order and needs its own allowance.
            self.scratch_reservation.resize(
                self.pending_order
                    .len()
                    .saturating_mul(std::mem::size_of::<StateKey>())
                    .saturating_add(4096),
            )?;
            sort_flink_hashmap_keys(&mut self.pending_order, |key| &key.key);
            self.pending_order.reverse();
            self.control_flushing = true;
        }
        let count = CONTROL_MAX_GROUPS.min(self.pending_order.len());
        let selected = &self.pending_order[self.pending_order.len() - count..];
        let allowance = self.control_output_admission(selected)?;
        self.scratch_reservation.resize(allowance)?;
        let mut events = BundleOutputEvents::new(self.calls.len());
        let mut mutations = Vec::with_capacity(count);
        for _ in 0..count {
            let key = self.pending_order.pop().expect("admitted control group");
            let group = self
                .pending
                .remove(&key)
                .expect("pending order matches state");
            let first = group
                .original
                .as_ref()
                .is_none_or(|state| state.row_count == 0);
            let Some(current) = group.current else {
                continue;
            };
            let previous = group
                .original
                .as_ref()
                .map(|state| state.values(&self.calls))
                .unwrap_or_else(|| vec![None; self.calls.len()]);
            if current.row_count == 0 {
                if !first {
                    events.push(group.grouping_row, DELETE, previous);
                    mutations.push(StateMutation { key, value: None });
                }
            } else {
                let values = current.values(&self.calls);
                if first {
                    events.push(group.grouping_row, INSERT, values);
                } else if previous != values {
                    if self.plan.generate_update_before {
                        events.push(group.grouping_row.clone(), UPDATE_BEFORE, previous);
                    }
                    events.push(group.grouping_row, UPDATE_AFTER, values);
                }
                mutations.push(StateMutation {
                    key,
                    value: Some(encode_state(&current)),
                });
            }
        }
        let output = self.bundle_output_batch(events)?;
        if output.get_array_memory_size() > allowance {
            return Err(DataFusionError::Internal(
                "group aggregate control output exceeded its pre-admitted capacity".into(),
            ));
        }
        // Attach the output lease before committing state; no post-commit allocation admission.
        let memory = self.scratch_reservation.split(
            output.get_array_memory_size(),
            "group aggregate control output",
        )?;
        let output = crate::memory_pool::arrow_lease::host_batch(output, memory)?;
        if !mutations.is_empty() {
            self.state.write_batch(mutations)?;
            self.state_write_batches = self.state_write_batches.saturating_add(1);
        }
        self.bundle_reservation
            .resize(self.estimated_pending_bytes())?;
        Ok(Some(output))
    }
}
