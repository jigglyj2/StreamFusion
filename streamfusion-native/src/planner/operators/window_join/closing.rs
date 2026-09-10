// Copyright 2026 StreamFusion Authors
// Licensed under the Apache License, Version 2.0.

//! One closed window at a time, with durable completion after its native output drains.

use super::*;
use crate::memory_pool::{arrow_lease::host_batch, buffer_size::batch_bytes};

pub(super) struct PendingWindow {
    key_group: u32,
    timer: TimerKey,
    deletes: Vec<StateMutation>,
    // Deletion keys and the copied timer stay admitted until durable completion.
    _memory: HostMemoryReservation,
}

pub(crate) struct ClosedWindow {
    /// SQL payloads in original per-side arrival order. Each batch owns its buffer allowance.
    pub(crate) inputs: [RecordBatch; 2],
}

impl WindowJoinProcessor {
    pub(super) fn require_idle(&self) -> Result<()> {
        if self.draining_watermark.is_some() || self.closing_failed {
            return Err(DataFusionError::Execution(
                "window join watermark output must drain before input, checkpoint, or restore"
                    .into(),
            ));
        }
        Ok(())
    }

    pub(crate) fn begin_watermark(&mut self, watermark: i64) -> Result<()> {
        self.require_idle()?;
        if watermark > self.current_event_time {
            self.current_event_time = watermark;
            self.draining_watermark = Some(watermark);
        }
        Ok(())
    }

    /// Decode only the next due window. Errors leave the drain active: the task must fail
    /// and recover its previous Flink checkpoint, never checkpoint partially emitted output.
    pub(crate) fn next_closed_window(&mut self) -> Result<Option<ClosedWindow>> {
        self.require_usable_drain()?;
        let result = self.load_closed_window();
        if result.is_err() {
            self.closing_failed = true;
        }
        result
    }

    fn require_usable_drain(&self) -> Result<()> {
        if self.closing_failed {
            return Err(DataFusionError::Execution(
                "window join drain failed; recover the Flink checkpoint".into(),
            ));
        }
        Ok(())
    }

    fn load_closed_window(&mut self) -> Result<Option<ClosedWindow>> {
        if self.pending_window.is_some() {
            return Err(DataFusionError::Execution(
                "window join must finish the current window before loading another".into(),
            ));
        }
        let Some(watermark) = self.draining_watermark else {
            return Ok(None);
        };
        let Some((key_group, timer)) = self.timers.next_due(TimerDomain::EventTime, watermark)
        else {
            self.draining_watermark = None;
            return Ok(None);
        };
        let mut workspace = self
            .scratch_reservation
            .sibling("window join closed-window decode");
        // Admit copies and the fixed-size header before touching payload state.
        workspace.resize(
            timer
                .key
                .len()
                .saturating_mul(8)
                .saturating_add(timer.namespace.len())
                .saturating_add(4096),
        )?;
        let timer = timer.clone();
        let keys = WindowKeys::new(
            &StateKey {
                key_group,
                key: timer.key.clone(),
            },
            &mut self.window_key_converter,
        )?;
        let mut values = self.state.get_batch(
            &[StateKeyRef {
                key_group,
                key: &keys.header.key,
            }],
            &workspace,
        )?;
        self.state_read_batches = self.state_read_batches.saturating_add(1);
        let header = values.pop().flatten().ok_or_else(|| {
            DataFusionError::Execution("window join timer has no window index".into())
        })?;
        let header = Header::decode(header.as_ref())?;
        let fields = self
            .visible_schemas
            .iter()
            .map(|s| s.fields().len())
            .sum::<usize>();
        workspace.try_grow(
            header
                .workspace_bound(&keys)
                .saturating_add(fields.saturating_mul(1024))
                .saturating_add(super::super::sortable_state::PAGE_BYTES),
        )?;
        let mut rows = Vec::new();
        let mut deletes = Vec::new();
        indexed_state::read_window(
            self.state.as_ref(),
            &keys,
            &header,
            0,
            &mut rows,
            &mut deletes,
        )?;
        self.state_read_batches = self.state_read_batches.saturating_add(1);
        deletes.push(StateMutation {
            key: keys.header,
            value: None,
        });
        let mut batches = Vec::with_capacity(2);
        for side in 0..2 {
            let converter = &self.row_converters[side];
            let parser = converter.parser();
            let columns = converter.convert_rows(
                rows.iter()
                    .filter(|(_, s, _)| *s == side as i8)
                    .map(|(_, _, row)| parser.parse(row)),
            )?;
            let batch = RecordBatch::try_new(self.visible_schemas[side].clone(), columns)?;
            let memory = workspace.split(batch_bytes(&batch)?, "closed window Arrow payload")?;
            batches.push(host_batch(batch, memory)?);
        }
        // Drop decode payloads before returning their workspace to Flink.
        drop(rows);
        let retained = deletes
            .iter()
            .fold(
                timer
                    .key
                    .capacity()
                    .saturating_add(timer.namespace.capacity()),
                |n, delete| n.saturating_add(delete.key.key.capacity()),
            )
            .saturating_add(
                deletes
                    .capacity()
                    .saturating_mul(std::mem::size_of::<StateMutation>()),
            );
        let pending_memory = workspace.split(retained, "window join pending completion")?;
        self.pending_window = Some(PendingWindow {
            key_group,
            timer,
            deletes,
            _memory: pending_memory,
        });
        let right = batches.pop().expect("right window batch");
        let left = batches.pop().expect("left window batch");
        Ok(Some(ClosedWindow {
            inputs: [left, right],
        }))
    }

    /// Call only after the DataFusion output stream reaches EOF. The shared execution guard
    /// must keep cancelled/failed invocations unusable; dropping output alone is not completion.
    pub(crate) fn finish_closed_window(&mut self) -> Result<()> {
        self.require_usable_drain()?;
        let result = self.commit_closed_window();
        if result.is_err() {
            self.closing_failed = true;
        }
        result
    }

    fn commit_closed_window(&mut self) -> Result<()> {
        let pending = self.pending_window.as_mut().ok_or_else(|| {
            DataFusionError::Execution("window join has no pending closed window".into())
        })?;
        self.state
            .write_batch(std::mem::take(&mut pending.deletes))?;
        self.state_write_batches = self.state_write_batches.saturating_add(1);
        if !self
            .timers
            .delete(pending.key_group, TimerDomain::EventTime, &pending.timer)?
        {
            return Err(DataFusionError::Execution(
                "window join pending timer disappeared".into(),
            ));
        }
        self.dirty_timer_groups.insert(pending.key_group);
        self.timers_fired = self.timers_fired.saturating_add(1);
        self.pending_window = None;
        Ok(())
    }
}

#[cfg(test)]
mod tests;
