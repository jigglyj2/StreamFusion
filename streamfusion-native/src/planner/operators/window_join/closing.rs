// Copyright 2026 StreamFusion Authors
// Licensed under the Apache License, Version 2.0.

//! Ordered left pages from one closed window, acknowledged after native output drains.

use super::*;
use crate::memory_pool::{arrow_lease::host_batch, buffer_size::batch_bytes};

mod pages;

pub(super) struct PendingWindow {
    key_group: u32,
    timer: TimerKey,
    keys: WindowKeys,
    header: Header,
    right: pages::Decoded,
    left_cursor: u64,
    seen_bytes: u64,
    in_flight_rows: Option<(u64, pages::PayloadEntries)>,
    // The cursor is invocation-local. No checkpoint may observe a partly closed window.
    _memory: HostMemoryReservation,
}

pub(crate) struct ClosedWindow {
    /// SQL payloads in original per-side arrival order. Each batch owns its buffer allowance.
    pub(crate) inputs: [RecordBatch; 2],
    pub(super) max_row_bytes: [usize; 2],
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

    /// Decode the next left page against a complete right window. Errors leave the drain active: the task must fail
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
        if self
            .pending_window
            .as_ref()
            .is_some_and(|p| p.in_flight_rows.is_some())
        {
            return Err(DataFusionError::Execution(
                "window join must finish the current page before loading another".into(),
            ));
        }
        if self.pending_window.is_none() && !self.open_closed_window()? {
            return Ok(None);
        }
        let mut pending = self.pending_window.take().expect("opened window");
        let left = pages::decode(
            self,
            &pending.keys,
            &pending.header,
            0,
            pending.left_cursor,
            false,
        )?;
        pending.seen_bytes = pending
            .seen_bytes
            .checked_add(left.bytes)
            .ok_or_else(pages::invalid_index)?;
        if pending.seen_bytes > pending.header.bytes() {
            return Err(pages::invalid_index());
        }
        pending.in_flight_rows = Some((left.batch.num_rows() as u64, left.entries));
        let closed = ClosedWindow {
            inputs: [left.batch, pending.right.batch.clone()],
            max_row_bytes: [left.max_row_bytes, pending.right.max_row_bytes],
        };
        self.pending_window = Some(pending);
        Ok(Some(closed))
    }

    fn open_closed_window(&mut self) -> Result<bool> {
        let Some(watermark) = self.draining_watermark else {
            return Ok(false);
        };
        let Some((key_group, timer)) = self.timers.next_due(TimerDomain::EventTime, watermark)
        else {
            self.draining_watermark = None;
            return Ok(false);
        };
        let mut memory = self
            .scratch_reservation
            .sibling("window join pending completion");
        memory.resize(
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
            &memory,
        )?;
        self.state_read_batches = self.state_read_batches.saturating_add(1);
        let value = values.pop().flatten().ok_or_else(|| {
            DataFusionError::Execution("window join timer has no window index".into())
        })?;
        let header = Header::decode(value.as_ref())?;
        drop(value);
        let right = pages::decode(self, &keys, &header, 1, 0, true)?;
        self.pending_window = Some(PendingWindow {
            key_group,
            timer,
            keys,
            header,
            seen_bytes: right.bytes,
            right,
            left_cursor: 0,
            in_flight_rows: None,
            _memory: memory,
        });
        Ok(true)
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
        let mut pending = self.pending_window.take().ok_or_else(|| {
            DataFusionError::Execution("window join has no pending closed window".into())
        })?;
        let (rows, entries) = pending.in_flight_rows.take().ok_or_else(|| {
            DataFusionError::Execution("window join has no output page to acknowledge".into())
        })?;
        let next = pending
            .left_cursor
            .checked_add(rows)
            .ok_or_else(pages::invalid_index)?;
        let finished = next == pending.header.counts()[0];
        if finished {
            // A final bounded lookahead rejects extra payloads beyond the header's count.
            // Do this before mutation, including for an empty left input.
            let tail = pages::decode(self, &pending.keys, &pending.header, 0, next, false)?;
            if tail.batch.num_rows() != 0 || pending.seen_bytes != pending.header.bytes() {
                return Err(pages::invalid_index());
            }
        }
        // The page's DataFusion output reached EOF. Reclaim just these payloads now;
        // input/checkpoint/restore remain blocked throughout the watermark invocation.
        // Failure or cancellation can only recover the previous Flink checkpoint.
        pages::delete_rows(self, &pending.keys, 0, &entries)?;
        if !finished {
            pending.left_cursor = next;
            self.pending_window = Some(pending);
            return Ok(());
        }
        pages::delete_rows(self, &pending.keys, 1, &pending.right.entries)?;
        self.state.write_batch(vec![StateMutation {
            key: pending.keys.header,
            value: None,
        }])?;
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
        Ok(())
    }
}

#[cfg(test)]
mod tests;
