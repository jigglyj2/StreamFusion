// Copyright 2026 StreamFusion Authors. Licensed under the Apache License, Version 2.0.

use super::*;

pub(in super::super) fn restore_timer_state(
    state: &mut dyn KeyedState,
    timers: &mut NativeTimerService,
    key_group: u32,
    bytes: &[u8],
    timer_state_key: &[u8],
    state_read_batches: &mut u64,
    owner: &HostMemoryReservation,
) -> Result<()> {
    state.restore_key_group(key_group, bytes, owner)?;
    reload_timers(
        state,
        timers,
        key_group,
        timer_state_key,
        state_read_batches,
        owner,
    )
}

/// Import Flink's stable RocksDB checkpoint without building a canonical whole-group copy.
/// The target is initialization-only and must be discarded after a failed partial import.
pub(in super::super) fn restore_timer_checkpoint(
    state: &mut dyn KeyedState,
    timers: &mut NativeTimerService,
    key_group: u32,
    source: &dyn KeyedState,
    timer_state_key: &[u8],
    state_read_batches: &mut u64,
    owner: &HostMemoryReservation,
) -> Result<()> {
    crate::state::import_key_group(state, source, key_group, owner, &mut |_, _| Ok(()))?;
    reload_timers(
        state,
        timers,
        key_group,
        timer_state_key,
        state_read_batches,
        owner,
    )
}

fn reload_timers(
    state: &dyn KeyedState,
    timers: &mut NativeTimerService,
    key_group: u32,
    timer_state_key: &[u8],
    state_read_batches: &mut u64,
    owner: &HostMemoryReservation,
) -> Result<()> {
    let timer = state.get_batch(
        &[StateKeyRef {
            key_group,
            key: timer_state_key,
        }],
        owner,
    )?;
    *state_read_batches = state_read_batches.saturating_add(1);
    if let Some(bytes) = timer.into_iter().next().flatten() {
        timers.restore_key_group(key_group, bytes.as_ref())?;
    }
    Ok(())
}

#[cfg(test)]
mod tests;
