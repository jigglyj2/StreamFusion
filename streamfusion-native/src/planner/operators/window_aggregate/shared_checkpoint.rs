// Copyright 2026 StreamFusion Authors
// Licensed under the Apache License, Version 2.0.

use super::*;
use crate::state::StateBatchWriter;

/// Persist control records before Flink takes the backend checkpoint. Keep only one bounded
/// write page, rather than encoding the timers of every key group simultaneously. A single
/// group's timer record retains its canonical encoding and must still fit the managed budget.
pub(super) fn flush_timers(
    kernel: &mut WindowAggregateProcessor,
    groups: std::ops::RangeInclusive<u32>,
    marker_key: &[u8],
    marker: &[u8],
) -> Result<()> {
    let mut memory = kernel
        .scratch_reservation
        .sibling("window checkpoint control page");
    // Preserve existing operator statistics: checkpoint control writes are not input batches.
    let mut writes = 0;
    let mut writer = StateBatchWriter::new(kernel.state.as_mut(), &mut memory, 0, &mut writes)?;
    for group in groups {
        let bytes = kernel.timers.snapshot_size(group)?;
        writer.push(TIMER_STATE_KEY.len().saturating_add(bytes), 0, || {
            Ok(StateMutation {
                key: StateKey {
                    key_group: group,
                    key: TIMER_STATE_KEY.to_vec(),
                },
                value: Some(kernel.timers.snapshot_key_group(group)?),
            })
        })?;
        writer.push(marker_key.len().saturating_add(marker.len()), 0, || {
            Ok(StateMutation {
                key: StateKey {
                    key_group: group,
                    key: marker_key.to_vec(),
                },
                value: Some(marker.to_vec()),
            })
        })?;
    }
    writer.finish()
}

#[cfg(test)]
mod tests;
