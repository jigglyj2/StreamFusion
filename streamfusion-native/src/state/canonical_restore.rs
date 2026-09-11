// Copyright 2026 StreamFusion Authors
// Licensed under the Apache License, Version 2.0.

use super::*;
use crate::memory_pool::HostMemoryReservation;
use datafusion::error::DataFusionError;

/// Validate the complete old-format frame, then copy only one write page at a time. Restore is
/// initialization: on a backend/admission failure the owning context must be discarded.
pub(super) fn restore(
    destination: &mut dyn KeyedState,
    group: u32,
    bytes: &[u8],
    owner: &HostMemoryReservation,
) -> Result<()> {
    let entries = streamfusion_state_abi::key_group_snapshot_entries(group, bytes)
        .map_err(|error| DataFusionError::Execution(error.to_string()))?;
    require_empty(destination, group, owner)?;
    let mut entries = entries.peekable();
    while entries.peek().is_some() {
        // Fixed bounded descriptors borrow the admitted canonical input and copy no payload.
        let mut page = [(&[][..], &[][..]); 1024];
        let mut count = 0;
        let mut size = 0usize;
        while let Some(&(key, value)) = entries.peek() {
            let next = key.len().saturating_add(value.len());
            if count != 0 && size.saturating_add(next) > 256 << 10 {
                break;
            }
            page[count] = entries.next().unwrap();
            size = size.saturating_add(next);
            count += 1;
            if count == page.len() {
                break;
            }
        }
        write_page(destination, group, &page[..count], owner)?;
    }
    Ok(())
}

pub(crate) fn require_empty(
    destination: &dyn KeyedState,
    group: u32,
    owner: &HostMemoryReservation,
) -> Result<()> {
    let mut probe = owner.sibling("checkpoint destination validation");
    probe.resize(4096)?;
    destination.visit_key_group(group, 1, 4096, &mut |_| {
        Err(DataFusionError::Execution(format!(
            "key group {group} was restored more than once"
        )))
    })
}

pub(super) fn write_page(
    destination: &mut dyn KeyedState,
    group: u32,
    page: &[(&[u8], &[u8])],
    owner: &HostMemoryReservation,
) -> Result<()> {
    let mut workspace = owner.sibling("checkpoint state write page");
    let bytes = page.iter().try_fold(4096usize, |total, (key, value)| {
        key.len()
            .checked_add(value.len())
            .and_then(|payload| payload.checked_mul(4))
            .and_then(|payload| payload.checked_add(384))
            .and_then(|bytes| total.checked_add(bytes))
            .ok_or_else(|| {
                DataFusionError::ResourcesExhausted(
                    "checkpoint state write page size overflow".into(),
                )
            })
    })?;
    workspace.resize(bytes)?;
    destination.write_batch(
        page.iter()
            .map(|(key, value)| StateMutation {
                key: StateKey {
                    key_group: group,
                    key: key.to_vec(),
                },
                value: Some(value.to_vec()),
            })
            .collect(),
    )
}

#[cfg(test)]
mod tests;
