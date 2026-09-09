// Copyright 2026 StreamFusion Authors
// Licensed under the Apache License, Version 2.0.

use super::*;
use crate::memory_pool::HostMemoryReservation;
use datafusion::error::DataFusionError;

/// Populate an empty assigned key group from a stable, Flink-materialized RocksDB checkpoint.
/// Import is an initialization operation: a failure may leave earlier pages installed, so the
/// owning execution context must be discarded. It is never a live-state merge or transaction.
pub(crate) fn import_key_group(
    destination: &mut dyn KeyedState,
    source: &RocksPluginKeyedState,
    group: u32,
    owner: &HostMemoryReservation,
    validate: &mut dyn FnMut(&[u8], &[u8]) -> Result<()>,
) -> Result<()> {
    {
        let mut probe = owner.sibling("checkpoint destination validation");
        probe.resize(4096)?;
        destination.visit_key_group(group, 1, 4096, &mut |_| {
            Err(DataFusionError::Execution(format!(
                "key group {group} was restored more than once"
            )))
        })?;
    }
    source.visit_key_group_admitted(group, 1024, 256 << 10, owner, &mut |page| {
        for (key, value) in page {
            validate(key, value)?;
        }
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
    })
}

#[cfg(test)]
mod tests;
