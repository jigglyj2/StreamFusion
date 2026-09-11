// Copyright 2026 StreamFusion Authors
// Licensed under the Apache License, Version 2.0.

use super::*;
use crate::memory_pool::HostMemoryReservation;

/// Populate an empty assigned key group from a stable, read-only checkpoint source.
/// Import is an initialization operation: a failure may leave earlier pages installed, so the
/// owning execution context must be discarded. It is never a live-state merge or transaction.
pub(crate) fn import_key_group(
    destination: &mut dyn KeyedState,
    source: &dyn KeyedState,
    group: u32,
    owner: &HostMemoryReservation,
    validate: &mut dyn FnMut(&[u8], &[u8]) -> Result<()>,
) -> Result<()> {
    super::canonical_restore::require_empty(destination, group, owner)?;
    source.visit_key_group_admitted(group, 1024, 256 << 10, owner, &mut |page| {
        for (key, value) in page {
            validate(key, value)?;
        }
        super::canonical_restore::write_page(destination, group, page, owner)
    })
}

#[cfg(test)]
mod tests;
