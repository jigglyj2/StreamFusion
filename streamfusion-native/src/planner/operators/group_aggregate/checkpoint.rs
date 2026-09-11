// Copyright 2026 StreamFusion Authors
// Licensed under the Apache License, Version 2.0.

use super::*;

impl GroupAggregateProcessor {
    pub(crate) fn state_memory(&self) -> HostMemoryReservation {
        self.scratch_reservation.sibling("native state transfer")
    }

    pub(crate) fn snapshot_key_group(&self, key_group: u32) -> Result<crate::state::SnapshotBytes> {
        self.invocation.require_idle("group aggregate")?;
        self.state
            .snapshot_key_group(key_group, &self.scratch_reservation)
    }

    pub(crate) fn restore_key_group(&mut self, key_group: u32, bytes: &[u8]) -> Result<()> {
        self.invocation.require_idle("group aggregate")?;
        for (_, value) in streamfusion_state_abi::key_group_snapshot_entries(key_group, bytes)
            .map_err(|error| DataFusionError::Execution(error.to_string()))?
        {
            membership::validate_restored_header(self.membership_layout.as_ref(), value)?;
        }
        self.state
            .restore_key_group(key_group, bytes, &self.scratch_reservation)
    }

    pub(crate) fn checkpoint(&self, directory: &std::path::Path) -> Result<()> {
        self.invocation.require_idle("group aggregate")?;
        self.state.checkpoint(directory)
    }

    /// Physical restore is initialization-only. Import pages are admitted independently;
    /// a failed import requires discarding the destination processor.
    pub(crate) fn restore_physical_key_group(
        &mut self,
        group: u32,
        source: &dyn crate::state::KeyedState,
        owner: &HostMemoryReservation,
    ) -> Result<()> {
        self.invocation.require_idle("group aggregate")?;
        let Self {
            state,
            membership_layout,
            ..
        } = self;
        crate::state::import_key_group(state.as_mut(), source, group, owner, &mut |_, value| {
            membership::validate_restored_header(membership_layout.as_ref(), value)
        })
    }
}
