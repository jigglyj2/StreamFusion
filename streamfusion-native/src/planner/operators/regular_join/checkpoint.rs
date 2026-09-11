// Copyright 2026 StreamFusion Authors
// Licensed under the Apache License, Version 2.0.

use super::*;

impl RegularJoinProcessor {
    pub(crate) fn state_memory(&self) -> HostMemoryReservation {
        self.scratch_reservation.sibling("native state transfer")
    }

    pub(crate) fn snapshot_key_group(&self, key_group: u32) -> Result<crate::state::SnapshotBytes> {
        self.require_idle_stream()?;
        self.state
            .snapshot_key_group(key_group, &self.scratch_reservation)
    }

    pub(crate) fn restore_key_group(&mut self, key_group: u32, bytes: &[u8]) -> Result<()> {
        self.require_idle_stream()?;
        paged_state::restore(
            self.state.as_mut(),
            key_group,
            bytes,
            &self.scratch_reservation,
        )
    }

    pub(crate) fn checkpoint(&self, directory: &std::path::Path) -> Result<()> {
        self.require_idle_stream()?;
        self.state.checkpoint(directory)
    }

    /// Physical restore is initialization-only. Import pages are admitted independently;
    /// a failed import requires discarding the destination processor.
    pub(crate) fn restore_physical_key_group(
        &mut self,
        group: u32,
        source: &crate::state::RocksPluginKeyedState,
        owner: &HostMemoryReservation,
    ) -> Result<()> {
        self.require_idle_stream()?;
        paged_state::restore_from_checkpoint(self.state.as_mut(), source, group, owner)
    }
}
