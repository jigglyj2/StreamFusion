// Copyright 2026 StreamFusion Authors
// Licensed under the Apache License, Version 2.0.

use super::*;

const MARKER_KEY: &[u8] = b"\0streamfusion-shared-slices";

impl SharedSlices {
    fn marker(&self) -> Vec<u8> {
        let mut marker = STATE_MAGIC.to_vec();
        marker.extend_from_slice(
            &self
                .original_plan_fingerprint
                .unwrap_or_else(|| Self::fingerprint(&self.kernel.plan)),
        );
        marker
    }

    fn flush_timers(&mut self, groups: std::ops::RangeInclusive<u32>) -> Result<()> {
        let bytes = groups.clone().try_fold(0usize, |bytes, group| {
            Ok::<_, DataFusionError>(
                bytes
                    .saturating_add(self.kernel.timers.snapshot_size(group)?)
                    .saturating_add(512),
            )
        })?;
        self.admit(bytes.saturating_mul(2))?;
        let mut mutations = Vec::new();
        for group in groups {
            mutations.push(StateMutation {
                key: StateKey {
                    key_group: group,
                    key: TIMER_STATE_KEY.to_vec(),
                },
                value: Some(self.kernel.timers.snapshot_key_group(group)?),
            });
            mutations.push(StateMutation {
                key: StateKey {
                    key_group: group,
                    key: MARKER_KEY.to_vec(),
                },
                value: Some(self.marker()),
            });
        }
        self.kernel.state.write_batch(mutations)?;
        Ok(())
    }

    pub(super) fn snapshot(&mut self, group: u32) -> Result<crate::state::SnapshotBytes> {
        self.require_healthy()?;
        self.flush_timers(group..=group)?;
        let result = self.kernel.snapshot_key_group(group);
        self.kernel.scratch_reservation.resize(0)?;
        result
    }

    pub(super) fn write_snapshot(
        &mut self,
        group: u32,
        sink: &mut crate::state::snapshot_stream::SnapshotSink<'_>,
    ) -> Result<usize> {
        self.require_healthy()?;
        self.flush_timers(group..=group)?;
        let result =
            self.kernel
                .state
                .write_snapshot(group, &self.kernel.scratch_reservation, sink);
        self.kernel.scratch_reservation.resize(0)?;
        result
    }

    pub(super) fn checkpoint(&mut self, directory: &std::path::Path) -> Result<()> {
        self.require_healthy()?;
        self.flush_timers(self.kernel.timers.key_group_range())?;
        let result = self.kernel.checkpoint(directory);
        self.kernel.scratch_reservation.resize(0)?;
        result
    }

    /// Flink supplies the minimum restored union-operator watermark independently of keyed
    /// slice state. Never encode an operator clock in a key-group snapshot during rescaling.
    pub(super) fn restore(&mut self, group: u32, bytes: &[u8], watermark: i64) -> Result<()> {
        self.require_restore_ready(watermark)?;
        let entries = streamfusion_state_abi::key_group_snapshot_entries(group, bytes)
            .map_err(|error| DataFusionError::Execution(error.to_string()))?;
        let marker = self.marker();
        let mut found_marker = false;
        {
            let mut workspace = self
                .kernel
                .scratch_reservation
                .sibling("slice checkpoint validation");
            for (key, value) in entries {
                self.validate_entry(key, value, &marker, &mut found_marker, &mut workspace)?;
            }
        }
        require_marker(found_marker)?;
        self.kernel.restore_key_group(group, bytes)?;
        self.finish_restore(watermark)
    }

    pub(super) fn restore_physical(
        &mut self,
        group: u32,
        source: &crate::state::RocksPluginKeyedState,
        watermark: i64,
    ) -> Result<()> {
        self.require_restore_ready(watermark)?;
        let marker = self.marker();
        let mut found_marker = false;
        {
            let mut workspace = self
                .kernel
                .scratch_reservation
                .sibling("slice checkpoint validation");
            source.visit_key_group_admitted(
                group,
                1024,
                256 << 10,
                &self.kernel.scratch_reservation,
                &mut |page| {
                    for (key, value) in page {
                        self.validate_entry(
                            key,
                            value,
                            &marker,
                            &mut found_marker,
                            &mut workspace,
                        )?;
                    }
                    Ok(())
                },
            )?;
        }
        require_marker(found_marker)?;
        self.kernel.restore_physical_key_group(group, source)?;
        self.finish_restore(watermark)
    }

    fn require_restore_ready(&self, watermark: i64) -> Result<()> {
        self.require_healthy()?;
        if self.started
            || self
                .restored_watermark
                .is_some_and(|previous| previous != watermark)
        {
            return Err(DataFusionError::Execution(
                "shared-slice restore needs one Flink watermark before processing input".into(),
            ));
        }
        Ok(())
    }

    fn finish_restore(&mut self, watermark: i64) -> Result<()> {
        self.restored_watermark = Some(watermark);
        self.kernel.current_event_time = watermark;
        self.kernel.scratch_reservation.resize(0)
    }

    fn validate_entry(
        &self,
        key: &[u8],
        value: &[u8],
        marker: &[u8],
        found_marker: &mut bool,
        workspace: &mut HostMemoryReservation,
    ) -> Result<()> {
        if key == MARKER_KEY {
            require_marker(value == marker)?;
            *found_marker = true;
            return Ok(());
        }
        if key == TIMER_STATE_KEY {
            return Ok(());
        }
        if key.len() < 20 {
            return Err(DataFusionError::Execution(
                "truncated shared-slice state key".into(),
            ));
        }
        codec::grouping_row(&key[..key.len() - 9])?;
        if key[key.len() - 9] != 1 {
            return Err(DataFusionError::Execution(
                "shared-slice end must be a non-null Arrow BIGINT row".into(),
            ));
        }
        // Decode one accumulator under a reusable reservation. A single large legacy value
        // still needs admission, but unrelated retained partitions do not multiply the workspace.
        let bound = value.len().saturating_mul(8).saturating_add(4096);
        if bound > workspace.size() {
            workspace.resize(bound)?;
        }
        codec::decode(value, &self.kernel.calls)?;
        Ok(())
    }
}

fn require_marker(valid: bool) -> Result<()> {
    if !valid {
        return Err(DataFusionError::Execution("shared-slice restore requires matching versioned slice state; expanded-window state cannot be reinterpreted".into()));
    }
    Ok(())
}
