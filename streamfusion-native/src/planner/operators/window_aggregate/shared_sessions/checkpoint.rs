// Copyright 2026 StreamFusion Authors
// Licensed under the Apache License, Version 2.0.

use super::*;
mod validation;
use validation::CurrentState;

pub(super) const MARKER_KEY: &[u8] = b"\0streamfusion-shared-sessions";

impl SharedSessions {
    pub(super) fn marker(&self) -> Vec<u8> {
        use prost::Message;
        use sha2::{Digest, Sha256};
        let mut marker = STATE_MAGIC.to_vec();
        marker.extend_from_slice(&Sha256::digest(self.kernel.plan.encode_to_vec()));
        marker
    }

    fn flush_timers(&mut self, groups: std::ops::RangeInclusive<u32>) -> Result<()> {
        let marker = self.marker();
        super::super::shared_checkpoint::flush_timers(&mut self.kernel, groups, MARKER_KEY, &marker)
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

    pub(super) fn restore(&mut self, group: u32, bytes: &[u8], watermark: i64) -> Result<()> {
        self.require_restore_ready(watermark)?;
        let entries = streamfusion_state_abi::key_group_snapshot_entries(group, bytes)
            .map_err(|error| DataFusionError::Execution(error.to_string()))?;
        if !entries.into_iter().any(|(key, _)| key == MARKER_KEY) {
            self.migrate_legacy(
                group,
                crate::state::CheckpointSource::Canonical(bytes),
                watermark,
            )?;
            return self.finish_restore(watermark);
        }
        let entries = || {
            streamfusion_state_abi::key_group_snapshot_entries(group, bytes)
                .map_err(|error| DataFusionError::Execution(error.to_string()))
        };
        // Current writers order keys. Older canonical frames are allowed to contain the same
        // entries in another order: sort only borrowed descriptors, never accumulator payloads.
        let mut previous: Option<&[u8]> = None;
        let ordered = entries()?.all(|(key, _)| {
            let ordered = previous.is_none_or(|previous| previous <= key);
            previous = Some(key);
            ordered
        });
        {
            let marker = self.marker();
            let mut validator = CurrentState::new(
                &self.kernel.calls,
                &mut self.end_codec,
                self.kernel
                    .scratch_reservation
                    .sibling("session checkpoint validation"),
                &marker,
                watermark,
            );
            if ordered {
                for (key, value) in entries()? {
                    validator.entry(key, value)?;
                }
            } else {
                let entries = entries()?;
                let mut descriptors = self
                    .kernel
                    .scratch_reservation
                    .sibling("unordered session checkpoint descriptors");
                descriptors.resize(entries.len().saturating_mul(64).saturating_add(4096))?;
                let mut entries = entries.collect::<Vec<_>>();
                entries.sort_unstable_by(|left, right| left.0.cmp(right.0));
                for (key, value) in entries {
                    validator.entry(key, value)?;
                }
            }
        }
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
        let marker_key = StateKeyRef {
            key_group: group,
            key: MARKER_KEY,
        };
        let marker = source.get_batch(&[marker_key], &self.kernel.scratch_reservation)?;
        if marker[0].is_none() {
            drop(marker);
            self.migrate_legacy(
                group,
                crate::state::CheckpointSource::Physical(source),
                watermark,
            )?;
            return self.finish_restore(watermark);
        }
        {
            let marker = self.marker();
            let mut validator = CurrentState::new(
                &self.kernel.calls,
                &mut self.end_codec,
                self.kernel
                    .scratch_reservation
                    .sibling("session checkpoint validation"),
                &marker,
                watermark,
            );
            source.visit_key_group_admitted(
                group,
                1024,
                256 << 10,
                &self.kernel.scratch_reservation,
                &mut |page| {
                    for (key, value) in page {
                        validator.entry(key, value)?;
                    }
                    Ok(())
                },
            )?;
        }
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
                "shared session restore needs one Flink watermark before input".into(),
            ));
        }
        Ok(())
    }

    fn finish_restore(&mut self, watermark: i64) -> Result<()> {
        self.restored_watermark = Some(watermark);
        self.kernel.current_event_time = watermark;
        self.kernel.scratch_reservation.resize(0)
    }
}
