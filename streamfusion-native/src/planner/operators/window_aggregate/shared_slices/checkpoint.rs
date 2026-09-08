// Copyright 2026 StreamFusion Authors
// Licensed under the Apache License, Version 2.0.

use super::*;

const MARKER_KEY: &[u8] = b"\0streamfusion-shared-slices";

impl SharedSlices {
    fn marker(&self) -> Vec<u8> {
        let mut marker = STATE_MAGIC.to_vec();
        use prost::Message;
        use sha2::{Digest, Sha256};
        marker.extend_from_slice(&Sha256::digest(self.kernel.plan.encode_to_vec()));
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
        self.admit(bytes.len().saturating_mul(8).saturating_add(4096))?;
        let entries = crate::state::decode_key_group_snapshot(group, bytes)?;
        let marker = self.marker();
        if !entries
            .iter()
            .any(|(key, value)| key == MARKER_KEY && *value == marker)
        {
            return Err(DataFusionError::Execution("shared-slice restore requires matching versioned slice state; expanded-window state cannot be reinterpreted".into()));
        }
        for (key, value) in &entries {
            if key == MARKER_KEY || key == TIMER_STATE_KEY {
                continue;
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
            codec::decode(value, &self.kernel.calls)?;
        }
        drop(entries);
        self.kernel.restore_key_group(group, bytes)?;
        self.restored_watermark = Some(watermark);
        self.kernel.current_event_time = watermark;
        self.kernel.scratch_reservation.resize(0)?;
        Ok(())
    }
}
