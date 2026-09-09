// Copyright 2026 StreamFusion Authors
// Licensed under the Apache License, Version 2.0.

use super::*;

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
        let size = groups.clone().try_fold(0usize, |bytes, group| {
            Ok::<_, DataFusionError>(
                bytes
                    .saturating_add(self.kernel.timers.snapshot_size(group)?)
                    .saturating_add(512),
            )
        })?;
        self.admit(size.saturating_mul(2))?;
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
        self.kernel.state.write_batch(mutations)
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

    pub(super) fn restore(&mut self, group: u32, bytes: &[u8], watermark: i64) -> Result<()> {
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
        self.admit(bytes.len().saturating_mul(8).saturating_add(64 * 1024))?;
        let entries = crate::state::decode_key_group_snapshot(group, bytes)?;
        let marker = self.marker();
        if !entries.iter().any(|(key, _)| key == MARKER_KEY) {
            self.migrate_legacy(group, bytes, &entries, watermark)?;
            self.restored_watermark = Some(watermark);
            self.kernel.current_event_time = watermark;
            self.kernel.scratch_reservation.resize(0)?;
            return Ok(());
        }
        if !entries
            .iter()
            .any(|(key, value)| key == MARKER_KEY && *value == marker)
        {
            return Err(DataFusionError::Execution(
                "shared session state plan/version marker differs".into(),
            ));
        }
        let mut partitions = std::collections::BTreeMap::<Vec<u8>, assignments::Assignments>::new();
        for (key, value) in &entries {
            if key == MARKER_KEY || key == TIMER_STATE_KEY {
                continue;
            }
            if key.len() < 20 || key[key.len() - 9] != 1 {
                return Err(DataFusionError::Execution(
                    "invalid ordered session end key".into(),
                ));
            }
            let prefix = &key[..key.len() - 9];
            codec::grouping(prefix)?;
            let (start, end, _) = codec::decode(value, &self.kernel.calls)?;
            let row = self
                .end_codec
                .convert_columns(&[Arc::new(Int64Array::from(vec![end])) as ArrayRef])?;
            if &key[key.len() - 9..] != row.row(0).as_ref() || end - 1 <= watermark {
                return Err(DataFusionError::Execution(
                    "session key or restored watermark differs from live interval".into(),
                ));
            }
            partitions
                .entry(prefix.to_vec())
                .or_default()
                .existing(start, end)?;
        }
        drop(partitions);
        drop(entries);
        self.kernel.restore_key_group(group, bytes)?;
        self.restored_watermark = Some(watermark);
        self.kernel.current_event_time = watermark;
        self.kernel.scratch_reservation.resize(0)?;
        Ok(())
    }
}
