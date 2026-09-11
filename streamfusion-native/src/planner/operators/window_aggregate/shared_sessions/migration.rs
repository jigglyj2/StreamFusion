// Copyright 2026 StreamFusion Authors
// Licensed under the Apache License, Version 2.0.

//! Validate legacy SFWS/SFWI state before migration, then write only the current format.
//! Source payloads remain borrowed and destination writes share the native bounded writer.

use super::*;
use crate::state::{CheckpointSource as Source, StateBatchWriter};
mod timers;
use timers::TimerBatch;
mod validation;

impl SharedSessions {
    pub(super) fn migrate_legacy(
        &mut self,
        group: u32,
        source: Source<'_>,
        watermark: i64,
    ) -> Result<()> {
        let owner = self
            .kernel
            .scratch_reservation
            .sibling("session legacy migration");
        validation::validate(source, group, &self.kernel.calls, watermark, &owner)?;
        crate::state::require_empty_key_group(self.kernel.state.as_ref(), group, &owner)?;
        self.kernel.timers.visit_key_group(group, &mut |_, _| {
            Err(DataFusionError::Execution(format!(
                "timer key group {group} was restored more than once"
            )))
        })?;
        let result = self.write_legacy_migration(group, source, &owner);
        if result.is_err() {
            self.failed = true;
        }
        result
    }

    fn write_legacy_migration(
        &mut self,
        group: u32,
        source: Source<'_>,
        owner: &HostMemoryReservation,
    ) -> Result<()> {
        let marker = self.marker();
        let mut page = owner.sibling("session migration write page");
        let mut workspace = owner.sibling("session migration value");
        let mut writes = 0;
        let mut writer =
            StateBatchWriter::new(self.kernel.state.as_mut(), &mut page, 0, &mut writes)?;
        let mut pending = TimerBatch::new(owner);
        source.visit(group, owner, &mut |key, value| {
            if key.first() != Some(&WINDOW_KEY_PREFIX) {
                return Ok(());
            }
            admit_workspace(
                &mut workspace,
                value_workspace(key, value, self.kernel.calls.len()),
            )?;
            let (start, end) = decode_window_state_key_bounds(key)?;
            let (grouping, state) = decode_append_only_session_state(value, &self.kernel.calls)?;
            let prefix = codec::prefix(&grouping)?;
            let end_rows = self
                .end_codec
                .convert_columns(&[Arc::new(Int64Array::from(vec![end])) as ArrayRef])?;
            let key = codec::key(group, &prefix, end_rows.row(0).as_ref());
            pending.push(
                group,
                end - 1,
                &key.key,
                &end.to_le_bytes(),
                &mut self.kernel.timers,
            )?;
            writer.push(
                key.key.len().saturating_add(value.len()).saturating_add(64),
                key.key.capacity(),
                || {
                    Ok(StateMutation {
                        key,
                        value: Some(codec::encode(start, end, &state)),
                    })
                },
            )
        })?;
        pending.flush(&mut self.kernel.timers)?;
        drop((pending, workspace));
        let timer_bytes = self.kernel.timers.snapshot_size(group)?;
        writer.push(TIMER_STATE_KEY.len().saturating_add(timer_bytes), 0, || {
            Ok(StateMutation {
                key: StateKey {
                    key_group: group,
                    key: TIMER_STATE_KEY.to_vec(),
                },
                value: Some(self.kernel.timers.snapshot_key_group(group)?),
            })
        })?;
        writer.push(
            checkpoint::MARKER_KEY.len().saturating_add(marker.len()),
            marker.capacity(),
            || {
                Ok(StateMutation {
                    key: StateKey {
                        key_group: group,
                        key: checkpoint::MARKER_KEY.to_vec(),
                    },
                    value: Some(marker),
                })
            },
        )?;
        writer.finish()
    }
}

fn value_workspace(key: &[u8], value: &[u8], calls: usize) -> usize {
    value
        .len()
        .saturating_mul(8)
        .saturating_add(key.len().saturating_mul(4))
        .saturating_add(calls.saturating_mul(512))
        .saturating_add(65536)
}
