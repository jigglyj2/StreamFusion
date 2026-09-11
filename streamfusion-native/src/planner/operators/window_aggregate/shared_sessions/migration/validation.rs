// Copyright 2026 StreamFusion Authors
// Licensed under the Apache License, Version 2.0.

use super::*;
use std::collections::{BTreeMap, BTreeSet};

/// Retain only interval/key metadata and admitted timers. Accumulator payloads are decoded and
/// released one entry at a time, regardless of the size of the complete source key group.
pub(super) fn validate(
    source: Source<'_>,
    group: u32,
    calls: &[Call],
    watermark: i64,
    owner: &HostMemoryReservation,
) -> Result<()> {
    source.validate_unique_keys(group, owner)?;
    let mut indexes = BTreeMap::new();
    let mut expected = BTreeMap::<Vec<u8>, BTreeSet<(i64, i64)>>::new();
    let mut partitions = BTreeMap::<Vec<u8>, assignments::Assignments>::new();
    let mut metadata = owner.sibling("legacy session interval validation");
    let mut retained = 4096usize;
    let mut workspace = owner.sibling("legacy session value validation");
    let mut timers = NativeTimerService::new(
        group,
        group,
        owner.sibling("legacy session timer validation"),
    )?;
    let mut pending = TimerBatch::new(owner);
    let mut windows = 0usize;
    source.visit(group, owner, &mut |key, value| {
        if key == TIMER_STATE_KEY {
            return Ok(());
        }
        admit_workspace(&mut workspace, value_workspace(key, value, calls.len()))?;
        if key.first() == Some(&SESSION_INDEX_PREFIX) {
            let intervals = decode_session_index(value)?;
            retained = retained
                .saturating_add(intervals.len().saturating_mul(96))
                .saturating_add(key.len().saturating_mul(2))
                .saturating_add(512);
            admit_workspace(&mut metadata, retained)?;
            let count = intervals.len();
            let intervals = intervals.into_iter().collect::<BTreeSet<_>>();
            if intervals.len() != count || indexes.insert(key[1..].to_vec(), intervals).is_some() {
                return Err(DataFusionError::Execution(
                    "duplicate legacy session index".into(),
                ));
            }
        } else if key.first() == Some(&WINDOW_KEY_PREFIX) {
            let (start, end) = decode_window_state_key_bounds(key)?;
            let (grouping, state) = decode_append_only_session_state(value, calls)?;
            if start >= end || end - 1 <= watermark || state.row_count <= 0 {
                return Err(DataFusionError::Execution(
                    "legacy migration requires live append-only session state".into(),
                ));
            }
            retained = retained
                .saturating_add(key.len().saturating_mul(2))
                .saturating_add(grouping.len().saturating_mul(2))
                .saturating_add(512);
            admit_workspace(&mut metadata, retained)?;
            partitions
                .entry(codec::prefix(&grouping)?)
                .or_default()
                .existing(start, end)?;
            expected
                .entry(group_key_from_window_state_key(key)?.to_vec())
                .or_default()
                .insert((start, end));
            pending.push(
                group,
                end - 1,
                key,
                &window_namespace(start, end),
                &mut timers,
            )?;
            windows += 1;
        } else {
            return Err(DataFusionError::Execution(
                "unsupported legacy session state entry".into(),
            ));
        }
        Ok(())
    })?;
    if indexes != expected {
        return Err(DataFusionError::Execution(
            "legacy session index differs from its accumulators".into(),
        ));
    }
    drop((indexes, expected, partitions, metadata, workspace));
    pending.flush(&mut timers)?;
    drop(pending);
    let mut snapshot = owner.sibling("legacy session timer comparison");
    snapshot.resize(
        timers
            .snapshot_size(group)?
            .saturating_mul(2)
            .saturating_add(4096),
    )?;
    let expected = timers.snapshot_key_group(group)?;
    source.with_value(group, TIMER_STATE_KEY, owner, &mut |value| match value {
        Some(value) if value != expected => Err(DataFusionError::Execution(
            "legacy session timers differ from live state".into(),
        )),
        None if windows != 0 => Err(DataFusionError::Execution(
            "legacy session snapshot is missing timers".into(),
        )),
        _ => Ok(()),
    })
}
