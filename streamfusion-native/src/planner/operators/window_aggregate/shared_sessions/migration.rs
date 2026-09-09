// Copyright 2026 StreamFusion Authors
// Licensed under the Apache License, Version 2.0.

//! Canonical migration from the retained SFWS session payload / SFWI interval-list format.
//! Build and validate the complete replacement before changing this key group's state.

use super::*;
use std::collections::{BTreeMap, BTreeSet};

impl SharedSessions {
    pub(super) fn migrate_legacy(
        &mut self,
        group: u32,
        original: &[u8],
        entries: &[(Vec<u8>, Vec<u8>)],
        watermark: i64,
    ) -> Result<()> {
        let mut indexes = BTreeMap::new();
        let mut expected_indexes = BTreeMap::<Vec<u8>, BTreeSet<(i64, i64)>>::new();
        let mut partitions = BTreeMap::<Vec<u8>, assignments::Assignments>::new();
        let mut rows = Vec::new();
        let mut old_registrations = Vec::new();
        let mut old_timer_bytes = None;
        for (key, value) in entries {
            if key == TIMER_STATE_KEY {
                old_timer_bytes = Some(value.as_slice());
            } else if key.first() == Some(&SESSION_INDEX_PREFIX) {
                let intervals = decode_session_index(value)?;
                let count = intervals.len();
                let intervals = intervals.into_iter().collect::<BTreeSet<_>>();
                if intervals.len() != count
                    || indexes.insert(key[1..].to_vec(), intervals).is_some()
                {
                    return Err(DataFusionError::Execution(
                        "duplicate legacy session index".into(),
                    ));
                }
            } else if key.first() == Some(&WINDOW_KEY_PREFIX) {
                let (start, end) = decode_window_state_key_bounds(key)?;
                let (grouping, state, events) = decode_session_state(value, &self.kernel.calls)?;
                if !events.is_empty() || start >= end || end - 1 <= watermark {
                    return Err(DataFusionError::Execution(
                        "legacy migration requires live append-only session state".into(),
                    ));
                }
                let prefix = codec::prefix(&grouping)?;
                partitions
                    .entry(prefix.clone())
                    .or_default()
                    .existing(start, end)?;
                expected_indexes
                    .entry(group_key_from_window_state_key(key)?.to_vec())
                    .or_default()
                    .insert((start, end));
                rows.push((prefix, start, end, state));
                old_registrations.push((
                    group,
                    TimerDomain::EventTime,
                    TimerKey {
                        timestamp: end - 1,
                        key: key.to_vec(),
                        namespace: window_namespace(start, end),
                    },
                ));
            } else {
                return Err(DataFusionError::Execution(
                    "unsupported legacy session state entry".into(),
                ));
            }
        }
        if indexes != expected_indexes {
            return Err(DataFusionError::Execution(
                "legacy session index differs from its accumulators".into(),
            ));
        }
        let mut old_timers = NativeTimerService::new(
            group,
            group,
            self.kernel
                .scratch_reservation
                .sibling("legacy session timer validation"),
        )?;
        old_timers.register_batch(old_registrations)?;
        if let Some(bytes) = old_timer_bytes {
            if old_timers.snapshot_key_group(group)? != bytes {
                return Err(DataFusionError::Execution(
                    "legacy session timers differ from live state".into(),
                ));
            }
        } else if !rows.is_empty() {
            return Err(DataFusionError::Execution(
                "legacy session snapshot is missing timers".into(),
            ));
        }
        drop(old_timers);
        let end_rows = self
            .end_codec
            .convert_columns(&[Arc::new(Int64Array::from_iter_values(
                rows.iter().map(|(_, _, end, _)| *end),
            )) as ArrayRef])?;
        let mut mutations = entries
            .iter()
            .map(|(key, _)| StateMutation {
                key: StateKey {
                    key_group: group,
                    key: key.to_vec(),
                },
                value: None,
            })
            .collect::<Vec<_>>();
        let mut registrations = Vec::new();
        for (index, (prefix, start, end, state)) in rows.into_iter().enumerate() {
            let key = codec::key(group, &prefix, end_rows.row(index).as_ref());
            registrations.push((
                group,
                TimerDomain::EventTime,
                TimerKey {
                    timestamp: end - 1,
                    key: key.key.clone(),
                    namespace: end.to_le_bytes().to_vec(),
                },
            ));
            mutations.push(StateMutation {
                key,
                value: Some(codec::encode(start, end, &state)),
            });
        }
        let mut timers = NativeTimerService::new(
            group,
            group,
            self.kernel
                .scratch_reservation
                .sibling("migrated session timers"),
        )?;
        timers.register_batch(registrations)?;
        let timer_bytes = timers.snapshot_key_group(group)?;
        mutations.push(StateMutation {
            key: StateKey {
                key_group: group,
                key: TIMER_STATE_KEY.to_vec(),
            },
            value: Some(timer_bytes.clone()),
        });
        mutations.push(StateMutation {
            key: StateKey {
                key_group: group,
                key: checkpoint::MARKER_KEY.to_vec(),
            },
            value: Some(self.marker()),
        });
        drop(timers);
        self.kernel
            .state
            .restore_key_group(group, original, &self.kernel.scratch_reservation)?;
        if let Err(error) = self
            .kernel
            .state
            .write_batch(mutations)
            .and_then(|()| self.kernel.timers.restore_key_group(group, &timer_bytes))
        {
            self.failed = true;
            return Err(error);
        }
        Ok(())
    }
}
