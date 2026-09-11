// Copyright 2026 StreamFusion Authors
// Licensed under the Apache License, Version 2.0.

//! Timer markers are part of the same keyed mutation batch as their rows. The in-memory timer
//! service is an admitted index rebuilt on restore; it is never serialized on the batch data path.

use super::*;

const PREFIX: u8 = 8;
const VERSION: u8 = 1;
const PAGE_TIMERS: usize = 1024;

fn invalid(message: &str) -> DataFusionError {
    DataFusionError::Execution(format!("invalid temporal sort timer state: {message}"))
}

pub(super) fn marker_key(group: u32, domain: TimerDomain, timestamp: i64) -> StateKey {
    let mut key = Vec::with_capacity(11);
    key.extend_from_slice(&[
        PREFIX,
        VERSION,
        match domain {
            TimerDomain::EventTime => 0,
            TimerDomain::ProcessingTime => 1,
        },
    ]);
    // The sign flip preserves signed timestamp order in the backend's bytewise index.
    key.extend_from_slice(&((timestamp as u64) ^ (1 << 63)).to_be_bytes());
    StateKey {
        key_group: group,
        key,
    }
}

fn identity(group: u32, domain: TimerDomain, timestamp: i64) -> TimerKey {
    let root = match domain {
        TimerDomain::EventTime => rows_state_key(group, timestamp),
        TimerDomain::ProcessingTime => processing_time_rows_state_key(group),
    };
    TimerKey {
        timestamp,
        key: root.key,
        namespace: Vec::new(),
    }
}

fn timestamp(key: &[u8], value: &[u8], domain: TimerDomain) -> Result<i64> {
    let expected_domain = if domain == TimerDomain::EventTime {
        0
    } else {
        1
    };
    if key.len() != 11 || key[..3] != [PREFIX, VERSION, expected_domain] || !value.is_empty() {
        return Err(invalid("marker version, domain, or payload"));
    }
    Ok((u64::from_be_bytes(key[3..].try_into().unwrap()) ^ (1 << 63)) as i64)
}

pub(super) fn register(
    timers: &mut NativeTimerService,
    group: u32,
    domain: TimerDomain,
    timestamps: Vec<i64>,
    mutations: &mut Vec<StateMutation>,
    owner: &HostMemoryReservation,
) -> Result<(usize, HostMemoryReservation)> {
    let mut memory = owner.sibling("temporal timer registration batch");
    memory.resize(timestamps.len().saturating_mul(512))?;
    let requested = timestamps
        .iter()
        .map(|&timestamp| (group, domain, identity(group, domain, timestamp)))
        .collect();
    let inserted = timers.register_batch(requested)?.len();
    // Re-registering a pending processing-time identity is idempotent in both representations.
    mutations.extend(timestamps.into_iter().map(|timestamp| StateMutation {
        key: marker_key(group, domain, timestamp),
        value: Some(Vec::new()),
    }));
    Ok((inserted, memory))
}

/// Restore is initialization. A failed validation or admission discards the processor, including
/// any pages already imported; it must never merge into a live execution's timer index.
pub(super) fn restore(
    state: &mut dyn KeyedState,
    timers: &mut NativeTimerService,
    group: u32,
    domain: TimerDomain,
    owner: &HostMemoryReservation,
) -> Result<i64> {
    timers.visit_key_group(group, &mut |_, _| {
        Err(invalid("timer group restored more than once"))
    })?;
    let values = state.get_batch(
        &[
            StateKeyRef {
                key_group: group,
                key: TIMER_STATE_KEY,
            },
            StateKeyRef {
                key_group: group,
                key: LAST_TRIGGER_STATE_KEY,
            },
        ],
        owner,
    )?;
    let last_trigger = values[1]
        .as_ref()
        .map(|bytes| {
            bytes
                .as_ref()
                .try_into()
                .map(i64::from_le_bytes)
                .map_err(|_| invalid("last-trigger timestamp"))
        })
        .transpose()?
        .unwrap_or(i64::MIN);
    let legacy = values[0].as_ref();
    let mut page_memory = owner.sibling("temporal timer restore page");
    state.visit_prefix_admitted(
        group,
        &[PREFIX],
        PAGE_TIMERS,
        256 << 10,
        owner,
        &mut |page| {
            if legacy.is_some() {
                return Err(invalid("mixed legacy snapshot and timer markers"));
            }
            page_memory.resize(page.len().saturating_mul(512))?;
            let requested = page
                .iter()
                .map(|(key, value)| {
                    let timestamp = timestamp(key, value, domain)?;
                    Ok((group, domain, identity(group, domain, timestamp)))
                })
                .collect::<Result<Vec<_>>>()?;
            if timers.register_batch(requested)?.len() != page.len() {
                return Err(invalid("duplicate timer marker"));
            }
            Ok(())
        },
    )?;
    if let Some(bytes) = legacy {
        // Only old checkpoints retain a whole timer vector. Admit decoding once during migration;
        // new checkpoint restore copies timer identities one bounded page at a time.
        let mut legacy_memory = owner.sibling("legacy temporal timer decoding");
        legacy_memory.resize(bytes.as_ref().len().saturating_mul(16))?;
        timers.restore_key_group(group, bytes.as_ref())?;
        timers.visit_key_group(group, &mut |actual_domain, timer| {
            if actual_domain != domain || *timer != identity(group, domain, timer.timestamp) {
                return Err(invalid("legacy timer identity does not match the plan"));
            }
            Ok(())
        })?;
    }
    let migrate = legacy.is_some();
    drop(values);
    if migrate {
        // Page the new markers instead of constructing a second vector proportional to all timers.
        page_memory.resize(PAGE_TIMERS * 192)?;
        let mut mutations = Vec::with_capacity(PAGE_TIMERS);
        timers.visit_key_group(group, &mut |domain, timer| {
            mutations.push(StateMutation {
                key: marker_key(group, domain, timer.timestamp),
                value: Some(Vec::new()),
            });
            if mutations.len() == PAGE_TIMERS {
                state.write_batch(std::mem::replace(
                    &mut mutations,
                    Vec::with_capacity(PAGE_TIMERS),
                ))?;
            }
            Ok(())
        })?;
        mutations.push(StateMutation {
            key: StateKey {
                key_group: group,
                key: TIMER_STATE_KEY.to_vec(),
            },
            value: None,
        });
        state.write_batch(mutations)?;
    }
    Ok(last_trigger)
}

#[cfg(test)]
mod tests;
