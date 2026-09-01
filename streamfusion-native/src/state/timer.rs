// Copyright 2026 StreamFusion Authors
// Licensed under the Apache License, Version 2.0

use std::collections::BTreeSet;
use std::mem::size_of;

use datafusion::error::{DataFusionError, Result};

use crate::memory_pool::HostMemoryReservation;

const SNAPSHOT_MAGIC: &[u8; 4] = b"SFTM";
const SNAPSHOT_VERSION: u8 = 1;

/// Clock domain used by a native SQL timer.
#[derive(Clone, Copy, Debug, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub(crate) enum TimerDomain {
    EventTime,
    ProcessingTime,
}

/// Flink-compatible timer identity. Timer registration is idempotent for the complete tuple.
#[derive(Clone, Debug, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub(crate) struct TimerKey {
    pub(crate) timestamp: i64,
    pub(crate) key: Vec<u8>,
    pub(crate) namespace: Vec<u8>,
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub(crate) struct FiredTimer {
    pub(crate) key_group: u32,
    pub(crate) domain: TimerDomain,
    pub(crate) timer: TimerKey,
}

/// Backend-neutral timer index for native keyed operators.
///
/// Timers are partitioned by Flink key group, ordered by timestamp and then by the canonical key
/// and namespace bytes. The per-key-group encoding is independent of the state backend, so it can
/// be embedded in the same raw-keyed savepoint envelope as operator state and redistributed during
/// rescaling. All tree/key/namespace allocations are charged to the operator's Flink managed
/// memory reservation.
pub(crate) struct NativeTimerService {
    first_key_group: u32,
    groups: Vec<TimerGroup>,
    reservation: HostMemoryReservation,
    accounted_bytes: usize,
}

#[derive(Default)]
struct TimerGroup {
    event_time: BTreeSet<TimerKey>,
    processing_time: BTreeSet<TimerKey>,
}

impl NativeTimerService {
    pub(crate) fn new(
        first_key_group: u32,
        last_key_group: u32,
        mut reservation: HostMemoryReservation,
    ) -> Result<Self> {
        if first_key_group > last_key_group {
            return Err(DataFusionError::Execution(format!(
                "invalid timer key-group range {first_key_group}..={last_key_group}"
            )));
        }
        let group_count = usize::try_from(last_key_group - first_key_group + 1).unwrap();
        let accounted_bytes = group_count.saturating_mul(size_of::<TimerGroup>());
        reservation.resize(accounted_bytes)?;
        Ok(Self {
            first_key_group,
            groups: (0..group_count).map(|_| TimerGroup::default()).collect(),
            reservation,
            accounted_bytes,
        })
    }

    pub(crate) fn register(
        &mut self,
        key_group: u32,
        domain: TimerDomain,
        timer: TimerKey,
    ) -> Result<bool> {
        let allocation_bound = timer
            .key
            .capacity()
            .saturating_add(timer.namespace.capacity())
            .saturating_add(timer_node_bytes());
        let old_size = self.accounted_bytes;
        let new_size = old_size.saturating_add(allocation_bound);
        self.reservation.resize(new_size)?;
        let inserted = self.timer_set_mut(key_group, domain)?.insert(timer);
        if inserted {
            self.accounted_bytes = new_size;
        } else {
            self.reservation.resize(old_size)?;
        }
        Ok(inserted)
    }

    pub(crate) fn delete(
        &mut self,
        key_group: u32,
        domain: TimerDomain,
        timer: &TimerKey,
    ) -> Result<bool> {
        let removed = self.timer_set_mut(key_group, domain)?.take(timer);
        if let Some(removed) = removed.as_ref() {
            self.accounted_bytes = self
                .accounted_bytes
                .saturating_sub(timer_heap_size(removed));
            self.reservation.resize(self.accounted_bytes)?;
        }
        Ok(removed.is_some())
    }

    /// Removes all timers at or before `progress`, preserving deterministic Flink key-group and
    /// timer ordering. Operators may register more timers while handling the returned values.
    pub(crate) fn advance(
        &mut self,
        domain: TimerDomain,
        progress: i64,
    ) -> Result<Vec<FiredTimer>> {
        let mut fired = Vec::new();
        let mut released_bytes = 0usize;
        for (offset, group) in self.groups.iter_mut().enumerate() {
            let key_group = self.first_key_group + u32::try_from(offset).unwrap();
            let timers = timer_set(group, domain);
            loop {
                let Some(first) = timers.first() else {
                    break;
                };
                if first.timestamp > progress {
                    break;
                }
                let timer = timers.pop_first().expect("first timer exists");
                released_bytes = released_bytes.saturating_add(timer_heap_size(&timer));
                fired.push(FiredTimer {
                    key_group,
                    domain,
                    timer,
                });
            }
        }
        self.accounted_bytes = self.accounted_bytes.saturating_sub(released_bytes);
        self.reservation.resize(self.accounted_bytes)?;
        Ok(fired)
    }

    pub(crate) fn timer_count(&self, domain: TimerDomain) -> usize {
        self.groups
            .iter()
            .map(|group| timer_set_ref(group, domain).len())
            .sum()
    }

    pub(crate) fn next_timestamp(&self, domain: TimerDomain) -> Option<i64> {
        self.groups
            .iter()
            .filter_map(|group| timer_set_ref(group, domain).first())
            .map(|timer| timer.timestamp)
            .min()
    }

    pub(crate) fn snapshot_key_group(&self, key_group: u32) -> Result<Vec<u8>> {
        let group = self.group(key_group)?;
        let count = group
            .event_time
            .len()
            .checked_add(group.processing_time.len())
            .ok_or_else(|| DataFusionError::Execution("native timer count overflow".to_string()))?;
        let mut output = Vec::new();
        output.extend_from_slice(SNAPSHOT_MAGIC);
        output.push(SNAPSHOT_VERSION);
        output.extend_from_slice(&key_group.to_le_bytes());
        write_u32(&mut output, count, "timer count")?;
        for (domain, timer) in group
            .event_time
            .iter()
            .map(|timer| (TimerDomain::EventTime, timer))
            .chain(
                group
                    .processing_time
                    .iter()
                    .map(|timer| (TimerDomain::ProcessingTime, timer)),
            )
        {
            output.push(match domain {
                TimerDomain::EventTime => 0,
                TimerDomain::ProcessingTime => 1,
            });
            output.extend_from_slice(&timer.timestamp.to_le_bytes());
            write_bytes(&mut output, &timer.key, "timer key")?;
            write_bytes(&mut output, &timer.namespace, "timer namespace")?;
        }
        Ok(output)
    }

    pub(crate) fn restore_key_group(&mut self, key_group: u32, bytes: &[u8]) -> Result<()> {
        if !self.group(key_group)?.event_time.is_empty()
            || !self.group(key_group)?.processing_time.is_empty()
        {
            return Err(DataFusionError::Execution(format!(
                "timer key group {key_group} was restored more than once"
            )));
        }
        let decoded = decode_snapshot(key_group, bytes)?;
        let old_size = self.accounted_bytes;
        let allocation_bound = decoded.iter().fold(0usize, |total, (_, timer)| {
            total
                .saturating_add(timer.key.capacity())
                .saturating_add(timer.namespace.capacity())
                .saturating_add(timer_node_bytes())
        });
        self.reservation
            .resize(old_size.saturating_add(allocation_bound))?;
        let group = self.group_mut(key_group)?;
        for (domain, timer) in decoded {
            if !timer_set(group, domain).insert(timer) {
                return Err(DataFusionError::Execution(
                    "native timer snapshot contains a duplicate timer".to_string(),
                ));
            }
        }
        self.accounted_bytes = old_size.saturating_add(allocation_bound);
        self.reservation.resize(self.accounted_bytes)?;
        Ok(())
    }

    fn timer_set_mut(
        &mut self,
        key_group: u32,
        domain: TimerDomain,
    ) -> Result<&mut BTreeSet<TimerKey>> {
        Ok(timer_set(self.group_mut(key_group)?, domain))
    }

    fn group(&self, key_group: u32) -> Result<&TimerGroup> {
        let index = self.group_index(key_group)?;
        Ok(&self.groups[index])
    }

    fn group_mut(&mut self, key_group: u32) -> Result<&mut TimerGroup> {
        let index = self.group_index(key_group)?;
        Ok(&mut self.groups[index])
    }

    fn group_index(&self, key_group: u32) -> Result<usize> {
        let index = key_group.checked_sub(self.first_key_group).ok_or_else(|| {
            DataFusionError::Execution(format!(
                "timer key group {key_group} is not owned by this subtask"
            ))
        })? as usize;
        if index >= self.groups.len() {
            return Err(DataFusionError::Execution(format!(
                "timer key group {key_group} is not owned by this subtask"
            )));
        }
        Ok(index)
    }
}

fn timer_set(group: &mut TimerGroup, domain: TimerDomain) -> &mut BTreeSet<TimerKey> {
    match domain {
        TimerDomain::EventTime => &mut group.event_time,
        TimerDomain::ProcessingTime => &mut group.processing_time,
    }
}

fn timer_set_ref(group: &TimerGroup, domain: TimerDomain) -> &BTreeSet<TimerKey> {
    match domain {
        TimerDomain::EventTime => &group.event_time,
        TimerDomain::ProcessingTime => &group.processing_time,
    }
}

fn timer_heap_size(timer: &TimerKey) -> usize {
    timer_node_bytes()
        .saturating_add(timer.key.capacity())
        .saturating_add(timer.namespace.capacity())
}

const fn timer_node_bytes() -> usize {
    // Rust does not expose BTree node capacity. Charge a conservative per-entry share for links,
    // occupancy metadata, and allocator alignment in addition to the TimerKey itself.
    size_of::<TimerKey>() + 4 * size_of::<usize>()
}

fn write_u32(output: &mut Vec<u8>, value: usize, description: &str) -> Result<()> {
    let value = u32::try_from(value).map_err(|_| {
        DataFusionError::Execution(format!("native {description} exceeds the snapshot format"))
    })?;
    output.extend_from_slice(&value.to_le_bytes());
    Ok(())
}

fn write_bytes(output: &mut Vec<u8>, bytes: &[u8], description: &str) -> Result<()> {
    write_u32(output, bytes.len(), description)?;
    output.extend_from_slice(bytes);
    Ok(())
}

fn decode_snapshot(key_group: u32, bytes: &[u8]) -> Result<Vec<(TimerDomain, TimerKey)>> {
    let mut input = SnapshotInput { bytes, offset: 0 };
    if input.read_exact(4, "timer magic")? != SNAPSHOT_MAGIC {
        return Err(DataFusionError::Execution(
            "invalid native timer snapshot magic".to_string(),
        ));
    }
    let version = input.read_u8("timer version")?;
    if version != SNAPSHOT_VERSION {
        return Err(DataFusionError::Execution(format!(
            "unsupported native timer snapshot version {version}"
        )));
    }
    let encoded_key_group = input.read_u32("timer key group")?;
    if encoded_key_group != key_group {
        return Err(DataFusionError::Execution(format!(
            "timer snapshot key group {encoded_key_group} does not match assigned group {key_group}"
        )));
    }
    let count = input.read_u32("timer count")? as usize;
    let mut timers = Vec::with_capacity(count);
    for _ in 0..count {
        let domain = match input.read_u8("timer domain")? {
            0 => TimerDomain::EventTime,
            1 => TimerDomain::ProcessingTime,
            value => {
                return Err(DataFusionError::Execution(format!(
                    "invalid native timer domain {value}"
                )));
            }
        };
        let timestamp = input.read_i64("timer timestamp")?;
        let key = input.read_bytes("timer key")?;
        let namespace = input.read_bytes("timer namespace")?;
        timers.push((
            domain,
            TimerKey {
                timestamp,
                key,
                namespace,
            },
        ));
    }
    if input.offset != bytes.len() {
        return Err(DataFusionError::Execution(
            "native timer snapshot has trailing bytes".to_string(),
        ));
    }
    Ok(timers)
}

struct SnapshotInput<'a> {
    bytes: &'a [u8],
    offset: usize,
}

impl<'a> SnapshotInput<'a> {
    fn read_u8(&mut self, description: &str) -> Result<u8> {
        Ok(self.read_exact(1, description)?[0])
    }

    fn read_u32(&mut self, description: &str) -> Result<u32> {
        Ok(u32::from_le_bytes(
            self.read_exact(4, description)?.try_into().unwrap(),
        ))
    }

    fn read_i64(&mut self, description: &str) -> Result<i64> {
        Ok(i64::from_le_bytes(
            self.read_exact(8, description)?.try_into().unwrap(),
        ))
    }

    fn read_bytes(&mut self, description: &str) -> Result<Vec<u8>> {
        let length = self.read_u32(&format!("{description} length"))? as usize;
        Ok(self.read_exact(length, description)?.to_vec())
    }

    fn read_exact(&mut self, length: usize, description: &str) -> Result<&'a [u8]> {
        let end = self.offset.checked_add(length).ok_or_else(|| {
            DataFusionError::Execution(format!("native {description} length overflow"))
        })?;
        let value = self
            .bytes
            .get(self.offset..end)
            .ok_or_else(|| DataFusionError::Execution(format!("truncated native {description}")))?;
        self.offset = end;
        Ok(value)
    }
}

#[cfg(test)]
mod tests {
    use std::sync::Arc;

    use super::*;
    use crate::memory_pool::tests_support::TestBroker;

    fn service(first: u32, last: u32, broker: Arc<TestBroker>) -> NativeTimerService {
        NativeTimerService::new(
            first,
            last,
            HostMemoryReservation::new(broker, "native timer test"),
        )
        .unwrap()
    }

    fn timer(timestamp: i64, key: &[u8], namespace: &[u8]) -> TimerKey {
        TimerKey {
            timestamp,
            key: key.to_vec(),
            namespace: namespace.to_vec(),
        }
    }

    #[test]
    fn registration_is_idempotent_and_firing_is_deterministic() {
        let broker = Arc::new(TestBroker::new(1 << 20));
        let mut timers = service(2, 3, broker.clone());
        let base_bytes = 2 * size_of::<TimerGroup>();
        assert_eq!(broker.reserved(), base_bytes);
        assert!(timers
            .register(3, TimerDomain::EventTime, timer(9, b"b", b"w"))
            .unwrap());
        assert!(timers
            .register(2, TimerDomain::EventTime, timer(9, b"a", b"w"))
            .unwrap());
        assert!(!timers
            .register(2, TimerDomain::EventTime, timer(9, b"a", b"w"))
            .unwrap());
        assert_eq!(broker.reserved(), base_bytes + 2 * (timer_node_bytes() + 2));
        timers
            .register(2, TimerDomain::EventTime, timer(10, b"a", b"w"))
            .unwrap();

        let fired = timers.advance(TimerDomain::EventTime, 9).unwrap();
        assert_eq!(
            fired,
            vec![
                FiredTimer {
                    key_group: 2,
                    domain: TimerDomain::EventTime,
                    timer: timer(9, b"a", b"w"),
                },
                FiredTimer {
                    key_group: 3,
                    domain: TimerDomain::EventTime,
                    timer: timer(9, b"b", b"w"),
                },
            ]
        );
        assert_eq!(timers.timer_count(TimerDomain::EventTime), 1);
        assert_eq!(broker.reserved(), base_bytes + timer_node_bytes() + 2);
        assert!(timers
            .delete(2, TimerDomain::EventTime, &timer(10, b"a", b"w"))
            .unwrap());
        assert_eq!(broker.reserved(), base_bytes);
        drop(timers);
        assert_eq!(broker.reserved(), 0);
    }

    #[test]
    fn canonical_key_group_snapshots_restore_across_rescaling() {
        let broker = Arc::new(TestBroker::new(1 << 20));
        let mut before = service(0, 3, broker.clone());
        before
            .register(1, TimerDomain::EventTime, timer(20, b"key", b"window"))
            .unwrap();
        before
            .register(
                2,
                TimerDomain::ProcessingTime,
                timer(30, b"other", b"session"),
            )
            .unwrap();
        let group_one = before.snapshot_key_group(1).unwrap();
        let group_two = before.snapshot_key_group(2).unwrap();

        let mut left = service(0, 1, broker.clone());
        left.restore_key_group(1, &group_one).unwrap();
        let mut right = service(2, 3, broker.clone());
        right.restore_key_group(2, &group_two).unwrap();
        assert_eq!(
            left.advance(TimerDomain::EventTime, 20).unwrap()[0].timer,
            timer(20, b"key", b"window")
        );
        assert_eq!(
            right.advance(TimerDomain::ProcessingTime, 30).unwrap()[0].timer,
            timer(30, b"other", b"session")
        );
        drop(right);
        drop(left);
        drop(before);
        assert_eq!(broker.reserved(), 0);
    }

    #[test]
    fn rejects_corrupt_and_wrong_key_group_snapshots() {
        let broker = Arc::new(TestBroker::new(1 << 20));
        let timers = service(1, 1, broker.clone());
        let snapshot = timers.snapshot_key_group(1).unwrap();
        let mut restored = service(2, 2, broker.clone());
        assert!(restored.restore_key_group(2, &snapshot).is_err());
        assert!(decode_snapshot(1, &snapshot[..snapshot.len() - 1]).is_err());
    }
}
