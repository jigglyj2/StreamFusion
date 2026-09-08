// Copyright 2026 StreamFusion Authors
// Licensed under the Apache License, Version 2.0.

use super::*;
use crate::memory_pool::{tests_support::TestBroker, MemoryReservationBroker};
use std::sync::{
    atomic::{AtomicBool, AtomicUsize, Ordering},
    Arc,
};

#[derive(Debug)]
struct Broker {
    inner: TestBroker,
    calls: AtomicUsize,
    deny: AtomicBool,
}
impl Broker {
    fn new() -> Arc<Self> {
        Arc::new(Self {
            inner: TestBroker::new(16 << 20),
            calls: AtomicUsize::new(0),
            deny: AtomicBool::new(false),
        })
    }
}
impl MemoryReservationBroker for Broker {
    fn try_reserve(&self, bytes: usize) -> Result<bool> {
        self.calls.fetch_add(1, Ordering::Relaxed);
        if self.deny.load(Ordering::Relaxed) {
            return Ok(false);
        }
        self.inner.try_reserve(bytes)
    }
    fn release(&self, bytes: usize) -> Result<()> {
        self.inner.release(bytes)
    }
}
fn service(broker: Arc<Broker>) -> NativeTimerService {
    NativeTimerService::new(2, 3, HostMemoryReservation::new(broker, "batched timers")).unwrap()
}
fn registration(group: u32, timestamp: i64, bytes: usize) -> (u32, TimerDomain, TimerKey) {
    (
        group,
        TimerDomain::EventTime,
        TimerKey {
            timestamp,
            key: vec![7; bytes],
            namespace: vec![3; 16],
        },
    )
}

#[test]
fn registration_admits_once_and_rejects_atomically_without_changing_existing_timers() {
    let broker = Broker::new();
    let mut timers = service(broker.clone());
    let baseline = broker.calls.load(Ordering::Relaxed);
    let inputs = (0..4096)
        .map(|i| registration(2 + i % 2, (i % 100) as i64, 8))
        .collect();
    assert_eq!(timers.register_batch(inputs).unwrap().len(), 100);
    assert_eq!(broker.calls.load(Ordering::Relaxed) - baseline, 1);
    let before = timers.snapshot_key_group(2).unwrap();
    let reserved = broker.inner.reserved();
    broker.deny.store(true, Ordering::Relaxed);
    assert!(timers
        .register_batch(vec![registration(2, 1000, 1024)])
        .is_err());
    assert_eq!(timers.snapshot_key_group(2).unwrap(), before);
    assert_eq!(broker.inner.reserved(), reserved);
    // An existing timer requires no additional reservation, even under pressure.
    assert!(timers
        .register_batch(vec![registration(2, 0, 8)])
        .unwrap()
        .is_empty());
    broker.deny.store(false, Ordering::Relaxed);
    assert!(timers
        .register_batch(vec![registration(2, 1000, 8), registration(4, 1000, 8)])
        .is_err());
    assert_eq!(timers.snapshot_key_group(2).unwrap(), before);
    assert_eq!(broker.inner.reserved(), reserved);
    drop(timers);
    assert_eq!(broker.inner.reserved(), 0);
}

#[test]
fn owned_firing_keeps_large_keys_admitted_until_the_callback_batch_drops() {
    let broker = Broker::new();
    let mut timers = service(broker.clone());
    timers
        .register_batch(vec![
            registration(2, 9, 64 << 10),
            registration(3, 8, 64 << 10),
        ])
        .unwrap();
    let before = broker.inner.reserved();
    let snapshot = timers.snapshot_key_group(2).unwrap();
    broker.deny.store(true, Ordering::Relaxed);
    assert!(timers
        .advance_owned_limited(TimerDomain::EventTime, 10, 2)
        .is_err());
    assert_eq!(timers.snapshot_key_group(2).unwrap(), snapshot);
    assert_eq!(timers.timer_count(TimerDomain::EventTime), 2);
    broker.deny.store(false, Ordering::Relaxed);
    let fired = timers
        .advance_owned_limited(TimerDomain::EventTime, 10, 2)
        .unwrap();
    assert_eq!(
        fired
            .iter()
            .map(|timer| timer.timer.timestamp)
            .collect::<Vec<_>>(),
        [9, 8]
    );
    assert!(broker.inner.reserved() >= before);
    assert_eq!(timers.timer_count(TimerDomain::EventTime), 0);
    drop(timers);
    assert!(broker.inner.reserved() >= 128 << 10);
    assert_eq!(fired[1].timer.key.len(), 64 << 10);
    drop(fired);
    assert_eq!(broker.inner.reserved(), 0);
}

#[test]
fn owned_pages_preserve_legacy_timer_order_and_canonical_restore() {
    let broker = Broker::new();
    let mut source = service(broker.clone());
    source
        .register_batch(
            (0..101)
                .map(|i| registration(2 + i % 2, i as i64, 8))
                .collect(),
        )
        .unwrap();
    let mut restored = service(broker.clone());
    for group in 2..=3 {
        restored
            .restore_key_group(group, &source.snapshot_key_group(group).unwrap())
            .unwrap();
    }
    let expected = source.advance(TimerDomain::EventTime, 80).unwrap();
    let mut offset = 0;
    while let Some(next) = restored.next_timestamp(TimerDomain::EventTime) {
        if next > 80 {
            break;
        }
        let batch = restored
            .advance_owned_limited(TimerDomain::EventTime, 80, 7)
            .unwrap();
        assert_eq!(&*batch, &expected[offset..offset + batch.len()]);
        offset += batch.len();
    }
    assert_eq!(offset, expected.len());
    for group in 2..=3 {
        assert_eq!(
            restored.snapshot_key_group(group).unwrap(),
            source.snapshot_key_group(group).unwrap()
        );
    }
    drop(restored);
    drop(source);
    assert_eq!(broker.inner.reserved(), 0);
}
