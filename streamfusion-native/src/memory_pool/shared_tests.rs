// Copyright 2026 StreamFusion Authors
// Licensed under the Apache License, Version 2.0.

use super::tests_support::TestBroker;
use super::*;
use datafusion::execution::memory_pool::MemoryConsumer;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::Barrier;

#[derive(Debug)]
struct ObservedBroker {
    inner: TestBroker,
    limit_queries: AtomicUsize,
}

impl MemoryReservationBroker for ObservedBroker {
    fn limit(&self) -> Result<Option<usize>> {
        self.limit_queries.fetch_add(1, Ordering::Relaxed);
        self.inner.limit()
    }
    fn try_reserve(&self, bytes: usize) -> Result<bool> {
        self.inner.try_reserve(bytes)
    }
    fn release(&self, bytes: usize) -> Result<()> {
        self.inner.release(bytes)
    }
    fn available(&self) -> Result<Option<usize>> {
        self.inner.available()
    }
}

#[test]
fn sub_pool_can_use_capacity_released_after_its_creation() {
    let broker = Arc::new(ObservedBroker {
        inner: TestBroker::new(128),
        limit_queries: AtomicUsize::new(0),
    });
    let mut peer = HostMemoryReservation::new(broker.clone(), "another Flink operator");
    peer.resize(96).unwrap();
    let pool = peer.datafusion_pool().unwrap();
    let consumer = MemoryConsumer::new("DataFusion state").register(&pool);
    assert!(matches!(pool.memory_limit(), MemoryLimit::Finite(128)));
    consumer.try_grow(32).unwrap();
    assert!(consumer.try_grow(1).is_err());
    assert_eq!(pool.reserved(), 32);
    peer.resize(0).unwrap();
    consumer.try_grow(96).unwrap();
    assert_eq!(pool.reserved(), 128);
    assert_eq!(broker.inner.reserved(), 128);
    assert!(consumer.try_grow(1).is_err());
    assert!(matches!(pool.memory_limit(), MemoryLimit::Finite(128)));
    assert_eq!(broker.limit_queries.load(Ordering::Relaxed), 1);
    drop(consumer);
    assert_eq!(broker.inner.reserved(), 0);
}

#[test]
fn sibling_pools_keep_local_usage_and_share_actual_admission() {
    let broker = Arc::new(TestBroker::new(128));
    let owner = HostMemoryReservation::new(broker.clone(), "native region");
    let first = owner.datafusion_pool().unwrap();
    let left = MemoryConsumer::new("left").register(&first);
    left.try_grow(96).unwrap();
    let second = owner.datafusion_pool().unwrap();
    let right = MemoryConsumer::new("right").register(&second);
    right.try_grow(32).unwrap();
    assert_eq!((first.reserved(), second.reserved()), (96, 32));
    assert!(right.try_grow(1).is_err());
    drop(left);
    right.try_grow(96).unwrap();
    assert_eq!((first.reserved(), second.reserved()), (0, 128));
    drop(right);
    assert_eq!(broker.reserved(), 0);
}

#[test]
fn concurrent_sub_pools_cannot_reserve_the_same_free_capacity() {
    let broker = Arc::new(TestBroker::new(128));
    let owner = HostMemoryReservation::new(broker.clone(), "native region");
    let ready = Barrier::new(3);
    let release = Barrier::new(3);
    std::thread::scope(|scope| {
        for _ in 0..2 {
            let pool = owner.datafusion_pool().unwrap();
            let ready = &ready;
            let release = &release;
            scope.spawn(move || {
                let reservation = MemoryConsumer::new("concurrent DataFusion").register(&pool);
                let _admitted = reservation.try_grow(80);
                ready.wait();
                release.wait();
            });
        }
        ready.wait();
        let held = broker.reserved();
        release.wait();
        assert_eq!(held, 80);
    });
    assert_eq!(broker.reserved(), 0);
}

#[derive(Debug)]
struct OpaqueBroker(TestBroker);

impl MemoryReservationBroker for OpaqueBroker {
    fn try_reserve(&self, bytes: usize) -> Result<bool> {
        self.0.try_reserve(bytes)
    }
    fn release(&self, bytes: usize) -> Result<()> {
        self.0.release(bytes)
    }
}

#[test]
fn unknown_limit_remains_host_governed_without_inventing_a_budget() {
    let broker = Arc::new(OpaqueBroker(TestBroker::new(128)));
    let mut owner = HostMemoryReservation::new(broker.clone(), "opaque host");
    owner.resize(96).unwrap();
    let pool = owner.datafusion_pool().unwrap();
    assert!(matches!(pool.memory_limit(), MemoryLimit::Unknown));
    let reservation = MemoryConsumer::new("opaque DataFusion").register(&pool);
    assert!(reservation.try_grow(33).is_err());
    owner.resize(0).unwrap();
    reservation.try_grow(128).unwrap();
    assert!(reservation.try_grow(1).is_err());
    drop(reservation);
    assert_eq!(broker.0.reserved(), 0);
}

#[derive(Debug)]
struct FailedAssignment;

impl MemoryReservationBroker for FailedAssignment {
    fn limit(&self) -> Result<Option<usize>> {
        Err(DataFusionError::Execution(
            "assigned budget unavailable".into(),
        ))
    }
    fn try_reserve(&self, _: usize) -> Result<bool> {
        panic!("failed setup must not reserve memory")
    }
    fn release(&self, _: usize) -> Result<()> {
        panic!("failed setup must not release unreserved memory")
    }
}

#[test]
fn failed_assignment_is_propagated_before_pool_creation() {
    let owner = HostMemoryReservation::new(Arc::new(FailedAssignment), "failed host");
    assert!(owner
        .datafusion_pool()
        .unwrap_err()
        .to_string()
        .contains("assigned budget unavailable"));
}

#[test]
fn zero_assigned_budget_is_not_an_unlimited_pool() {
    let broker = Arc::new(TestBroker::new(0));
    let owner = HostMemoryReservation::new(broker, "zero assignment");
    let pool = owner.datafusion_pool().unwrap();
    assert!(matches!(pool.memory_limit(), MemoryLimit::Finite(0)));
    let reservation = MemoryConsumer::new("zero DataFusion").register(&pool);
    assert!(reservation.try_grow(1).is_err());
}
