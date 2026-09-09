// Copyright 2026 StreamFusion Authors
// Licensed under the Apache License, Version 2.0.

use super::*;
use crate::memory_pool::MemoryReservationBroker;
use std::sync::atomic::{AtomicUsize, Ordering};

#[derive(Debug)]
struct Broker {
    inner: TestBroker,
    calls: AtomicUsize,
}
impl MemoryReservationBroker for Broker {
    fn try_reserve(&self, bytes: usize) -> Result<bool> {
        self.calls.fetch_add(1, Ordering::Relaxed);
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
fn distinct_partition_reads_amortize_host_admission() {
    for rocks in [false, true] {
        let broker = Arc::new(Broker {
            inner: TestBroker::new(64 << 20),
            calls: AtomicUsize::new(0),
        });
        let directory = tempfile::tempdir().unwrap();
        let mut target = processor(broker.clone(), rocks.then_some(directory.path()));
        let input = RecordBatch::try_from_iter(vec![
            (
                "key",
                Arc::new(Int64Array::from_iter_values(0..1024)) as ArrayRef,
            ),
            (
                "ts",
                Arc::new(TimestampMillisecondArray::from(vec![10000; 1024])) as ArrayRef,
            ),
        ])
        .unwrap();
        target.process(&input).unwrap();
        broker.calls.store(0, Ordering::Relaxed);
        target.process(&input).unwrap();
        // More than a thousand existing interval pages need only coarse host admissions.
        // This includes all workspace, timer and backend reservations, not just the read loop.
        assert!(broker.calls.load(Ordering::Relaxed) < 64);
        assert_eq!(advance(&mut target, 19999), vec![(2, 10000, 20000); 1024]);
        drop(target);
        assert_eq!(broker.inner.reserved(), 0);
    }
}

#[test]
fn optional_workspace_headroom_does_not_reject_an_exact_fit() {
    let broker = Arc::new(TestBroker::new(100_001));
    let mut reservation = HostMemoryReservation::new(broker.clone(), "workspace");
    admit_workspace(&mut reservation, 65_537).unwrap();
    assert_eq!(reservation.size(), 65_537);
    admit_workspace(&mut reservation, 100_001).unwrap();
    assert_eq!(broker.reserved(), 100_001);
    assert!(matches!(
        admit_workspace(&mut reservation, 100_002),
        Err(DataFusionError::ResourcesExhausted(_))
    ));
    assert_eq!(broker.reserved(), 100_001);
    drop(reservation);
    assert_eq!(broker.reserved(), 0);
}
