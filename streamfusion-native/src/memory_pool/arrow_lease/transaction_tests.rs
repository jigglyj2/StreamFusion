// Copyright 2026 StreamFusion Authors
// Licensed under the Apache License, Version 2.0.

use super::*;
use crate::memory_pool::{tests_support::TestBroker, FlinkMemoryPool, MemoryReservationBroker};
use arrow::array::Int32Array;
use arrow::buffer::ScalarBuffer;
use arrow::datatypes::{DataType, Field, Schema};
use datafusion::execution::memory_pool::{MemoryConsumer, MemoryPool};
use std::sync::atomic::{AtomicBool, AtomicUsize, Ordering};

struct Payload {
    bytes: Vec<u8>,
    freed: Arc<AtomicBool>,
}
impl Drop for Payload {
    fn drop(&mut self) {
        self.bytes.clear();
        self.bytes.shrink_to_fit();
        self.freed.store(true, Ordering::Relaxed);
    }
}

#[derive(Debug)]
struct ReleaseOrder {
    inner: TestBroker,
    freed: Arc<AtomicBool>,
    releases: AtomicUsize,
}
impl MemoryReservationBroker for ReleaseOrder {
    fn try_reserve(&self, bytes: usize) -> Result<bool> {
        self.inner.try_reserve(bytes)
    }
    fn release(&self, bytes: usize) -> Result<()> {
        assert!(
            self.freed.load(Ordering::Relaxed),
            "credit returned while payload is live"
        );
        self.releases.fetch_add(1, Ordering::Relaxed);
        self.inner.release(bytes)
    }
    fn available(&self) -> Result<Option<usize>> {
        self.inner.available()
    }
}

#[test]
fn expression_lease_denial_frees_payload_before_returning_credit() {
    for undersized in [false, true] {
        let freed = Arc::new(AtomicBool::new(false));
        let owner = Arc::new(Payload {
            bytes: vec![1, 0, 0, 0],
            freed: freed.clone(),
        });
        let pointer = NonNull::new(owner.bytes.as_ptr() as *mut u8).unwrap();
        // SAFETY: producer owns exactly these four bytes until the last buffer releases it.
        let buffer = unsafe { Buffer::from_custom_allocation(pointer, 4, owner) };
        let array = Arc::new(Int32Array::new(ScalarBuffer::new(buffer, 0, 1), None));
        let credit = 3 - usize::from(undersized);
        let broker = Arc::new(ReleaseOrder {
            inner: TestBroker::new(credit),
            freed: freed.clone(),
            releases: AtomicUsize::new(0),
        });
        let pool: Arc<dyn MemoryPool> = Arc::new(FlinkMemoryPool::new(broker.clone(), credit));
        let reservation = MemoryConsumer::new("expression lease denial").register(&pool);
        reservation.try_grow(credit).unwrap();
        assert!(datafusion_array(array, reservation).is_err());
        assert!(freed.load(Ordering::Relaxed));
        assert_eq!(broker.inner.reserved(), 0);
        assert_eq!(broker.releases.load(Ordering::Relaxed), 1);
    }
}

#[test]
fn batch_lease_denial_frees_payload_before_returning_credit() {
    for host in [false, true] {
        for undersized in [false, true] {
            let freed = Arc::new(AtomicBool::new(false));
            let owner = Arc::new(Payload {
                bytes: vec![1, 0, 0, 0],
                freed: freed.clone(),
            });
            let pointer = NonNull::new(owner.bytes.as_ptr() as *mut u8).unwrap();
            // SAFETY: owner keeps these four immutable bytes alive until buffer release.
            let buffer = unsafe { Buffer::from_custom_allocation(pointer, 4, owner) };
            let array = Arc::new(Int32Array::new(ScalarBuffer::new(buffer, 0, 1), None));
            let batch = RecordBatch::try_new(
                Arc::new(Schema::new(vec![Field::new(
                    "value",
                    DataType::Int32,
                    false,
                )])),
                vec![array],
            )
            .unwrap();
            let credit = 3 - usize::from(undersized);
            let broker = Arc::new(ReleaseOrder {
                inner: TestBroker::new(credit),
                freed: freed.clone(),
                releases: AtomicUsize::new(0),
            });
            let result = if host {
                let mut reservation = HostMemoryReservation::new(broker.clone(), "batch denial");
                reservation.try_grow(credit).unwrap();
                host_batch(batch, reservation)
            } else {
                let pool: Arc<dyn MemoryPool> =
                    Arc::new(FlinkMemoryPool::new(broker.clone(), credit));
                let reservation = MemoryConsumer::new("batch denial").register(&pool);
                reservation.try_grow(credit).unwrap();
                datafusion_batch(batch, reservation)
            };
            assert!(result.is_err());
            assert!(freed.load(Ordering::Relaxed));
            assert_eq!(broker.inner.reserved(), 0);
            assert_eq!(broker.releases.load(Ordering::Relaxed), 1);
        }
    }
}
