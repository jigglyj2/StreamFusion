// Copyright 2026 StreamFusion Authors
// Licensed under the Apache License, Version 2.0

use super::*;
use crate::memory_pool::MemoryReservationBroker;
use std::sync::atomic::{AtomicUsize, Ordering};

#[derive(Debug)]
struct CountingBroker {
    inner: TestBroker,
    calls: AtomicUsize,
}

impl MemoryReservationBroker for CountingBroker {
    fn try_reserve(&self, bytes: usize) -> Result<bool> {
        self.calls.fetch_add(1, Ordering::Relaxed);
        self.inner.try_reserve(bytes)
    }

    fn release(&self, bytes: usize) -> Result<()> {
        if bytes != 0 {
            self.calls.fetch_add(1, Ordering::Relaxed);
        }
        self.inner.release(bytes)
    }

    fn available(&self) -> Result<Option<usize>> {
        self.inner.available()
    }
}

#[test]
fn streaming_reservation_calls_follow_buffer_growth_instead_of_rows_or_state_keys() {
    let broker = Arc::new(CountingBroker {
        inner: TestBroker::new(128 << 20),
        calls: AtomicUsize::new(0),
    });
    let mut join = RegularJoinProcessor::new(
        &plan(proto::RegularJoinType::Inner),
        128,
        0,
        127,
        HostMemoryReservation::new(broker.clone(), "coarse join budget"),
    )
    .unwrap();
    let keys = (0..1024).collect::<Vec<i64>>();
    for (side, expected) in [(0, 0), (1, keys.len()), (0, keys.len())] {
        broker.calls.store(0, Ordering::Relaxed);
        join.begin_streaming_batch(
            side,
            batch(&keys, &vec!["value"; keys.len()], &vec![INSERT; keys.len()]),
        )
        .unwrap();
        let mut rows = 0;
        while let Some(batch) = join.next_streaming_batch().unwrap() {
            rows += batch.num_rows();
        }
        assert_eq!(rows, expected);
        let calls = broker.calls.load(Ordering::Relaxed);
        assert!(
            calls < 64,
            "{calls} broker calls for {} input rows on side {side}",
            keys.len()
        );
    }
    drop(join);
    assert_eq!(broker.inner.reserved(), 0);
}
