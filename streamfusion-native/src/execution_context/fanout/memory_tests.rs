// Copyright 2026 StreamFusion Authors
// Licensed under the Apache License, Version 2.0.
use super::*;
use crate::memory_pool::{
    arrow_lease, buffer_size, tests_support::TestBroker, HostMemoryReservation,
};
use arrow::array::Int32Array;
use datafusion::physical_plan::memory::MemoryStream;
use futures::{FutureExt, StreamExt};

#[test]
fn large_shared_buffer_has_one_flink_lease_until_the_last_consumer_releases_it() {
    for cancel in [false, true] {
        let batch = RecordBatch::try_from_iter(vec![(
            "n",
            Arc::new(Int32Array::from(vec![7; 1 << 18])) as arrow::array::ArrayRef,
        )])
        .unwrap();
        let bytes = buffer_size::batch_bytes(&batch).unwrap();
        let broker = Arc::new(TestBroker::new(bytes));
        let mut memory = HostMemoryReservation::new(broker.clone(), "shared batch producer");
        memory.resize(bytes).unwrap();
        let batch = arrow_lease::host_batch(batch, memory).unwrap();
        let source = MemoryStream::try_new(vec![batch.clone()], batch.schema(), None).unwrap();
        drop(batch);
        let mut readers = split(Box::pin(source), 3, Box::new(|_| {}));
        let held = readers[0].next().now_or_never().unwrap().unwrap().unwrap();
        assert_eq!(broker.reserved(), bytes);
        if cancel {
            drop(readers.remove(1));
        } else {
            let second = readers[1].next().now_or_never().unwrap().unwrap().unwrap();
            let third = readers[2].next().now_or_never().unwrap().unwrap().unwrap();
            assert!(Arc::ptr_eq(held.column(0), second.column(0)));
            assert!(Arc::ptr_eq(held.column(0), third.column(0)));
            assert_eq!(broker.reserved(), bytes);
            for reader in &mut readers {
                assert!(reader.next().now_or_never().unwrap().is_none());
            }
            drop(second);
            drop(third);
        }
        drop(readers);
        assert_eq!(broker.reserved(), bytes);
        assert_eq!(held.num_rows(), 1 << 18);
        drop(held);
        assert_eq!(broker.reserved(), 0);
    }
}
