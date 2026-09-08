// Copyright 2026 StreamFusion Authors
// Licensed under the Apache License, Version 2.0.
use super::*;
use crate::memory_pool::{
    arrow_lease, buffer_size, tests_support::TestBroker, FlinkMemoryPool, HostMemoryReservation,
};
use crate::planner::operators::reusable_input::ReusableInputExec;

#[test]
fn multi_megabyte_shared_payload_keeps_one_flink_lease_through_last_output() {
    for cancel in [false, true] {
        let size = 1 << 18;
        let batch = RecordBatch::try_from_iter(vec![
            ("n", Arc::new(Int32Array::from(vec![7; size])) as ArrayRef),
            (
                "__streamfusion_owned_timestamp_v1",
                Arc::new(Int64Array::from(vec![99; size])) as ArrayRef,
            ),
            (
                "__streamfusion_row_kind",
                Arc::new(Int8Array::from(vec![0; size])) as ArrayRef,
            ),
            (
                "__streamfusion_input_row",
                Arc::new(Int32Array::from_iter_values(0..size as i32)) as ArrayRef,
            ),
        ])
        .unwrap();
        let bytes = buffer_size::batch_bytes(&batch).unwrap();
        let broker = Arc::new(TestBroker::new(16 << 20));
        let pool: Arc<dyn MemoryPool> = Arc::new(FlinkMemoryPool::new(broker.clone(), 16 << 20));
        let input = Arc::new(ReusableInputExec::new(batch.schema()));
        let mut host = HostMemoryReservation::new(broker.clone(), "large region input");
        host.resize(bytes).unwrap();
        input
            .replace_batch(arrow_lease::host_batch(batch, host).unwrap())
            .unwrap();
        let contract = RegionPlan::decode(&message().encode_to_vec(), &pool).unwrap();
        let region =
            PhysicalRegion::lower(contract, vec![input.clone()], &[], pool.clone()).unwrap();
        let baseline = broker.reserved();
        let task = task(pool.clone());
        let mut stream = region
            .start(
                task.clone(),
                Box::new(move |success| assert_eq!(success, !cancel)),
            )
            .unwrap();
        let held = stream
            .next()
            .now_or_never()
            .unwrap()
            .unwrap()
            .unwrap()
            .batch;
        assert_eq!(broker.reserved(), baseline);
        if !cancel {
            let other = stream
                .next()
                .now_or_never()
                .unwrap()
                .unwrap()
                .unwrap()
                .batch;
            for index in 0..4 {
                assert!(Arc::ptr_eq(held.column(index), other.column(index)));
            }
            assert!(stream.next().now_or_never().unwrap().is_none());
            drop(other);
        }
        drop(stream);
        input.clear();
        drop(region);
        drop(task);
        drop(input);
        assert_eq!(pool.reserved(), 0);
        assert_eq!(broker.reserved(), bytes);
        assert_eq!(held.num_rows(), size);
        drop(held);
        assert_eq!(broker.reserved(), 0);
    }
}
