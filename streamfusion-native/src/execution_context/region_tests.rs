// Copyright 2026 StreamFusion Authors
// Licensed under the Apache License, Version 2.0.

use super::*;
use crate::memory_pool::tests_support::TestBroker;
use arrow::array::{ArrayRef, Int32Array, Int64Array, Int8Array};
use arrow::record_batch::RecordBatch;
use futures::{FutureExt, StreamExt};
const LIMIT: usize = 8 << 20;
fn context() -> (Arc<NativeExecutionContext>, Arc<TestBroker>) {
    let broker = Arc::new(TestBroker::new(LIMIT));
    let pool: Arc<dyn MemoryPool> = Arc::new(FlinkMemoryPool::new(broker.clone(), LIMIT));
    let context = NativeExecutionContext::new_region(
        include_bytes!(concat!(
            env!("CARGO_MANIFEST_DIR"),
            "/../streamfusion-proto/src/test/resources/native-region-v1.pb"
        )),
        pool,
    )
    .unwrap();
    (Arc::new(context), broker)
}
fn batch(value: i32) -> RecordBatch {
    RecordBatch::try_from_iter(vec![
        (
            "n",
            Arc::new(Int32Array::from(vec![Some(value), None])) as ArrayRef,
        ),
        (
            "__streamfusion_owned_timestamp_v1",
            Arc::new(Int64Array::from(vec![Some(10), None])) as ArrayRef,
        ),
        (
            "__streamfusion_row_kind",
            Arc::new(Int8Array::from(vec![0, 1])) as ArrayRef,
        ),
        (
            "__streamfusion_input_row",
            Arc::new(Int32Array::from(vec![0, 1])) as ArrayRef,
        ),
    ])
    .unwrap()
}
#[test]
fn region_context_retains_one_plan_and_counts_each_stage_once_across_invocations() {
    let (context, broker) = context();
    assert!(context.requires_input_envelope());
    assert_eq!(context.protocol_version(), 3);
    assert!(context.tree_plan().is_err());
    assert!(context.start(vec![batch(1)]).is_err());
    context.require_idle().unwrap();
    for index in 1..=3 {
        let input = batch(index);
        let mut output = context.start_region(vec![input.clone()]).unwrap();
        let first = output.next().now_or_never().unwrap().unwrap().unwrap();
        assert!(context.require_idle().is_err());
        assert!(context.start_region(vec![input.clone()]).is_err());
        let second = output.next().now_or_never().unwrap().unwrap().unwrap();
        assert_ne!(first.port, second.port);
        for column in 0..4 {
            assert!(Arc::ptr_eq(
                first.batch.column(column),
                input.column(column)
            ));
            assert!(Arc::ptr_eq(
                second.batch.column(column),
                input.column(column)
            ));
        }
        assert!(output.next().now_or_never().unwrap().is_none());
        context.require_idle().unwrap();
        assert_eq!(
            context.metric_snapshot().unwrap(),
            vec![
                4294967301,
                index as i64 * 2,
                index as i64 * 2,
                4294967302,
                index as i64 * 2,
                index as i64 * 2,
                4294967303,
                index as i64 * 2,
                index as i64 * 2
            ]
        );
        assert_eq!(
            context
                .input_batch_count(&[4294967301, 4294967303])
                .unwrap(),
            index as u64 * 2
        );
        assert_eq!(context.metric_value(0, "nonexistent").unwrap(), 0);
        assert_eq!(context.metric_value(4294967302, "nonexistent").unwrap(), 0);
        assert!(context.metric_value(99, "nonexistent").is_err());
    }
    assert_eq!(context.stream_creations.load(Ordering::Relaxed), 3);
    drop(context);
    assert_eq!(broker.reserved(), 0);
}
#[test]
fn failed_region_lowering_can_retry_and_cancellation_requires_recovery() {
    let (context, broker) = context();
    let baseline = broker.reserved();
    let pressure = context.reservation("other Flink consumer");
    pressure.try_grow(LIMIT - baseline).unwrap();
    assert!(context.start_region(vec![batch(1)]).is_err());
    context.require_idle().unwrap();
    assert!(context.physical_plan.lock().unwrap().is_none());
    drop(pressure);
    assert_eq!(broker.reserved(), baseline);
    let mut output = context.start_region(vec![batch(1)]).unwrap();
    let held = output.next().now_or_never().unwrap().unwrap().unwrap();
    drop(output);
    assert!(context.require_idle().is_err());
    assert!(context.start_region(vec![batch(2)]).is_err());
    drop(held);
    drop(context);
    assert_eq!(broker.reserved(), 0);
}
