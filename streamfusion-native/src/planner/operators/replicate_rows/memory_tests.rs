// Copyright 2026 StreamFusion Authors
// Licensed under the Apache License, Version 2.0

use super::*;
use crate::memory_pool::{tests_support::TestBroker, FlinkMemoryPool, MemoryReservationBroker};
use arrow::array::{Int32Array, StringArray};
use arrow::datatypes::{DataType, Field};
use datafusion::execution::memory_pool::MemoryPool;
use datafusion::physical_expr::expressions::Column;

fn work(
    broker: &Arc<impl MemoryReservationBroker + 'static>,
    count: i64,
    wide: bool,
) -> ReplicationWork {
    let mut counts = vec![0i64; 1000];
    counts[17] = count;
    work_with_counts(broker, counts, wide)
}

fn work_with_counts(
    broker: &Arc<impl MemoryReservationBroker + 'static>,
    counts: Vec<i64>,
    wide: bool,
) -> ReplicationWork {
    let wide_value = "é".repeat(if wide { 32 * 1024 } else { 1 });
    let mut values = vec![""; 1000];
    values[17] = &wide_value;
    let schema = Arc::new(Schema::new(vec![
        Field::new("count", DataType::Int64, false),
        Field::new("value", DataType::Utf8, false),
        Field::new("__streamfusion_input_row", DataType::Int32, false),
    ]));
    let batch = RecordBatch::try_new(
        schema.clone(),
        vec![
            Arc::new(Int64Array::from(counts)),
            Arc::new(StringArray::from(values)),
            Arc::new(Int32Array::from_iter_values(0..1000)),
        ],
    )
    .unwrap();
    let output_schema = Arc::new(Schema::new(vec![
        schema.field(0).as_ref().clone(),
        schema.field(1).as_ref().clone(),
        Field::new("replicated", DataType::Utf8, false),
        schema.field(2).as_ref().clone(),
    ]));
    let pool: Arc<dyn MemoryPool> = Arc::new(FlinkMemoryPool::new(broker.clone(), 64 << 20));
    ReplicationWork::new(
        batch,
        &Column::new("count", 0),
        &[Arc::new(Column::new("value", 1))],
        output_schema,
        MemoryConsumer::new("replication test").register(&pool),
        None,
    )
    .unwrap()
}

#[test]
fn normal_batch_admission_does_not_call_the_host_broker_per_input_row() {
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
            self.inner.release(bytes)
        }
    }
    let broker = Arc::new(CountingBroker {
        inner: TestBroker::new(64 << 20),
        calls: AtomicUsize::new(0),
    });
    let mut work = work_with_counts(&broker, vec![1; 1000], false);
    let before = broker.calls.load(Ordering::Relaxed);
    let output = work.next_batch().unwrap();
    assert_eq!(output.num_rows(), 1000);
    assert!(broker.calls.load(Ordering::Relaxed) - before <= 4);
    drop(work);
    assert!(broker.inner.reserved() > 0);
    drop(output);
    assert_eq!(broker.inner.reserved(), 0);
}

#[test]
fn wide_skewed_rows_use_smaller_pulls_and_keep_sources_admitted() {
    let broker = Arc::new(TestBroker::new(64 << 20));
    let mut work = work(&broker, 101, true);
    let baseline = broker.reserved();
    assert!(baseline > 0);
    let mut rows = 0;
    let mut pulls = 0;
    while !work.is_finished() {
        let output = work.next_batch().unwrap();
        assert!(output.num_rows() < 101);
        for row in 0..output.num_rows() {
            assert_eq!(
                output
                    .column(1)
                    .as_any()
                    .downcast_ref::<StringArray>()
                    .unwrap()
                    .value(row)
                    .len(),
                64 * 1024
            );
            assert_eq!(
                output
                    .column(3)
                    .as_any()
                    .downcast_ref::<Int32Array>()
                    .unwrap()
                    .value(row),
                17
            );
        }
        rows += output.num_rows();
        pulls += 1;
        drop(output);
        assert_eq!(broker.reserved(), baseline);
    }
    assert_eq!(rows, 101);
    assert!(pulls > 1);
    drop(work);
    assert_eq!(broker.reserved(), 0);
}

#[test]
fn denied_gather_does_not_advance_cursor_or_release_live_source_buffers() {
    let broker = Arc::new(TestBroker::new(256 << 10));
    let mut work = work(&broker, 101, true);
    let cursor = (work.row, work.emitted_for_row);
    assert!(matches!(
        work.next_batch(),
        Err(DataFusionError::ResourcesExhausted(_))
    ));
    assert_eq!((work.row, work.emitted_for_row), cursor);
    assert!(broker.reserved() > 0);
    drop(work);
    assert_eq!(broker.reserved(), 0);
}

#[test]
fn chunk_size_adapts_to_flink_memory_denial_before_gathering() {
    let broker = Arc::new(TestBroker::new(1 << 20));
    let mut work = work(&broker, 5, true);
    let mut rows = 0;
    while !work.is_finished() {
        let batch = work.next_batch().unwrap();
        assert_eq!(batch.num_rows(), 1);
        rows += batch.num_rows();
    }
    assert_eq!(rows, 5);
    drop(work);
    assert_eq!(broker.reserved(), 0);
}
