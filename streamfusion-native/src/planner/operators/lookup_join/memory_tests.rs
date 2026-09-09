// Copyright 2026 StreamFusion Authors
// Licensed under the Apache License, Version 2.0

use super::{test_support::*, LookupJoinExec, LookupTable};
use crate::planner::operators::{identified::IdentifiedExec, reusable_input::ReusableInputExec};
use arrow::array::{Array, Int64Array, RecordBatch};
use datafusion::physical_plan::{collect, ExecutionPlan};
use futures::StreamExt;
use std::sync::Arc;

#[test]
fn repeated_lookup_does_not_leave_growing_unreserved_heap_state() {
    let (context, broker) = context(8 << 20, 32);
    let table = LookupTable::new(
        batch(&rows(1, 37, false), false),
        vec![0],
        context.runtime_env().memory_pool.clone(),
    )
    .unwrap();
    let input = Arc::new(ReusableInputExec::new(schema(true)));
    let fixture = batch(&rows(1, 7, false), true);
    input.replace_batch(fixture.clone()).unwrap();
    let join = LookupJoinExec::new(table.clone(), input.clone(), vec![0]).unwrap();
    drop(futures::executor::block_on(collect(join.clone(), context.task_ctx())).unwrap());
    for invocations in [4, 2_048] {
        let (_, allocations) = crate::allocation_test_support::measure(|| {
            for _ in 0..invocations {
                input.replace_batch(fixture.clone()).unwrap();
                drop(
                    futures::executor::block_on(collect(join.clone(), context.task_ctx())).unwrap(),
                );
            }
        });
        assert!(
            allocations.live <= 4096,
            "lookup retained heap grows: {allocations:?}"
        );
        assert!(
            allocations.peak < 256 * 1024,
            "lookup workspace grows: {allocations:?}"
        );
    }
    drop(join);
    drop(table);
    assert_eq!(broker.reserved(), 0);
}

#[tokio::test]
async fn source_and_output_buffers_keep_their_single_owner_through_native_projection() {
    use crate::memory_pool::{arrow_lease, buffer_registry, buffer_size};
    use datafusion::execution::memory_pool::MemoryConsumer;
    use datafusion::physical_expr::{expressions::Column, PhysicalExpr};
    use datafusion::physical_plan::projection::ProjectionExec;
    let (context, broker) = context(8 << 20, 32);
    let pool = context.runtime_env().memory_pool.clone();
    let source = batch(&rows(1, 17, false), false);
    let memory = MemoryConsumer::new("test Arrow source owner").register(&pool);
    memory
        .try_grow(buffer_size::batch_bytes(&source).unwrap())
        .unwrap();
    let source =
        arrow_lease::datafusion_batch_registered(source, memory, buffer_registry(&pool)).unwrap();
    let source_bytes = broker.reserved();
    let pointer = source.column(0).to_data().buffers()[0].as_ptr();
    let table = LookupTable::new(source.clone(), vec![0], pool).unwrap();
    assert_eq!(
        table.batch.column(0).to_data().buffers()[0].as_ptr(),
        pointer
    );
    assert_eq!(broker.reserved(), source_bytes + 17 * 64 + 64 * 1024);
    drop(source);
    let input = Arc::new(ReusableInputExec::new(schema(true)));
    input
        .replace_batch(batch(&rows(1, 7, false), true))
        .unwrap();
    let join = LookupJoinExec::new(table.clone(), input, vec![0]).unwrap();
    let projection = Arc::new(
        ProjectionExec::try_new(
            vec![
                (
                    Arc::new(Column::new("id", 5)) as Arc<dyn PhysicalExpr>,
                    "match_id".into(),
                ),
                (
                    Arc::new(Column::new("text", 4)) as Arc<dyn PhysicalExpr>,
                    "match_text".into(),
                ),
            ],
            join.clone(),
        )
        .unwrap(),
    );
    let output = collect(projection.clone(), context.task_ctx())
        .await
        .unwrap();
    assert!(!output.is_empty());
    drop(projection);
    drop(join);
    drop(table);
    assert!(broker.reserved() > 0);
    assert!(output.iter().map(RecordBatch::num_rows).sum::<usize>() > 0);
    drop(output);
    assert_eq!(broker.reserved(), 0);
}

#[tokio::test]
async fn a_denied_probe_can_retry_after_credit_returns_without_rebuilding_the_cache() {
    use datafusion::execution::memory_pool::MemoryConsumer;
    let (context, broker) = context(1 << 20, 32);
    let pool = context.runtime_env().memory_pool.clone();
    let table = LookupTable::new(batch(&rows(1, 17, false), false), vec![0], pool.clone()).unwrap();
    let retained = broker.reserved();
    let input = Arc::new(ReusableInputExec::new(schema(true)));
    input
        .replace_batch(batch(&rows(1, 7, false), true))
        .unwrap();
    let join = LookupJoinExec::new(table.clone(), input, vec![0]).unwrap();
    let pressure = MemoryConsumer::new("test concurrent pressure").register(&pool);
    pressure.try_grow((1 << 20) - broker.reserved()).unwrap();
    let error = collect(join.clone(), context.task_ctx()).await.unwrap_err();
    assert!(error.to_string().contains("Resources exhausted"));
    drop(pressure);
    assert_eq!(broker.reserved(), retained);
    let output = collect(join.clone(), context.task_ctx()).await.unwrap();
    assert!(output.iter().map(RecordBatch::num_rows).sum::<usize>() > 0);
    drop(output);
    drop(join);
    drop(table);
    assert_eq!(broker.reserved(), 0);
}

#[tokio::test]
async fn a_live_native_stream_emits_small_batches_before_polling_the_next_input() {
    use futures::FutureExt;
    let (context, broker) = context(8 << 20, 1024);
    let table = LookupTable::new(
        batch(&rows(1, 17, false), false),
        vec![0],
        context.runtime_env().memory_pool.clone(),
    )
    .unwrap();
    let retained = broker.reserved();
    let input = Arc::new(ReusableInputExec::new(schema(true)));
    input.streaming(true);
    let join = LookupJoinExec::new(table.clone(), input.clone(), vec![0]).unwrap();
    let mut stream = join.execute(0, context.task_ctx()).unwrap();
    assert!(stream.next().now_or_never().is_none());
    for _ in 0..17 {
        input
            .replace_batch(batch(&rows(1, 7, false), true))
            .unwrap();
        let output = stream
            .next()
            .now_or_never()
            .expect("lookup buffered past its incoming batch")
            .unwrap()
            .unwrap();
        assert!(output.num_rows() > 0 && output.num_rows() < 1024);
        drop(output);
        assert!(stream.next().now_or_never().is_none());
        assert_eq!(broker.reserved(), retained);
    }
    drop(stream);
    drop(join);
    drop(table);
    assert_eq!(broker.reserved(), 0);
}

#[tokio::test]
async fn thousands_of_invocations_keep_cache_credit_and_metric_storage_constant() {
    let (context, broker) = context(8 << 20, 32);
    let table = LookupTable::new(
        batch(&rows(1, 37, false), false),
        vec![0],
        context.runtime_env().memory_pool.clone(),
    )
    .unwrap();
    let retained = broker.reserved();
    let input = Arc::new(ReusableInputExec::new(schema(true)));
    let join = LookupJoinExec::new(table.clone(), input.clone(), vec![0]).unwrap();
    let stage = IdentifiedExec::wrap(42, join.clone());
    let fixture = batch(&rows(1, 7, false), true);
    let mut output_rows = 0;
    for _ in 0..2_000 {
        input.replace_batch(fixture.clone()).unwrap();
        let result = collect(stage.clone(), context.task_ctx()).await.unwrap();
        output_rows += result
            .iter()
            .map(|batch| batch.num_rows() as u64)
            .sum::<u64>();
        drop(result);
        assert_eq!(broker.reserved(), retained);
        assert!(join.metrics().is_none()); // Stable IdentifiedExec counters own logical metrics.
    }
    assert_eq!(
        stage
            .downcast_ref::<IdentifiedExec>()
            .unwrap()
            .output_rows(),
        output_rows
    );
    drop(stage);
    drop(join);
    drop(table);
    assert_eq!(broker.reserved(), 0);
}

#[tokio::test]
async fn wide_duplicate_fanout_shrinks_work_and_can_be_cancelled_without_leaking() {
    let (context, broker) = context(4 << 20, 32);
    let side = (0..7)
        .map(|id| Row {
            key: Some(1),
            text: Some("v".repeat(96 * 1024)),
            id,
        })
        .collect::<Vec<_>>();
    let table = LookupTable::new(
        batch(&side, false),
        vec![0],
        context.runtime_env().memory_pool.clone(),
    )
    .unwrap();
    let retained = broker.reserved();
    let input = Arc::new(ReusableInputExec::new(schema(true)));
    let join = LookupJoinExec::new(table.clone(), input.clone(), vec![0]).unwrap();
    let fixture = vec![
        Row {
            key: Some(1),
            text: None,
            id: 3
        };
        17
    ];
    for cancel in [true, false] {
        input.replace_batch(batch(&fixture, true)).unwrap();
        let mut stream = join.execute(0, context.task_ctx()).unwrap();
        let mut count = 0;
        while let Some(output) = stream.next().await {
            let output = output.unwrap();
            assert!(output.num_rows() < 17 * 7);
            let ids = output
                .column(5)
                .as_any()
                .downcast_ref::<Int64Array>()
                .unwrap();
            for index in 0..ids.len() {
                assert_eq!(ids.value(index), count % 7);
                count += 1;
            }
            assert!(broker.reserved() <= 4 << 20);
            if cancel {
                break;
            }
        }
        drop(stream);
        if !cancel {
            assert_eq!(count, 17 * 7);
        }
        assert_eq!(broker.reserved(), retained);
    }
    drop(join);
    drop(table);
    assert_eq!(broker.reserved(), 0);
}

#[tokio::test]
async fn denial_and_empty_cache_leave_no_retained_work() {
    let (small, broker) = context(1024, 32);
    assert!(LookupTable::new(
        batch(&rows(1, 1000, false), false),
        vec![0],
        small.runtime_env().memory_pool.clone()
    )
    .is_err());
    assert_eq!(broker.reserved(), 0);
    let (context, broker) = context(1 << 20, 32);
    let table = LookupTable::new(
        batch(&[], false),
        vec![0],
        context.runtime_env().memory_pool.clone(),
    )
    .unwrap();
    let input = Arc::new(ReusableInputExec::new(schema(true)));
    input
        .replace_batch(batch(&rows(1, 17, false), true))
        .unwrap();
    let join = LookupJoinExec::new(table.clone(), input, vec![0]).unwrap();
    assert!(collect(join.clone(), context.task_ctx())
        .await
        .unwrap()
        .is_empty());
    drop(join);
    drop(table);
    assert_eq!(broker.reserved(), 0);
}
