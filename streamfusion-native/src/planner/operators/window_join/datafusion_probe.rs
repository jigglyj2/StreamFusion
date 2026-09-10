// Copyright 2026 StreamFusion Authors
// Licensed under the Apache License, Version 2.0

//! Test-only compute investigation for closed, insert-only inner window joins. This is not
//! production admission: Flink timer/state/metric integration and buffer ownership remain required.

use std::sync::Arc;

use arrow::array::{Array, Int64Array, UInt64Array};
use arrow::datatypes::{DataType, Field, Schema, SchemaRef};
use arrow::record_batch::RecordBatch;
use datafusion::common::JoinType;
use datafusion::datasource::memory::MemorySourceConfig;
use datafusion::execution::context::{SessionConfig, SessionContext};
use datafusion::execution::runtime_env::RuntimeEnvBuilder;
use datafusion::logical_expr::Operator;
use datafusion::physical_expr::expressions::{BinaryExpr, Column};
use datafusion::physical_plan::joins::{utils::JoinFilter, NestedLoopJoinExec};
use datafusion::physical_plan::ExecutionPlan;
use futures::StreamExt;

use crate::memory_pool::{tests_support::TestBroker, FlinkMemoryPool};

fn schema() -> SchemaRef {
    Arc::new(Schema::new(vec![
        Field::new("value", DataType::Int64, true),
        Field::new("ordinal", DataType::UInt64, false),
    ]))
}

fn batch(values: &[Option<i64>], first: usize) -> RecordBatch {
    RecordBatch::try_new(
        schema(),
        vec![
            Arc::new(Int64Array::from(values.to_vec())),
            Arc::new(UInt64Array::from_iter_values(
                first as u64..(first + values.len()) as u64,
            )),
        ],
    )
    .unwrap()
}

fn context(batch_size: usize) -> (SessionContext, Arc<TestBroker>) {
    let broker = Arc::new(TestBroker::new(16 << 20));
    let pool = Arc::new(FlinkMemoryPool::new(broker.clone(), 16 << 20));
    let runtime = RuntimeEnvBuilder::new()
        .with_memory_pool(pool)
        .build_arc()
        .unwrap();
    (
        SessionContext::new_with_config_rt(
            SessionConfig::new().with_batch_size(batch_size),
            runtime,
        ),
        broker,
    )
}

fn join(
    left: Vec<RecordBatch>,
    right: Vec<RecordBatch>,
    preserve_right_batch: bool,
) -> Arc<dyn ExecutionPlan> {
    let right: Arc<dyn ExecutionPlan> = if preserve_right_batch {
        assert_eq!(right.len(), 1);
        let input =
            Arc::new(crate::planner::operators::reusable_input::ReusableInputExec::new(schema()));
        input
            .replace_batch(right.into_iter().next().unwrap())
            .unwrap();
        input
    } else {
        MemorySourceConfig::try_new_exec(&[right], schema(), None).unwrap()
    };
    let filter_schema = Arc::new(Schema::new(vec![
        Field::new("left", DataType::Int64, true),
        Field::new("right", DataType::Int64, true),
    ]));
    let filter = JoinFilter::new(
        Arc::new(BinaryExpr::new(
            Arc::new(Column::new("left", 0)),
            Operator::GtEq,
            Arc::new(Column::new("right", 1)),
        )),
        JoinFilter::build_column_indices(vec![0], vec![0]),
        filter_schema,
    );
    Arc::new(
        NestedLoopJoinExec::try_new(
            MemorySourceConfig::try_new_exec(&[left], schema(), None).unwrap(),
            right,
            Some(filter),
            &JoinType::Inner,
            None,
        )
        .unwrap(),
    )
}

fn identities(batch: &RecordBatch) -> Vec<(u64, u64)> {
    let left = batch
        .column(1)
        .as_any()
        .downcast_ref::<UInt64Array>()
        .unwrap();
    let right = batch
        .column(3)
        .as_any()
        .downcast_ref::<UInt64Array>()
        .unwrap();
    assert_eq!(left.null_count(), 0);
    assert_eq!(right.null_count(), 0);
    (0..batch.num_rows())
        .map(|row| (left.value(row), right.value(row)))
        .collect()
}

fn expected(left: &[Option<i64>], right: &[Option<i64>]) -> Vec<(u64, u64)> {
    // Flink WindowJoinHelper.InnerWindowJoinProcessor traverses left rows, then right rows.
    left.iter()
        .enumerate()
        .flat_map(|(l, a)| {
            right.iter().enumerate().filter_map(move |(r, b)| {
                a.zip(*b)
                    .filter(|(a, b)| a >= b)
                    .map(|_| (l as u64, r as u64))
            })
        })
        .collect()
}

#[tokio::test]
async fn complete_right_window_preserves_left_major_filtered_duplicate_order() {
    for seed in 0..3 {
        let left = (0..301)
            .map(|i| (i % 13 != 0).then_some((i * 7 + seed) % 19 - 9))
            .collect::<Vec<_>>();
        let right = (0..257)
            .map(|i| (i % 11 != 0).then_some((i * 3 + seed) % 17 - 8))
            .collect::<Vec<_>>();
        for batch_size in [64, 4096] {
            let (context, broker) = context(batch_size);
            let plan = join(
                vec![batch(&left[..101], 0), batch(&left[101..], 101)],
                vec![batch(&right, 0)],
                true,
            );
            let mut stream = plan.execute(0, context.task_ctx()).unwrap();
            let mut actual = Vec::new();
            let mut batches = 0;
            while let Some(batch) = stream.next().await {
                let batch = batch.unwrap();
                assert!(batch.num_rows() <= batch_size);
                actual.extend(identities(&batch));
                batches += 1;
            }
            assert!(batches > 1);
            let expected = expected(&left, &right);
            assert_eq!(actual.len(), expected.len());
            for (position, (actual, expected)) in actual.iter().zip(&expected).enumerate() {
                assert_eq!(
                    actual, expected,
                    "seed={seed} batch_size={batch_size} position={position}"
                );
            }
            drop(stream);
            drop(plan);
            assert_eq!(broker.reserved(), 0);
        }
    }
}

#[tokio::test]
async fn splitting_the_right_window_changes_flink_emission_order() {
    let left = vec![Some(7), Some(7)];
    let right = vec![Some(3), Some(3), Some(3), Some(3)];
    // Even a single MemorySource batch is split by the configured source batch size.
    for right_batches in [
        vec![batch(&right, 0)],
        vec![batch(&right[..2], 0), batch(&right[2..], 2)],
    ] {
        let (context, broker) = context(2);
        let plan = join(vec![batch(&left, 0)], right_batches, false);
        let mut stream = plan.execute(0, context.task_ctx()).unwrap();
        let mut actual = Vec::new();
        while let Some(batch) = stream.next().await {
            actual.extend(identities(&batch.unwrap()));
        }
        assert_ne!(actual, expected(&left, &right));
        actual.sort_unstable();
        assert_eq!(actual, expected(&left, &right));
        drop(stream);
        drop(plan);
        assert_eq!(broker.reserved(), 0);
    }
}

#[tokio::test]
async fn empty_and_null_only_closed_windows_emit_nothing() {
    for (left, right) in [
        (vec![], vec![Some(3)]),
        (vec![Some(3)], vec![]),
        (vec![None], vec![Some(3)]),
        (vec![Some(3)], vec![None]),
    ] {
        let (context, broker) = context(64);
        let plan = join(vec![batch(&left, 0)], vec![batch(&right, 0)], true);
        let mut stream = plan.execute(0, context.task_ctx()).unwrap();
        while let Some(batch) = stream.next().await {
            assert_eq!(batch.unwrap().num_rows(), 0);
        }
        drop(stream);
        drop(plan);
        assert_eq!(broker.reserved(), 0);
    }
}

#[tokio::test]
async fn cancelling_a_fanout_releases_datafusion_build_credit() {
    let (context, broker) = context(64);
    let plan = join(
        vec![batch(&vec![Some(7); 301], 0)],
        vec![batch(&vec![Some(3); 257], 0)],
        true,
    );
    let mut stream = plan.execute(0, context.task_ctx()).unwrap();
    let first = stream.next().await.unwrap().unwrap();
    assert_eq!(first.num_rows(), 64);
    assert!(broker.reserved() > 0);
    drop(stream);
    drop(plan);
    assert_eq!(broker.reserved(), 0);
    // Output arrays remain usable after cancelling the producer. This alone does not verify
    // the output's Flink reservation lifetime; the production adapter still needs that lease.
    assert_eq!(identities(&first).len(), 64);
}
