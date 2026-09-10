// Copyright 2026 StreamFusion Authors
// Licensed under the Apache License, Version 2.0.

use super::super::indexed_tests::{backends, processor};
use super::super::tests::batch;
use super::*;
use crate::memory_pool::tests_support::TestBroker;
use datafusion::execution::context::{SessionConfig, SessionContext};
use datafusion::execution::memory_pool::{MemoryConsumer, MemoryPool};
use futures::StreamExt;

pub(super) fn context(owner: &HostMemoryReservation, rows: usize) -> Arc<TaskContext> {
    let runtime = RuntimeEnvBuilder::new()
        .with_memory_pool(owner.datafusion_pool(512 << 20))
        .build_arc()
        .unwrap();
    SessionContext::new_with_config_rt(SessionConfig::new().with_batch_size(rows), runtime)
        .task_ctx()
}

fn filter(p: &WindowJoinProcessor) -> Option<JoinFilter> {
    let mut plan = p.plan.clone();
    plan.join_type = proto::RegularJoinType::Inner as i32;
    plan.filter_nulls = vec![true];
    let reference = |index| {
        Box::new(proto::Expression {
            expression: Some(proto::expression::Expression::InputReference(
                proto::InputReference {
                    index,
                    r#type: None,
                },
            )),
        })
    };
    plan.residual_condition = Some(proto::Expression {
        expression: Some(proto::expression::Expression::Comparison(Box::new(
            proto::Comparison {
                left: Some(reference(2)),
                right: Some(reference(5)),
                operator: proto::ComparisonOperator::GreaterThanOrEqual as i32,
            },
        ))),
    });
    super::super::planning::filter(&plan).unwrap()
}

pub(super) fn pairs(batch: &RecordBatch) -> Vec<(Vec<u8>, Vec<u8>)> {
    let a = batch
        .column(2)
        .as_any()
        .downcast_ref::<BinaryArray>()
        .unwrap();
    let b = batch
        .column(5)
        .as_any()
        .downcast_ref::<BinaryArray>()
        .unwrap();
    (0..batch.num_rows())
        .map(|r| (a.value(r).to_vec(), b.value(r).to_vec()))
        .collect()
}

#[tokio::test]
async fn generated_closed_state_runs_datafusion_in_flink_order_with_retained_output_on_both_backends(
) {
    for rocks in backends() {
        for seed in 0..3 {
            for batch_size in [7, 64] {
                let (mut p, broker, _dir) = processor(rocks, 0, 127);
                let left = (0..101)
                    .map(|i| format!("{:02}-{}", (i * 7 + seed) % 19, "x".repeat(128)).into_bytes())
                    .collect::<Vec<_>>();
                let right = (0..79)
                    .map(|i| format!("{:02}-{}", (i * 3 + seed) % 17, "x".repeat(128)).into_bytes())
                    .collect::<Vec<_>>();
                for (side, rows) in [&left, &right].iter().enumerate() {
                    for chunk in rows.chunks(13) {
                        p.ingest_arrow(
                            side,
                            batch(
                                &vec![9; chunk.len()],
                                &vec![100; chunk.len()],
                                &chunk.iter().map(|r| r.as_slice()).collect::<Vec<_>>(),
                                &vec![INSERT; chunk.len()],
                            ),
                        )
                        .unwrap();
                    }
                }
                let predicate = filter(&p);
                p.begin_watermark(99).unwrap();
                let owner = p.state_memory();
                let closed = p.next_closed_window().unwrap().unwrap();
                let mut stream = ClosedWindowStream::try_new(
                    closed,
                    predicate,
                    context(&owner, batch_size),
                    &owner,
                )
                .unwrap();
                let mut outputs = Vec::new();
                let mut actual = Vec::new();
                while let Some(batch) = stream.next().await {
                    let batch = batch.unwrap();
                    assert!(batch.num_rows() <= batch_size);
                    actual.extend(pairs(&batch));
                    outputs.push(batch);
                }
                assert!(stream.completed());
                assert!(outputs.len() > 1);
                p.finish_closed_window().unwrap();
                assert!(p.next_closed_window().unwrap().is_none());
                drop(stream);
                drop(p);
                let expected = left
                    .iter()
                    .flat_map(|a| {
                        right
                            .iter()
                            .filter(move |b| a >= *b)
                            .map(move |b| (a.clone(), b.clone()))
                    })
                    .collect::<Vec<_>>();
                assert_eq!(actual.len(), expected.len());
                assert!(actual.iter().zip(&expected).all(|(a, b)| a == b));
                assert!(broker.reserved() > 0);
                assert_eq!(
                    outputs.iter().map(|b| pairs(b).len()).sum::<usize>(),
                    expected.len()
                );
                drop(outputs);
                assert_eq!(broker.reserved(), 0);
            }
        }
    }
}

#[tokio::test]
async fn retained_output_causes_recoverable_budget_denial_before_the_next_kernel_poll() {
    for rocks in backends() {
        let (mut p, broker, _dir) = processor(rocks, 0, 127);
        let payload = vec![42; 4096];
        for side in 0..2 {
            p.ingest_arrow(
                side,
                batch(
                    &[9; 32],
                    &[100; 32],
                    &[payload.as_slice(); 32],
                    &[INSERT; 32],
                ),
            )
            .unwrap();
        }
        p.begin_watermark(99).unwrap();
        let owner = p.state_memory();
        let closed = p.next_closed_window().unwrap().unwrap();
        let mut stream =
            ClosedWindowStream::try_new(closed, None, context(&owner, 16), &owner).unwrap();
        let output = stream.next().await.unwrap().unwrap();
        assert!(!stream.completed());
        let mut pressure = p.state_memory();
        pressure
            .resize(pressure.available_capacity().unwrap().unwrap())
            .unwrap();
        assert!(stream
            .next()
            .await
            .unwrap()
            .unwrap_err()
            .to_string()
            .contains("Flink denied"));
        assert!(!stream.completed());
        assert!(stream.next().await.is_none());
        assert_eq!(p.timers.timer_count(TimerDomain::EventTime), 1);
        assert!(p
            .snapshot_key_group(*p.dirty_timer_groups.first().unwrap())
            .is_err());
        drop(pressure);
        drop(stream);
        drop(p);
        assert_eq!(pairs(&output), vec![(payload.clone(), payload); 16]);
        assert!(broker.reserved() > 0);
        drop(output);
        assert_eq!(broker.reserved(), 0);
    }
}

#[tokio::test]
async fn cancelling_after_a_batch_releases_compute_but_keeps_output_and_pending_timer() {
    for rocks in backends() {
        let (mut p, broker, _dir) = processor(rocks, 0, 127);
        for side in 0..2 {
            p.ingest_arrow(
                side,
                batch(
                    &[9; 64],
                    &[100; 64],
                    &[b"same".as_slice(); 64],
                    &[INSERT; 64],
                ),
            )
            .unwrap();
        }
        p.begin_watermark(99).unwrap();
        let owner = p.state_memory();
        let closed = p.next_closed_window().unwrap().unwrap();
        let mut stream =
            ClosedWindowStream::try_new(closed, None, context(&owner, 7), &owner).unwrap();
        let output = stream.next().await.unwrap().unwrap();
        let active = broker.reserved();
        drop(stream);
        assert!(broker.reserved() < active);
        assert_eq!(p.timers.timer_count(TimerDomain::EventTime), 1);
        assert!(p
            .snapshot_key_group(*p.dirty_timer_groups.first().unwrap())
            .is_err());
        drop(p);
        assert_eq!(
            pairs(&output),
            vec![(b"same".to_vec(), b"same".to_vec()); 7]
        );
        drop(output);
        assert_eq!(broker.reserved(), 0);
    }
}

#[test]
fn datafusion_build_reuses_input_credit_and_rejects_unrelated_or_excess_allocations() {
    let broker = Arc::new(TestBroker::new(1 << 20));
    let source = batch(
        &[9; 64],
        &[100; 64],
        &[b"same".as_slice(); 64],
        &[INSERT; 64],
    );
    let mut credit = HostMemoryReservation::new(broker.clone(), "input");
    credit.resize(batch_bytes(&source).unwrap()).unwrap();
    let source = arrow_lease::host_batch(source, credit).unwrap();
    let logical_size = source.get_array_memory_size();
    let before = broker.reserved();
    let pool: Arc<dyn MemoryPool> = Arc::new(WindowBuildPool::new(source));
    let build = MemoryConsumer::new("NestedLoopJoinLoad[0]").register(&pool);
    build.try_grow(logical_size).unwrap();
    assert_eq!(broker.reserved(), before);
    assert_eq!(pool.reserved(), logical_size);
    assert!(build.try_grow(1).is_err());
    assert!(MemoryConsumer::new("unexpected")
        .register(&pool)
        .try_grow(1)
        .is_err());
    drop(pool);
    assert_eq!(broker.reserved(), before);
    drop(build);
    assert_eq!(broker.reserved(), 0);
}

#[tokio::test]
async fn empty_windows_complete_and_predicate_errors_release_compute_without_acknowledging_state() {
    use datafusion::common::ScalarValue;
    use datafusion::logical_expr::Operator;
    use datafusion::physical_expr::expressions::{BinaryExpr, Column, Literal};
    for rocks in backends() {
        for error_case in [false, true] {
            let (mut p, broker, _dir) = processor(rocks, 0, 127);
            p.ingest_arrow(0, batch(&[9], &[100], &[b"left"], &[INSERT]))
                .unwrap();
            if error_case {
                p.ingest_arrow(1, batch(&[9], &[100], &[b"right"], &[INSERT]))
                    .unwrap();
            }
            p.begin_watermark(99).unwrap();
            let owner = p.state_memory();
            let closed = p.next_closed_window().unwrap().unwrap();
            let predicate = error_case.then(|| {
                let divide = Arc::new(BinaryExpr::new(
                    Arc::new(Column::new("key", 0)),
                    Operator::Divide,
                    Arc::new(Literal::new(ScalarValue::Int64(Some(0)))),
                ));
                JoinFilter::new(
                    Arc::new(BinaryExpr::new(
                        divide,
                        Operator::Eq,
                        Arc::new(Literal::new(ScalarValue::Int64(Some(0)))),
                    )),
                    JoinFilter::build_column_indices(vec![0], vec![]),
                    Arc::new(Schema::new(vec![Field::new("key", DataType::Int64, false)])),
                )
            });
            let mut stream =
                ClosedWindowStream::try_new(closed, predicate, context(&owner, 8), &owner).unwrap();
            if error_case {
                assert!(stream.next().await.unwrap().is_err());
                assert!(!stream.completed());
                assert!(stream.next().await.is_none());
                assert_eq!(p.timers.timer_count(TimerDomain::EventTime), 1);
            } else {
                while let Some(batch) = stream.next().await {
                    assert_eq!(batch.unwrap().num_rows(), 0);
                }
                assert!(stream.completed());
                p.finish_closed_window().unwrap();
                assert!(p.next_closed_window().unwrap().is_none());
            }
            drop(stream);
            drop(p);
            assert_eq!(broker.reserved(), 0);
        }
    }
}

#[test]
fn expanding_predicates_are_rejected_before_allocating_candidate_batches() {
    use datafusion::common::ScalarValue;
    use datafusion::logical_expr::Operator;
    use datafusion::physical_expr::expressions::{BinaryExpr, Column, Literal};
    let concat = Arc::new(BinaryExpr::new(
        Arc::new(Column::new("text", 0)),
        Operator::StringConcat,
        Arc::new(Column::new("text", 0)),
    ));
    let predicate = JoinFilter::new(
        Arc::new(BinaryExpr::new(
            concat,
            Operator::Eq,
            Arc::new(Literal::new(ScalarValue::Utf8(Some("xx".into())))),
        )),
        JoinFilter::build_column_indices(vec![0], vec![]),
        Arc::new(Schema::new(vec![Field::new("text", DataType::Utf8, true)])),
    );
    assert!(admission::predicate_row_bytes(Some(&predicate), 1024)
        .unwrap_err()
        .to_string()
        .contains("expanding or unsupported"));
}

#[tokio::test]
async fn tiny_windows_bound_capacity_by_possible_pairs_not_default_batch_size() {
    for rocks in backends() {
        let (mut p, broker, _dir) = processor(rocks, 0, 127);
        for side in 0..2 {
            p.ingest_arrow(
                side,
                batch(&[9; 3], &[100; 3], &[b"x".as_slice(); 3], &[INSERT; 3]),
            )
            .unwrap();
        }
        p.begin_watermark(99).unwrap();
        let owner = p.state_memory();
        let closed = p.next_closed_window().unwrap().unwrap();
        let mut pressure = p.state_memory();
        pressure
            .resize(pressure.available_capacity().unwrap().unwrap() - (1 << 20))
            .unwrap();
        let mut stream =
            ClosedWindowStream::try_new(closed, None, context(&owner, 8192), &owner).unwrap();
        let batch = stream.next().await.unwrap().unwrap();
        assert_eq!(batch.num_rows(), 9);
        assert!(stream.next().await.is_none());
        assert!(stream.completed());
        p.finish_closed_window().unwrap();
        assert!(p.next_closed_window().unwrap().is_none());
        drop((batch, stream, pressure, p));
        assert_eq!(broker.reserved(), 0);
    }
}
