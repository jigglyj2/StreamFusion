// Copyright 2026 StreamFusion Authors
// Licensed under the Apache License, Version 2.0

use super::*;
use crate::memory_pool::{tests_support::TestBroker, FlinkMemoryPool};
use crate::proto;
use arrow::array::{Decimal128Array, Int32Array, Int8Array, ListArray, StringArray};
use arrow::datatypes::{Field, Int32Type, Schema};
use datafusion::datasource::memory::MemorySourceConfig;
use datafusion::execution::memory_pool::MemoryPool;
use datafusion::execution::runtime_env::RuntimeEnvBuilder;
use datafusion::prelude::{SessionConfig, SessionContext};
use futures::StreamExt;

fn reference(index: u32) -> proto::Expression {
    proto::Expression {
        expression: Some(proto::expression::Expression::InputReference(
            proto::InputReference {
                index,
                r#type: None,
            },
        )),
    }
}

fn fixture(rows: usize) -> RecordBatch {
    let strings: ArrayRef = Arc::new(StringArray::from_iter((0..rows + 2).map(|row| {
        (row % 5 != 0).then(|| {
            if row % 17 == 0 {
                "wide é".repeat(1024)
            } else {
                format!("尾-{row}")
            }
        })
    })));
    let lists: ArrayRef = Arc::new(ListArray::from_iter_primitive::<Int32Type, _, _>(
        (0..rows + 2).map(|row| (row % 3 != 0).then(|| vec![Some(row as i32), None])),
    ));
    let decimals: ArrayRef = Arc::new(
        Decimal128Array::from_iter_values((0..rows + 2).map(|row| row as i128 * 100 - 999))
            .with_precision_and_scale(20, 3)
            .unwrap(),
    );
    let columns = vec![
        strings,
        lists,
        decimals,
        Arc::new(Int8Array::from_iter_values(
            (0..rows + 2).map(|row| (row % 4) as i8),
        )),
        Arc::new(Int32Array::from_iter_values(
            (0..rows + 2).map(|row| 500 + row as i32 * 3),
        )),
    ];
    let fields = columns
        .iter()
        .enumerate()
        .map(|(index, array)| {
            Field::new(
                match index {
                    3 => "__streamfusion_row_kind".to_string(),
                    4 => "__streamfusion_input_row".to_string(),
                    _ => format!("f{index}"),
                },
                array.data_type().clone(),
                index < 3,
            )
        })
        .collect::<Vec<_>>();
    RecordBatch::try_new(Arc::new(Schema::new(fields)), columns)
        .unwrap()
        .slice(1, rows)
}

fn expand(projections: usize) -> proto::Expand {
    proto::Expand {
        input: None,
        projections: (0..projections)
            .map(|id| proto::ExpandProjection {
                expressions: vec![
                    reference(0),
                    reference(1),
                    reference(2),
                    proto::Expression {
                        expression: Some(proto::expression::Expression::IntegerLiteral(
                            proto::IntegerLiteral { value: id as i32 },
                        )),
                    },
                ],
            })
            .collect(),
    }
}

fn session(broker: Arc<TestBroker>) -> SessionContext {
    let pool: Arc<dyn MemoryPool> = Arc::new(FlinkMemoryPool::new(broker, 512 << 20));
    SessionContext::new_with_config_rt(
        SessionConfig::new(),
        Arc::new(
            RuntimeEnvBuilder::new()
                .with_memory_pool(pool)
                .build()
                .unwrap(),
        ),
    )
}

#[tokio::test]
async fn bounded_pulls_preserve_sliced_nested_values_row_kinds_and_row_major_order() {
    for (rows, projection_count) in [(3001, 3), (2, MAX_OUTPUT_ROWS + 7)] {
        let batch = fixture(rows);
        let child =
            MemorySourceConfig::try_new_exec(&[vec![batch.clone()]], batch.schema(), None).unwrap();
        let plan =
            crate::planner::operators::expand::create(&expand(projection_count), child).unwrap();
        let broker = Arc::new(TestBroker::new(512 << 20));
        let context = session(broker.clone());
        let mut stream = plan.execute(0, context.task_ctx()).unwrap();
        let mut emitted = 0;
        let mut pulls = 0;
        let mut retained = None;
        while let Some(output) = stream.next().await {
            let output = output.unwrap();
            assert!(output.num_rows() <= MAX_OUTPUT_ROWS);
            for index in 0..output.num_rows() {
                let input_row = emitted / projection_count;
                let grouping = emitted % projection_count;
                for column in 0..3 {
                    assert_eq!(
                        output.column(column).slice(index, 1).to_data(),
                        batch.column(column).slice(input_row, 1).to_data()
                    );
                }
                assert_eq!(
                    output
                        .column(3)
                        .as_any()
                        .downcast_ref::<Int32Array>()
                        .unwrap()
                        .value(index),
                    grouping as i32
                );
                for (source, target) in [(3, 4), (4, 5)] {
                    assert_eq!(
                        output.column(target).slice(index, 1).to_data(),
                        batch.column(source).slice(input_row, 1).to_data()
                    );
                }
                emitted += 1;
            }
            retained = Some(output.column(0).slice(0, 1));
            pulls += 1;
        }
        assert_eq!(emitted, rows * projection_count);
        assert!(pulls > 1);
        drop(stream);
        drop(plan);
        drop(context);
        assert!(broker.reserved() > 0);
        drop(retained);
        assert_eq!(broker.reserved(), 0);
    }
}

#[tokio::test]
async fn denied_workspace_and_cancelled_fanout_release_all_reservations() {
    for limit in [1, 512 << 20] {
        let batch = fixture(3001);
        let child =
            MemorySourceConfig::try_new_exec(&[vec![batch.clone()]], batch.schema(), None).unwrap();
        let plan = crate::planner::operators::expand::create(&expand(3), child).unwrap();
        let broker = Arc::new(TestBroker::new(limit));
        let context = session(broker.clone());
        let mut stream = plan.execute(0, context.task_ctx()).unwrap();
        let first = stream.next().await.unwrap();
        if limit == 1 {
            assert!(matches!(first, Err(DataFusionError::ResourcesExhausted(_))));
        } else {
            assert!(first.as_ref().unwrap().num_rows() < 3001 * 3);
        }
        drop(first);
        drop(stream);
        drop(plan);
        drop(context);
        assert_eq!(broker.reserved(), 0);
    }
}

#[tokio::test]
async fn wide_literal_expansion_is_denied_by_managed_memory() {
    let batch = fixture(3001);
    let child =
        MemorySourceConfig::try_new_exec(&[vec![batch.clone()]], batch.schema(), None).unwrap();
    let mut definition = expand(1);
    definition.projections[0].expressions[0] = proto::Expression {
        expression: Some(proto::expression::Expression::StringLiteral(
            proto::StringLiteral {
                value: "x".repeat(64 * 1024),
            },
        )),
    };
    let plan = crate::planner::operators::expand::create(&definition, child).unwrap();
    let broker = Arc::new(TestBroker::new(128 * 1024));
    let context = session(broker.clone());
    let mut stream = plan.execute(0, context.task_ctx()).unwrap();
    assert!(matches!(
        stream.next().await.unwrap(),
        Err(DataFusionError::ResourcesExhausted(_))
    ));
    drop(stream);
    drop(plan);
    drop(context);
    assert_eq!(broker.reserved(), 0);
}

#[tokio::test]
async fn single_projection_reuses_payload_buffers_instead_of_gathering_them() {
    let batch = fixture(7);
    let child =
        MemorySourceConfig::try_new_exec(&[vec![batch.clone()]], batch.schema(), None).unwrap();
    let plan = crate::planner::operators::expand::create(&expand(1), child).unwrap();
    let context = session(Arc::new(TestBroker::new(512 << 20)));
    let mut stream = plan.execute(0, context.task_ctx()).unwrap();
    let output = stream.next().await.unwrap().unwrap();
    for index in 0..3 {
        let expected = batch.column(index).to_data();
        let actual = output.column(index).to_data();
        assert_eq!(expected, actual);
        for (expected, actual) in expected.buffers().iter().zip(actual.buffers()) {
            assert_eq!(expected.as_ptr(), actual.as_ptr());
        }
    }
    assert!(stream.next().await.is_none());
}

#[tokio::test]
async fn empty_input_finishes_without_materializing_projections() {
    let batch = fixture(0);
    let child =
        MemorySourceConfig::try_new_exec(&[vec![batch.clone()]], batch.schema(), None).unwrap();
    let plan = crate::planner::operators::expand::create(&expand(3), child).unwrap();
    let broker = Arc::new(TestBroker::new(1));
    let context = session(broker.clone());
    let mut stream = plan.execute(0, context.task_ctx()).unwrap();
    assert!(stream.next().await.is_none());
    assert_eq!(broker.reserved(), 0);
}

#[test]
fn wide_bufferless_input_slices_do_not_require_payload_sized_descriptor_credit() {
    let mut columns = (0..512)
        .map(|index| {
            (
                format!("null_{index}"),
                Arc::new(arrow::array::NullArray::new(7)) as ArrayRef,
            )
        })
        .collect::<Vec<_>>();
    columns.push((
        "__streamfusion_input_row".into(),
        Arc::new(Int32Array::from_iter_values(0..7)) as ArrayRef,
    ));
    let batch = RecordBatch::try_from_iter(columns).unwrap();
    let child =
        MemorySourceConfig::try_new_exec(&[vec![batch.clone()]], batch.schema(), None).unwrap();
    let definition = proto::Expand {
        input: None,
        projections: vec![proto::ExpandProjection {
            expressions: vec![reference(0)],
        }],
    };
    let plan = crate::planner::operators::expand::create(&definition, child).unwrap();
    let broker = Arc::new(TestBroker::new(16 << 10));
    let context = session(broker.clone());
    let mut stream = plan.execute(0, context.task_ctx()).unwrap();
    let result = futures::executor::block_on(stream.next()).unwrap().unwrap();
    assert_eq!(result.num_rows(), 7);
    assert_eq!(result.column(0).logical_null_count(), 7);
    drop(result);
    drop(stream);
    drop(plan);
    drop(context);
    assert_eq!(broker.reserved(), 0);
}
