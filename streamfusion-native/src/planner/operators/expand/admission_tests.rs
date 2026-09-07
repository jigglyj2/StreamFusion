// Copyright 2026 StreamFusion Authors
// Licensed under the Apache License, Version 2.0.

use super::*;
use crate::allocation_test_support::measure;
use crate::execution_context::NativeExecutionContext;
use crate::memory_pool::{tests_support::TestBroker, FlinkMemoryPool};
use arrow::array::{Int32Array, RecordBatch, StringArray};
use futures::StreamExt;
use prost::Message;

fn context(expression: proto::Expression, broker: &Arc<TestBroker>) -> Arc<NativeExecutionContext> {
    let plan = proto::NativePlan {
        protocol_version: crate::PLAN_PROTOCOL_VERSION,
        root: Some(proto::Operator {
            plan_node_id: 2,
            operator: Some(proto::operator::Operator::Expand(Box::new(proto::Expand {
                input: Some(Box::new(proto::Operator {
                    plan_node_id: 1,
                    operator: Some(proto::operator::Operator::Input(proto::Input::default())),
                    ..Default::default()
                })),
                projections: vec![proto::ExpandProjection {
                    expressions: vec![expression],
                }],
            }))),
            ..Default::default()
        }),
    };
    Arc::new(
        NativeExecutionContext::new(
            &plan.encode_to_vec(),
            Arc::new(FlinkMemoryPool::new(broker.clone(), 2 << 20)),
        )
        .unwrap(),
    )
}

fn input(rows: usize) -> RecordBatch {
    RecordBatch::try_from_iter([
        (
            "text",
            Arc::new(StringArray::from(vec!["x".repeat(1024); rows])) as arrow::array::ArrayRef,
        ),
        (
            "__streamfusion_input_row",
            Arc::new(Int32Array::from_iter_values(0..rows as i32)) as arrow::array::ArrayRef,
        ),
    ])
    .unwrap()
}

#[test]
fn shared_tree_expand_installs_the_same_repeat_kernel_admission_as_calc() {
    let expression = proto::Expression {
        expression: Some(proto::expression::Expression::StringRepeat(Box::new(
            proto::StringRepeat {
                value: Some(Box::new(proto::Expression {
                    expression: Some(proto::expression::Expression::InputReference(
                        proto::InputReference {
                            index: 0,
                            r#type: None,
                        },
                    )),
                })),
                count: Some(Box::new(proto::Expression {
                    expression: Some(proto::expression::Expression::LongLiteral(
                        proto::LongLiteral { value: 64 },
                    )),
                })),
            },
        ))),
    };
    denied_before_payload(expression, "REPEAT workspace");
}

#[test]
fn null_fixed_binary_broadcast_is_admitted_by_declared_width_before_allocation() {
    let expression = proto::Expression {
        expression: Some(proto::expression::Expression::NullLiteral(
            proto::NullLiteral {
                r#type: Some(proto::LogicalType {
                    nullable: true,
                    r#type: Some(proto::logical_type::Type::FixedBinary(proto::LengthType {
                        length: 1 << 20,
                    })),
                }),
            },
        )),
    };
    denied_before_payload(expression, "projection scalar broadcast");
}

fn denied_before_payload(expression: proto::Expression, consumer: &str) {
    let broker = Arc::new(TestBroker::new(2 << 20));
    let context = context(expression, &broker);
    let mut stream = context.start(vec![input(64)]).unwrap();
    let (result, observed) = measure(|| context.runtime().block_on(stream.next()).unwrap());
    let error = result.unwrap_err();
    assert!(error.to_string().contains(consumer), "{error}");
    assert!(
        observed.peak < 128 << 10,
        "large output must not be allocated on denial: {observed:?}"
    );
    drop(stream);
    drop(context);
    assert_eq!(broker.reserved(), 0);
}
