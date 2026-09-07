// Copyright 2026 StreamFusion Authors
// Licensed under the Apache License, Version 2.0.

use super::*;
use crate::allocation_test_support::measure;
use crate::memory_pool::tests_support::TestBroker;
use arrow::array::{RecordBatch, StringArray};
use futures::StreamExt;

#[test]
fn shared_projection_denies_repeat_before_allocating_its_output() {
    let (bytes, template) = allocation_tests::wide_plan(1, false, 1);
    let mut plan = proto::NativePlan::decode(bytes.as_slice()).unwrap();
    let Some(proto::operator::Operator::Calc(calc)) = plan.root.as_mut().unwrap().operator.as_mut()
    else {
        unreachable!()
    };
    calc.projections[0] = proto::Expression {
        expression: Some(proto::expression::Expression::StringRepeat(Box::new(
            proto::StringRepeat {
                value: Some(Box::new(calc.projections[0].clone())),
                count: Some(Box::new(proto::Expression {
                    expression: Some(proto::expression::Expression::LongLiteral(
                        proto::LongLiteral { value: 64 },
                    )),
                })),
            },
        ))),
    };
    let broker = Arc::new(TestBroker::new(2 << 20));
    let context = Arc::new(
        NativeExecutionContext::new(
            &plan.encode_to_vec(),
            Arc::new(FlinkMemoryPool::new(broker.clone(), 2 << 20)),
        )
        .unwrap(),
    );
    let text = "x".repeat(1024);
    let input = RecordBatch::try_new(
        template.schema(),
        vec![Arc::new(StringArray::from(vec![text.as_str(); 64]))],
    )
    .unwrap();
    let mut stream = context.start(vec![input]).unwrap();
    let (result, observed) = measure(|| context.runtime().block_on(stream.next()).unwrap());
    let error = result.unwrap_err();
    assert!(error.to_string().contains("REPEAT workspace"), "{error}");
    assert!(
        observed.peak < 128 << 10,
        "4 MiB output must not be allocated before denial: {observed:?}"
    );
    drop(stream);
    drop(context);
    assert_eq!(broker.reserved(), 0);
}

#[test]
fn shared_projection_installs_math_admission_before_scalar_broadcast() {
    let (bytes, template) = allocation_tests::wide_plan(1, false, 1);
    let mut plan = proto::NativePlan::decode(bytes.as_slice()).unwrap();
    let Some(proto::operator::Operator::Calc(calc)) = plan.root.as_mut().unwrap().operator.as_mut()
    else {
        unreachable!()
    };
    calc.projections[0] = proto::Expression {
        expression: Some(proto::expression::Expression::Sine(Box::new(proto::Sine {
            operand: Some(Box::new(proto::Expression {
                expression: Some(proto::expression::Expression::DoubleLiteral(
                    proto::DoubleLiteral { value: 0.5 },
                )),
            })),
        }))),
    };
    let broker = Arc::new(TestBroker::new(2 << 20));
    let context = Arc::new(
        NativeExecutionContext::new(
            &plan.encode_to_vec(),
            Arc::new(FlinkMemoryPool::new(broker.clone(), 2 << 20)),
        )
        .unwrap(),
    );
    let input = RecordBatch::try_new(
        template.schema(),
        vec![Arc::new(StringArray::from(vec![""; 65536]))],
    )
    .unwrap();
    let mut stream = context.start(vec![input]).unwrap();
    let (result, observed) = measure(|| context.runtime().block_on(stream.next()).unwrap());
    let error = result.unwrap_err();
    assert!(
        error.to_string().contains("fixed math workspace"),
        "{error}"
    );
    assert!(observed.peak < 128 << 10, "{observed:?}");
    drop(stream);
    drop(context);
    assert_eq!(broker.reserved(), 0);
}

#[test]
fn shared_calc_rejects_wide_input_without_a_gather_reservation() {
    let (bytes, template) = allocation_tests::wide_plan(1, false, 1);
    let mut plan = proto::NativePlan::decode(bytes.as_slice()).unwrap();
    let Some(proto::operator::Operator::Calc(calc)) = plan.root.as_mut().unwrap().operator.as_mut()
    else {
        unreachable!()
    };
    calc.condition = Some(proto::Expression {
        expression: Some(proto::expression::Expression::BooleanLiteral(
            proto::BooleanLiteral { value: false },
        )),
    });
    let broker = Arc::new(TestBroker::new(2 << 20));
    let context = Arc::new(
        NativeExecutionContext::new(
            &plan.encode_to_vec(),
            Arc::new(FlinkMemoryPool::new(broker.clone(), 2 << 20)),
        )
        .unwrap(),
    );
    let input = RecordBatch::try_new(
        template.schema(),
        vec![Arc::new(StringArray::from(vec!["x".repeat(1024); 4096]))],
    )
    .unwrap();
    let mut stream = context.start(vec![input]).unwrap();
    assert!(context.runtime().block_on(stream.next()).is_none());
    drop(stream);
    drop(context);
    assert_eq!(broker.reserved(), 0);
}
