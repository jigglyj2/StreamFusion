// Copyright 2026 StreamFusion Authors
// Licensed under the Apache License, Version 2.0.

use super::coarse_memory::CountingBroker;
use super::*;
use arrow::array::TimestampMillisecondArray;
use std::sync::atomic::{AtomicUsize, Ordering};

fn timestamp_plan() -> Vec<u8> {
    let bound = |operator| proto::Expression {
        expression: Some(proto::expression::Expression::Arithmetic(Box::new(
            proto::Arithmetic {
                left: Some(Box::new(input_reference(1))),
                right: Some(Box::new(proto::Expression {
                    expression: Some(proto::expression::Expression::IntervalDayTimeLiteral(
                        proto::IntervalDayTimeLiteral { milliseconds: 10 },
                    )),
                })),
                operator: operator as i32,
                result_type: None,
            },
        ))),
    };
    let compare = |operator, arithmetic| proto::Expression {
        expression: Some(proto::expression::Expression::Comparison(Box::new(
            proto::Comparison {
                left: Some(Box::new(input_reference(3))),
                right: Some(Box::new(bound(arithmetic))),
                operator: operator as i32,
            },
        ))),
    };
    let condition = proto::Expression {
        expression: Some(proto::expression::Expression::BooleanBinary(Box::new(
            proto::BooleanBinary {
                left: Some(Box::new(compare(
                    proto::ComparisonOperator::GreaterThanOrEqual,
                    proto::ArithmeticOperator::Subtract,
                ))),
                right: Some(Box::new(compare(
                    proto::ComparisonOperator::LessThanOrEqual,
                    proto::ArithmeticOperator::Add,
                ))),
                operator: proto::BooleanOperator::And as i32,
            },
        ))),
    };
    let mut plan = proto::NativePlan::decode(
        plan_contract(proto::RegularJoinType::Inner, true, Some(condition)).as_slice(),
    )
    .unwrap();
    let Some(proto::operator::Operator::RegularJoin(join)) =
        plan.root.as_mut().unwrap().operator.as_mut()
    else {
        unreachable!()
    };
    let mut schema = schema();
    schema.fields[1] = field(
        "value",
        proto::logical_type::Type::Timestamp(proto::PrecisionType { precision: 3 }),
    );
    join.left_schema = Some(schema.clone());
    join.right_schema = Some(schema);
    plan.encode_to_vec()
}

fn timestamps(values: &[Option<i64>]) -> RecordBatch {
    RecordBatch::try_from_iter(vec![
        (
            "key",
            Arc::new(Int64Array::from(vec![1; values.len()])) as ArrayRef,
        ),
        (
            "value",
            Arc::new(TimestampMillisecondArray::from(values.to_vec())) as ArrayRef,
        ),
        (
            "__streamfusion_input_row_kind",
            Arc::new(Int8Array::from(vec![INSERT; values.len()])) as ArrayRef,
        ),
    ])
    .unwrap()
}

#[test]
fn timestamp_predicates_keep_bounded_arrow_workspace_for_batched_and_hot_key_candidates() {
    for side in 0..2 {
        let broker = Arc::new(CountingBroker {
            inner: TestBroker::new(8 << 20),
            calls: AtomicUsize::new(0),
            peak: AtomicUsize::new(0),
        });
        let join = RegularJoinProcessor::new(
            &timestamp_plan(),
            128,
            0,
            127,
            HostMemoryReservation::new(broker.clone(), "timestamp residual test"),
        )
        .unwrap();
        let input = timestamps(&[Some(-15)]);
        let encoded_input = join.row_converters[side]
            .convert_columns(&input.columns()[..2])
            .unwrap();
        let values = [
            Some(-26),
            Some(-25),
            Some(-15),
            Some(-5),
            Some(-4),
            None,
            Some(i64::MIN),
            Some(i64::MAX),
        ];
        let opposite = timestamps(&values);
        let encoded = join.row_converters[1 - side]
            .convert_columns(&opposite.columns()[..2])
            .unwrap();
        let payloads = (0..values.len())
            .map(|i| Arc::<[u8]>::from(encoded.row(i).data()))
            .collect::<Vec<_>>();
        for count in [1024, 50_003] {
            let candidates = (0..count)
                .map(|i| StoredRow {
                    id: i as u64,
                    row: payloads[i % payloads.len()].clone(),
                    associations: 0,
                })
                .collect::<Vec<_>>();
            let mut value = JoinState::default();
            if side == 0 {
                value.right = candidates;
            } else {
                value.left = candidates;
            }
            let state = StagedState {
                key: StateKey {
                    key_group: 0,
                    key: vec![],
                },
                original: JoinState::default(),
                value,
                touched: false,
            };
            let candidates = if side == 0 {
                &state.value.right
            } else {
                &state.value.left
            };
            let retained = broker.inner.reserved();
            broker.peak.store(retained, Ordering::Relaxed);
            broker.calls.store(0, Ordering::Relaxed);
            let (mask, observed) = crate::allocation_test_support::measure(|| {
                if count > 4096 {
                    join.condition_matches_row(
                        side,
                        &input,
                        0,
                        encoded_input.row(0).data(),
                        candidates,
                    )
                    .unwrap()
                } else {
                    join.condition_matches_batch(
                        side,
                        &input,
                        &encoded_input,
                        std::slice::from_ref(&state),
                        &[0],
                        0,
                    )
                    .unwrap()
                    .pop()
                    .unwrap()
                }
            });
            for (i, actual) in mask.iter().enumerate() {
                let expected = values[i % values.len()].is_some_and(|v| {
                    let (left, right) = if side == 0 {
                        (-15_i64, v)
                    } else {
                        (v, -15_i64)
                    };
                    right >= left.wrapping_sub(10) && right <= left.wrapping_add(10)
                });
                assert_eq!(actual, expected, "side={side} candidate={i}");
            }
            assert!(
                observed.peak <= broker.peak.load(Ordering::Relaxed) - retained,
                "{observed:?}"
            );
            assert!(broker.calls.load(Ordering::Relaxed) < 96);
            drop(mask);
            assert_eq!(broker.inner.reserved(), retained);
            let mut pressure = HostMemoryReservation::new(broker.clone(), "other operator");
            pressure.resize((8 << 20) - retained - 1024).unwrap();
            assert!(matches!(
                join.condition_matches_row(
                    side,
                    &input,
                    0,
                    encoded_input.row(0).data(),
                    candidates
                ),
                Err(DataFusionError::ResourcesExhausted(_))
            ));
            drop(pressure);
            assert_eq!(broker.inner.reserved(), retained);
        }
        drop(join);
        assert_eq!(broker.inner.reserved(), 0);
    }
}
