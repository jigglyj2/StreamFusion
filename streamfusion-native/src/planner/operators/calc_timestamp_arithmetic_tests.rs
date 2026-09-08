// Copyright 2026 StreamFusion Authors
// Licensed under the Apache License, Version 2.0.

use crate::proto;
use arrow::array::{Array, TimestampMillisecondArray};
use arrow::datatypes::{DataType, Field, Schema, TimeUnit};
use arrow::record_batch::RecordBatch;
use std::sync::Arc;

#[test]
fn timestamp_day_time_literals_preserve_flink_wrapping_milliseconds_and_nulls() {
    let values = [
        None,
        Some(i64::MIN),
        Some(i64::MAX),
        Some(-86_400_001),
        Some(-1),
        Some(0),
        Some(1),
        Some(1_600_000_000_000),
    ];
    let schema = Arc::new(Schema::new(vec![Field::new(
        "ts",
        DataType::Timestamp(TimeUnit::Millisecond, None),
        true,
    )]));
    let input = RecordBatch::try_new(
        schema.clone(),
        vec![Arc::new(TimestampMillisecondArray::from(values.to_vec()))],
    )
    .unwrap();
    for offset in [
        -86_400_001,
        -10_000,
        0,
        10_000,
        86_400_001,
        i64::MIN,
        i64::MAX,
    ] {
        for operator in [
            proto::ArithmeticOperator::Add,
            proto::ArithmeticOperator::Subtract,
        ] {
            let expression = proto::Expression {
                expression: Some(proto::expression::Expression::Arithmetic(Box::new(
                    proto::Arithmetic {
                        left: Some(Box::new(proto::Expression {
                            expression: Some(proto::expression::Expression::InputReference(
                                proto::InputReference {
                                    index: 0,
                                    r#type: None,
                                },
                            )),
                        })),
                        right: Some(Box::new(proto::Expression {
                            expression: Some(
                                proto::expression::Expression::IntervalDayTimeLiteral(
                                    proto::IntervalDayTimeLiteral {
                                        milliseconds: offset,
                                    },
                                ),
                            ),
                        })),
                        operator: operator as i32,
                        result_type: None,
                    },
                ))),
            };
            let expr = super::calc::create_expression(&expression, &schema).unwrap();
            for batch in [input.clone(), input.slice(1, 4)] {
                let actual = expr
                    .evaluate(&batch)
                    .unwrap()
                    .into_array(batch.num_rows())
                    .unwrap();
                let input = batch
                    .column(0)
                    .as_any()
                    .downcast_ref::<TimestampMillisecondArray>()
                    .unwrap();
                let expected = TimestampMillisecondArray::from_iter(input.iter().map(|value| {
                    value.map(|value| {
                        if operator == proto::ArithmeticOperator::Add {
                            value.wrapping_add(offset)
                        } else {
                            value.wrapping_sub(offset)
                        }
                    })
                }));
                assert_eq!(actual.as_ref(), &expected, "{operator:?} offset={offset}");
            }
        }
    }
}
