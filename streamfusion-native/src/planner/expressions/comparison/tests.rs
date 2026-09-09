// Copyright 2026 StreamFusion Authors. Licensed under the Apache License, Version 2.0.
use super::*;
use arrow::array::{Array, ArrayRef, BooleanArray, Decimal128Array, Int64Array};
use arrow::datatypes::Field;
use arrow::record_batch::RecordBatch;
use datafusion::physical_expr::expressions::{Column, Literal};
use datafusion::scalar::ScalarValue;

fn batch(left: ArrayRef, right: ArrayRef) -> RecordBatch {
    let schema = Arc::new(Schema::new(vec![
        Field::new("left", left.data_type().clone(), true),
        Field::new("right", right.data_type().clone(), true),
    ]));
    RecordBatch::try_new(schema, vec![left, right]).unwrap()
}

fn verify(batch: &RecordBatch, orders: &[Option<i8>]) {
    use proto::ComparisonOperator::*;
    for operator in [
        Equal,
        NotEqual,
        LessThan,
        LessThanOrEqual,
        GreaterThan,
        GreaterThanOrEqual,
        IsDistinctFrom,
        IsNotDistinctFrom,
    ] {
        for reverse in [false, true] {
            let (left, right) = if reverse { (1, 0) } else { (0, 1) };
            let expression = create(
                Arc::new(Column::new(if left == 0 { "left" } else { "right" }, left)),
                Arc::new(Column::new(
                    if right == 0 { "left" } else { "right" },
                    right,
                )),
                operator,
                batch.schema().as_ref(),
            )
            .unwrap();
            let result = expression
                .evaluate(batch)
                .unwrap()
                .into_array(batch.num_rows())
                .unwrap();
            let actual = result.as_any().downcast_ref::<BooleanArray>().unwrap();
            for (row, order) in orders.iter().enumerate() {
                let order = order.map(|v| if reverse { -v } else { v });
                let expected = if matches!(operator, IsDistinctFrom | IsNotDistinctFrom) {
                    let equal = if let Some(order) = order {
                        order == 0
                    } else {
                        batch.column(0).is_null(row) && batch.column(1).is_null(row)
                    };
                    Some(if operator == IsDistinctFrom {
                        !equal
                    } else {
                        equal
                    })
                } else {
                    order.map(|order| match operator {
                        Equal => order == 0,
                        NotEqual => order != 0,
                        LessThan => order < 0,
                        LessThanOrEqual => order <= 0,
                        GreaterThan => order > 0,
                        GreaterThanOrEqual => order >= 0,
                        _ => unreachable!(),
                    })
                };
                assert_eq!(
                    actual.is_valid(row).then(|| actual.value(row)),
                    expected,
                    "{operator:?}, row={row}, reverse={reverse}"
                );
            }
            assert_eq!(
                expression
                    .evaluate(&RecordBatch::new_empty(batch.schema()))
                    .unwrap()
                    .into_array(0)
                    .unwrap()
                    .len(),
                0
            );
        }
    }
}

#[test]
fn decimal_integer_comparison_preserves_fractional_precision_and_integer_extremes() {
    let limit = 10_i128.pow(38) - 1;
    let left = Decimal128Array::from(vec![
        Some(42),
        None,
        Some(-1),
        Some(0),
        Some(1),
        Some(limit),
        Some(-limit),
        Some(limit),
        Some(-limit),
        None,
        Some(0),
    ])
    .with_precision_and_scale(38, 38)
    .unwrap();
    let right = Int64Array::from(vec![
        Some(42),
        Some(0),
        Some(0),
        Some(0),
        Some(0),
        Some(1),
        Some(-1),
        Some(i64::MAX),
        Some(i64::MIN),
        None,
        None,
    ]);
    let batch = batch(Arc::new(left.slice(1, 10)), Arc::new(right.slice(1, 10)));
    verify(
        &batch,
        &[
            None,
            Some(-1),
            Some(0),
            Some(1),
            Some(-1),
            Some(1),
            Some(-1),
            Some(1),
            None,
            None,
        ],
    );
    let scalar = create(
        Arc::new(Literal::new(ScalarValue::Decimal128(Some(-1), 38, 38))),
        Arc::new(Literal::new(ScalarValue::Int64(Some(0)))),
        proto::ComparisonOperator::LessThan,
        batch.schema().as_ref(),
    )
    .unwrap();
    assert!(matches!(
        scalar.evaluate(&batch).unwrap(),
        datafusion::logical_expr::ColumnarValue::Scalar(ScalarValue::Boolean(Some(true)))
    ));
}

#[test]
fn decimal_comparison_can_require_the_full_76_digit_common_representation() {
    let limit = 10_i128.pow(38) - 1;
    let left = Decimal128Array::from(vec![
        Some(limit),
        Some(-limit),
        Some(0),
        Some(1),
        Some(-1),
        None,
    ])
    .with_precision_and_scale(38, 0)
    .unwrap();
    let right = Decimal128Array::from(vec![Some(1), Some(-1), Some(1), Some(1), Some(-1), Some(0)])
        .with_precision_and_scale(38, 38)
        .unwrap();
    verify(
        &batch(Arc::new(left), Arc::new(right)),
        &[Some(1), Some(-1), Some(-1), Some(1), Some(-1), None],
    );
}

#[test]
fn common_decimal_type_stays_128_when_lossless_and_256_when_needed() {
    for (decimal, integer, expected) in [
        (
            DataType::Decimal128(23, 3),
            DataType::Int32,
            DataType::Decimal128(23, 3),
        ),
        (
            DataType::Decimal128(38, 38),
            DataType::Int64,
            DataType::Decimal256(58, 38),
        ),
    ] {
        let schema = Schema::new(vec![
            Field::new("left", decimal, true),
            Field::new("right", integer, true),
        ]);
        let (left, right) = exact_numeric::coerce(
            Arc::new(Column::new("left", 0)),
            Arc::new(Column::new("right", 1)),
            &schema,
        )
        .unwrap();
        assert_eq!(left.data_type(&schema).unwrap(), expected);
        assert_eq!(right.data_type(&schema).unwrap(), expected);
    }
}

#[test]
fn wide_comparison_buffers_use_the_existing_managed_allowance() {
    use crate::memory_pool::{tests_support::TestBroker, FlinkMemoryPool};
    use datafusion::execution::memory_pool::MemoryPool;
    let batch = batch(
        Arc::new(
            Decimal128Array::from(vec![Some(1); 4096])
                .with_precision_and_scale(38, 38)
                .unwrap(),
        ),
        Arc::new(Int64Array::from(vec![Some(0); 4096])),
    );
    for limit in [128 << 10, 16 << 20] {
        let broker = Arc::new(TestBroker::new(limit));
        let pool: Arc<dyn MemoryPool> = Arc::new(FlinkMemoryPool::new(broker.clone(), limit));
        let expression = super::super::managed_scalar::install(
            create(
                Arc::new(Column::new("left", 0)),
                Arc::new(Column::new("right", 1)),
                proto::ComparisonOperator::GreaterThan,
                batch.schema().as_ref(),
            )
            .unwrap(),
            Some(&pool),
            batch.schema().as_ref(),
        )
        .unwrap();
        let result = expression.evaluate(&batch);
        if limit == 128 << 10 {
            assert!(matches!(
                result,
                Err(DataFusionError::ResourcesExhausted(_))
            ));
        } else {
            let output = result.unwrap().into_array(4096).unwrap();
            assert!(output
                .as_any()
                .downcast_ref::<BooleanArray>()
                .unwrap()
                .value(0));
            let slice = output.slice(1, 17);
            drop(output);
            assert!(broker.reserved() > 0);
            drop(slice);
        }
        assert_eq!(broker.reserved(), 0);
    }
}
