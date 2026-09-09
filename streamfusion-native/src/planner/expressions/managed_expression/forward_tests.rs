// Copyright 2026 StreamFusion Authors
// Licensed under the Apache License, Version 2.0.

use super::tests::{broker, same_result};
use super::*;
use arrow::array::{ArrayRef, Int64Array, StringArray};
use datafusion::logical_expr::Operator;
use datafusion::physical_expr::expressions::{BinaryExpr, CastExpr, Column, Literal};
use datafusion::scalar::ScalarValue;

fn input(kind: &DataType) -> RecordBatch {
    let values = Arc::new(Int64Array::from(vec![
        Some(9),
        None,
        Some(3),
        Some(4),
        Some(5),
        Some(6),
    ])) as ArrayRef;
    RecordBatch::try_from_iter(vec![
        (
            "value",
            arrow::compute::cast(values.as_ref(), kind)
                .unwrap()
                .slice(1, 4),
        ),
        (
            "unrelated",
            Arc::new(StringArray::from(vec!["x".repeat(1 << 20); 4])) as ArrayRef,
        ),
    ])
    .unwrap()
}

#[test]
fn identity_casts_forward_sliced_buffers_without_reserving_unrelated_payload() {
    for kind in [
        DataType::Int64,
        DataType::Timestamp(arrow::datatypes::TimeUnit::Millisecond, None),
        DataType::Decimal128(20, 2),
    ] {
        let input = input(&kind);
        let raw: Arc<dyn PhysicalExpr> =
            Arc::new(CastExpr::new(Arc::new(Column::new("value", 0)), kind, None));
        let (broker, pool) = broker(1);
        let admitted = install(raw.clone(), &pool, input.schema_ref())
            .unwrap()
            .data;
        for mask in [None, Some(BooleanArray::from(vec![true; 4]))] {
            let expected = match &mask {
                None => raw.evaluate(&input),
                Some(mask) => raw.evaluate_selection(&input, mask),
            };
            let actual = match &mask {
                None => admitted.evaluate(&input),
                Some(mask) => admitted.evaluate_selection(&input, mask),
            };
            let result = same_result(expected, actual).unwrap();
            assert_eq!(
                result.to_data().buffers()[0].as_ptr(),
                input.column(0).to_data().buffers()[0].as_ptr()
            );
            assert_eq!(broker.inner.reserved(), 0);
        }
    }
}

#[test]
fn identity_cast_preserves_computed_child_credit_until_the_last_buffer_owner_drops() {
    let input = input(&DataType::Int64);
    let child: Arc<dyn PhysicalExpr> = Arc::new(BinaryExpr::new(
        Arc::new(Column::new("value", 0)),
        Operator::Plus,
        Arc::new(Literal::new(ScalarValue::Int64(Some(1)))),
    ));
    let raw: Arc<dyn PhysicalExpr> = Arc::new(CastExpr::new(child, DataType::Int64, None));
    let (broker, pool) = broker(1 << 20);
    let admitted =
        super::super::managed_scalar::install(raw.clone(), Some(&pool), input.schema_ref())
            .unwrap();
    let result = same_result(raw.evaluate(&input), admitted.evaluate(&input)).unwrap();
    assert!(broker.inner.reserved() > 0);
    drop(admitted);
    drop(input);
    assert!(broker.inner.reserved() > 0);
    drop(result);
    assert_eq!(broker.inner.reserved(), 0);
}

#[test]
fn masked_identity_cast_still_admits_datafusion_gather_and_scatter() {
    let input = input(&DataType::Int64);
    let raw: Arc<dyn PhysicalExpr> = Arc::new(CastExpr::new(
        Arc::new(Column::new("value", 0)),
        DataType::Int64,
        None,
    ));
    let mask = BooleanArray::from(vec![true, false, true, false]);
    let (denied, pool) = broker(1 << 20);
    let admitted = install(raw.clone(), &pool, input.schema_ref())
        .unwrap()
        .data;
    assert!(matches!(
        admitted.evaluate_selection(&input, &mask),
        Err(DataFusionError::ResourcesExhausted(_))
    ));
    assert_eq!(denied.inner.reserved(), 0);
    let (allowed, pool) = broker(32 << 20);
    let admitted = install(raw.clone(), &pool, input.schema_ref())
        .unwrap()
        .data;
    let result = same_result(
        raw.evaluate_selection(&input, &mask),
        admitted.evaluate_selection(&input, &mask),
    )
    .unwrap();
    assert!(allowed.inner.reserved() > 0);
    drop(result);
    assert_eq!(allowed.inner.reserved(), 0);
}
