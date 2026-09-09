// Copyright 2026 StreamFusion Authors
// Licensed under the Apache License, Version 2.0.

use super::tests::{broker, same_result};
use super::*;
use arrow::array::{ArrayRef, Int64Array, StringArray, StructArray};
use arrow::buffer::NullBuffer;
use arrow::datatypes::Field;
use datafusion::common::tree_node::{TransformedResult, TreeNode};
use datafusion::logical_expr::Operator;
use datafusion::physical_expr::expressions::{BinaryExpr, Column, Literal};
use datafusion::scalar::ScalarValue;

fn fixture() -> (RecordBatch, Arc<dyn PhysicalExpr>) {
    let values: ArrayRef = Arc::new(Int64Array::from_iter(
        (0..43).map(|i| (i % 7 != 0).then_some(i % 6 - 2)),
    ));
    let wide: ArrayRef = Arc::new(StringArray::from(vec!["x".repeat(1 << 18); 43]));
    let nested: ArrayRef = Arc::new(StructArray::new(
        vec![
            Arc::new(Field::new("value", DataType::Int64, true)),
            Arc::new(Field::new("unused", DataType::Utf8, true)),
        ]
        .into(),
        vec![values, wide.clone()],
        Some(NullBuffer::from_iter((0..43).map(|i| i % 11 != 0))),
    ));
    let parent: ArrayRef = Arc::new(StructArray::new(
        vec![Arc::new(Field::new(
            "nested",
            nested.data_type().clone(),
            true,
        ))]
        .into(),
        vec![nested],
        Some(NullBuffer::from_iter((0..43).map(|i| i % 13 != 0))),
    ));
    let batch = RecordBatch::try_from_iter(vec![("unused", wide), ("payload", parent)])
        .unwrap()
        .slice(3, 37);
    let column: Arc<dyn PhysicalExpr> = Arc::new(Column::new("payload", 1));
    let nested = super::super::struct_field::create(column, "nested", batch.schema_ref()).unwrap();
    let value = super::super::struct_field::create(nested, "value", batch.schema_ref()).unwrap();
    (batch, value)
}

fn binary(
    left: Arc<dyn PhysicalExpr>,
    op: Operator,
    right: Arc<dyn PhysicalExpr>,
) -> Arc<dyn PhysicalExpr> {
    Arc::new(BinaryExpr::new(left, op, right))
}
fn number(value: i64) -> Arc<dyn PhysicalExpr> {
    Arc::new(Literal::new(ScalarValue::Int64(Some(value))))
}

#[test]
fn nested_conditional_inputs_exclude_unused_payload_and_preserve_masks_and_buffer_ownership() {
    let (input, field) = fixture();
    for op in [Operator::And, Operator::Or] {
        let raw = binary(
            binary(field.clone(), Operator::GtEq, number(0)),
            op,
            binary(field.clone(), Operator::Lt, number(2)),
        );
        let (broker, pool) = broker(1 << 20);
        let admitted =
            super::super::managed_scalar::install(raw.clone(), Some(&pool), input.schema_ref())
                .unwrap();
        let scope = admitted
            .downcast_ref::<AdmittedExpression>()
            .unwrap()
            .scope
            .as_ref()
            .unwrap();
        let view = scope.project(&input).unwrap();
        assert_eq!(view.num_columns(), 1);
        assert_eq!(view.num_rows(), input.num_rows());
        let original = field
            .evaluate(&input)
            .unwrap()
            .into_array(input.num_rows())
            .unwrap();
        assert_eq!(
            view.column(0).to_data().buffers()[0].as_ptr(),
            original.to_data().buffers()[0].as_ptr()
        );
        for selection in [
            None,
            Some(BooleanArray::from(vec![true; 37])),
            Some(BooleanArray::from(vec![false; 37])),
            Some(BooleanArray::from_iter((0..37).map(|i| {
                if i % 7 == 0 {
                    None
                } else {
                    Some(i % 3 != 0)
                }
            }))),
        ] {
            let (expected, actual) = match selection {
                None => (raw.evaluate(&input), admitted.evaluate(&input)),
                Some(mask) => (
                    raw.evaluate_selection(&input, &mask),
                    admitted.evaluate_selection(&input, &mask),
                ),
            };
            let output = same_result(expected, actual).unwrap();
            assert!(broker.inner.reserved() > 0);
            drop(output);
            assert_eq!(broker.inner.reserved(), 0);
        }
        let rewritten = admitted
            .transform_down(|expression| {
                if let Some(column) = expression.downcast_ref::<Column>() {
                    assert_eq!(column.index(), 1); // Optimizers see the original column, not a cached slot.
                    Ok(Transformed::yes(
                        Arc::new(Column::new("payload", 0)) as Arc<dyn PhysicalExpr>
                    ))
                } else {
                    Ok(Transformed::no(expression))
                }
            })
            .data()
            .unwrap();
        let output = same_result(
            raw.evaluate(&input),
            rewritten.evaluate(&input.project(&[1]).unwrap()),
        );
        drop(output);
        assert_eq!(broker.inner.reserved(), 0);
    }
}

#[test]
fn conditional_scoping_does_not_hoist_computed_or_fallible_operands() {
    let input = RecordBatch::try_from_iter(vec![(
        "v",
        Arc::new(Int64Array::from(vec![0, 0, 0, 2])) as ArrayRef,
    )])
    .unwrap();
    let value: Arc<dyn PhysicalExpr> = Arc::new(Column::new("v", 0));
    let divide = binary(number(10), Operator::Divide, value.clone());
    let raw = binary(
        binary(value, Operator::NotEq, number(0)),
        Operator::And,
        binary(divide, Operator::Gt, number(1)),
    );
    let (broker, pool) = broker(1 << 20);
    let admitted =
        super::super::managed_scalar::install(raw.clone(), Some(&pool), input.schema_ref())
            .unwrap();
    let output = same_result(raw.evaluate(&input), admitted.evaluate(&input));
    drop(output);
    assert_eq!(broker.inner.reserved(), 0);
    let raw = binary(
        Arc::new(Literal::new(ScalarValue::Boolean(Some(false)))),
        Operator::And,
        raw,
    );
    let admitted =
        super::super::managed_scalar::install(raw.clone(), Some(&pool), input.schema_ref())
            .unwrap();
    let output = same_result(raw.evaluate(&input), admitted.evaluate(&input));
    drop(output);
    assert_eq!(broker.inner.reserved(), 0);
}

#[test]
fn conditional_view_is_admitted_before_access_and_scalar_only_batches_keep_row_count() {
    let (input, field) = fixture();
    let raw = binary(
        binary(field.clone(), Operator::GtEq, number(0)),
        Operator::And,
        binary(field, Operator::Lt, number(2)),
    );
    let (denied, pool) = broker(1024);
    let admitted =
        super::super::managed_scalar::install(raw, Some(&pool), input.schema_ref()).unwrap();
    let (result, allocation) =
        crate::allocation_test_support::measure(|| admitted.evaluate(&input));
    assert!(matches!(
        result,
        Err(DataFusionError::ResourcesExhausted(_))
    ));
    assert!(allocation.peak < 32 << 10, "{allocation:?}");
    assert_eq!(denied.inner.reserved(), 0);
    let (_, pool) = broker(1 << 20);
    let raw = binary(
        Arc::new(Literal::new(ScalarValue::Boolean(Some(true)))),
        Operator::Or,
        Arc::new(Literal::new(ScalarValue::Boolean(Some(false)))),
    );
    let admitted =
        super::super::managed_scalar::install(raw.clone(), Some(&pool), input.schema_ref())
            .unwrap();
    for rows in [0, 1, 37] {
        let batch = input.slice(0, rows);
        same_result(raw.evaluate(&batch), admitted.evaluate(&batch));
    }
}
