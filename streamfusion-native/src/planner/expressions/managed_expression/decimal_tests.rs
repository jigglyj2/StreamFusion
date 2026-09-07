// Copyright 2026 StreamFusion Authors
// Licensed under the Apache License, Version 2.0.

use super::tests::{batch, broker, column, same_result};
use super::*;
use crate::allocation_test_support::measure;
use arrow::array::{ArrayRef, Decimal128Array, Int64Array};
use datafusion::physical_expr::expressions::Literal;
use datafusion::scalar::ScalarValue;
use std::sync::atomic::Ordering;

#[test]
fn decimal_rescaling_and_flink_division_keep_bytes_and_measured_allocation_credit() {
    for scale in [0, 4, 18, 38] {
        let integers: ArrayRef = Arc::new(Int64Array::from_iter(
            (0..4096).map(|i| (i % 17 != 0).then_some(i64::MAX - i)),
        ));
        let decimal: ArrayRef = Arc::new(
            Decimal128Array::from_iter(
                (0..4096).map(|i| (i % 19 != 0).then_some(10i128.pow(37) - i)),
            )
            .with_precision_and_scale(38, scale)
            .unwrap(),
        );
        for array in [integers, decimal] {
            let input = batch(array);
            for target_scale in [0, 2, 18, 38] {
                let raw = super::super::decimal::cast(
                    column(),
                    DataType::Decimal128(38, target_scale),
                    input.schema_ref(),
                )
                .unwrap();
                check(raw, &input);
                if matches!(input.column(0).data_type(), DataType::Decimal128(_, _)) {
                    for divisor in [0, 3, 10i128.pow(36)] {
                        let right = Arc::new(Literal::new(ScalarValue::Decimal128(
                            Some(divisor),
                            38,
                            scale,
                        )));
                        let raw = super::super::decimal::divide(
                            column(),
                            right,
                            DataType::Decimal128(38, target_scale),
                            input.schema_ref(),
                        )
                        .unwrap();
                        check(raw, &input);
                    }
                }
            }
        }
    }
}
fn check(raw: Arc<dyn PhysicalExpr>, input: &RecordBatch) {
    let (broker, pool) = broker(32 << 20);
    let admitted =
        super::super::managed_scalar::install(raw.clone(), Some(&pool), input.schema_ref())
            .unwrap();
    assert!(admitted.downcast_ref::<AdmittedExpression>().is_some());
    let expected = raw.evaluate(input);
    let (actual, observed) = measure(|| admitted.evaluate(input));
    let result = same_result(expected, actual);
    if result.is_some() {
        assert!(
            result
                .as_ref()
                .map_or(0, |array| crate::memory_pool::buffer_size::arrays_bytes(
                    std::slice::from_ref(array)
                )
                .unwrap())
                <= broker.inner.reserved(),
            "{raw} {observed:?}"
        );
    } // Error text is returned to the caller; it is not retained Arrow output.
    assert!(
        observed.peak <= broker.peak.load(Ordering::Relaxed),
        "{raw} {observed:?}"
    );
    drop(result);
    assert_eq!(broker.inner.reserved(), 0);
}
