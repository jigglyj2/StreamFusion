// Copyright 2026 StreamFusion Authors
// Licensed under the Apache License, Version 2.0.

use super::*;
use crate::allocation_test_support::measure;
use crate::planner::expressions::managed_expression::tests::{batch, broker, column};
use arrow::array::{ArrayRef, Int32Array, ListArray, StructArray};
use arrow::datatypes::{Field, Int32Type, TimeUnit};
use std::sync::atomic::Ordering;

fn values() -> Vec<ScalarValue> {
    let mut values = vec![
        ScalarValue::Boolean(Some(true)),
        ScalarValue::Int8(Some(-128)),
        ScalarValue::Int16(Some(32767)),
        ScalarValue::Int32(Some(i32::MIN)),
        ScalarValue::Int64(Some(i64::MAX)),
        ScalarValue::Float32(Some(-0.0)),
        ScalarValue::Float64(Some(f64::NAN)),
        ScalarValue::Decimal128(Some(123456789), 38, 7),
        ScalarValue::Date32(Some(-123)),
        ScalarValue::Time32Millisecond(Some(12345)),
        ScalarValue::TimestampMicrosecond(Some(-12345), None),
        ScalarValue::Utf8(Some("é東京🙂".repeat(16))),
        ScalarValue::Binary(Some(vec![0, 128, 255])),
        ScalarValue::FixedSizeBinary(5, Some(vec![1, 2, 3, 4, 5])),
        ScalarValue::FixedSizeBinary(8192, None),
        ScalarValue::Utf8View(Some("a long view value repeated without row copies".into())),
    ];
    let list = Arc::new(ListArray::from_iter_primitive::<Int32Type, _, _>([Some(
        vec![Some(1), None, Some(-3)],
    )]));
    values.push(ScalarValue::List(list.clone()));
    values.push(ScalarValue::Struct(Arc::new(StructArray::from(vec![(
        Arc::new(Field::new("nested", list.data_type().clone(), true)),
        list as ArrayRef,
    )]))));
    for kind in [
        DataType::Null,
        DataType::Boolean,
        DataType::Int8,
        DataType::Int16,
        DataType::Int32,
        DataType::Int64,
        DataType::Float32,
        DataType::Float64,
        DataType::Decimal128(38, 7),
        DataType::Date32,
        DataType::Time32(TimeUnit::Millisecond),
        DataType::Timestamp(TimeUnit::Microsecond, None),
        DataType::Utf8,
        DataType::Binary,
        DataType::List(Arc::new(Field::new("item", DataType::Utf8, true))),
        DataType::Struct(vec![Field::new("value", DataType::FixedSizeBinary(128), true)].into()),
    ] {
        values.push(ScalarValue::try_from(&kind).unwrap());
    }
    values
}

#[test]
fn scalar_broadcast_keeps_datafusion_bytes_and_admits_retained_payload() {
    for value in values() {
        for rows in [0, 1, 17, 1024] {
            let expected = value.to_array_of_size(rows).unwrap();
            let (broker, pool) = broker(128 << 20);
            let (actual, observed) = measure(|| scalar(&value, rows, &pool));
            let actual = actual.unwrap().into_array(rows).unwrap();
            assert_eq!(
                actual.to_data(),
                expected.to_data(),
                "{} rows={rows}",
                value.data_type()
            );
            assert_eq!(actual.len(), rows);
            assert!(
                observed.peak <= broker.peak.load(Ordering::Relaxed),
                "{} {observed:?}",
                value.data_type()
            );
            if actual.get_buffer_memory_size() > 0 {
                assert!(
                    crate::memory_pool::buffer_size::arrays_bytes(std::slice::from_ref(&actual))
                        .unwrap()
                        <= broker.inner.reserved(),
                    "{} {observed:?}",
                    value.data_type()
                );
            } // Bufferless metadata ownership is separate from scalar payload admission.
            drop(pool);
            drop(actual);
            assert_eq!(broker.inner.reserved(), 0);
        }
    }
}

#[test]
fn large_literal_denial_happens_before_cloning_or_broadcasting_its_payload() {
    let (broker, pool) = broker(2 << 20);
    let input = batch(Arc::new(Int32Array::from(vec![1; 4096])));
    let literal: Arc<dyn PhysicalExpr> =
        Arc::new(Literal::new(ScalarValue::Utf8(Some("x".repeat(4 << 20)))));
    let projected = super::super::projection(literal, Some(&pool));
    let (result, observed) = measure(|| projected.evaluate(&input));
    assert!(matches!(
        result,
        Err(DataFusionError::ResourcesExhausted(_))
    ));
    assert!(
        observed.peak < 64 << 10,
        "must not clone the 4 MiB literal first: {observed:?}"
    );
    assert_eq!(broker.inner.reserved(), 0);
}

#[test]
fn primitive_source_batch_broadcast_fits_one_mib_and_preserves_datafusion_values() {
    for value in [
        ScalarValue::Int64(Some(i64::MAX)),
        ScalarValue::Int64(None),
        ScalarValue::TimestampMillisecond(Some(-12345), None),
        ScalarValue::TimestampMillisecond(None, None),
        ScalarValue::Float64(Some(-0.0)),
        ScalarValue::Boolean(Some(true)),
        ScalarValue::Boolean(None),
    ] {
        let expected = value.to_array_of_size(16_384).unwrap();
        let (broker, pool) = broker(1 << 20);
        let (actual, observed) = measure(|| scalar(&value, 16_384, &pool));
        let actual = actual.unwrap().into_array(16_384).unwrap();
        assert_eq!(actual.to_data(), expected.to_data());
        assert!(observed.peak <= broker.peak.load(Ordering::Relaxed));
        drop(pool);
        assert!(broker.inner.reserved() > 0);
        drop(actual);
        assert_eq!(broker.inner.reserved(), 0);
    }
}

#[test]
fn projection_borrows_array_results_without_copying_or_new_payload_credit() {
    let input = batch(Arc::new(Int32Array::from(vec![1, 2, 3])));
    let (broker, pool) = broker(0);
    let expression = column();
    assert!(Arc::ptr_eq(
        &expression,
        &super::super::projection(expression.clone(), Some(&pool))
    ));
    let result = evaluate(&expression, &input, &pool)
        .unwrap()
        .into_array(3)
        .unwrap();
    assert!(Arc::ptr_eq(&result, input.column(0)));
    assert_eq!(broker.inner.reserved(), 0);
}

#[test]
fn materialized_output_keeps_credit_after_producer_and_pool_close() {
    let (broker, pool) = broker(32 << 20);
    let input = batch(Arc::new(Int32Array::from(vec![1; 1024])));
    let literal: Arc<dyn PhysicalExpr> =
        Arc::new(Literal::new(ScalarValue::Utf8(Some("é".repeat(128)))));
    let projected = super::super::projection(literal, Some(&pool));
    let output = projected
        .evaluate(&input)
        .unwrap()
        .into_array(1024)
        .unwrap();
    let tail = output.slice(3, 4);
    drop(output);
    drop(projected);
    drop(input);
    drop(pool);
    assert!(broker.inner.reserved() > 0);
    std::thread::spawn(move || drop(tail)).join().unwrap();
    assert_eq!(broker.inner.reserved(), 0);
    assert!(workspace(&ScalarValue::Int64(Some(1)), usize::MAX).is_err());
}

#[test]
fn masked_projection_preserves_df_scatter_and_accounts_for_scalar_clones() {
    for value in [
        ScalarValue::Utf8(Some("é".repeat(512 * 1024))),
        ScalarValue::Boolean(Some(true)),
        ScalarValue::Boolean(None),
    ] {
        let (broker, pool) = broker(64 << 20);
        let input = batch(Arc::new(Int32Array::from(vec![1; 3])));
        let raw: Arc<dyn PhysicalExpr> = Arc::new(Literal::new(value));
        let projected = super::super::projection(raw.clone(), Some(&pool));
        for mask in [vec![true, false, true], vec![false; 3], vec![true; 3]] {
            let selection = BooleanArray::from(mask);
            let expected = raw
                .evaluate_selection(&input, &selection)
                .unwrap()
                .into_array(3)
                .unwrap();
            let (actual, observed) = measure(|| projected.evaluate_selection(&input, &selection));
            let actual = actual.unwrap().into_array(3).unwrap();
            assert_eq!(expected.to_data(), actual.to_data());
            assert!(
                observed.peak <= broker.peak.load(Ordering::Relaxed),
                "{observed:?}"
            );
            drop(actual);
            assert_eq!(broker.inner.reserved(), 0);
        }
    }
}
