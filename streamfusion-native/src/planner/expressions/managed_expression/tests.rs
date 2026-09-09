// Copyright 2026 StreamFusion Authors
// Licensed under the Apache License, Version 2.0.

use super::*;
use crate::allocation_test_support::measure;
use crate::memory_pool::{tests_support::TestBroker, FlinkMemoryPool, MemoryReservationBroker};
use arrow::array::{ArrayRef, Int64Array, StringArray};
use arrow::datatypes::Field;
use datafusion::logical_expr::Operator;
use datafusion::physical_expr::expressions::{BinaryExpr, CastExpr, Column, Literal};
use datafusion::scalar::ScalarValue;
use std::sync::atomic::{AtomicUsize, Ordering};

#[derive(Debug)]
pub(super) struct PeakBroker {
    pub(super) inner: TestBroker,
    pub(super) peak: AtomicUsize,
}
impl MemoryReservationBroker for PeakBroker {
    fn try_reserve(&self, bytes: usize) -> Result<bool> {
        let accepted = self.inner.try_reserve(bytes)?;
        if accepted {
            self.peak
                .fetch_max(self.inner.reserved(), Ordering::Relaxed);
        }
        Ok(accepted)
    }
    fn release(&self, bytes: usize) -> Result<()> {
        self.inner.release(bytes)
    }
    fn available(&self) -> Result<Option<usize>> {
        self.inner.available()
    }
}
pub(super) fn broker(limit: usize) -> (Arc<PeakBroker>, Arc<dyn MemoryPool>) {
    let broker = Arc::new(PeakBroker {
        inner: TestBroker::new(limit),
        peak: AtomicUsize::new(0),
    });
    let pool = Arc::new(FlinkMemoryPool::new(broker.clone(), limit));
    (broker, pool)
}
pub(super) fn column() -> Arc<dyn PhysicalExpr> {
    Arc::new(Column::new("v", 0))
}
pub(super) fn batch(array: ArrayRef) -> RecordBatch {
    RecordBatch::try_new(
        Arc::new(Schema::new(vec![Field::new(
            "v",
            array.data_type().clone(),
            true,
        )])),
        vec![array],
    )
    .unwrap()
}
pub(super) fn same_result(
    expected: Result<ColumnarValue>,
    actual: Result<ColumnarValue>,
) -> Option<ArrayRef> {
    match (expected, actual) {
        (Ok(ColumnarValue::Array(expected)), Ok(ColumnarValue::Array(actual))) => {
            assert_eq!(expected.to_data(), actual.to_data());
            for (expected, actual) in expected
                .to_data()
                .buffers()
                .iter()
                .zip(actual.to_data().buffers())
            {
                assert_eq!(expected.as_slice(), actual.as_slice());
            }
            Some(actual)
        }
        (Ok(ColumnarValue::Scalar(expected)), Ok(ColumnarValue::Scalar(actual))) => {
            assert_eq!(expected, actual);
            None
        }
        (Err(expected), Err(actual)) => {
            assert_eq!(expected.to_string(), actual.to_string());
            None
        }
        (expected, actual) => panic!("changed result kind: {expected:?} vs {actual:?}"),
    }
}

#[test]
fn generated_numeric_kernels_keep_bytes_and_admit_heap_before_execution() {
    for rows in [0, 1, 257, 4096] {
        for kind in [
            DataType::Int8,
            DataType::Int16,
            DataType::Int32,
            DataType::Int64,
            DataType::Float32,
            DataType::Float64,
            DataType::Decimal128(20, 2),
        ] {
            let values: ArrayRef = Arc::new(Int64Array::from_iter(
                (0..rows + 11).map(|i| (i % 17 != 0).then_some((i % 50) as i64 - 20)),
            ));
            let values = arrow::compute::cast(&values, &kind)
                .unwrap()
                .slice(11, rows);
            let input = batch(values);
            let mut expressions: Vec<Arc<dyn PhysicalExpr>> =
                vec![Arc::new(CastExpr::new(column(), DataType::Float64, None))];
            for op in [
                Operator::Plus,
                Operator::Minus,
                Operator::Multiply,
                Operator::Divide,
                Operator::Modulo,
                Operator::Eq,
                Operator::NotEq,
                Operator::Lt,
                Operator::LtEq,
                Operator::Gt,
                Operator::GtEq,
                Operator::IsDistinctFrom,
                Operator::IsNotDistinctFrom,
            ] {
                let scalar = ScalarValue::new_one(&kind).unwrap();
                expressions.push(Arc::new(BinaryExpr::new(
                    column(),
                    op,
                    Arc::new(Literal::new(scalar)),
                )));
            }
            for raw in expressions {
                let (broker, pool) = broker(32 << 20);
                let admitted = install(raw.clone(), &pool, input.schema_ref()).unwrap();
                assert!(admitted.transformed);
                let expected = raw.evaluate(&input);
                let (actual, observed) = measure(|| admitted.data.evaluate(&input));
                let result = same_result(expected, actual);
                if raw
                    .downcast_ref::<CastExpr>()
                    .is_some_and(|cast| cast.cast_type() == input.column(0).data_type())
                {
                    // Identity casts borrow the caller's input credit. Prove buffer sharing
                    // instead of requiring a second reservation for the same allocation.
                    let result = result.as_ref().unwrap().to_data();
                    let source = input.column(0).to_data();
                    assert_eq!(result.buffers().len(), source.buffers().len());
                    for (result, source) in result.buffers().iter().zip(source.buffers()) {
                        assert_eq!(result.as_ptr(), source.as_ptr());
                    }
                    assert_eq!(broker.inner.reserved(), 0);
                } else {
                    assert!(
                        result.as_ref().map_or(0, |array| {
                            crate::memory_pool::buffer_size::arrays_bytes(std::slice::from_ref(
                                array,
                            ))
                            .unwrap()
                        }) <= broker.inner.reserved(),
                        "{raw} {kind} {observed:?}"
                    );
                }
                assert!(
                    observed.peak <= broker.peak.load(Ordering::Relaxed),
                    "{raw} {kind} {observed:?}"
                );
                drop(admitted);
                drop(pool);
                drop(result);
                assert_eq!(broker.inner.reserved(), 0);
            }
        }
    }
}

#[test]
fn selection_and_short_circuit_keep_conditional_errors_and_admit_gather() {
    let input = RecordBatch::try_from_iter(vec![
        (
            "v",
            Arc::new(Int64Array::from(vec![0, 2, 0, 4])) as ArrayRef,
        ),
        (
            "wide",
            Arc::new(StringArray::from(vec!["wide".repeat(8192); 4])) as ArrayRef,
        ),
    ])
    .unwrap();
    let divide: Arc<dyn PhysicalExpr> = Arc::new(BinaryExpr::new(
        Arc::new(Literal::new(ScalarValue::Int64(Some(8)))),
        Operator::Divide,
        column(),
    ));
    for mask in [
        vec![false, true, false, true],
        vec![false; 4],
        vec![true; 4],
    ] {
        let (broker, pool) = broker(32 << 20);
        let admitted = install(divide.clone(), &pool, input.schema_ref())
            .unwrap()
            .data;
        let mask = BooleanArray::from(mask);
        let expected = divide.evaluate_selection(&input, &mask);
        let (actual, observed) = measure(|| admitted.evaluate_selection(&input, &mask));
        let result = same_result(expected, actual);
        assert!(
            observed.peak <= broker.peak.load(Ordering::Relaxed),
            "{observed:?}"
        );
        drop(result);
        assert_eq!(broker.inner.reserved(), 0);
    }
    // A mixed AND must not evaluate divide-by-zero rows on the RHS.
    let nonzero: Arc<dyn PhysicalExpr> = Arc::new(BinaryExpr::new(
        column(),
        Operator::NotEq,
        Arc::new(Literal::new(ScalarValue::Int64(Some(0)))),
    ));
    let positive: Arc<dyn PhysicalExpr> = Arc::new(BinaryExpr::new(
        divide,
        Operator::Gt,
        Arc::new(Literal::new(ScalarValue::Int64(Some(0)))),
    ));
    let raw: Arc<dyn PhysicalExpr> = Arc::new(BinaryExpr::new(nonzero, Operator::And, positive));
    let (broker, pool) = broker(32 << 20);
    let admitted =
        super::super::managed_scalar::install(raw.clone(), Some(&pool), input.schema_ref())
            .unwrap();
    let result = same_result(raw.evaluate(&input), admitted.evaluate(&input));
    drop(result);
    assert_eq!(broker.inner.reserved(), 0);
    let (denied, pool) = self::broker(32 << 10);
    let admitted = install(raw, &pool, input.schema_ref()).unwrap().data;
    let (result, observed) = measure(|| admitted.evaluate(&input));
    assert!(matches!(
        result,
        Err(DataFusionError::ResourcesExhausted(_))
    ));
    assert!(observed.peak < 32 << 10, "{observed:?}");
    assert_eq!(denied.inner.reserved(), 0);
}

#[test]
fn scalars_remain_scalars_and_new_children_keep_metadata_and_admission() {
    let input = batch(Arc::new(Int64Array::from(vec![1, 2, 3])));
    let raw: Arc<dyn PhysicalExpr> = Arc::new(CastExpr::new(column(), DataType::Float64, None));
    let (broker, pool) = broker(32 << 20);
    let admitted = install(raw.clone(), &pool, input.schema_ref())
        .unwrap()
        .data;
    assert_eq!(
        admitted.return_field(input.schema_ref()).unwrap(),
        raw.return_field(input.schema_ref()).unwrap()
    );
    assert_eq!(admitted.to_string(), raw.to_string());
    let bounds = Interval::make(Some(1i64), Some(3)).unwrap();
    assert_eq!(
        admitted.evaluate_bounds(&[&bounds]).unwrap(),
        raw.evaluate_bounds(&[&bounds]).unwrap()
    );
    let child: Arc<dyn PhysicalExpr> = Arc::new(Literal::new(ScalarValue::Int64(Some(123))));
    let raw = raw.with_new_children(vec![child.clone()]).unwrap();
    let admitted = admitted.with_new_children(vec![child]).unwrap();
    assert!(admitted.downcast_ref::<AdmittedExpression>().is_some());
    same_result(raw.evaluate(&input), admitted.evaluate(&input));
    assert_eq!(broker.inner.reserved(), 0);
}

#[test]
fn tiny_slice_keeps_large_validity_storage_admitted_until_final_consumer_release() {
    let values: ArrayRef = Arc::new(Int64Array::from_iter(
        (0..1_000_000).map(|i| (i % 17 != 0).then_some(i)),
    ));
    let input = batch(values.slice(11, 1));
    let raw: Arc<dyn PhysicalExpr> = Arc::new(CastExpr::new(column(), DataType::Float64, None));
    let (broker, pool) = broker(32 << 20);
    let admitted = install(raw.clone(), &pool, input.schema_ref())
        .unwrap()
        .data;
    let output = same_result(raw.evaluate(&input), admitted.evaluate(&input)).unwrap();
    let sliced = output.slice(0, 1);
    drop(output);
    drop(input);
    drop(values);
    drop(admitted);
    drop(pool);
    assert!(broker.inner.reserved() >= sliced.get_array_memory_size());
    std::thread::spawn(move || drop(sliced)).join().unwrap();
    assert_eq!(broker.inner.reserved(), 0);
}

#[test]
fn tiny_struct_field_slice_accounts_for_the_retained_child_bitmap() {
    let values: ArrayRef = Arc::new(Int64Array::from_iter(
        (0..1_000_000).map(|i| (i % 17 != 0).then_some(i)),
    ));
    let parent: ArrayRef = Arc::new(arrow::array::StructArray::from(vec![(
        Arc::new(Field::new("nested", DataType::Int64, true)),
        values,
    )]));
    let input = batch(parent.slice(11, 1));
    let field = super::super::struct_field::create(column(), "nested", input.schema_ref()).unwrap();
    let raw: Arc<dyn PhysicalExpr> = Arc::new(CastExpr::new(field, DataType::Float64, None));
    let (broker, pool) = broker(32 << 20);
    let admitted = install(raw.clone(), &pool, input.schema_ref())
        .unwrap()
        .data;
    let result = same_result(raw.evaluate(&input), admitted.evaluate(&input));
    drop(result);
    assert_eq!(broker.inner.reserved(), 0);
    let (_, observed) = measure(|| super::validity::retained_bytes(input.column(0).as_ref()));
    assert_eq!(
        observed.peak, 0,
        "sizing must not construct ArrayData descriptors"
    );
}
