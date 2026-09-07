// Copyright 2026 StreamFusion Authors
// Licensed under the Apache License, Version 2.0.

use super::tests::{args, PeakBroker};
use super::*;
use crate::allocation_test_support::measure;
use crate::memory_pool::{tests_support::TestBroker, FlinkMemoryPool};
use arrow::array::Float64Array;
use arrow::datatypes::Field;
use datafusion::scalar::ScalarValue;
use std::sync::atomic::{AtomicUsize, Ordering};

fn functions() -> Vec<(Arc<ScalarUDF>, usize)> {
    use datafusion_functions::math::*;
    vec![
        (acos(), 1),
        (asin(), 1),
        (atan(), 1),
        (atan2(), 2),
        (cos(), 1),
        (sin(), 1),
        (tan(), 1),
        (sinh(), 1),
        (cosh(), 1),
        (tanh(), 1),
        (exp(), 1),
        (ln(), 1),
        (log10(), 1),
        (log2(), 1),
        (power(), 2),
        (ceil(), 1),
        (floor(), 1),
        (signum(), 1),
        (degrees(), 1),
        (radians(), 1),
    ]
}

#[test]
fn fixed_math_preserves_datafusion_bytes_and_measured_heap_admission() {
    for (inner, arity) in functions() {
        for scalar in [false, true] {
            for single in [false, true] {
                let broker = Arc::new(PeakBroker {
                    inner: TestBroker::new(32 << 20),
                    peak: AtomicUsize::new(0),
                });
                let function = AdmittedFunction {
                    inner: inner.as_ref().clone(),
                    pool: Arc::new(FlinkMemoryPool::new(broker.clone(), 32 << 20)),
                    policy: Policy::FixedMath,
                };
                let values = if scalar {
                    ColumnarValue::Scalar(ScalarValue::Float64(Some(0.25)))
                } else {
                    ColumnarValue::Array(Arc::new(Float64Array::from_iter(
                        (0..4096)
                            .map(|i| (i % 17 != 0).then_some(((i * 137) % 1024) as f64 / 1024.0)),
                    )))
                };
                let values = if single {
                    match values {
                        ColumnarValue::Scalar(ScalarValue::Float64(value)) => {
                            ColumnarValue::Scalar(ScalarValue::Float32(value.map(|v| v as f32)))
                        }
                        ColumnarValue::Array(array) => ColumnarValue::Array(
                            arrow::compute::cast(&array, &DataType::Float32).unwrap(),
                        ),
                        _ => unreachable!(),
                    }
                } else {
                    values
                };
                let mut operands = vec![values];
                if arity == 2 {
                    operands.push(ColumnarValue::Scalar(if single {
                        ScalarValue::Float32(Some(0.5))
                    } else {
                        ScalarValue::Float64(Some(0.5))
                    }));
                }
                let make_args = || {
                    let mut result = args(operands.clone(), 4096);
                    let output_type = inner
                        .return_type(
                            &operands
                                .iter()
                                .map(ColumnarValue::data_type)
                                .collect::<Vec<_>>(),
                        )
                        .unwrap();
                    result.return_field = Arc::new(Field::new("math", output_type, true));
                    result
                };
                let expected = inner.invoke_with_args(make_args());
                let input = make_args();
                let (actual, observed) = measure(|| function.invoke_with_args(input));
                match (expected, actual) {
                    (Ok(expected), Ok(actual)) => {
                        let expected = expected.into_array(4096).unwrap();
                        let actual = actual.into_array(4096).unwrap();
                        let a = actual.to_data();
                        let e = expected.to_data();
                        assert_eq!(a.nulls(), e.nulls(), "{}", inner.name());
                        for (a, e) in a.buffers().iter().zip(e.buffers()) {
                            assert_eq!(a.as_slice(), e.as_slice(), "{}", inner.name());
                        }
                        assert!(
                            observed.live <= broker.inner.reserved() as isize,
                            "{} {observed:?}",
                            inner.name()
                        );
                    }
                    (Err(expected), Err(actual)) => {
                        assert_eq!(expected.to_string(), actual.to_string())
                    }
                    (expected, actual) => panic!(
                        "{} changed result kind: {expected:?} vs {actual:?}",
                        inner.name()
                    ),
                }
                assert!(
                    observed.peak <= broker.peak.load(Ordering::Relaxed),
                    "{} {observed:?}",
                    inner.name()
                );
                assert_eq!(broker.inner.reserved(), 0);
            }
        }
    }
}

#[test]
fn managed_math_preserves_optimizer_metadata_and_pool_identity() {
    for (inner, arity) in functions() {
        let broker = Arc::new(TestBroker::new(1 << 20));
        let pool: Arc<dyn MemoryPool> = Arc::new(FlinkMemoryPool::new(broker.clone(), 1 << 20));
        let function = AdmittedFunction {
            inner: inner.as_ref().clone(),
            pool: pool.clone(),
            policy: Policy::FixedMath,
        };
        let same = AdmittedFunction {
            inner: inner.as_ref().clone(),
            pool,
            policy: Policy::FixedMath,
        };
        let other = AdmittedFunction {
            inner: inner.as_ref().clone(),
            pool: Arc::new(FlinkMemoryPool::new(broker, 1 << 20)),
            policy: Policy::FixedMath,
        };
        assert_eq!(function, same);
        assert_ne!(function, other);
        assert_eq!(function.signature(), inner.signature());
        assert_eq!(function.is_strict(), inner.inner().is_strict());
        let intervals = (0..arity)
            .map(|_| Interval::make_unbounded(&DataType::Float64).unwrap())
            .collect::<Vec<_>>();
        let refs = intervals.iter().collect::<Vec<_>>();
        assert_eq!(
            format!("{:?}", function.evaluate_bounds(&refs)),
            format!("{:?}", inner.evaluate_bounds(&refs))
        );
        let inputs = intervals
            .into_iter()
            .map(|range| ExprProperties {
                sort_properties: SortProperties::Ordered(arrow::compute::SortOptions::default()),
                range,
                preserves_lex_ordering: true,
                strictly_order_preserving: true,
            })
            .collect::<Vec<_>>();
        assert_eq!(
            format!("{:?}", function.output_ordering(&inputs)),
            format!("{:?}", inner.output_ordering(&inputs))
        );
        assert_eq!(
            function.preserves_lex_ordering(&inputs).unwrap(),
            inner.preserves_lex_ordering(&inputs).unwrap()
        );
        assert_eq!(
            function.strictly_order_preserving(&inputs).unwrap(),
            inner.strictly_order_preserving(&inputs).unwrap()
        );
    }
}

#[test]
fn fixed_math_rejects_large_scalar_materialization_before_allocation() {
    let broker = Arc::new(TestBroker::new(64 << 10));
    let function = AdmittedFunction {
        inner: datafusion_functions::math::sin().as_ref().clone(),
        pool: Arc::new(FlinkMemoryPool::new(broker.clone(), 64 << 10)),
        policy: Policy::FixedMath,
    };
    let mut arguments = args(
        vec![ColumnarValue::Scalar(ScalarValue::Float64(Some(0.5)))],
        65536,
    );
    arguments.return_field = Arc::new(Field::new("math", DataType::Float64, true));
    let (result, observed) = measure(|| function.invoke_with_args(arguments));
    assert!(matches!(
        result,
        Err(DataFusionError::ResourcesExhausted(_))
    ));
    assert!(observed.peak < 32 << 10, "{observed:?}");
    assert_eq!(broker.reserved(), 0);
}
