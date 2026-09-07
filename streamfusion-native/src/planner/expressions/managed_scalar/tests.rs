// Copyright 2026 StreamFusion Authors
// Licensed under the Apache License, Version 2.0.

use super::*;
use crate::allocation_test_support::measure;
use crate::memory_pool::{tests_support::TestBroker, FlinkMemoryPool, MemoryReservationBroker};
use arrow::array::{Array, Int64Array, StringArray};
use arrow::datatypes::Field;
use datafusion::common::config::ConfigOptions;
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

pub(super) fn args(values: Vec<ColumnarValue>, rows: usize) -> ScalarFunctionArgs {
    ScalarFunctionArgs {
        arg_fields: values
            .iter()
            .map(|v| Arc::new(Field::new("arg", v.data_type(), true)))
            .collect(),
        args: values,
        number_rows: rows,
        return_field: Arc::new(Field::new("repeat", DataType::Utf8, true)),
        config_options: Arc::new(ConfigOptions::default()),
    }
}

#[test]
fn rejects_repeat_growth_before_the_datafusion_kernel_allocates() {
    let broker = Arc::new(TestBroker::new(1 << 20));
    let function = AdmittedFunction {
        policy: Policy::Repeat,
        inner: datafusion_functions::string::repeat().as_ref().clone(),
        pool: Arc::new(FlinkMemoryPool::new(broker.clone(), 1 << 20)),
    };
    let arguments = args(
        vec![
            ColumnarValue::Scalar(ScalarValue::Utf8(Some("x".repeat(1024)))),
            ColumnarValue::Scalar(ScalarValue::Int64(Some(4096))),
        ],
        4,
    );
    let (result, observed) = measure(|| function.invoke_with_args(arguments));
    assert!(matches!(
        result,
        Err(DataFusionError::ResourcesExhausted(_))
    ));
    assert!(
        observed.peak < 64 * 1024,
        "must deny before constructing the 16 MiB result: {observed:?}"
    );
    assert_eq!(broker.reserved(), 0);
}

#[test]
fn repeat_keeps_datafusion_values_and_charges_output_slices_until_last_release() {
    for scalar_string in [false, true] {
        for scalar_count in [false, true] {
            let broker = Arc::new(PeakBroker {
                inner: TestBroker::new(64 << 20),
                peak: AtomicUsize::new(0),
            });
            let function = AdmittedFunction {
                policy: Policy::Repeat,
                inner: datafusion_functions::string::repeat().as_ref().clone(),
                pool: Arc::new(FlinkMemoryPool::new(broker.clone(), 64 << 20)),
            };
            let string = if scalar_string {
                ColumnarValue::Scalar(ScalarValue::Utf8(Some("é".repeat(1024))))
            } else {
                ColumnarValue::Array(Arc::new(StringArray::from(vec![
                    Some("wide-value"),
                    None,
                    Some("é"),
                    Some(""),
                ])))
            };
            let count = if scalar_count {
                ColumnarValue::Scalar(ScalarValue::Int64(Some(64)))
            } else {
                ColumnarValue::Array(Arc::new(Int64Array::from(vec![
                    Some(64),
                    Some(-1),
                    Some(0),
                    None,
                ])))
            };
            let expected = function
                .inner
                .invoke_with_args(args(vec![string.clone(), count.clone()], 4))
                .unwrap()
                .into_array(4)
                .unwrap();
            let arguments = args(vec![string, count], 4);
            let (output, observed) = measure(|| {
                function
                    .invoke_with_args(arguments)
                    .unwrap()
                    .into_array(4)
                    .unwrap()
            });
            assert_eq!(output.to_data(), expected.to_data());
            assert!(
                observed.peak <= broker.peak.load(Ordering::Relaxed),
                "{observed:?}"
            );
            assert!(
                observed.live <= broker.inner.reserved() as isize,
                "{observed:?}"
            );
            let slice = output.slice(1, 2);
            let source_buffers = output.to_data();
            let slice_buffers = slice.to_data();
            // The value buffer is shared through leases, including sliced output.
            assert_eq!(
                source_buffers.buffers()[1].as_ptr(),
                slice_buffers.buffers()[1].as_ptr()
            );
            drop(source_buffers);
            drop(slice_buffers);
            drop(output);
            drop(function);
            assert!(broker.inner.reserved() > 0);
            drop(slice);
            assert_eq!(broker.inner.reserved(), 0);
        }
    }
}

#[test]
fn empty_repeat_batch_does_not_construct_a_scalar_payload() {
    let broker = Arc::new(TestBroker::new(1 << 20));
    let function = AdmittedFunction {
        policy: Policy::Repeat,
        inner: datafusion_functions::string::repeat().as_ref().clone(),
        pool: Arc::new(FlinkMemoryPool::new(broker.clone(), 1 << 20)),
    };
    let output = function
        .invoke_with_args(args(
            vec![
                ColumnarValue::Scalar(ScalarValue::Utf8(Some("x".into()))),
                ColumnarValue::Scalar(ScalarValue::Int64(Some(i64::MAX))),
            ],
            0,
        ))
        .unwrap()
        .into_array(0)
        .unwrap();
    assert_eq!(output.len(), 0);
    drop(output);
    assert_eq!(broker.reserved(), 0);
}
