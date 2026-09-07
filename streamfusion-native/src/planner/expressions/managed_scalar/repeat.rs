// Copyright 2026 StreamFusion Authors
// Licensed under the Apache License, Version 2.0.

use arrow::array::{Array, Int64Array, LargeStringArray, StringArray, StringViewArray};
use datafusion::error::{DataFusionError, Result};
use datafusion::logical_expr::{ColumnarValue, ScalarFunctionArgs};
use datafusion::scalar::ScalarValue;

pub(super) fn workspace(args: &ScalarFunctionArgs) -> Result<usize> {
    if args.args.len() != 2 {
        return Err(DataFusionError::Internal(
            "REPEAT needs two arguments".into(),
        ));
    }
    let mut payload = 0usize;
    let mut broadcasts = 0usize;
    for row in 0..args.number_rows {
        let length = string_len(&args.args[0], row)?;
        let count = count(&args.args[1], row)?;
        if let (Some(length), Some(count)) = (length, count) {
            let count = usize::try_from(count.max(0)).map_err(|_| overflow())?;
            payload = payload
                .checked_add(length.checked_mul(count).ok_or_else(overflow)?)
                .ok_or_else(overflow)?;
        }
        if matches!(args.args[0], ColumnarValue::Scalar(_)) {
            broadcasts = broadcasts
                .checked_add(length.unwrap_or(0))
                .ok_or_else(overflow)?;
        }
    }
    // DataFusion repeat preallocates output and a largest-item scratch buffer; mixed
    // scalar/array arguments may be broadcast, and scalar output must be materialized.
    // Include builder capacity, offsets, validity, descriptors, and scalar count broadcast.
    payload
        .checked_mul(4)
        .and_then(|n| broadcasts.checked_mul(2)?.checked_add(n))
        .and_then(|n| args.number_rows.checked_mul(128)?.checked_add(n))
        .and_then(|n| n.checked_add(64 * 1024))
        .ok_or_else(overflow)
}

fn string_len(value: &ColumnarValue, row: usize) -> Result<Option<usize>> {
    match value {
        ColumnarValue::Scalar(
            ScalarValue::Utf8(value) | ScalarValue::LargeUtf8(value) | ScalarValue::Utf8View(value),
        ) => Ok(value.as_ref().map(String::len)),
        ColumnarValue::Array(array) => {
            if row >= array.len() {
                return Err(DataFusionError::Execution(
                    "REPEAT string row outside batch".into(),
                ));
            }
            if array.is_null(row) {
                return Ok(None);
            }
            if let Some(array) = array.as_any().downcast_ref::<StringArray>() {
                return Ok(Some(array.value(row).len()));
            }
            if let Some(array) = array.as_any().downcast_ref::<LargeStringArray>() {
                return Ok(Some(array.value(row).len()));
            }
            if let Some(array) = array.as_any().downcast_ref::<StringViewArray>() {
                return Ok(Some(array.value(row).len()));
            }
            Err(DataFusionError::Execution(
                "REPEAT needs Arrow string values".into(),
            ))
        }
        _ => Err(DataFusionError::Execution(
            "REPEAT needs string values".into(),
        )),
    }
}
fn count(value: &ColumnarValue, row: usize) -> Result<Option<i64>> {
    match value {
        ColumnarValue::Scalar(ScalarValue::Int64(value)) => Ok(*value),
        ColumnarValue::Array(array) => {
            let array = array
                .as_any()
                .downcast_ref::<Int64Array>()
                .ok_or_else(|| DataFusionError::Execution("REPEAT count must be Int64".into()))?;
            if row >= array.len() {
                return Err(DataFusionError::Execution(
                    "REPEAT count row outside batch".into(),
                ));
            }
            Ok((!array.is_null(row)).then(|| array.value(row)))
        }
        _ => Err(DataFusionError::Execution(
            "REPEAT count must be Int64".into(),
        )),
    }
}
fn overflow() -> DataFusionError {
    DataFusionError::ResourcesExhausted("REPEAT output admission overflow".into())
}
