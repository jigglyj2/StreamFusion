// Copyright 2026 StreamFusion Authors. Licensed under the Apache License, Version 2.0.
use arrow::array::StringArray;
use datafusion::error::{DataFusionError, Result};
use datafusion::logical_expr::{ColumnarValue, ScalarFunctionArgs};
use datafusion::scalar::ScalarValue;

pub(in crate::planner::expressions) fn workspace(args: &ScalarFunctionArgs) -> Result<usize> {
    if args.number_rows == 0 {
        return Ok(64 << 10);
    }
    let payload = match args.args.first() {
        Some(ColumnarValue::Scalar(ScalarValue::Utf8(value))) => value
            .as_ref()
            .map_or(0, String::len)
            .checked_mul(args.number_rows)
            .ok_or_else(overflow)?,
        Some(ColumnarValue::Array(array)) => {
            let strings = array
                .as_any()
                .downcast_ref::<StringArray>()
                .ok_or_else(|| {
                    DataFusionError::Execution("SPLIT_INDEX requires Utf8 values".into())
                })?;
            let offsets = strings.value_offsets();
            usize::try_from(offsets[offsets.len() - 1] - offsets[0]).map_err(|_| overflow())?
        }
        _ => {
            return Err(DataFusionError::Execution(
                "SPLIT_INDEX requires Utf8 values".into(),
            ))
        }
    };
    // With a nonempty delimiter there are at most input_bytes + rows tokens. Cover
    // the list/string builders (including capacity growth), CASE selection/gather,
    // output extraction, and scalar broadcasts before invoking either DF kernel.
    payload
        .checked_mul(24)
        .and_then(|n| args.number_rows.checked_mul(256)?.checked_add(n))
        .and_then(|n| n.checked_add(64 << 10))
        .ok_or_else(overflow)
}
fn overflow() -> DataFusionError {
    DataFusionError::ResourcesExhausted("SPLIT_INDEX workspace overflow".into())
}
