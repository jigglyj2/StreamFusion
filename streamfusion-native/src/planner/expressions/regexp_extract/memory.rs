// Copyright 2026 StreamFusion Authors. Licensed under the Apache License, Version 2.0.
use arrow::array::StringArray;
use datafusion::error::{DataFusionError, Result};
use datafusion::logical_expr::{ColumnarValue, ScalarFunctionArgs};
use datafusion::scalar::ScalarValue;

pub(in crate::planner::expressions) fn workspace(args: &ScalarFunctionArgs) -> Result<usize> {
    if args.number_rows == 0 {
        return Ok(64 << 10);
    }
    let payload = match args.args.as_slice() {
        [ColumnarValue::Scalar(ScalarValue::Utf8(value))] => value
            .as_ref()
            .map_or(0, String::len)
            .checked_mul(args.number_rows)
            .ok_or_else(overflow)?,
        [ColumnarValue::Array(array)] => {
            let strings = array
                .as_any()
                .downcast_ref::<StringArray>()
                .ok_or_else(|| {
                    DataFusionError::Execution("REGEXP_EXTRACT requires Utf8 values".into())
                })?;
            let offsets = strings.value_offsets();
            usize::try_from(offsets[offsets.len() - 1] - offsets[0]).map_err(|_| overflow())?
        }
        _ => {
            return Err(DataFusionError::Execution(
                "REGEXP_EXTRACT requires one Utf8 argument".into(),
            ))
        }
    };
    // A sole capture cannot exceed its input. Cover list/string builders, their capacity
    // growth, element gathering and scalar broadcast together at this batch boundary.
    // This restricted grammar has bounded compilation work (no counted or group
    // repetition). Reserve 4 MiB compilation headroom plus two 2 MiB regex DFA caches.
    // Boundary-pattern allocation tests cover maximal literals/classes/alternations;
    // the unrestricted regex compiler's 10 MiB rejection limit is not an allocation.
    // Keep this allowance independent of input size and reserve growing payloads above.
    payload
        .checked_mul(6)
        .and_then(|n| args.number_rows.checked_mul(128)?.checked_add(n))
        .and_then(|n| n.checked_add(8 << 20))
        .ok_or_else(overflow)
}
fn overflow() -> DataFusionError {
    DataFusionError::ResourcesExhausted("REGEXP_EXTRACT workspace overflow".into())
}
