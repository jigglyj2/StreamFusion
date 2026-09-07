// Copyright 2026 StreamFusion Authors
// Licensed under the Apache License, Version 2.0.

//! Content-independent capacity policy for DataFusion's floating-point math kernels.
//! Numeric broadcasting, output, validity, and scalar-to-array materialization are linear
//! in row count. Do not infer this policy merely from an arbitrary UDF's return type.

use arrow::datatypes::{DataType, Schema};
use datafusion::error::{DataFusionError, Result};
use datafusion::logical_expr::ScalarFunctionArgs;
use datafusion::physical_expr::ScalarFunctionExpr;

fn floating(kind: &DataType) -> bool {
    matches!(kind, DataType::Float32 | DataType::Float64)
}

pub(super) fn supports(function: &ScalarFunctionExpr, schema: &Schema) -> Result<bool> {
    if !matches!(
        function.name(),
        "acos"
            | "asin"
            | "atan"
            | "atan2"
            | "cos"
            | "sin"
            | "tan"
            | "sinh"
            | "cosh"
            | "tanh"
            | "exp"
            | "ln"
            | "log10"
            | "log2"
            | "power"
            | "ceil"
            | "floor"
            | "signum"
            | "degrees"
            | "radians"
    ) || !floating(function.return_type())
    {
        return Ok(false);
    }
    for argument in function.args() {
        if !floating(&argument.data_type(schema)?) {
            return Ok(false);
        }
    }
    Ok(true)
}

pub(super) fn workspace(args: &ScalarFunctionArgs) -> Result<usize> {
    if !floating(args.return_field.data_type())
        || args.args.iter().any(|arg| !floating(&arg.data_type()))
    {
        return Err(DataFusionError::Execution(
            "fixed math admission requires floating-point arguments and output".into(),
        ));
    }
    // Each operand may be scalar-broadcast. Include output, an additional working array,
    // validity/rounding, and a generous descriptor/control allowance. Existing argument
    // buffers retain their own owners; no input payload is copied by admission itself.
    args.args
        .len()
        .checked_add(2)
        .and_then(|arrays| arrays.checked_mul(32))
        .and_then(|per_row| args.number_rows.checked_mul(per_row))
        .and_then(|bytes| bytes.checked_add(16 * 1024))
        .ok_or_else(|| DataFusionError::ResourcesExhausted("fixed math admission overflow".into()))
}
