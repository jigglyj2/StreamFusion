// Copyright 2026 StreamFusion Authors
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
// http://www.apache.org/licenses/LICENSE-2.0

use std::sync::Arc;

use arrow::datatypes::Schema;
use datafusion::common::{config::ConfigOptions, ScalarValue};
use datafusion::error::Result;
use datafusion::physical_expr::{expressions::Literal, PhysicalExpr, ScalarFunctionExpr};

pub(crate) fn create(
    array: Arc<dyn PhysicalExpr>,
    start: i64,
    end: Option<i64>,
    schema: &Schema,
) -> Result<Arc<dyn PhysicalExpr>> {
    // Flink arrays and Arrow List offsets are signed 32-bit. DataFusion rejects
    // indices above that domain before clamping, so this is the representable
    // unbounded end rather than i64::MAX.
    let end = match end {
        // Flink normalizes an explicit zero end to the first element before
        // comparing it with the normalized start. DataFusion otherwise treats
        // zero as an empty exclusive boundary.
        Some(0) => 1,
        Some(end) => end,
        None => i64::from(i32::MAX),
    };
    Ok(Arc::new(ScalarFunctionExpr::try_new(
        datafusion_functions_nested::extract::array_slice_udf(),
        vec![
            array,
            Arc::new(Literal::new(ScalarValue::Int64(Some(start)))),
            Arc::new(Literal::new(ScalarValue::Int64(Some(end)))),
        ],
        schema,
        Arc::new(ConfigOptions::new()),
    )?))
}
