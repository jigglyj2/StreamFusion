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
use datafusion::logical_expr::Operator;
use datafusion::physical_expr::{
    expressions::{BinaryExpr, CaseExpr, CastExpr, Literal},
    PhysicalExpr, ScalarFunctionExpr,
};

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

pub(crate) fn create_dynamic(
    array: Arc<dyn PhysicalExpr>,
    start: Arc<dyn PhysicalExpr>,
    end: Option<Arc<dyn PhysicalExpr>>,
    schema: &Schema,
) -> Result<Arc<dyn PhysicalExpr>> {
    let start = Arc::new(CastExpr::new(
        start,
        arrow::datatypes::DataType::Int64,
        None,
    )) as Arc<dyn PhysicalExpr>;
    let end = match end {
        Some(end) => normalize_end(Arc::new(CastExpr::new(
            end,
            arrow::datatypes::DataType::Int64,
            None,
        )))?,
        None => Arc::new(Literal::new(ScalarValue::Int64(Some(i64::from(i32::MAX)))))
            as Arc<dyn PhysicalExpr>,
    };
    Ok(Arc::new(ScalarFunctionExpr::try_new(
        datafusion_functions_nested::extract::array_slice_udf(),
        vec![array, start, end],
        schema,
        Arc::new(ConfigOptions::new()),
    )?))
}

fn normalize_end(end: Arc<dyn PhysicalExpr>) -> Result<Arc<dyn PhysicalExpr>> {
    let zero = Arc::new(Literal::new(ScalarValue::Int64(Some(0)))) as Arc<dyn PhysicalExpr>;
    let one = Arc::new(Literal::new(ScalarValue::Int64(Some(1)))) as Arc<dyn PhysicalExpr>;
    let is_zero =
        Arc::new(BinaryExpr::new(Arc::clone(&end), Operator::Eq, zero)) as Arc<dyn PhysicalExpr>;
    Ok(Arc::new(CaseExpr::try_new(
        None,
        vec![(is_zero, one)],
        Some(end),
    )?))
}
