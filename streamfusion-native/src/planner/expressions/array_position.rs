// Copyright 2026 StreamFusion Authors
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
// http://www.apache.org/licenses/LICENSE-2.0

use std::sync::Arc;

use arrow::datatypes::{DataType, Schema};
use datafusion::common::config::ConfigOptions;
use datafusion::error::Result;
use datafusion::logical_expr::Operator;
use datafusion::physical_expr::expressions::{BinaryExpr, CaseExpr, CastExpr, IsNullExpr, Literal};
use datafusion::physical_expr::{PhysicalExpr, ScalarFunctionExpr};
use datafusion::scalar::ScalarValue;

pub(crate) fn create(
    array: Arc<dyn PhysicalExpr>,
    needle: Arc<dyn PhysicalExpr>,
    schema: &Schema,
) -> Result<Arc<dyn PhysicalExpr>> {
    let null_input = Arc::new(BinaryExpr::new(
        Arc::new(IsNullExpr::new(Arc::clone(&array))),
        Operator::Or,
        Arc::new(IsNullExpr::new(Arc::clone(&needle))),
    )) as Arc<dyn PhysicalExpr>;
    let position = Arc::new(ScalarFunctionExpr::try_new(
        datafusion_functions_nested::position::array_position_udf(),
        vec![array, needle],
        schema,
        Arc::new(ConfigOptions::new()),
    )?) as Arc<dyn PhysicalExpr>;
    let position =
        Arc::new(CastExpr::new(position, DataType::Int32, None)) as Arc<dyn PhysicalExpr>;
    let zero = Arc::new(Literal::new(ScalarValue::Int32(Some(0)))) as Arc<dyn PhysicalExpr>;
    let found_or_zero = super::coalesce::create(vec![position, zero])?;
    let null_result = Arc::new(Literal::new(ScalarValue::Int32(None))) as Arc<dyn PhysicalExpr>;
    Ok(Arc::new(CaseExpr::try_new(
        None,
        vec![(null_input, null_result)],
        Some(found_or_zero),
    )?))
}
