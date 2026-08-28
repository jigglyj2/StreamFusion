// Copyright 2026 StreamFusion Authors
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
// http://www.apache.org/licenses/LICENSE-2.0

use std::sync::Arc;

use arrow::datatypes::Schema;
use datafusion::common::config::ConfigOptions;
use datafusion::error::{DataFusionError, Result};
use datafusion::logical_expr::Operator;
use datafusion::physical_expr::expressions::{BinaryExpr, CaseExpr, IsNullExpr, Literal};
use datafusion::physical_expr::{PhysicalExpr, ScalarFunctionExpr};
use datafusion::scalar::ScalarValue;

pub(crate) fn create(
    arrays: Vec<Arc<dyn PhysicalExpr>>,
    schema: &Schema,
) -> Result<Arc<dyn PhysicalExpr>> {
    if arrays.len() < 2 {
        return Err(DataFusionError::Plan(
            "array concat requires at least two arrays".into(),
        ));
    }
    let any_null = arrays
        .iter()
        .map(|array| Arc::new(IsNullExpr::new(Arc::clone(array))) as Arc<dyn PhysicalExpr>)
        .reduce(|left, right| {
            Arc::new(BinaryExpr::new(left, Operator::Or, right)) as Arc<dyn PhysicalExpr>
        })
        .expect("at least two arrays were checked");
    let concatenated = Arc::new(ScalarFunctionExpr::try_new(
        datafusion_functions_nested::concat::array_concat_udf(),
        arrays,
        schema,
        Arc::new(ConfigOptions::new()),
    )?) as Arc<dyn PhysicalExpr>;
    let null_array = Arc::new(Literal::new(ScalarValue::try_new_null(
        &concatenated.data_type(schema)?,
    )?)) as Arc<dyn PhysicalExpr>;
    Ok(Arc::new(CaseExpr::try_new(
        None,
        vec![(any_null, null_array)],
        Some(concatenated),
    )?))
}
