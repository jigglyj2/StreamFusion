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
use datafusion::error::Result;
use datafusion::physical_expr::expressions::{CaseExpr, IsNullExpr, Literal};
use datafusion::physical_expr::{PhysicalExpr, ScalarFunctionExpr};
use datafusion::scalar::ScalarValue;

pub(crate) fn create(
    array: Arc<dyn PhysicalExpr>,
    element: Arc<dyn PhysicalExpr>,
    schema: &Schema,
) -> Result<Arc<dyn PhysicalExpr>> {
    let array_is_null = Arc::new(IsNullExpr::new(Arc::clone(&array))) as Arc<dyn PhysicalExpr>;
    let prepended = Arc::new(ScalarFunctionExpr::try_new(
        datafusion_functions_nested::concat::array_prepend_udf(),
        vec![element, array],
        schema,
        Arc::new(ConfigOptions::new()),
    )?) as Arc<dyn PhysicalExpr>;
    let null_array = Arc::new(Literal::new(ScalarValue::try_new_null(
        &prepended.data_type(schema)?,
    )?)) as Arc<dyn PhysicalExpr>;
    Ok(Arc::new(CaseExpr::try_new(
        None,
        vec![(array_is_null, null_array)],
        Some(prepended),
    )?))
}
