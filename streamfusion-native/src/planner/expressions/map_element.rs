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
use datafusion::physical_expr::expressions::Literal;
use datafusion::physical_expr::{PhysicalExpr, ScalarFunctionExpr};
use datafusion::scalar::ScalarValue;

pub(crate) fn create(
    map: Arc<dyn PhysicalExpr>,
    key: Arc<dyn PhysicalExpr>,
    schema: &Schema,
) -> Result<Arc<dyn PhysicalExpr>> {
    let options = Arc::new(ConfigOptions::new());
    let values = Arc::new(ScalarFunctionExpr::try_new(
        datafusion_functions_nested::map_extract::map_extract_udf(),
        vec![map, key],
        schema,
        Arc::clone(&options),
    )?) as Arc<dyn PhysicalExpr>;
    let first = Arc::new(Literal::new(ScalarValue::Int64(Some(1)))) as Arc<dyn PhysicalExpr>;
    Ok(Arc::new(ScalarFunctionExpr::try_new(
        datafusion_functions_nested::extract::array_element_udf(),
        vec![values, first],
        schema,
        options,
    )?))
}
