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
use datafusion::physical_expr::{PhysicalExpr, ScalarFunctionExpr};

pub(crate) fn create(
    elements: Vec<Arc<dyn PhysicalExpr>>,
    schema: &Schema,
) -> Result<Arc<dyn PhysicalExpr>> {
    if elements.is_empty() {
        return Err(DataFusionError::Plan(
            "empty array constructors require an explicit element type".into(),
        ));
    }
    Ok(Arc::new(ScalarFunctionExpr::try_new(
        datafusion_functions_nested::make_array::make_array_udf(),
        elements,
        schema,
        Arc::new(ConfigOptions::new()),
    )?))
}
