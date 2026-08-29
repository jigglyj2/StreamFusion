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
    separator: Arc<dyn PhysicalExpr>,
    values: Vec<Arc<dyn PhysicalExpr>>,
    schema: &Schema,
) -> Result<Arc<dyn PhysicalExpr>> {
    if values.is_empty() {
        return Err(DataFusionError::Plan(
            "CONCAT_WS requires at least one value".to_string(),
        ));
    }
    let mut arguments = Vec::with_capacity(values.len() + 1);
    arguments.push(separator);
    arguments.extend(values);
    Ok(Arc::new(ScalarFunctionExpr::try_new(
        datafusion_functions::string::concat_ws(),
        arguments,
        schema,
        Arc::new(ConfigOptions::new()),
    )?))
}
