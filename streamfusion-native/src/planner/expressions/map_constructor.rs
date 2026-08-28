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

use super::array_constructor;

pub(crate) fn create(
    keys: Vec<Arc<dyn PhysicalExpr>>,
    values: Vec<Arc<dyn PhysicalExpr>>,
    schema: &Schema,
) -> Result<Arc<dyn PhysicalExpr>> {
    if keys.is_empty() || keys.len() != values.len() {
        return Err(DataFusionError::Plan(
            "map constructors require equally sized nonempty key and value lists".into(),
        ));
    }
    Ok(Arc::new(ScalarFunctionExpr::try_new(
        datafusion_functions_nested::map::map_udf(),
        vec![
            array_constructor::create(keys, schema)?,
            array_constructor::create(values, schema)?,
        ],
        schema,
        Arc::new(ConfigOptions::new()),
    )?))
}
