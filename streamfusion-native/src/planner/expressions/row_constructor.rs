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
use datafusion::error::{DataFusionError, Result};
use datafusion::physical_expr::{expressions::Literal, PhysicalExpr, ScalarFunctionExpr};

pub(crate) fn create(
    fields: Vec<(String, Arc<dyn PhysicalExpr>)>,
    schema: &Schema,
) -> Result<Arc<dyn PhysicalExpr>> {
    if fields.is_empty() {
        return Err(DataFusionError::Plan(
            "row constructors require at least one field".into(),
        ));
    }
    let mut arguments = Vec::with_capacity(fields.len() * 2);
    for (name, value) in fields {
        arguments
            .push(Arc::new(Literal::new(ScalarValue::Utf8(Some(name)))) as Arc<dyn PhysicalExpr>);
        arguments.push(value);
    }
    Ok(Arc::new(ScalarFunctionExpr::try_new(
        datafusion_functions::core::named_struct(),
        arguments,
        schema,
        Arc::new(ConfigOptions::new()),
    )?))
}
