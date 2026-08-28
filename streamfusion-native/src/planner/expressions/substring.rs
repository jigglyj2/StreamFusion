// Copyright 2026 StreamFusion Authors
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0

use std::sync::Arc;

use arrow::datatypes::Schema;
use datafusion::common::config::ConfigOptions;
use datafusion::error::Result;
use datafusion::physical_expr::expressions::Literal;
use datafusion::physical_expr::{PhysicalExpr, ScalarFunctionExpr};
use datafusion::scalar::ScalarValue;
use datafusion_functions::unicode::substr;

pub(crate) fn create(
    operand: Arc<dyn PhysicalExpr>,
    start: i32,
    length: Option<i32>,
    schema: &Schema,
) -> Result<Arc<dyn PhysicalExpr>> {
    let mut arguments = vec![
        operand,
        Arc::new(Literal::new(ScalarValue::Int64(Some(i64::from(start))))) as Arc<dyn PhysicalExpr>,
    ];
    if let Some(length) = length {
        arguments.push(Arc::new(Literal::new(ScalarValue::Int64(Some(i64::from(
            length,
        ))))));
    }
    Ok(Arc::new(ScalarFunctionExpr::try_new(
        substr(),
        arguments,
        schema,
        Arc::new(ConfigOptions::new()),
    )?))
}
