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
use datafusion::error::{DataFusionError, Result};
use datafusion::logical_expr::Operator;
use datafusion::physical_expr::expressions::{BinaryExpr, CaseExpr, IsNotNullExpr, Literal};
use datafusion::physical_expr::{PhysicalExpr, ScalarFunctionExpr};
use datafusion::scalar::ScalarValue;
use datafusion_functions::string::concat;

pub(crate) fn create(
    arguments: Vec<Arc<dyn PhysicalExpr>>,
    schema: &Schema,
) -> Result<Arc<dyn PhysicalExpr>> {
    if arguments.len() < 2 {
        return Err(DataFusionError::Plan(
            "CONCAT requires at least two arguments".to_string(),
        ));
    }
    let all_present = arguments
        .iter()
        .map(|argument| Arc::new(IsNotNullExpr::new(Arc::clone(argument))) as Arc<dyn PhysicalExpr>)
        .reduce(|left, right| {
            Arc::new(BinaryExpr::new(left, Operator::And, right)) as Arc<dyn PhysicalExpr>
        })
        .expect("CONCAT argument count was checked");
    let concatenated = Arc::new(ScalarFunctionExpr::try_new(
        concat(),
        arguments,
        schema,
        Arc::new(ConfigOptions::new()),
    )?) as Arc<dyn PhysicalExpr>;
    Ok(Arc::new(CaseExpr::try_new(
        None,
        vec![(all_present, concatenated)],
        Some(Arc::new(Literal::new(ScalarValue::Utf8(None)))),
    )?))
}
