// Copyright 2026 StreamFusion Authors
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0

use std::sync::Arc;

use arrow::datatypes::{DataType, Schema};
use datafusion::common::config::ConfigOptions;
use datafusion::error::{DataFusionError, Result};
use datafusion::logical_expr::Operator;
use datafusion::physical_expr::expressions::{BinaryExpr, CaseExpr, IsNullExpr, Literal};
use datafusion::physical_expr::{PhysicalExpr, ScalarFunctionExpr};
use datafusion::scalar::ScalarValue;

pub(crate) fn create(
    arguments: Vec<Arc<dyn PhysicalExpr>>,
    greatest: bool,
    schema: &Schema,
) -> Result<Arc<dyn PhysicalExpr>> {
    let first = arguments.first().ok_or_else(|| {
        DataFusionError::Plan("GREATEST/LEAST requires at least one argument".to_string())
    })?;
    let data_type = first.data_type(schema)?;
    if !matches!(
        data_type,
        DataType::Int8 | DataType::Int16 | DataType::Int32 | DataType::Int64 | DataType::Date32
    ) {
        return Err(DataFusionError::Plan(
            "GREATEST/LEAST requires one parity-approved type for every argument".to_string(),
        ));
    }
    for argument in &arguments {
        if argument.data_type(schema)? != data_type {
            return Err(DataFusionError::Plan(
                "GREATEST/LEAST requires one parity-approved type for every argument".to_string(),
            ));
        }
    }

    let function = if greatest {
        datafusion_functions::core::greatest()
    } else {
        datafusion_functions::core::least()
    };
    let extremum = Arc::new(ScalarFunctionExpr::try_new(
        function,
        arguments.clone(),
        schema,
        Arc::new(ConfigOptions::new()),
    )?) as Arc<dyn PhysicalExpr>;
    let any_null = arguments
        .iter()
        .cloned()
        .map(|argument| Arc::new(IsNullExpr::new(argument)) as Arc<dyn PhysicalExpr>)
        .reduce(|left, right| Arc::new(BinaryExpr::new(left, Operator::Or, right)));
    let Some(any_null) = any_null else {
        return Err(DataFusionError::Plan(
            "GREATEST/LEAST requires at least one argument".to_string(),
        ));
    };
    let null = Arc::new(Literal::new(ScalarValue::try_from(&data_type)?)) as Arc<dyn PhysicalExpr>;
    Ok(Arc::new(CaseExpr::try_new(
        None,
        vec![(any_null, null)],
        Some(extremum),
    )?))
}
