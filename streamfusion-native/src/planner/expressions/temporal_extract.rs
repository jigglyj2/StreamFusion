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
use datafusion::physical_expr::expressions::{BinaryExpr, CastExpr, Literal};
use datafusion::physical_expr::{PhysicalExpr, ScalarFunctionExpr};
use datafusion::scalar::ScalarValue;

pub(crate) fn create(
    operand: Arc<dyn PhysicalExpr>,
    field: &str,
    result_is_bigint: bool,
    schema: &Schema,
) -> Result<Arc<dyn PhysicalExpr>> {
    if operand.data_type(schema)? != DataType::Date32 {
        return Err(DataFusionError::Plan(
            "calendar EXTRACT currently requires Arrow Date32 input".to_string(),
        ));
    }
    let datafusion_field = if field == "flink_dow" { "dow" } else { field };
    let mut extracted = Arc::new(ScalarFunctionExpr::try_new(
        datafusion_functions::datetime::date_part(),
        vec![
            Arc::new(Literal::new(ScalarValue::Utf8(Some(
                datafusion_field.to_string(),
            )))),
            operand,
        ],
        schema,
        Arc::new(ConfigOptions::new()),
    )?) as Arc<dyn PhysicalExpr>;
    if field == "flink_dow" {
        extracted = Arc::new(BinaryExpr::new(
            extracted,
            Operator::Plus,
            Arc::new(Literal::new(ScalarValue::Int32(Some(1)))),
        ));
    }
    if result_is_bigint {
        Ok(Arc::new(CastExpr::new(extracted, DataType::Int64, None)))
    } else {
        Ok(extracted)
    }
}
