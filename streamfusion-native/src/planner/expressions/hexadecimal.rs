// Copyright 2026 StreamFusion Authors
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
// http://www.apache.org/licenses/LICENSE-2.0

use std::sync::Arc;

use arrow::datatypes::{DataType, Schema};
use datafusion::common::config::ConfigOptions;
use datafusion::common::ScalarValue;
use datafusion::error::{DataFusionError, Result};
use datafusion::physical_expr::expressions::{CastExpr, Literal};
use datafusion::physical_expr::{PhysicalExpr, ScalarFunctionExpr};
use datafusion_functions::encoding::encode;
use datafusion_functions::string::{to_hex, upper};

pub(crate) fn create(
    operand: Arc<dyn PhysicalExpr>,
    schema: &Schema,
) -> Result<Arc<dyn PhysicalExpr>> {
    let encoded = match operand.data_type(schema)? {
        DataType::Int8 | DataType::Int16 | DataType::Int32 | DataType::Int64 => {
            let widened: Arc<dyn PhysicalExpr> =
                Arc::new(CastExpr::new(operand, DataType::Int64, None));
            scalar(to_hex(), vec![widened], schema)?
        }
        DataType::Utf8 => {
            let bytes: Arc<dyn PhysicalExpr> =
                Arc::new(CastExpr::new(operand, DataType::Binary, None));
            scalar(
                encode(),
                vec![
                    bytes,
                    Arc::new(Literal::new(ScalarValue::Utf8(Some("hex".to_string())))),
                ],
                schema,
            )?
        }
        data_type => {
            return Err(DataFusionError::Plan(format!(
                "HEX does not support Arrow type {data_type}"
            )));
        }
    };
    scalar(upper(), vec![encoded], schema)
}

fn scalar(
    function: Arc<datafusion::logical_expr::ScalarUDF>,
    arguments: Vec<Arc<dyn PhysicalExpr>>,
    schema: &Schema,
) -> Result<Arc<dyn PhysicalExpr>> {
    Ok(Arc::new(ScalarFunctionExpr::try_new(
        function,
        arguments,
        schema,
        Arc::new(ConfigOptions::new()),
    )?))
}
