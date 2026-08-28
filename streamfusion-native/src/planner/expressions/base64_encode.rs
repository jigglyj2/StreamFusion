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
use datafusion::error::{DataFusionError, Result};
use datafusion::physical_expr::expressions::{CastExpr, Literal};
use datafusion::physical_expr::{PhysicalExpr, ScalarFunctionExpr};
use datafusion::scalar::ScalarValue;
use datafusion_functions::encoding::encode;

pub(crate) fn create(
    operand: Arc<dyn PhysicalExpr>,
    schema: &Schema,
) -> Result<Arc<dyn PhysicalExpr>> {
    let bytes: Arc<dyn PhysicalExpr> = match operand.data_type(schema)? {
        DataType::Utf8 => Arc::new(CastExpr::new(operand, DataType::Binary, None)),
        DataType::Binary | DataType::FixedSizeBinary(_) => operand,
        data_type => {
            return Err(DataFusionError::Plan(format!(
                "TO_BASE64 does not support Arrow type {data_type}"
            )));
        }
    };
    Ok(Arc::new(ScalarFunctionExpr::try_new(
        encode(),
        vec![
            bytes,
            Arc::new(Literal::new(ScalarValue::Utf8(Some(
                "base64pad".to_string(),
            )))),
        ],
        schema,
        Arc::new(ConfigOptions::new()),
    )?))
}
