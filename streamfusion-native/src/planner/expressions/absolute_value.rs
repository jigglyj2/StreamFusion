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
use datafusion::error::Result;
use datafusion::logical_expr::Operator;
use datafusion::physical_expr::expressions::{BinaryExpr, CaseExpr, Literal, NegativeExpr};
use datafusion::physical_expr::PhysicalExpr;
use datafusion::physical_expr::ScalarFunctionExpr;
use datafusion::scalar::ScalarValue;
use datafusion_functions::math::abs;

pub(crate) fn create(
    operand: Arc<dyn PhysicalExpr>,
    schema: &Schema,
) -> Result<Arc<dyn PhysicalExpr>> {
    let zero = match operand.data_type(schema)? {
        DataType::Int8 => Some(ScalarValue::Int8(Some(0))),
        DataType::Int16 => Some(ScalarValue::Int16(Some(0))),
        DataType::Int32 => Some(ScalarValue::Int32(Some(0))),
        DataType::Int64 => Some(ScalarValue::Int64(Some(0))),
        _ => None,
    };
    if let Some(zero) = zero {
        let is_negative = Arc::new(BinaryExpr::new(
            Arc::clone(&operand),
            Operator::Lt,
            Arc::new(Literal::new(zero)),
        )) as Arc<dyn PhysicalExpr>;
        let magnitude = Arc::new(NegativeExpr::new(Arc::clone(&operand))) as Arc<dyn PhysicalExpr>;
        return Ok(Arc::new(CaseExpr::try_new(
            None,
            vec![(is_negative, magnitude)],
            Some(operand),
        )?));
    }
    Ok(Arc::new(ScalarFunctionExpr::try_new(
        abs(),
        vec![operand],
        schema,
        Arc::new(ConfigOptions::new()),
    )?))
}
