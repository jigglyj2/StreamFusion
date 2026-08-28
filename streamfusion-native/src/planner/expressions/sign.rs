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
use datafusion::physical_expr::expressions::{BinaryExpr, CaseExpr, Literal};
use datafusion::physical_expr::{PhysicalExpr, ScalarFunctionExpr};
use datafusion::scalar::ScalarValue;
use datafusion_functions::math::signum;

pub(crate) fn create(
    operand: Arc<dyn PhysicalExpr>,
    schema: &Schema,
) -> Result<Arc<dyn PhysicalExpr>> {
    let (negative_one, zero, positive_one) = constants(&operand.data_type(schema)?)?;
    if matches!(
        operand.data_type(schema)?,
        DataType::Float32 | DataType::Float64
    ) {
        let is_zero = Arc::new(BinaryExpr::new(
            Arc::clone(&operand),
            Operator::Eq,
            Arc::new(Literal::new(zero)),
        )) as Arc<dyn PhysicalExpr>;
        let nonzero_sign = Arc::new(ScalarFunctionExpr::try_new(
            signum(),
            vec![Arc::clone(&operand)],
            schema,
            Arc::new(ConfigOptions::new()),
        )?) as Arc<dyn PhysicalExpr>;
        return Ok(Arc::new(CaseExpr::try_new(
            None,
            vec![(is_zero, operand)],
            Some(nonzero_sign),
        )?));
    }
    let greater_than_zero = Arc::new(BinaryExpr::new(
        Arc::clone(&operand),
        Operator::Gt,
        Arc::new(Literal::new(zero.clone())),
    )) as Arc<dyn PhysicalExpr>;
    let less_than_zero = Arc::new(BinaryExpr::new(
        Arc::clone(&operand),
        Operator::Lt,
        Arc::new(Literal::new(zero)),
    )) as Arc<dyn PhysicalExpr>;
    Ok(Arc::new(CaseExpr::try_new(
        None,
        vec![
            (greater_than_zero, Arc::new(Literal::new(positive_one))),
            (less_than_zero, Arc::new(Literal::new(negative_one))),
        ],
        Some(operand),
    )?))
}

fn constants(data_type: &DataType) -> Result<(ScalarValue, ScalarValue, ScalarValue)> {
    let values = match data_type {
        DataType::Int8 => (
            ScalarValue::Int8(Some(-1)),
            ScalarValue::Int8(Some(0)),
            ScalarValue::Int8(Some(1)),
        ),
        DataType::Int16 => (
            ScalarValue::Int16(Some(-1)),
            ScalarValue::Int16(Some(0)),
            ScalarValue::Int16(Some(1)),
        ),
        DataType::Int32 => (
            ScalarValue::Int32(Some(-1)),
            ScalarValue::Int32(Some(0)),
            ScalarValue::Int32(Some(1)),
        ),
        DataType::Int64 => (
            ScalarValue::Int64(Some(-1)),
            ScalarValue::Int64(Some(0)),
            ScalarValue::Int64(Some(1)),
        ),
        DataType::Float32 => (
            ScalarValue::Float32(Some(-1.0)),
            ScalarValue::Float32(Some(0.0)),
            ScalarValue::Float32(Some(1.0)),
        ),
        DataType::Float64 => (
            ScalarValue::Float64(Some(-1.0)),
            ScalarValue::Float64(Some(0.0)),
            ScalarValue::Float64(Some(1.0)),
        ),
        DataType::Decimal128(precision, scale) => {
            let one = 10_i128
                .checked_pow((*scale).try_into().map_err(|_| {
                    DataFusionError::Plan(format!(
                        "SIGN does not support negative decimal scale {scale}"
                    ))
                })?)
                .ok_or_else(|| {
                    DataFusionError::Plan(format!("SIGN decimal scale {scale} exceeds Decimal128"))
                })?;
            (
                ScalarValue::Decimal128(Some(-one), *precision, *scale),
                ScalarValue::Decimal128(Some(0), *precision, *scale),
                ScalarValue::Decimal128(Some(one), *precision, *scale),
            )
        }
        other => {
            return Err(DataFusionError::Plan(format!(
                "SIGN does not support Arrow type {other}"
            )))
        }
    };
    Ok(values)
}
