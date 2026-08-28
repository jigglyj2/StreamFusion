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
use datafusion::logical_expr::Operator;
use datafusion::physical_expr::expressions::{BinaryExpr, NotExpr};
use datafusion::physical_expr::{PhysicalExpr, ScalarFunctionExpr};
use datafusion_functions::math::isnan;

use crate::proto;

pub(crate) fn create(
    left: Arc<dyn PhysicalExpr>,
    right: Arc<dyn PhysicalExpr>,
    operator: proto::ComparisonOperator,
    schema: &Schema,
) -> Result<Arc<dyn PhysicalExpr>> {
    let datafusion_operator = match operator {
        proto::ComparisonOperator::Equal => Operator::Eq,
        proto::ComparisonOperator::NotEqual => Operator::NotEq,
        proto::ComparisonOperator::LessThan => Operator::Lt,
        proto::ComparisonOperator::LessThanOrEqual => Operator::LtEq,
        proto::ComparisonOperator::GreaterThan => Operator::Gt,
        proto::ComparisonOperator::GreaterThanOrEqual => Operator::GtEq,
        proto::ComparisonOperator::IsDistinctFrom => Operator::IsDistinctFrom,
        proto::ComparisonOperator::IsNotDistinctFrom => Operator::IsNotDistinctFrom,
        proto::ComparisonOperator::Unspecified => {
            return Err(DataFusionError::Plan(
                "comparison operator is unspecified".to_string(),
            ));
        }
    };
    let comparison: Arc<dyn PhysicalExpr> = Arc::new(BinaryExpr::new(
        Arc::clone(&left),
        datafusion_operator,
        Arc::clone(&right),
    ));
    if !matches!(
        operator,
        proto::ComparisonOperator::Equal
            | proto::ComparisonOperator::LessThan
            | proto::ComparisonOperator::LessThanOrEqual
            | proto::ComparisonOperator::GreaterThan
            | proto::ComparisonOperator::GreaterThanOrEqual
    ) || !matches!(
        left.data_type(schema)?,
        DataType::Float32 | DataType::Float64
    ) {
        return Ok(comparison);
    }

    let left_not_nan = not_nan(left, schema)?;
    let right_not_nan = not_nan(right, schema)?;
    Ok(Arc::new(BinaryExpr::new(
        Arc::new(BinaryExpr::new(left_not_nan, Operator::And, right_not_nan)),
        Operator::And,
        comparison,
    )))
}

fn not_nan(operand: Arc<dyn PhysicalExpr>, schema: &Schema) -> Result<Arc<dyn PhysicalExpr>> {
    let is_nan: Arc<dyn PhysicalExpr> = Arc::new(ScalarFunctionExpr::try_new(
        isnan(),
        vec![operand],
        schema,
        Arc::new(ConfigOptions::new()),
    )?);
    Ok(Arc::new(NotExpr::new(is_nan)))
}
