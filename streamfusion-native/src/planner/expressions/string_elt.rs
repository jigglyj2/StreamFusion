// Copyright 2026 StreamFusion Authors
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
// http://www.apache.org/licenses/LICENSE-2.0

use std::sync::Arc;

use arrow::datatypes::{DataType, Schema};
use datafusion::error::{DataFusionError, Result};
use datafusion::logical_expr::Operator;
use datafusion::physical_expr::expressions::{BinaryExpr, CaseExpr, Literal};
use datafusion::physical_expr::PhysicalExpr;
use datafusion::scalar::ScalarValue;

pub(crate) fn create(
    index: Arc<dyn PhysicalExpr>,
    values: Vec<Arc<dyn PhysicalExpr>>,
    schema: &Schema,
) -> Result<Arc<dyn PhysicalExpr>> {
    let output_type = values
        .first()
        .ok_or_else(|| DataFusionError::Plan("ELT requires at least one value".to_string()))?
        .data_type(schema)?;
    let index_type = index.data_type(schema)?;
    let mut branches = Vec::with_capacity(values.len());
    for (offset, value) in values.into_iter().enumerate() {
        if let Some(ordinal) = ordinal_literal(&index_type, offset + 1)? {
            let matches = Arc::new(BinaryExpr::new(
                Arc::clone(&index),
                Operator::Eq,
                Arc::new(Literal::new(ordinal)),
            )) as Arc<dyn PhysicalExpr>;
            branches.push((matches, value));
        }
    }
    let null_value =
        Arc::new(Literal::new(ScalarValue::try_new_null(&output_type)?)) as Arc<dyn PhysicalExpr>;
    Ok(Arc::new(CaseExpr::try_new(
        None,
        branches,
        Some(null_value),
    )?))
}

fn ordinal_literal(data_type: &DataType, ordinal: usize) -> Result<Option<ScalarValue>> {
    match data_type {
        DataType::Int8 => Ok(i8::try_from(ordinal)
            .ok()
            .map(|value| ScalarValue::Int8(Some(value)))),
        DataType::Int16 => Ok(i16::try_from(ordinal)
            .ok()
            .map(|value| ScalarValue::Int16(Some(value)))),
        DataType::Int32 => Ok(i32::try_from(ordinal)
            .ok()
            .map(|value| ScalarValue::Int32(Some(value)))),
        DataType::Int64 => Ok(i64::try_from(ordinal)
            .ok()
            .map(|value| ScalarValue::Int64(Some(value)))),
        other => Err(DataFusionError::Plan(format!(
            "ELT index type {other} is not a signed integer"
        ))),
    }
}
