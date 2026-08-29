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
use datafusion::physical_expr::expressions::{BinaryExpr, CaseExpr, CastExpr, Literal};
use datafusion::physical_expr::{PhysicalExpr, ScalarFunctionExpr};
use datafusion::scalar::ScalarValue;

pub(crate) fn create(
    value: Arc<dyn PhysicalExpr>,
    delimiter: &str,
    index: Arc<dyn PhysicalExpr>,
    schema: &Schema,
) -> Result<Arc<dyn PhysicalExpr>> {
    if delimiter.is_empty() {
        return Err(DataFusionError::Plan(
            "SPLIT_INDEX delimiter must be nonempty".to_string(),
        ));
    }
    let index = Arc::new(CastExpr::new(index, DataType::Int64, None)) as Arc<dyn PhysicalExpr>;
    let nonnegative = Arc::new(BinaryExpr::new(
        Arc::clone(&index),
        Operator::GtEq,
        Arc::new(Literal::new(ScalarValue::Int64(Some(0)))),
    )) as Arc<dyn PhysicalExpr>;
    let nonempty = Arc::new(BinaryExpr::new(
        Arc::clone(&value),
        Operator::NotEq,
        Arc::new(Literal::new(ScalarValue::Utf8(Some(String::new())))),
    )) as Arc<dyn PhysicalExpr>;
    let eligible =
        Arc::new(BinaryExpr::new(nonnegative, Operator::And, nonempty)) as Arc<dyn PhysicalExpr>;
    let one_based = Arc::new(BinaryExpr::new(
        index,
        Operator::Plus,
        Arc::new(Literal::new(ScalarValue::Int64(Some(1)))),
    )) as Arc<dyn PhysicalExpr>;
    let parts = super::split::create(
        Arc::clone(&value),
        Arc::new(Literal::new(ScalarValue::Utf8(Some(delimiter.to_string())))),
        schema,
    )?;
    let selected = Arc::new(ScalarFunctionExpr::try_new(
        datafusion_functions_nested::extract::array_element_udf(),
        vec![parts, one_based],
        schema,
        Arc::new(ConfigOptions::new()),
    )?) as Arc<dyn PhysicalExpr>;
    let null_value = Arc::new(Literal::new(ScalarValue::Utf8(None))) as Arc<dyn PhysicalExpr>;
    Ok(Arc::new(CaseExpr::try_new(
        None,
        vec![(eligible, selected)],
        Some(null_value),
    )?))
}
