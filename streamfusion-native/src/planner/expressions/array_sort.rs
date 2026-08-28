// Copyright 2026 StreamFusion Authors
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
// http://www.apache.org/licenses/LICENSE-2.0

use std::sync::Arc;

use arrow::datatypes::Schema;
use datafusion::common::{config::ConfigOptions, ScalarValue};
use datafusion::error::Result;
use datafusion::physical_expr::{expressions::Literal, PhysicalExpr, ScalarFunctionExpr};

pub(crate) fn create(
    array: Arc<dyn PhysicalExpr>,
    ascending: bool,
    null_first: bool,
    schema: &Schema,
) -> Result<Arc<dyn PhysicalExpr>> {
    let order = if ascending { "ASC" } else { "DESC" };
    let null_order = if null_first {
        "NULLS FIRST"
    } else {
        "NULLS LAST"
    };
    Ok(Arc::new(ScalarFunctionExpr::try_new(
        datafusion_functions_nested::sort::array_sort_udf(),
        vec![
            array,
            Arc::new(Literal::new(ScalarValue::Utf8(Some(order.to_string())))),
            Arc::new(Literal::new(ScalarValue::Utf8(Some(
                null_order.to_string(),
            )))),
        ],
        schema,
        Arc::new(ConfigOptions::new()),
    )?))
}
