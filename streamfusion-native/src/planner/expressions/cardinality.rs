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
use datafusion::error::Result;
use datafusion::physical_expr::expressions::CastExpr;
use datafusion::physical_expr::{PhysicalExpr, ScalarFunctionExpr};

pub(crate) fn create(
    collection: Arc<dyn PhysicalExpr>,
    schema: &Schema,
) -> Result<Arc<dyn PhysicalExpr>> {
    let cardinality = Arc::new(ScalarFunctionExpr::try_new(
        datafusion_functions_nested::cardinality::cardinality_udf(),
        vec![collection],
        schema,
        Arc::new(ConfigOptions::new()),
    )?) as Arc<dyn PhysicalExpr>;
    Ok(Arc::new(CastExpr::new(cardinality, DataType::Int32, None)))
}
