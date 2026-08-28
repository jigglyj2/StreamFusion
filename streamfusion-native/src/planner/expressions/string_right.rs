// Copyright 2026 StreamFusion Authors
// Licensed under the Apache License, Version 2.0

use std::sync::Arc;

use arrow::datatypes::Schema;
use datafusion::common::config::ConfigOptions;
use datafusion::error::Result;
use datafusion::physical_expr::{PhysicalExpr, ScalarFunctionExpr};

use super::string_count_support::flink_nonnegative_count;

pub(crate) fn create(
    value: Arc<dyn PhysicalExpr>,
    count: Arc<dyn PhysicalExpr>,
    schema: &Schema,
) -> Result<Arc<dyn PhysicalExpr>> {
    Ok(Arc::new(ScalarFunctionExpr::try_new(
        datafusion_functions::unicode::right(),
        vec![value, flink_nonnegative_count(count)?],
        schema,
        Arc::new(ConfigOptions::new()),
    )?))
}
