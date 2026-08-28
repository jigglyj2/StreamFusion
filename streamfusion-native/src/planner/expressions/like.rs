// Copyright 2026 StreamFusion Authors
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0

use std::sync::Arc;

use arrow::datatypes::Schema;
use datafusion::error::Result;
use datafusion::physical_expr::expressions::{like, Literal};
use datafusion::physical_expr::PhysicalExpr;
use datafusion::scalar::ScalarValue;

pub(crate) fn create(
    operand: Arc<dyn PhysicalExpr>,
    pattern: &str,
    schema: &Schema,
) -> Result<Arc<dyn PhysicalExpr>> {
    like(
        false,
        false,
        operand,
        Arc::new(Literal::new(ScalarValue::Utf8(Some(pattern.to_string())))),
        schema,
    )
}
