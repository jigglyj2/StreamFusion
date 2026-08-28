// Copyright 2026 StreamFusion Authors
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0

use std::sync::Arc;

use arrow::datatypes::DataType;
use datafusion::error::Result;
use datafusion::logical_expr::Operator;
use datafusion::physical_expr::expressions::{BinaryExpr, CaseExpr, CastExpr, Literal};
use datafusion::physical_expr::PhysicalExpr;
use datafusion::scalar::ScalarValue;

/// DataFusion treats negative LEFT/RIGHT counts as exclusion counts; Flink returns empty.
pub(super) fn flink_nonnegative_count(
    count: Arc<dyn PhysicalExpr>,
) -> Result<Arc<dyn PhysicalExpr>> {
    let count = Arc::new(CastExpr::new(count, DataType::Int64, None)) as Arc<dyn PhysicalExpr>;
    let nonpositive = Arc::new(BinaryExpr::new(
        Arc::clone(&count),
        Operator::LtEq,
        Arc::new(Literal::new(ScalarValue::Int64(Some(0)))),
    )) as Arc<dyn PhysicalExpr>;
    Ok(Arc::new(CaseExpr::try_new(
        None,
        vec![(
            nonpositive,
            Arc::new(Literal::new(ScalarValue::Int64(Some(0)))) as Arc<dyn PhysicalExpr>,
        )],
        Some(count),
    )?))
}
