// Copyright 2026 StreamFusion Authors
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0

use std::sync::Arc;

use datafusion::error::{DataFusionError, Result};
use datafusion::physical_expr::expressions::CaseExpr;
use datafusion::physical_expr::PhysicalExpr;

pub(crate) fn create(
    branches: Vec<(Arc<dyn PhysicalExpr>, Arc<dyn PhysicalExpr>)>,
    else_value: Arc<dyn PhysicalExpr>,
) -> Result<Arc<dyn PhysicalExpr>> {
    if branches.is_empty() {
        return Err(DataFusionError::Plan(
            "conditional expression requires at least one branch".to_string(),
        ));
    }
    Ok(Arc::new(CaseExpr::try_new(
        None,
        branches,
        Some(else_value),
    )?))
}
