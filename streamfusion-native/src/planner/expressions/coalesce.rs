// Copyright 2026 StreamFusion Authors
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0

use std::sync::Arc;

use datafusion::error::{DataFusionError, Result};
use datafusion::physical_expr::expressions::{CaseExpr, IsNotNullExpr};
use datafusion::physical_expr::PhysicalExpr;

pub(crate) fn create(arguments: Vec<Arc<dyn PhysicalExpr>>) -> Result<Arc<dyn PhysicalExpr>> {
    if arguments.len() < 2 {
        return Err(DataFusionError::Plan(
            "COALESCE requires at least two arguments".to_string(),
        ));
    }
    let last = arguments.last().cloned().ok_or_else(|| {
        DataFusionError::Plan("COALESCE requires at least two arguments".to_string())
    })?;
    let when_then = arguments[..arguments.len() - 1]
        .iter()
        .cloned()
        .map(|argument| {
            (
                Arc::new(IsNotNullExpr::new(Arc::clone(&argument))) as Arc<dyn PhysicalExpr>,
                argument,
            )
        })
        .collect();
    Ok(Arc::new(CaseExpr::try_new(None, when_then, Some(last))?))
}
