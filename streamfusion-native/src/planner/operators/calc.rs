// Copyright 2026 StreamFusion Authors
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0

use std::sync::Arc;

use datafusion::error::{DataFusionError, Result};
use datafusion::physical_expr::expressions::Column;
use datafusion::physical_plan::projection::ProjectionExec;
use datafusion::physical_plan::ExecutionPlan;

use crate::proto;

pub(crate) fn create(
    calc: &proto::Calc,
    child: Arc<dyn ExecutionPlan>,
) -> Result<Arc<dyn ExecutionPlan>> {
    if calc.condition.is_some() {
        return Err(DataFusionError::Plan(
            "calc conditions are not supported yet".to_string(),
        ));
    }
    let child_schema = child.schema();
    let expressions = calc
        .projections
        .iter()
        .map(|expression| {
            let reference = match expression.expression.as_ref() {
                Some(proto::expression::Expression::InputReference(reference)) => reference,
                None => {
                    return Err(DataFusionError::Plan(
                        "projection expression is empty".to_string(),
                    ));
                }
            };
            let index = reference.index as usize;
            let field = child_schema.fields().get(index).ok_or_else(|| {
                DataFusionError::Plan(format!(
                    "projection input index {index} is outside the {}-column input schema",
                    child_schema.fields().len()
                ))
            })?;
            Ok((
                Arc::new(Column::new(field.name(), index)) as _,
                field.name().to_string(),
            ))
        })
        .collect::<Result<Vec<_>>>()?;
    Ok(Arc::new(ProjectionExec::try_new(expressions, child)?))
}
