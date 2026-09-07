// Copyright 2026 StreamFusion Authors
// Licensed under the Apache License, Version 2.0.

//! Plan-declared record semantics, applied inside the common identified stage boundary.
//! This is an ordinary DataFusion projection, not an operator-family fusion adapter.

use std::sync::Arc;

use datafusion::error::{DataFusionError, Result};
use datafusion::execution::memory_pool::MemoryPool;
use datafusion::physical_expr::expressions::{Column, Literal};
use datafusion::physical_expr::PhysicalExpr;
use datafusion::physical_plan::projection::ProjectionExec;
use datafusion::physical_plan::ExecutionPlan;
use datafusion::scalar::ScalarValue;

use super::envelope::{Envelope, INPUT_ROW, OWNED_TIMESTAMP_V1, ROW_KIND};
use crate::planner::expressions::managed_expression;

pub(crate) fn clear_timestamps(
    child: Arc<dyn ExecutionPlan>,
    memory: Option<&Arc<dyn MemoryPool>>,
) -> Result<Arc<dyn ExecutionPlan>> {
    let schema = child.schema();
    let envelope = Envelope::from_schema(&schema)?;
    let kind = schema
        .fields()
        .len()
        .checked_sub(2)
        .filter(|&index| {
            matches!(
                schema.field(index).name().as_str(),
                ROW_KIND | "__streamfusion_input_row_kind"
            )
        })
        .ok_or_else(|| {
            DataFusionError::Plan(
                "record timestamp policy requires explicit native RowKind metadata".into(),
            )
        })?;
    let mut expressions: Vec<(Arc<dyn PhysicalExpr>, String)> = (0..envelope.payload_width)
        .map(|index| {
            let name = schema.field(index).name();
            (
                Arc::new(Column::new(name, index)) as Arc<dyn PhysicalExpr>,
                name.clone(),
            )
        })
        .collect();
    // Payload and RowKind arrays remain shared. Only the two envelope vectors are
    // materialized, using the same admission and producer leases as SQL projections.
    expressions.push((
        managed_expression::projection(Arc::new(Literal::new(ScalarValue::Int64(None))), memory),
        OWNED_TIMESTAMP_V1.into(),
    ));
    expressions.push((
        Arc::new(Column::new(schema.field(kind).name(), kind)),
        ROW_KIND.into(),
    ));
    expressions.push((
        managed_expression::projection(
            Arc::new(Literal::new(ScalarValue::Int32(Some(-1)))),
            memory,
        ),
        INPUT_ROW.into(),
    ));
    Ok(Arc::new(ProjectionExec::try_new(expressions, child)?))
}

#[cfg(test)]
mod tests;
