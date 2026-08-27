// Copyright 2026 StreamFusion Authors
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0

use std::sync::Arc;

use datafusion::error::{DataFusionError, Result};
use datafusion::logical_expr::Operator;
use datafusion::physical_expr::expressions::{BinaryExpr, Column, Literal};
use datafusion::physical_expr::PhysicalExpr;
use datafusion::physical_plan::filter::FilterExec;
use datafusion::physical_plan::projection::ProjectionExec;
use datafusion::physical_plan::ExecutionPlan;
use datafusion::scalar::ScalarValue;

use crate::proto;

pub(crate) fn create(
    calc: &proto::Calc,
    child: Arc<dyn ExecutionPlan>,
) -> Result<Arc<dyn ExecutionPlan>> {
    let child = match calc.condition.as_ref() {
        Some(condition) => Arc::new(FilterExec::try_new(
            create_expression(condition, child.schema().as_ref())?,
            child,
        )?) as Arc<dyn ExecutionPlan>,
        None => child,
    };
    let child_schema = child.schema();
    let expressions = calc
        .projections
        .iter()
        .map(|expression| {
            let reference = match expression.expression.as_ref() {
                Some(proto::expression::Expression::InputReference(reference)) => reference,
                _ => {
                    return Err(DataFusionError::Plan(
                        "only input-reference projections are supported".to_string(),
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

fn create_expression(
    expression: &proto::Expression,
    schema: &arrow::datatypes::Schema,
) -> Result<Arc<dyn PhysicalExpr>> {
    match expression.expression.as_ref() {
        Some(proto::expression::Expression::InputReference(reference)) => {
            let index = reference.index as usize;
            let field = schema.fields().get(index).ok_or_else(|| {
                DataFusionError::Plan(format!(
                    "expression input index {index} is outside the {}-column input schema",
                    schema.fields().len()
                ))
            })?;
            Ok(Arc::new(Column::new(field.name(), index)))
        }
        Some(proto::expression::Expression::IntegerLiteral(literal)) => Ok(Arc::new(Literal::new(
            ScalarValue::Int32(Some(literal.value)),
        ))),
        Some(proto::expression::Expression::GreaterThanOrEqual(comparison)) => {
            Ok(Arc::new(BinaryExpr::new(
                create_expression(
                    comparison.left.as_ref().ok_or_else(|| {
                        DataFusionError::Plan(
                            "greater-than-or-equal left operand is empty".to_string(),
                        )
                    })?,
                    schema,
                )?,
                Operator::GtEq,
                create_expression(
                    comparison.right.as_ref().ok_or_else(|| {
                        DataFusionError::Plan(
                            "greater-than-or-equal right operand is empty".to_string(),
                        )
                    })?,
                    schema,
                )?,
            )))
        }
        None => Err(DataFusionError::Plan("expression is empty".to_string())),
    }
}
