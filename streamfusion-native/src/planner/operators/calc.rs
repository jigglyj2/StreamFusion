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
use datafusion::physical_expr::expressions::{IsNotNullExpr, IsNullExpr, NotExpr};
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
        .enumerate()
        .map(|(index, expression)| {
            Ok((
                create_expression(expression, child_schema.as_ref())?,
                format!("projection_{index}"),
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
        Some(proto::expression::Expression::LongLiteral(literal)) => Ok(Arc::new(Literal::new(
            ScalarValue::Int64(Some(literal.value)),
        ))),
        Some(proto::expression::Expression::ByteLiteral(literal)) => Ok(Arc::new(Literal::new(
            ScalarValue::Int8(Some(literal.value.try_into().map_err(|_| {
                DataFusionError::Plan(format!("TINYINT literal {} is out of range", literal.value))
            })?)),
        ))),
        Some(proto::expression::Expression::ShortLiteral(literal)) => Ok(Arc::new(Literal::new(
            ScalarValue::Int16(Some(literal.value.try_into().map_err(|_| {
                DataFusionError::Plan(format!(
                    "SMALLINT literal {} is out of range",
                    literal.value
                ))
            })?)),
        ))),
        Some(proto::expression::Expression::FloatLiteral(literal)) => Ok(Arc::new(Literal::new(
            ScalarValue::Float32(Some(literal.value)),
        ))),
        Some(proto::expression::Expression::DoubleLiteral(literal)) => Ok(Arc::new(Literal::new(
            ScalarValue::Float64(Some(literal.value)),
        ))),
        Some(proto::expression::Expression::DateLiteral(literal)) => Ok(Arc::new(Literal::new(
            ScalarValue::Date32(Some(literal.epoch_day)),
        ))),
        Some(proto::expression::Expression::TimeLiteral(literal)) => {
            let millis = literal.millisecond_of_day;
            let value = match literal.precision {
                0 => ScalarValue::Time32Second(Some(millis / 1_000)),
                1..=3 => ScalarValue::Time32Millisecond(Some(millis)),
                4..=6 => ScalarValue::Time64Microsecond(Some(i64::from(millis) * 1_000)),
                7..=9 => ScalarValue::Time64Nanosecond(Some(i64::from(millis) * 1_000_000)),
                precision => {
                    return Err(DataFusionError::Plan(format!(
                        "TIME precision {precision} is outside Flink's supported range 0..=9"
                    )))
                }
            };
            Ok(Arc::new(Literal::new(value)))
        }
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
        Some(proto::expression::Expression::Comparison(comparison)) => {
            let operator = match comparison.operator() {
                proto::ComparisonOperator::Equal => Operator::Eq,
                proto::ComparisonOperator::NotEqual => Operator::NotEq,
                proto::ComparisonOperator::LessThan => Operator::Lt,
                proto::ComparisonOperator::LessThanOrEqual => Operator::LtEq,
                proto::ComparisonOperator::GreaterThan => Operator::Gt,
                proto::ComparisonOperator::GreaterThanOrEqual => Operator::GtEq,
                proto::ComparisonOperator::Unspecified => {
                    return Err(DataFusionError::Plan(
                        "comparison operator is unspecified".to_string(),
                    ));
                }
            };
            Ok(Arc::new(BinaryExpr::new(
                create_expression(
                    comparison.left.as_ref().ok_or_else(|| {
                        DataFusionError::Plan("comparison left operand is empty".to_string())
                    })?,
                    schema,
                )?,
                operator,
                create_expression(
                    comparison.right.as_ref().ok_or_else(|| {
                        DataFusionError::Plan("comparison right operand is empty".to_string())
                    })?,
                    schema,
                )?,
            )))
        }
        Some(proto::expression::Expression::Arithmetic(arithmetic)) => {
            let operator = match arithmetic.operator() {
                proto::ArithmeticOperator::Add => Operator::Plus,
                proto::ArithmeticOperator::Subtract => Operator::Minus,
                proto::ArithmeticOperator::Multiply => Operator::Multiply,
                proto::ArithmeticOperator::Unspecified => {
                    return Err(DataFusionError::Plan(
                        "arithmetic operator is unspecified".to_string(),
                    ));
                }
            };
            Ok(Arc::new(BinaryExpr::new(
                create_expression(
                    arithmetic.left.as_ref().ok_or_else(|| {
                        DataFusionError::Plan("arithmetic left operand is empty".to_string())
                    })?,
                    schema,
                )?,
                operator,
                create_expression(
                    arithmetic.right.as_ref().ok_or_else(|| {
                        DataFusionError::Plan("arithmetic right operand is empty".to_string())
                    })?,
                    schema,
                )?,
            )))
        }
        Some(proto::expression::Expression::NullCheck(null_check)) => {
            let operand = create_expression(
                null_check.operand.as_ref().ok_or_else(|| {
                    DataFusionError::Plan("null-check operand is empty".to_string())
                })?,
                schema,
            )?;
            if null_check.negated {
                Ok(Arc::new(IsNotNullExpr::new(operand)))
            } else {
                Ok(Arc::new(IsNullExpr::new(operand)))
            }
        }
        Some(proto::expression::Expression::BooleanBinary(boolean)) => {
            let operator = match boolean.operator() {
                proto::BooleanOperator::And => Operator::And,
                proto::BooleanOperator::Or => Operator::Or,
                proto::BooleanOperator::Unspecified => {
                    return Err(DataFusionError::Plan(
                        "boolean operator is unspecified".to_string(),
                    ));
                }
            };
            Ok(Arc::new(BinaryExpr::new(
                create_expression(
                    boolean.left.as_ref().ok_or_else(|| {
                        DataFusionError::Plan("boolean left operand is empty".to_string())
                    })?,
                    schema,
                )?,
                operator,
                create_expression(
                    boolean.right.as_ref().ok_or_else(|| {
                        DataFusionError::Plan("boolean right operand is empty".to_string())
                    })?,
                    schema,
                )?,
            )))
        }
        Some(proto::expression::Expression::BooleanNot(boolean)) => {
            Ok(Arc::new(NotExpr::new(create_expression(
                boolean.operand.as_ref().ok_or_else(|| {
                    DataFusionError::Plan("boolean NOT operand is empty".to_string())
                })?,
                schema,
            )?)))
        }
        None => Err(DataFusionError::Plan("expression is empty".to_string())),
    }
}
