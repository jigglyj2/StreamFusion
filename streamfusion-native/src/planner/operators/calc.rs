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
use datafusion::physical_expr::expressions::{BinaryExpr, Column, Literal, NegativeExpr};
use datafusion::physical_expr::expressions::{IsNotNullExpr, IsNullExpr, NotExpr};
use datafusion::physical_expr::PhysicalExpr;
use datafusion::physical_plan::filter::FilterExec;
use datafusion::physical_plan::projection::ProjectionExec;
use datafusion::physical_plan::ExecutionPlan;
use datafusion::scalar::ScalarValue;

use crate::{planner::expressions, proto};

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

pub(super) fn create_expression(
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
        Some(proto::expression::Expression::NullLiteral(literal)) => {
            expressions::null_literal::create(literal)
        }
        Some(proto::expression::Expression::Coalesce(coalesce)) => {
            let arguments = coalesce
                .arguments
                .iter()
                .map(|argument| create_expression(argument, schema))
                .collect::<Result<Vec<_>>>()?;
            expressions::coalesce::create(arguments)
        }
        Some(proto::expression::Expression::Conditional(conditional)) => {
            let branches = conditional
                .branches
                .iter()
                .map(|branch| {
                    let when = branch.when.as_ref().ok_or_else(|| {
                        DataFusionError::Plan("conditional WHEN expression is empty".to_string())
                    })?;
                    let then = branch.then.as_ref().ok_or_else(|| {
                        DataFusionError::Plan("conditional THEN expression is empty".to_string())
                    })?;
                    Ok((
                        create_expression(when, schema)?,
                        create_expression(then, schema)?,
                    ))
                })
                .collect::<Result<Vec<_>>>()?;
            let else_value = conditional.else_value.as_ref().ok_or_else(|| {
                DataFusionError::Plan("conditional ELSE expression is empty".to_string())
            })?;
            expressions::conditional::create(branches, create_expression(else_value, schema)?)
        }
        Some(proto::expression::Expression::AbsoluteValue(absolute)) => {
            let operand = absolute
                .operand
                .as_ref()
                .ok_or_else(|| DataFusionError::Plan("ABS operand is empty".to_string()))?;
            expressions::absolute_value::create(create_expression(operand, schema)?, schema)
        }
        Some(proto::expression::Expression::Ceiling(ceiling)) => {
            let operand = ceiling
                .operand
                .as_ref()
                .ok_or_else(|| DataFusionError::Plan("CEIL operand is empty".to_string()))?;
            expressions::ceiling::create(create_expression(operand, schema)?, schema)
        }
        Some(proto::expression::Expression::Floor(floor)) => {
            let operand = floor
                .operand
                .as_ref()
                .ok_or_else(|| DataFusionError::Plan("FLOOR operand is empty".to_string()))?;
            expressions::floor::create(create_expression(operand, schema)?, schema)
        }
        Some(proto::expression::Expression::Sign(sign)) => {
            let operand = sign
                .operand
                .as_ref()
                .ok_or_else(|| DataFusionError::Plan("SIGN operand is empty".to_string()))?;
            expressions::sign::create(create_expression(operand, schema)?, schema)
        }
        Some(proto::expression::Expression::SquareRoot(square_root)) => {
            let operand = square_root
                .operand
                .as_ref()
                .ok_or_else(|| DataFusionError::Plan("SQRT operand is empty".to_string()))?;
            expressions::square_root::create(create_expression(operand, schema)?, schema)
        }
        Some(proto::expression::Expression::Exponential(exponential)) => {
            let operand = exponential
                .operand
                .as_ref()
                .ok_or_else(|| DataFusionError::Plan("EXP operand is empty".to_string()))?;
            expressions::exponential::create(create_expression(operand, schema)?, schema)
        }
        Some(proto::expression::Expression::Sine(sine)) => {
            let operand = sine
                .operand
                .as_ref()
                .ok_or_else(|| DataFusionError::Plan("SIN operand is empty".to_string()))?;
            expressions::sine::create(create_expression(operand, schema)?, schema)
        }
        Some(proto::expression::Expression::Cosine(cosine)) => {
            let operand = cosine
                .operand
                .as_ref()
                .ok_or_else(|| DataFusionError::Plan("COS operand is empty".to_string()))?;
            expressions::cosine::create(create_expression(operand, schema)?, schema)
        }
        Some(proto::expression::Expression::Tangent(tangent)) => {
            let operand = tangent
                .operand
                .as_ref()
                .ok_or_else(|| DataFusionError::Plan("TAN operand is empty".to_string()))?;
            expressions::tangent::create(create_expression(operand, schema)?, schema)
        }
        Some(proto::expression::Expression::Cotangent(cotangent)) => {
            let operand = cotangent
                .operand
                .as_ref()
                .ok_or_else(|| DataFusionError::Plan("COT operand is empty".to_string()))?;
            expressions::cotangent::create(create_expression(operand, schema)?, schema)
        }
        Some(proto::expression::Expression::NaturalLogarithm(logarithm)) => {
            let operand = logarithm
                .operand
                .as_ref()
                .ok_or_else(|| DataFusionError::Plan("LN operand is empty".to_string()))?;
            expressions::natural_logarithm::create(create_expression(operand, schema)?, schema)
        }
        Some(proto::expression::Expression::CommonLogarithm(logarithm)) => {
            let operand = logarithm
                .operand
                .as_ref()
                .ok_or_else(|| DataFusionError::Plan("LOG10 operand is empty".to_string()))?;
            expressions::common_logarithm::create(create_expression(operand, schema)?, schema)
        }
        Some(proto::expression::Expression::ArbitraryLogarithm(logarithm)) => {
            let base = logarithm
                .base
                .as_ref()
                .ok_or_else(|| DataFusionError::Plan("LOG base operand is empty".to_string()))?;
            let value = logarithm
                .value
                .as_ref()
                .ok_or_else(|| DataFusionError::Plan("LOG value operand is empty".to_string()))?;
            expressions::arbitrary_logarithm::create(
                create_expression(base, schema)?,
                create_expression(value, schema)?,
                schema,
            )
        }
        Some(proto::expression::Expression::Power(power)) => {
            let base = power
                .base
                .as_ref()
                .ok_or_else(|| DataFusionError::Plan("POWER base operand is empty".to_string()))?;
            let exponent = power.exponent.as_ref().ok_or_else(|| {
                DataFusionError::Plan("POWER exponent operand is empty".to_string())
            })?;
            expressions::power::create(
                create_expression(base, schema)?,
                create_expression(exponent, schema)?,
                schema,
            )
        }
        Some(proto::expression::Expression::HyperbolicSine(sine)) => {
            let operand = sine
                .operand
                .as_ref()
                .ok_or_else(|| DataFusionError::Plan("SINH operand is empty".to_string()))?;
            expressions::hyperbolic_sine::create(create_expression(operand, schema)?, schema)
        }
        Some(proto::expression::Expression::HyperbolicTangent(tangent)) => {
            let operand = tangent
                .operand
                .as_ref()
                .ok_or_else(|| DataFusionError::Plan("TANH operand is empty".to_string()))?;
            expressions::hyperbolic_tangent::create(create_expression(operand, schema)?, schema)
        }
        Some(proto::expression::Expression::ArcSine(arc_sine)) => {
            let operand = arc_sine
                .operand
                .as_ref()
                .ok_or_else(|| DataFusionError::Plan("ASIN operand is empty".to_string()))?;
            expressions::arc_sine::create(create_expression(operand, schema)?, schema)
        }
        Some(proto::expression::Expression::ArcCosine(arc_cosine)) => {
            let operand = arc_cosine
                .operand
                .as_ref()
                .ok_or_else(|| DataFusionError::Plan("ACOS operand is empty".to_string()))?;
            expressions::arc_cosine::create(create_expression(operand, schema)?, schema)
        }
        Some(proto::expression::Expression::ArcTangent(arc_tangent)) => {
            let operand = arc_tangent
                .operand
                .as_ref()
                .ok_or_else(|| DataFusionError::Plan("ATAN operand is empty".to_string()))?;
            expressions::arc_tangent::create(create_expression(operand, schema)?, schema)
        }
        Some(proto::expression::Expression::Degrees(degrees)) => {
            let operand = degrees
                .operand
                .as_ref()
                .ok_or_else(|| DataFusionError::Plan("DEGREES operand is empty".to_string()))?;
            expressions::degrees::create(create_expression(operand, schema)?, schema)
        }
        Some(proto::expression::Expression::Radians(radians)) => {
            let operand = radians
                .operand
                .as_ref()
                .ok_or_else(|| DataFusionError::Plan("RADIANS operand is empty".to_string()))?;
            expressions::radians::create(create_expression(operand, schema)?, schema)
        }
        Some(proto::expression::Expression::ArcTangent2(arc_tangent2)) => {
            let y = arc_tangent2
                .y
                .as_ref()
                .ok_or_else(|| DataFusionError::Plan("ATAN2 y operand is empty".to_string()))?;
            let x = arc_tangent2
                .x
                .as_ref()
                .ok_or_else(|| DataFusionError::Plan("ATAN2 x operand is empty".to_string()))?;
            expressions::arc_tangent2::create(
                create_expression(y, schema)?,
                create_expression(x, schema)?,
                schema,
            )
        }
        Some(proto::expression::Expression::CharacterLength(character_length)) => {
            let operand = character_length
                .operand
                .as_ref()
                .ok_or_else(|| DataFusionError::Plan("CHAR_LENGTH operand is empty".to_string()))?;
            expressions::character_length::create(create_expression(operand, schema)?, schema)
        }
        Some(proto::expression::Expression::Lower(lower)) => {
            let operand = lower
                .operand
                .as_ref()
                .ok_or_else(|| DataFusionError::Plan("LOWER operand is empty".to_string()))?;
            expressions::lower::create(create_expression(operand, schema)?, schema)
        }
        Some(proto::expression::Expression::Upper(upper)) => {
            let operand = upper
                .operand
                .as_ref()
                .ok_or_else(|| DataFusionError::Plan("UPPER operand is empty".to_string()))?;
            expressions::upper::create(create_expression(operand, schema)?, schema)
        }
        Some(proto::expression::Expression::Concat(concat)) => {
            let arguments = concat
                .arguments
                .iter()
                .map(|argument| create_expression(argument, schema))
                .collect::<Result<Vec<_>>>()?;
            expressions::concat::create(arguments, schema)
        }
        Some(proto::expression::Expression::Like(like)) => {
            let operand = like
                .operand
                .as_ref()
                .ok_or_else(|| DataFusionError::Plan("LIKE operand is empty".to_string()))?;
            expressions::like::create(create_expression(operand, schema)?, &like.pattern, schema)
        }
        Some(proto::expression::Expression::StartsWith(starts_with)) => {
            let operand = starts_with
                .operand
                .as_ref()
                .ok_or_else(|| DataFusionError::Plan("STARTS_WITH operand is empty".to_string()))?;
            expressions::starts_with::create(
                create_expression(operand, schema)?,
                &starts_with.prefix,
                schema,
            )
        }
        Some(proto::expression::Expression::Substring(substring)) => {
            let operand = substring
                .operand
                .as_ref()
                .ok_or_else(|| DataFusionError::Plan("SUBSTRING operand is empty".to_string()))?;
            expressions::substring::create(
                create_expression(operand, schema)?,
                substring.start,
                substring.length,
                schema,
            )
        }
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
        Some(proto::expression::Expression::TimestampLiteral(literal)) => {
            let millis = literal.epoch_millisecond;
            let nanos = i64::from(literal.nano_of_millisecond);
            let value = match literal.precision {
                0 => ScalarValue::TimestampSecond(Some(millis / 1_000), None),
                1..=3 => ScalarValue::TimestampMillisecond(Some(millis), None),
                4..=6 => {
                    ScalarValue::TimestampMicrosecond(Some(millis * 1_000 + nanos / 1_000), None)
                }
                7..=9 => ScalarValue::TimestampNanosecond(Some(millis * 1_000_000 + nanos), None),
                precision => {
                    return Err(DataFusionError::Plan(format!(
                        "TIMESTAMP precision {precision} is outside Flink's supported range 0..=9"
                    )))
                }
            };
            Ok(Arc::new(Literal::new(value)))
        }
        Some(proto::expression::Expression::DecimalLiteral(literal)) => {
            let value = literal.unscaled_value.parse::<i128>().map_err(|error| {
                DataFusionError::Plan(format!(
                    "DECIMAL unscaled value '{}' is invalid: {error}",
                    literal.unscaled_value
                ))
            })?;
            let precision = u8::try_from(literal.precision).map_err(|_| {
                DataFusionError::Plan(format!(
                    "DECIMAL precision {} exceeds Decimal128",
                    literal.precision
                ))
            })?;
            let scale = i8::try_from(literal.scale).map_err(|_| {
                DataFusionError::Plan(format!("DECIMAL scale {} is invalid", literal.scale))
            })?;
            if precision == 0 || precision > 38 || scale < 0 || scale > precision as i8 {
                return Err(DataFusionError::Plan(format!(
                    "DECIMAL({precision}, {scale}) is outside Flink's supported range"
                )));
            }
            Ok(Arc::new(Literal::new(ScalarValue::Decimal128(
                Some(value),
                precision,
                scale,
            ))))
        }
        Some(proto::expression::Expression::BooleanLiteral(literal)) => Ok(Arc::new(Literal::new(
            ScalarValue::Boolean(Some(literal.value)),
        ))),
        Some(proto::expression::Expression::StringLiteral(literal)) => Ok(Arc::new(Literal::new(
            ScalarValue::Utf8(Some(literal.value.clone())),
        ))),
        Some(proto::expression::Expression::BinaryLiteral(literal)) => {
            let value = literal.value.clone();
            let scalar = if literal.fixed_width {
                let length = i32::try_from(literal.length).map_err(|_| {
                    DataFusionError::Plan(format!(
                        "BINARY length {} exceeds Arrow FixedSizeBinary",
                        literal.length
                    ))
                })?;
                if length <= 0 || value.len() != length as usize {
                    return Err(DataFusionError::Plan(format!(
                        "BINARY({length}) literal has {} bytes",
                        value.len()
                    )));
                }
                ScalarValue::FixedSizeBinary(length, Some(value))
            } else {
                ScalarValue::Binary(Some(value))
            };
            Ok(Arc::new(Literal::new(scalar)))
        }
        Some(proto::expression::Expression::UnaryMinus(unary)) => {
            let operand = unary
                .operand
                .as_ref()
                .ok_or_else(|| DataFusionError::Plan("unary minus operand is empty".to_string()))?;
            Ok(Arc::new(NegativeExpr::new(create_expression(
                operand, schema,
            )?)))
        }
        Some(proto::expression::Expression::TruthTest(test)) => {
            let operand = create_expression(
                test.operand.as_ref().ok_or_else(|| {
                    DataFusionError::Plan("truth test operand is empty".to_string())
                })?,
                schema,
            )?;
            let (operator, expected) = match test.operator() {
                proto::TruthTestOperator::IsTrue => (Operator::IsNotDistinctFrom, true),
                proto::TruthTestOperator::IsFalse => (Operator::IsNotDistinctFrom, false),
                proto::TruthTestOperator::IsNotTrue => (Operator::IsDistinctFrom, true),
                proto::TruthTestOperator::IsNotFalse => (Operator::IsDistinctFrom, false),
                proto::TruthTestOperator::Unspecified => {
                    return Err(DataFusionError::Plan(
                        "truth test operator is unspecified".to_string(),
                    ))
                }
            };
            Ok(Arc::new(BinaryExpr::new(
                operand,
                operator,
                Arc::new(Literal::new(ScalarValue::Boolean(Some(expected)))),
            )))
        }
        Some(proto::expression::Expression::Cast(cast)) => {
            let operand = create_expression(
                cast.operand
                    .as_ref()
                    .ok_or_else(|| DataFusionError::Plan("cast operand is empty".to_string()))?,
                schema,
            )?;
            expressions::cast::create(cast, operand, schema)
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
            expressions::comparison::create(
                create_expression(
                    comparison.left.as_ref().ok_or_else(|| {
                        DataFusionError::Plan("comparison left operand is empty".to_string())
                    })?,
                    schema,
                )?,
                create_expression(
                    comparison.right.as_ref().ok_or_else(|| {
                        DataFusionError::Plan("comparison right operand is empty".to_string())
                    })?,
                    schema,
                )?,
                comparison.operator(),
                schema,
            )
        }
        Some(proto::expression::Expression::Arithmetic(arithmetic)) => {
            let operator = match arithmetic.operator() {
                proto::ArithmeticOperator::Add => Operator::Plus,
                proto::ArithmeticOperator::Subtract => Operator::Minus,
                proto::ArithmeticOperator::Multiply => Operator::Multiply,
                proto::ArithmeticOperator::Divide => Operator::Divide,
                proto::ArithmeticOperator::Modulo => Operator::Modulo,
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
        Some(collection_expression) => super::collection::create(collection_expression, schema),
        None => Err(DataFusionError::Plan("expression is empty".to_string())),
    }
}
