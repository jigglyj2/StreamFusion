// Copyright 2026 StreamFusion Authors
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0

use std::sync::Arc;

use datafusion::common::tree_node::{Transformed, TransformedResult, TreeNode};
use datafusion::error::{DataFusionError, Result};
use datafusion::logical_expr::Operator;
use datafusion::physical_expr::expressions::{BinaryExpr, CastExpr, Column, Literal, NegativeExpr};
use datafusion::physical_expr::expressions::{IsNotNullExpr, IsNullExpr, NotExpr};
use datafusion::physical_expr::utils::collect_columns;
use datafusion::physical_expr::PhysicalExpr;
use datafusion::physical_plan::filter::FilterExecBuilder;
use datafusion::physical_plan::projection::ProjectionExec;
use datafusion::physical_plan::ExecutionPlan;
use datafusion::scalar::ScalarValue;

use crate::{planner::expressions, proto};

pub(crate) fn create(
    calc: &proto::Calc,
    child: Arc<dyn ExecutionPlan>,
) -> Result<Arc<dyn ExecutionPlan>> {
    let input_schema = child.schema();
    let mut expressions = calc
        .projections
        .iter()
        .enumerate()
        .map(|(index, expression)| {
            Ok((
                create_expression(expression, input_schema.as_ref())?,
                format!("projection_{index}"),
            ))
        })
        .collect::<Result<Vec<_>>>()?;
    let child = match calc.condition.as_ref() {
        Some(condition) => {
            let predicate = create_expression(condition, input_schema.as_ref())?;
            let projection = referenced_input_columns(&expressions, input_schema.fields().len());
            if projection.len() < input_schema.fields().len() {
                let filter = FilterExecBuilder::new(predicate, child)
                    .apply_projection(Some(projection.clone()))?
                    .build()?;
                expressions = expressions
                    .into_iter()
                    .map(|(expression, name)| Ok((remap_columns(expression, &projection)?, name)))
                    .collect::<Result<Vec<_>>>()?;
                Arc::new(filter) as Arc<dyn ExecutionPlan>
            } else {
                Arc::new(FilterExecBuilder::new(predicate, child).build()?)
                    as Arc<dyn ExecutionPlan>
            }
        }
        None => child,
    };
    Ok(Arc::new(ProjectionExec::try_new(expressions, child)?))
}

/// Columns retained by FilterExec after evaluating its predicate.
///
/// The predicate itself is evaluated against the unprojected input, so only columns consumed by
/// the final projection need to survive the filter. This avoids copying unrelated nested and
/// variable-width buffers from wide Flink rows.
fn referenced_input_columns(
    expressions: &[(Arc<dyn PhysicalExpr>, String)],
    input_field_count: usize,
) -> Vec<usize> {
    let mut columns = expressions
        .iter()
        .flat_map(|(expression, _)| collect_columns(expression))
        .map(|column| column.index())
        .collect::<Vec<_>>();
    columns.sort_unstable();
    columns.dedup();
    if columns.is_empty() && input_field_count != 0 {
        columns.push(0);
    }
    columns
}

fn remap_columns(
    expression: Arc<dyn PhysicalExpr>,
    retained_input_columns: &[usize],
) -> Result<Arc<dyn PhysicalExpr>> {
    expression
        .transform_down(|expression| {
            let Some(column) = expression.downcast_ref::<Column>() else {
                return Ok(Transformed::no(expression));
            };
            let index = retained_input_columns
                .binary_search(&column.index())
                .map_err(|_| {
                    DataFusionError::Internal(format!(
                        "Calc projection column {} was not retained by its filter",
                        column.index()
                    ))
                })?;
            Ok(Transformed::yes(Arc::new(Column::new(
                column.name(),
                index,
            ))))
        })
        .data()
}

pub(super) fn create_expression(
    expression: &proto::Expression,
    schema: &arrow::datatypes::Schema,
) -> Result<Arc<dyn PhysicalExpr>> {
    if let Some(value) = literal_scalar(expression)? {
        return Ok(Arc::new(Literal::new(value)));
    }
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
        Some(proto::expression::Expression::Hexadecimal(hexadecimal)) => {
            let operand = hexadecimal
                .operand
                .as_ref()
                .ok_or_else(|| DataFusionError::Plan("HEX operand is empty".to_string()))?;
            expressions::hexadecimal::create(create_expression(operand, schema)?, schema)
        }
        Some(proto::expression::Expression::Base64Encode(base64)) => {
            let operand = base64
                .operand
                .as_ref()
                .ok_or_else(|| DataFusionError::Plan("TO_BASE64 operand is empty".to_string()))?;
            expressions::base64_encode::create(create_expression(operand, schema)?, schema)
        }
        Some(proto::expression::Expression::Md5(md5)) => {
            let operand = md5
                .operand
                .as_ref()
                .ok_or_else(|| DataFusionError::Plan("MD5 operand is empty".to_string()))?;
            expressions::md5::create(create_expression(operand, schema)?, schema)
        }
        Some(proto::expression::Expression::ShaDigest(sha)) => {
            let operand = sha
                .operand
                .as_ref()
                .ok_or_else(|| DataFusionError::Plan("SHA digest operand is empty".to_string()))?;
            let algorithm = proto::ShaAlgorithm::try_from(sha.algorithm).map_err(|_| {
                DataFusionError::Plan(format!("unknown SHA digest algorithm {}", sha.algorithm))
            })?;
            expressions::sha_digest::create(create_expression(operand, schema)?, algorithm, schema)
        }
        Some(proto::expression::Expression::Sha1(sha1)) => {
            let operand = sha1
                .operand
                .as_ref()
                .ok_or_else(|| DataFusionError::Plan("SHA1 operand is empty".to_string()))?;
            expressions::sha1::create(create_expression(operand, schema)?, schema)
        }
        Some(proto::expression::Expression::Sha2Dynamic(sha2)) => {
            let operand = sha2
                .operand
                .as_ref()
                .ok_or_else(|| DataFusionError::Plan("SHA2 operand is empty".to_string()))?;
            let bit_length = sha2
                .bit_length
                .as_ref()
                .ok_or_else(|| DataFusionError::Plan("SHA2 bit length is empty".to_string()))?;
            expressions::sha2_dynamic::create(
                create_expression(operand, schema)?,
                create_expression(bit_length, schema)?,
                schema,
            )
        }
        Some(proto::expression::Expression::Chr(chr)) => {
            let operand = chr
                .operand
                .as_ref()
                .ok_or_else(|| DataFusionError::Plan("CHR operand is empty".to_string()))?;
            expressions::string_chr::create(create_expression(operand, schema)?, schema)
        }
        Some(proto::expression::Expression::StringReverse(reverse)) => {
            let operand = reverse
                .operand
                .as_ref()
                .ok_or_else(|| DataFusionError::Plan("REVERSE operand is empty".to_string()))?;
            expressions::string_reverse::create(create_expression(operand, schema)?, schema)
        }
        Some(proto::expression::Expression::StringInitCap(init_cap)) => {
            let operand = init_cap
                .operand
                .as_ref()
                .ok_or_else(|| DataFusionError::Plan("INITCAP operand is empty".to_string()))?;
            expressions::string_init_cap::create(create_expression(operand, schema)?, schema)
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
            let prefix = starts_with
                .prefix_expression
                .as_ref()
                .map(|prefix| create_expression(prefix, schema))
                .transpose()?
                .unwrap_or_else(|| {
                    Arc::new(Literal::new(ScalarValue::Utf8(Some(
                        starts_with.prefix.clone(),
                    ))))
                });
            expressions::starts_with::create(create_expression(operand, schema)?, prefix, schema)
        }
        Some(proto::expression::Expression::StringTrim(trim)) => {
            let value = trim
                .value
                .as_ref()
                .ok_or_else(|| DataFusionError::Plan("string trim value is empty".to_string()))?;
            let characters = trim
                .characters
                .as_ref()
                .map(|characters| create_expression(characters, schema))
                .transpose()?;
            expressions::string_trim::create(
                create_expression(value, schema)?,
                characters,
                trim.direction(),
                schema,
            )
        }
        Some(proto::expression::Expression::StringConcatWs(concat_ws)) => {
            let separator = concat_ws
                .separator
                .as_ref()
                .ok_or_else(|| DataFusionError::Plan("CONCAT_WS separator is empty".to_string()))?;
            let values = concat_ws
                .values
                .iter()
                .map(|value| create_expression(value, schema))
                .collect::<Result<Vec<_>>>()?;
            expressions::string_concat_ws::create(
                create_expression(separator, schema)?,
                values,
                schema,
            )
        }
        Some(proto::expression::Expression::StringTranslate(translate)) => {
            let value = translate
                .value
                .as_ref()
                .ok_or_else(|| DataFusionError::Plan("TRANSLATE value is empty".to_string()))?;
            let source_characters = translate.source_characters.as_ref().ok_or_else(|| {
                DataFusionError::Plan("TRANSLATE source characters are empty".to_string())
            })?;
            let target_characters = translate.target_characters.as_ref().ok_or_else(|| {
                DataFusionError::Plan("TRANSLATE target characters are empty".to_string())
            })?;
            expressions::string_translate::create(
                create_expression(value, schema)?,
                create_expression(source_characters, schema)?,
                create_expression(target_characters, schema)?,
                schema,
            )
        }
        Some(proto::expression::Expression::StringElt(elt)) => {
            let index = elt
                .index
                .as_ref()
                .ok_or_else(|| DataFusionError::Plan("ELT index is empty".to_string()))?;
            let values = elt
                .values
                .iter()
                .map(|value| create_expression(value, schema))
                .collect::<Result<Vec<_>>>()?;
            expressions::string_elt::create(create_expression(index, schema)?, values, schema)
        }
        Some(proto::expression::Expression::StringSplitIndex(split_index)) => {
            let value = split_index
                .value
                .as_ref()
                .ok_or_else(|| DataFusionError::Plan("SPLIT_INDEX value is empty".to_string()))?;
            let index = split_index
                .index
                .as_ref()
                .ok_or_else(|| DataFusionError::Plan("SPLIT_INDEX index is empty".to_string()))?;
            expressions::string_split_index::create(
                create_expression(value, schema)?,
                &split_index.delimiter,
                create_expression(index, schema)?,
                schema,
            )
        }
        Some(proto::expression::Expression::BinaryString(binary_string)) => {
            let operand = binary_string
                .operand
                .as_ref()
                .ok_or_else(|| DataFusionError::Plan("BIN operand is empty".to_string()))?;
            expressions::binary_string::create(create_expression(operand, schema)?, schema)
        }
        Some(proto::expression::Expression::Unhex(unhex)) => {
            let operand = unhex
                .operand
                .as_ref()
                .ok_or_else(|| DataFusionError::Plan("UNHEX operand is empty".to_string()))?;
            expressions::unhex::create(create_expression(operand, schema)?, schema)
        }
        Some(proto::expression::Expression::InetNtoa(inet_ntoa)) => {
            let operand = inet_ntoa
                .operand
                .as_ref()
                .ok_or_else(|| DataFusionError::Plan("INET_NTOA operand is empty".to_string()))?;
            expressions::inet_ntoa::create(create_expression(operand, schema)?, schema)
        }
        Some(proto::expression::Expression::InetAton(inet_aton)) => {
            let operand = inet_aton
                .operand
                .as_ref()
                .ok_or_else(|| DataFusionError::Plan("INET_ATON operand is empty".to_string()))?;
            expressions::inet_aton::create(create_expression(operand, schema)?, schema)
        }
        Some(proto::expression::Expression::ScalarExtremum(extremum)) => {
            let arguments = extremum
                .arguments
                .iter()
                .map(|argument| create_expression(argument, schema))
                .collect::<Result<Vec<_>>>()?;
            expressions::scalar_extremum::create(arguments, extremum.greatest, schema)
        }
        Some(proto::expression::Expression::UrlEncode(url_encode)) => {
            let operand = url_encode
                .operand
                .as_ref()
                .ok_or_else(|| DataFusionError::Plan("URL_ENCODE operand is empty".to_string()))?;
            expressions::url_encode::create(create_expression(operand, schema)?, schema)
        }
        Some(proto::expression::Expression::UrlDecode(url_decode)) => {
            let operand = url_decode
                .operand
                .as_ref()
                .ok_or_else(|| DataFusionError::Plan("URL_DECODE operand is empty".to_string()))?;
            expressions::url_decode::create(create_expression(operand, schema)?, schema)
        }
        Some(proto::expression::Expression::JsonQuote(json_quote)) => {
            let operand = json_quote
                .operand
                .as_ref()
                .ok_or_else(|| DataFusionError::Plan("JSON_QUOTE operand is empty".to_string()))?;
            expressions::json_quote::create(create_expression(operand, schema)?, schema)
        }
        Some(proto::expression::Expression::StringHashCode(hash_code)) => {
            let operand = hash_code
                .operand
                .as_ref()
                .ok_or_else(|| DataFusionError::Plan("HASH_CODE operand is empty".to_string()))?;
            expressions::string_hash_code::create(create_expression(operand, schema)?, schema)
        }
        Some(proto::expression::Expression::NumericTruncate(truncate)) => {
            let operand = truncate
                .operand
                .as_ref()
                .ok_or_else(|| DataFusionError::Plan("TRUNCATE operand is empty".to_string()))?;
            let scale = truncate
                .scale
                .as_ref()
                .ok_or_else(|| DataFusionError::Plan("TRUNCATE scale is empty".to_string()))?;
            expressions::numeric_truncate::create(
                create_expression(operand, schema)?,
                create_expression(scale, schema)?,
                schema,
            )
        }
        Some(proto::expression::Expression::TemporalExtract(extract)) => {
            let operand = extract
                .operand
                .as_ref()
                .ok_or_else(|| DataFusionError::Plan("EXTRACT operand is empty".to_string()))?;
            let field = match extract.field() {
                proto::TemporalExtractField::Year => "year",
                proto::TemporalExtractField::Quarter => "quarter",
                proto::TemporalExtractField::Month => "month",
                proto::TemporalExtractField::Week => "week",
                proto::TemporalExtractField::Day => "day",
                proto::TemporalExtractField::DayOfYear => "doy",
                proto::TemporalExtractField::DayOfWeek => "flink_dow",
                proto::TemporalExtractField::IsoDayOfWeek => "isodow",
                proto::TemporalExtractField::IsoYear => "isoyear",
                proto::TemporalExtractField::Hour => "hour",
                proto::TemporalExtractField::Minute => "minute",
                proto::TemporalExtractField::Second => "second",
                proto::TemporalExtractField::Epoch => "epoch",
                proto::TemporalExtractField::Millisecond => "flink_millisecond",
                proto::TemporalExtractField::Unspecified => {
                    return Err(DataFusionError::Plan(
                        "EXTRACT field is unspecified".to_string(),
                    ));
                }
            };
            expressions::temporal_extract::create(
                create_expression(operand, schema)?,
                field,
                extract.result_is_bigint,
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
            let left = create_expression(
                arithmetic.left.as_ref().ok_or_else(|| {
                    DataFusionError::Plan("arithmetic left operand is empty".to_string())
                })?,
                schema,
            )?;
            let right = create_expression(
                arithmetic.right.as_ref().ok_or_else(|| {
                    DataFusionError::Plan("arithmetic right operand is empty".to_string())
                })?,
                schema,
            )?;
            if let Some(result_type) = arithmetic.result_type.as_ref() {
                let target_type = expressions::null_literal::data_type(result_type)?;
                if !matches!(target_type, arrow::datatypes::DataType::Decimal128(_, _)) {
                    return Err(DataFusionError::Plan(
                        "arithmetic result type is only supported for DECIMAL".to_string(),
                    ));
                }
                let left = flink_decimal_operand(left, schema)?;
                let right = flink_decimal_operand(right, schema)?;
                let decimal = Arc::new(BinaryExpr::new(left, operator, right));
                if decimal.data_type(schema)? == target_type {
                    return Ok(decimal);
                }
                return Ok(Arc::new(CastExpr::new(decimal, target_type, None)));
            }
            Ok(Arc::new(BinaryExpr::new(left, operator, right)))
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

fn flink_decimal_operand(
    operand: Arc<dyn PhysicalExpr>,
    schema: &arrow::datatypes::Schema,
) -> Result<Arc<dyn PhysicalExpr>> {
    use arrow::datatypes::DataType;

    let target = match operand.data_type(schema)? {
        DataType::Decimal128(_, _) => return Ok(operand),
        DataType::Int8 => DataType::Decimal128(3, 0),
        DataType::Int16 => DataType::Decimal128(5, 0),
        DataType::Int32 => DataType::Decimal128(10, 0),
        // Flink reserves 19 decimal digits for BIGINT; DataFusion's generic
        // coercion uses 20, which changes the inferred result precision.
        DataType::Int64 => DataType::Decimal128(19, 0),
        other => {
            return Err(DataFusionError::Plan(format!(
                "Flink decimal arithmetic does not support operand type {other}"
            )))
        }
    };
    Ok(Arc::new(CastExpr::new(operand, target, None)))
}

pub(super) fn literal_scalar(expression: &proto::Expression) -> Result<Option<ScalarValue>> {
    let value = match expression.expression.as_ref() {
        Some(proto::expression::Expression::IntegerLiteral(literal)) => {
            ScalarValue::Int32(Some(literal.value))
        }
        Some(proto::expression::Expression::LongLiteral(literal)) => {
            ScalarValue::Int64(Some(literal.value))
        }
        Some(proto::expression::Expression::ByteLiteral(literal)) => {
            ScalarValue::Int8(Some(literal.value.try_into().map_err(|_| {
                DataFusionError::Plan(format!("TINYINT literal {} is out of range", literal.value))
            })?))
        }
        Some(proto::expression::Expression::ShortLiteral(literal)) => {
            ScalarValue::Int16(Some(literal.value.try_into().map_err(|_| {
                DataFusionError::Plan(format!(
                    "SMALLINT literal {} is out of range",
                    literal.value
                ))
            })?))
        }
        Some(proto::expression::Expression::FloatLiteral(literal)) => {
            ScalarValue::Float32(Some(literal.value))
        }
        Some(proto::expression::Expression::DoubleLiteral(literal)) => {
            ScalarValue::Float64(Some(literal.value))
        }
        Some(proto::expression::Expression::DateLiteral(literal)) => {
            ScalarValue::Date32(Some(literal.epoch_day))
        }
        Some(proto::expression::Expression::TimeLiteral(literal)) => {
            let millis = literal.millisecond_of_day;
            match literal.precision {
                0 => ScalarValue::Time32Second(Some(millis / 1_000)),
                1..=3 => ScalarValue::Time32Millisecond(Some(millis)),
                4..=6 => ScalarValue::Time64Microsecond(Some(i64::from(millis) * 1_000)),
                7..=9 => ScalarValue::Time64Nanosecond(Some(i64::from(millis) * 1_000_000)),
                precision => {
                    return Err(DataFusionError::Plan(format!(
                        "TIME precision {precision} is outside Flink's supported range 0..=9"
                    )))
                }
            }
        }
        Some(proto::expression::Expression::TimestampLiteral(literal)) => {
            let millis = literal.epoch_millisecond;
            let nanos = i64::from(literal.nano_of_millisecond);
            match literal.precision {
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
            }
        }
        Some(proto::expression::Expression::DecimalLiteral(literal)) => {
            let unscaled = literal.unscaled_value.parse::<i128>().map_err(|error| {
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
            ScalarValue::Decimal128(Some(unscaled), precision, scale)
        }
        Some(proto::expression::Expression::BooleanLiteral(literal)) => {
            ScalarValue::Boolean(Some(literal.value))
        }
        Some(proto::expression::Expression::StringLiteral(literal)) => {
            ScalarValue::Utf8(Some(literal.value.clone()))
        }
        Some(proto::expression::Expression::BinaryLiteral(literal)) => {
            let bytes = literal.value.clone();
            if literal.fixed_width {
                let length = i32::try_from(literal.length).map_err(|_| {
                    DataFusionError::Plan(format!(
                        "BINARY length {} exceeds Arrow FixedSizeBinary",
                        literal.length
                    ))
                })?;
                if length <= 0 || bytes.len() != length as usize {
                    return Err(DataFusionError::Plan(format!(
                        "BINARY({length}) literal has {} bytes",
                        bytes.len()
                    )));
                }
                ScalarValue::FixedSizeBinary(length, Some(bytes))
            } else {
                ScalarValue::Binary(Some(bytes))
            }
        }
        Some(proto::expression::Expression::NullLiteral(literal)) => {
            let logical_type = literal.r#type.as_ref().ok_or_else(|| {
                DataFusionError::Plan("NULL literal has no declared type".to_string())
            })?;
            ScalarValue::try_new_null(&expressions::null_literal::data_type(logical_type)?)?
        }
        _ => return Ok(None),
    };
    Ok(Some(value))
}
