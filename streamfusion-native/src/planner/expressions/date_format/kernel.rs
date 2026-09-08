// Copyright 2026 StreamFusion Authors
// Licensed under the Apache License, Version 2.0.
//! Flink's numeric DATE_FORMAT semantics lowered to DataFusion expressions.
//! Rebase onto a complete Gregorian 400-year cycle so Arrow's narrower chrono range
//! does not exclude valid Flink signed-millisecond timestamps. No calendar fields are
//! computed by a handwritten row loop.
use super::pattern;
use arrow::datatypes::{DataType, Schema, TimeUnit};
use datafusion::common::config::ConfigOptions;
use datafusion::error::{DataFusionError, Result};
use datafusion::logical_expr::{Operator, ScalarUDF};
use datafusion::physical_expr::expressions::{BinaryExpr, CaseExpr, CastExpr, IsNullExpr, Literal};
use datafusion::physical_expr::{PhysicalExpr, ScalarFunctionExpr};
use datafusion::scalar::ScalarValue;
use std::sync::Arc;
type Expr = Arc<dyn PhysicalExpr>;
// Every 400 Gregorian years contains exactly 146,097 days, including century leap rules.
const CYCLE_MILLIS: i64 = 146_097 * 86_400_000;
fn number(n: i64) -> Expr {
    Arc::new(Literal::new(ScalarValue::Int64(Some(n))))
}
fn text(s: &str) -> Expr {
    Arc::new(Literal::new(ScalarValue::Utf8(Some(s.into()))))
}
fn binary(a: Expr, op: Operator, b: Expr) -> Expr {
    Arc::new(BinaryExpr::new(a, op, b))
}
fn cast(a: Expr, ty: DataType) -> Expr {
    Arc::new(CastExpr::new(a, ty, None))
}
fn choose(condition: Expr, yes: Expr, no: Expr) -> Result<Expr> {
    Ok(Arc::new(CaseExpr::try_new(
        None,
        vec![(condition, yes)],
        Some(no),
    )?))
}
fn call(fun: Arc<ScalarUDF>, args: Vec<Expr>, schema: &Schema) -> Result<Expr> {
    Ok(Arc::new(ScalarFunctionExpr::try_new(
        fun,
        args,
        schema,
        Arc::new(ConfigOptions::new()),
    )?))
}
pub(super) fn create(value: Expr, pattern: &str, schema: &Schema) -> Result<Expr> {
    if value.data_type(schema)? != DataType::Timestamp(TimeUnit::Millisecond, None) {
        return Err(DataFusionError::Plan(
            "DATE_FORMAT numeric patterns require timezone-free TIMESTAMP(3)".into(),
        ));
    }
    let parts = pattern::parse(pattern)?;
    let millis = cast(value.clone(), DataType::Int64);
    let remainder = binary(millis.clone(), Operator::Modulo, number(CYCLE_MILLIS));
    let negative = cast(
        binary(remainder.clone(), Operator::Lt, number(0)),
        DataType::Int64,
    );
    let cycles = binary(
        binary(millis, Operator::Divide, number(CYCLE_MILLIS)),
        Operator::Minus,
        negative,
    );
    // Remainder magnitude is smaller than a cycle, so adding a cycle cannot overflow i64.
    let rebased = cast(
        binary(
            binary(remainder, Operator::Plus, number(CYCLE_MILLIS)),
            Operator::Modulo,
            number(CYCLE_MILLIS),
        ),
        DataType::Timestamp(TimeUnit::Millisecond, None),
    );
    let mut year = None;
    let mut arguments = Vec::with_capacity(parts.len());
    for part in parts {
        arguments.push(match part {
            pattern::Part::Format(format) => call(
                datafusion_functions::datetime::to_char(),
                vec![rebased.clone(), text(&format)],
                schema,
            )?,
            pattern::Part::YearOfEra => {
                if year.is_none() {
                    let base_year = cast(
                        call(
                            datafusion_functions::datetime::date_part(),
                            vec![text("year"), rebased.clone()],
                            schema,
                        )?,
                        DataType::Int64,
                    );
                    let proleptic = binary(
                        base_year,
                        Operator::Plus,
                        binary(cycles.clone(), Operator::Multiply, number(400)),
                    );
                    let era = choose(
                        binary(proleptic.clone(), Operator::Gt, number(0)),
                        proleptic.clone(),
                        binary(number(1), Operator::Minus, proleptic),
                    )?;
                    let digits = cast(era.clone(), DataType::Utf8);
                    let width = choose(
                        binary(era.clone(), Operator::Gt, number(9999)),
                        cast(
                            call(
                                datafusion_functions::unicode::character_length(),
                                vec![digits.clone()],
                                schema,
                            )?,
                            DataType::Int64,
                        ),
                        number(4),
                    )?;
                    let padded = call(
                        datafusion_functions::unicode::lpad(),
                        vec![digits, width, text("0")],
                        schema,
                    )?;
                    let sign =
                        choose(binary(era, Operator::Gt, number(9999)), text("+"), text(""))?;
                    year = Some(call(
                        datafusion_functions::string::concat(),
                        vec![sign, padded],
                        schema,
                    )?);
                }
                year.as_ref().unwrap().clone()
            }
        });
    }
    let result = if arguments.len() == 1 {
        arguments.pop().unwrap()
    } else {
        call(datafusion_functions::string::concat(), arguments, schema)?
    };
    choose(
        Arc::new(IsNullExpr::new(value)),
        Arc::new(Literal::new(ScalarValue::Utf8(None))),
        result,
    )
}
