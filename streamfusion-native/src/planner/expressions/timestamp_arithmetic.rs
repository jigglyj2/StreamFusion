// Copyright 2026 StreamFusion Authors
// Licensed under the Apache License, Version 2.0.

use crate::proto;
use arrow::datatypes::{DataType, Schema, TimeUnit};
use datafusion::common::{Result, ScalarValue};
use datafusion::logical_expr::Operator;
use datafusion::physical_expr::expressions::{BinaryExpr, CastExpr, Literal};
use datafusion::physical_expr::PhysicalExpr;
use std::sync::Arc;

/// Flink's TIMESTAMP(3) +/- day-time literal operates on a Java long millisecond
/// value, including wrapping overflow. Arrow's timestamp/interval kernel instead
/// uses calendar arithmetic with a narrower representable range. DataFusion's
/// wrapping integer kernel preserves Flink semantics; the casts reinterpret the
/// fixed-width timestamp buffers without calendar conversion.
pub(crate) fn literal_day_time(
    left: Arc<dyn PhysicalExpr>,
    operator: Operator,
    right: &proto::Expression,
    schema: &Schema,
) -> Result<Option<Arc<dyn PhysicalExpr>>> {
    let timestamp = DataType::Timestamp(TimeUnit::Millisecond, None);
    if !matches!(operator, Operator::Plus | Operator::Minus) || left.data_type(schema)? != timestamp
    {
        return Ok(None);
    }
    let Some(proto::expression::Expression::IntervalDayTimeLiteral(interval)) = &right.expression
    else {
        return Ok(None);
    };
    let integer = Arc::new(CastExpr::new(left, DataType::Int64, None));
    let interval = Arc::new(Literal::new(ScalarValue::Int64(Some(
        interval.milliseconds,
    ))));
    let result =
        Arc::new(BinaryExpr::new(integer, operator, interval).with_fail_on_overflow(false));
    Ok(Some(Arc::new(CastExpr::new(result, timestamp, None))))
}
