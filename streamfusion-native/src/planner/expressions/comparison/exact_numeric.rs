// Copyright 2026 StreamFusion Authors. Licensed under the Apache License, Version 2.0.

use super::*;
use datafusion::logical_expr::type_coercion::binary::comparison_coercion;
use datafusion::physical_expr::expressions::CastExpr;

pub(super) fn coerce(
    left: Arc<dyn PhysicalExpr>,
    right: Arc<dyn PhysicalExpr>,
    schema: &Schema,
) -> Result<(Arc<dyn PhysicalExpr>, Arc<dyn PhysicalExpr>)> {
    let left_type = left.data_type(schema)?;
    let right_type = right.data_type(schema)?;
    if !(matches!(left_type, DataType::Decimal128(_, _))
        || matches!(right_type, DataType::Decimal128(_, _)))
        || !exact(&left_type)
        || !exact(&right_type)
    {
        return Ok((left, right));
    }
    // Flink DecimalDataUtils.compare retains every fractional/integer digit.
    // DF's Decimal128 coercion caps common precision at 38. Ask its Decimal256
    // coercion first, then use Decimal128 whenever the complete range fits.
    // Flink's supported input types need at most 76 digits at a common scale.
    let common = comparison_coercion(&wide(&left_type), &wide(&right_type)).ok_or_else(|| {
        DataFusionError::Plan("exact comparison has no lossless common type".into())
    })?;
    let common = match common {
        DataType::Decimal256(precision, scale) if precision <= 38 => {
            DataType::Decimal128(precision, scale)
        }
        common => common,
    };
    let cast = |expression: Arc<dyn PhysicalExpr>, source: &DataType| {
        if *source == common {
            expression
        } else {
            Arc::new(CastExpr::new(expression, common.clone(), None)) as Arc<dyn PhysicalExpr>
        }
    };
    Ok((cast(left, &left_type), cast(right, &right_type)))
}

fn exact(data_type: &DataType) -> bool {
    match data_type {
        DataType::Int8 | DataType::Int16 | DataType::Int32 | DataType::Int64 => true,
        DataType::Decimal128(precision, scale) => {
            (1..=38).contains(precision) && *scale >= 0 && *scale <= *precision as i8
        }
        _ => false,
    }
}

fn wide(data_type: &DataType) -> DataType {
    match data_type {
        DataType::Decimal128(precision, scale) => DataType::Decimal256(*precision, *scale),
        other => other.clone(),
    }
}
