// Copyright 2026 StreamFusion Authors
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0

use std::sync::Arc;

use arrow::datatypes::{DataType, TimeUnit};
use datafusion::error::{DataFusionError, Result};
use datafusion::physical_expr::expressions::Literal;
use datafusion::physical_expr::PhysicalExpr;
use datafusion::scalar::ScalarValue;

use crate::proto;

pub(crate) fn create(literal: &proto::NullLiteral) -> Result<Arc<dyn PhysicalExpr>> {
    let logical_type = literal
        .r#type
        .as_ref()
        .ok_or_else(|| DataFusionError::Plan("NULL literal has no declared type".to_string()))?;
    let data_type = data_type(logical_type)?;
    Ok(Arc::new(Literal::new(ScalarValue::try_new_null(
        &data_type,
    )?)))
}

fn data_type(logical_type: &proto::LogicalType) -> Result<DataType> {
    match logical_type.r#type.as_ref() {
        Some(proto::logical_type::Type::Tinyint(_)) => Ok(DataType::Int8),
        Some(proto::logical_type::Type::Smallint(_)) => Ok(DataType::Int16),
        Some(proto::logical_type::Type::Integer(_)) => Ok(DataType::Int32),
        Some(proto::logical_type::Type::Bigint(_)) => Ok(DataType::Int64),
        Some(proto::logical_type::Type::Float(_)) => Ok(DataType::Float32),
        Some(proto::logical_type::Type::Double(_)) => Ok(DataType::Float64),
        Some(proto::logical_type::Type::Boolean(_)) => Ok(DataType::Boolean),
        Some(proto::logical_type::Type::Varchar(_)) => Ok(DataType::Utf8),
        Some(proto::logical_type::Type::Binary(_)) => Ok(DataType::Binary),
        Some(proto::logical_type::Type::Date(_)) => Ok(DataType::Date32),
        Some(proto::logical_type::Type::Time(precision)) => time_type(precision.precision),
        Some(proto::logical_type::Type::Timestamp(precision)) => {
            Ok(DataType::Timestamp(time_unit(precision.precision)?, None))
        }
        Some(proto::logical_type::Type::Decimal(decimal)) => {
            let precision = u8::try_from(decimal.precision).map_err(|_| {
                DataFusionError::Plan(format!(
                    "DECIMAL precision {} is invalid",
                    decimal.precision
                ))
            })?;
            let scale = i8::try_from(decimal.scale).map_err(|_| {
                DataFusionError::Plan(format!("DECIMAL scale {} is invalid", decimal.scale))
            })?;
            if precision == 0 || precision > 38 || scale < 0 || scale > precision as i8 {
                return Err(DataFusionError::Plan(format!(
                    "DECIMAL({precision}, {scale}) is outside Flink's supported range"
                )));
            }
            Ok(DataType::Decimal128(precision, scale))
        }
        _ => Err(DataFusionError::Plan(
            "NULL literal type is not supported".to_string(),
        )),
    }
}

fn time_type(precision: u32) -> Result<DataType> {
    match time_unit(precision)? {
        TimeUnit::Second => Ok(DataType::Time32(TimeUnit::Second)),
        TimeUnit::Millisecond => Ok(DataType::Time32(TimeUnit::Millisecond)),
        TimeUnit::Microsecond => Ok(DataType::Time64(TimeUnit::Microsecond)),
        TimeUnit::Nanosecond => Ok(DataType::Time64(TimeUnit::Nanosecond)),
    }
}

fn time_unit(precision: u32) -> Result<TimeUnit> {
    match precision {
        0 => Ok(TimeUnit::Second),
        1..=3 => Ok(TimeUnit::Millisecond),
        4..=6 => Ok(TimeUnit::Microsecond),
        7..=9 => Ok(TimeUnit::Nanosecond),
        _ => Err(DataFusionError::Plan(format!(
            "temporal precision {precision} is outside Flink's supported range"
        ))),
    }
}
