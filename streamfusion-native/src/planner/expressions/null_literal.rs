// Copyright 2026 StreamFusion Authors
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0

use std::sync::Arc;

use arrow::datatypes::{DataType, Field, Fields, TimeUnit};
use datafusion::error::{DataFusionError, Result};

use crate::proto;

pub(crate) fn data_type(logical_type: &proto::LogicalType) -> Result<DataType> {
    match logical_type.r#type.as_ref() {
        Some(proto::logical_type::Type::Tinyint(_)) => Ok(DataType::Int8),
        Some(proto::logical_type::Type::Smallint(_)) => Ok(DataType::Int16),
        Some(proto::logical_type::Type::Integer(_)) => Ok(DataType::Int32),
        Some(proto::logical_type::Type::Bigint(_)) => Ok(DataType::Int64),
        Some(proto::logical_type::Type::Float(_)) => Ok(DataType::Float32),
        Some(proto::logical_type::Type::Double(_)) => Ok(DataType::Float64),
        Some(proto::logical_type::Type::Boolean(_)) => Ok(DataType::Boolean),
        Some(proto::logical_type::Type::Varchar(_)) => Ok(DataType::Utf8),
        Some(proto::logical_type::Type::FixedChar(length)) => {
            validate_length("CHAR", length.length)?;
            Ok(DataType::Utf8)
        }
        Some(proto::logical_type::Type::Binary(_)) => Ok(DataType::Binary),
        Some(proto::logical_type::Type::FixedBinary(length)) => {
            let length = validate_length("BINARY", length.length)?;
            Ok(DataType::FixedSizeBinary(length))
        }
        Some(proto::logical_type::Type::Date(_)) => Ok(DataType::Date32),
        Some(proto::logical_type::Type::Time(precision)) => time_type(precision.precision),
        Some(proto::logical_type::Type::Timestamp(precision))
        | Some(proto::logical_type::Type::TimestampLtz(precision)) => {
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
        Some(proto::logical_type::Type::Array(array)) => {
            let element_type = required_type("ARRAY element", array.element_type.as_deref())?;
            Ok(DataType::List(Arc::new(Field::new(
                "$data$",
                data_type(element_type)?,
                element_type.nullable,
            ))))
        }
        Some(proto::logical_type::Type::Map(map)) => {
            let key_type = required_type("MAP key", map.key_type.as_deref())?;
            let value_type = required_type("MAP value", map.value_type.as_deref())?;
            let entries = DataType::Struct(Fields::from(vec![
                Field::new("key", data_type(key_type)?, false),
                Field::new("value", data_type(value_type)?, value_type.nullable),
            ]));
            Ok(DataType::Map(
                Arc::new(Field::new("entries", entries, false)),
                false,
            ))
        }
        Some(proto::logical_type::Type::Row(row)) => {
            let fields = row
                .fields
                .iter()
                .map(|field| {
                    let field_type = required_type("ROW field", field.r#type.as_ref())?;
                    Ok(Field::new(
                        &field.name,
                        data_type(field_type)?,
                        field_type.nullable,
                    ))
                })
                .collect::<Result<Vec<_>>>()?;
            Ok(DataType::Struct(Fields::from(fields)))
        }
        _ => Err(DataFusionError::Plan(
            "NULL literal type is not supported".to_string(),
        )),
    }
}

fn required_type<'a>(
    name: &str,
    logical_type: Option<&'a proto::LogicalType>,
) -> Result<&'a proto::LogicalType> {
    logical_type.ok_or_else(|| DataFusionError::Plan(format!("{name} type is missing")))
}

fn validate_length(name: &str, length: u32) -> Result<i32> {
    let length = i32::try_from(length)
        .map_err(|_| DataFusionError::Plan(format!("{name} length {length} is invalid")))?;
    if length <= 0 {
        return Err(DataFusionError::Plan(format!(
            "{name} length {length} is invalid"
        )));
    }
    Ok(length)
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

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn maps_flink_local_zoned_timestamps_for_exchange_schemas() {
        let logical_type = proto::LogicalType {
            nullable: true,
            r#type: Some(proto::logical_type::Type::TimestampLtz(
                proto::PrecisionType { precision: 6 },
            )),
        };

        assert_eq!(
            data_type(&logical_type).unwrap(),
            DataType::Timestamp(TimeUnit::Microsecond, None)
        );
    }
}
