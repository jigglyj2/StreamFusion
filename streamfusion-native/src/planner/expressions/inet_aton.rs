// Copyright 2026 StreamFusion Authors
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0

use std::hash::{Hash, Hasher};
use std::sync::Arc;

use arrow::array::{Int64Array, StringArray};
use arrow::datatypes::{DataType, Field, FieldRef, Schema};
use arrow::record_batch::RecordBatch;
use datafusion::error::{DataFusionError, Result};
use datafusion::logical_expr::ColumnarValue;
use datafusion::physical_expr::PhysicalExpr;
use datafusion::scalar::ScalarValue;

#[derive(Debug, Eq)]
struct InetAtonExpr {
    operand: Arc<dyn PhysicalExpr>,
}

impl PartialEq for InetAtonExpr {
    fn eq(&self, other: &Self) -> bool {
        self.operand.eq(&other.operand)
    }
}

impl Hash for InetAtonExpr {
    fn hash<H: Hasher>(&self, state: &mut H) {
        self.operand.hash(state);
    }
}

impl std::fmt::Display for InetAtonExpr {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(formatter, "INET_ATON({})", self.operand)
    }
}

impl PhysicalExpr for InetAtonExpr {
    fn data_type(&self, _input_schema: &Schema) -> Result<DataType> {
        Ok(DataType::Int64)
    }

    fn nullable(&self, _input_schema: &Schema) -> Result<bool> {
        Ok(true)
    }

    fn evaluate(&self, batch: &RecordBatch) -> Result<ColumnarValue> {
        match self.operand.evaluate(batch)? {
            ColumnarValue::Array(array) => {
                let strings = array
                    .as_any()
                    .downcast_ref::<StringArray>()
                    .ok_or_else(|| {
                        DataFusionError::Execution("INET_ATON expected Utf8 input".to_string())
                    })?;
                Ok(ColumnarValue::Array(Arc::new(Int64Array::from_iter(
                    strings.iter().map(|value| value.and_then(parse_ipv4)),
                ))))
            }
            ColumnarValue::Scalar(ScalarValue::Utf8(value)) => Ok(ColumnarValue::Scalar(
                ScalarValue::Int64(value.as_deref().and_then(parse_ipv4)),
            )),
            ColumnarValue::Scalar(value) => Err(DataFusionError::Execution(format!(
                "INET_ATON expected Utf8 scalar, got {}",
                value.data_type()
            ))),
        }
    }

    fn return_field(&self, input_schema: &Schema) -> Result<FieldRef> {
        let source = self.operand.return_field(input_schema)?;
        Ok(Arc::new(Field::new(source.name(), DataType::Int64, true)))
    }

    fn children(&self) -> Vec<&Arc<dyn PhysicalExpr>> {
        vec![&self.operand]
    }

    fn with_new_children(
        self: Arc<Self>,
        children: Vec<Arc<dyn PhysicalExpr>>,
    ) -> Result<Arc<dyn PhysicalExpr>> {
        Ok(Arc::new(Self {
            operand: Arc::clone(&children[0]),
        }))
    }

    fn fmt_sql(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(formatter, "INET_ATON(")?;
        self.operand.fmt_sql(formatter)?;
        write!(formatter, ")")
    }
}

fn parse_ipv4(value: &str) -> Option<i64> {
    if value.is_empty() {
        return None;
    }
    let bytes = value.as_bytes();
    let mut parts = [0_i64; 4];
    let mut part_count = 0;
    let mut part_start = 0;
    for index in 0..=bytes.len() {
        if index == bytes.len() || bytes[index] == b'.' {
            if part_count == 4 || part_start == index {
                return None;
            }
            let mut parsed = 0_i64;
            for byte in &bytes[part_start..index] {
                if !byte.is_ascii_digit() {
                    return None;
                }
                parsed = parsed * 10 + i64::from(byte - b'0');
                if parsed > 255 {
                    return None;
                }
            }
            parts[part_count] = parsed;
            part_count += 1;
            part_start = index + 1;
        }
    }
    match part_count {
        1 => Some(parts[0]),
        2 => Some((parts[0] << 24) | parts[1]),
        3 => Some((parts[0] << 24) | (parts[1] << 16) | parts[2]),
        4 => Some((parts[0] << 24) | (parts[1] << 16) | (parts[2] << 8) | parts[3]),
        _ => None,
    }
}

pub(crate) fn create(
    operand: Arc<dyn PhysicalExpr>,
    schema: &Schema,
) -> Result<Arc<dyn PhysicalExpr>> {
    if operand.data_type(schema)? != DataType::Utf8 {
        return Err(DataFusionError::Plan(
            "INET_ATON requires Arrow Utf8 input".to_string(),
        ));
    }
    Ok(Arc::new(InetAtonExpr { operand }))
}

#[cfg(test)]
mod tests {
    use super::parse_ipv4;

    #[test]
    fn matches_flinks_mysql_compatible_ipv4_grammar() {
        assert_eq!(parse_ipv4("1"), Some(1));
        assert_eq!(parse_ipv4("127.1"), Some(2_130_706_433));
        assert_eq!(parse_ipv4("127.0.1"), Some(2_130_706_433));
        assert_eq!(parse_ipv4("127.0.0.1"), Some(2_130_706_433));
        assert_eq!(parse_ipv4("010.000.000.001"), Some(167_772_161));
        assert_eq!(parse_ipv4("255.255.255.255"), Some(4_294_967_295));
        for invalid in [
            "",
            "256",
            "invalid",
            "256.0.0.1",
            "1.2.3.4.5",
            "1.2.3.",
            ".1.2.3",
            "1..2.3",
            " 127.0.0.1",
            "127.0.0.1 ",
        ] {
            assert_eq!(parse_ipv4(invalid), None, "{invalid}");
        }
    }
}
