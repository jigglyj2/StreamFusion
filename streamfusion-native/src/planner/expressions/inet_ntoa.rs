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
use datafusion::physical_expr::expressions::CastExpr;
use datafusion::physical_expr::PhysicalExpr;
use datafusion::scalar::ScalarValue;

#[derive(Debug, Eq)]
struct InetNtoaExpr {
    operand: Arc<dyn PhysicalExpr>,
}

impl PartialEq for InetNtoaExpr {
    fn eq(&self, other: &Self) -> bool {
        self.operand.eq(&other.operand)
    }
}

impl Hash for InetNtoaExpr {
    fn hash<H: Hasher>(&self, state: &mut H) {
        self.operand.hash(state);
    }
}

impl std::fmt::Display for InetNtoaExpr {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(formatter, "INET_NTOA({})", self.operand)
    }
}

impl PhysicalExpr for InetNtoaExpr {
    fn data_type(&self, _input_schema: &Schema) -> Result<DataType> {
        Ok(DataType::Utf8)
    }

    fn nullable(&self, _input_schema: &Schema) -> Result<bool> {
        Ok(true)
    }

    fn evaluate(&self, batch: &RecordBatch) -> Result<ColumnarValue> {
        match self.operand.evaluate(batch)? {
            ColumnarValue::Array(array) => {
                let integers = array.as_any().downcast_ref::<Int64Array>().ok_or_else(|| {
                    DataFusionError::Execution("INET_NTOA expected Int64 input".to_string())
                })?;
                let formatted = integers
                    .iter()
                    .map(|value| value.and_then(format_ipv4))
                    .collect::<Vec<_>>();
                Ok(ColumnarValue::Array(Arc::new(StringArray::from_iter(
                    formatted.iter().map(|value| value.as_deref()),
                ))))
            }
            ColumnarValue::Scalar(ScalarValue::Int64(value)) => Ok(ColumnarValue::Scalar(
                ScalarValue::Utf8(value.and_then(format_ipv4)),
            )),
            ColumnarValue::Scalar(value) => Err(DataFusionError::Execution(format!(
                "INET_NTOA expected Int64 scalar, got {}",
                value.data_type()
            ))),
        }
    }

    fn return_field(&self, input_schema: &Schema) -> Result<FieldRef> {
        let source = self.operand.return_field(input_schema)?;
        Ok(Arc::new(Field::new(source.name(), DataType::Utf8, true)))
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
        write!(formatter, "INET_NTOA(")?;
        self.operand.fmt_sql(formatter)?;
        write!(formatter, ")")
    }
}

fn format_ipv4(value: i64) -> Option<String> {
    let value = u32::try_from(value).ok()?;
    Some(format!(
        "{}.{}.{}.{}",
        value >> 24,
        (value >> 16) & 0xff,
        (value >> 8) & 0xff,
        value & 0xff
    ))
}

pub(crate) fn create(
    operand: Arc<dyn PhysicalExpr>,
    schema: &Schema,
) -> Result<Arc<dyn PhysicalExpr>> {
    let operand = match operand.data_type(schema)? {
        DataType::Int8 | DataType::Int16 | DataType::Int32 => {
            Arc::new(CastExpr::new(operand, DataType::Int64, None)) as Arc<dyn PhysicalExpr>
        }
        DataType::Int64 => operand,
        other => {
            return Err(DataFusionError::Plan(format!(
                "INET_NTOA requires signed integer input, got {other}"
            )))
        }
    };
    Ok(Arc::new(InetNtoaExpr { operand }))
}

#[cfg(test)]
mod tests {
    use super::format_ipv4;

    #[test]
    fn matches_flinks_unsigned_ipv4_range() {
        assert_eq!(format_ipv4(0).as_deref(), Some("0.0.0.0"));
        assert_eq!(format_ipv4(1).as_deref(), Some("0.0.0.1"));
        assert_eq!(format_ipv4(16_777_216).as_deref(), Some("1.0.0.0"));
        assert_eq!(format_ipv4(2_130_706_433).as_deref(), Some("127.0.0.1"));
        assert_eq!(
            format_ipv4(4_294_967_295).as_deref(),
            Some("255.255.255.255")
        );
        assert_eq!(format_ipv4(-1), None);
        assert_eq!(format_ipv4(4_294_967_296), None);
    }
}
