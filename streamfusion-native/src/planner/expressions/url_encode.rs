// Copyright 2026 StreamFusion Authors
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0

use std::hash::{Hash, Hasher};
use std::sync::Arc;

use arrow::array::StringArray;
use arrow::datatypes::{DataType, Field, FieldRef, Schema};
use arrow::record_batch::RecordBatch;
use datafusion::error::{DataFusionError, Result};
use datafusion::logical_expr::ColumnarValue;
use datafusion::physical_expr::PhysicalExpr;
use datafusion::scalar::ScalarValue;

#[derive(Debug, Eq)]
struct FlinkUrlEncodeExpr {
    value: Arc<dyn PhysicalExpr>,
}

impl PartialEq for FlinkUrlEncodeExpr {
    fn eq(&self, other: &Self) -> bool {
        self.value.eq(&other.value)
    }
}

impl Hash for FlinkUrlEncodeExpr {
    fn hash<H: Hasher>(&self, state: &mut H) {
        self.value.hash(state);
    }
}

impl std::fmt::Display for FlinkUrlEncodeExpr {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(formatter, "URL_ENCODE({})", self.value)
    }
}

impl PhysicalExpr for FlinkUrlEncodeExpr {
    fn data_type(&self, _input_schema: &Schema) -> Result<DataType> {
        Ok(DataType::Utf8)
    }

    fn nullable(&self, input_schema: &Schema) -> Result<bool> {
        self.value.nullable(input_schema)
    }

    fn evaluate(&self, batch: &RecordBatch) -> Result<ColumnarValue> {
        match self.value.evaluate(batch)? {
            ColumnarValue::Array(array) => {
                let strings = array
                    .as_any()
                    .downcast_ref::<StringArray>()
                    .ok_or_else(|| {
                        DataFusionError::Execution("URL_ENCODE expected Utf8 input".to_string())
                    })?;
                Ok(ColumnarValue::Array(Arc::new(StringArray::from_iter(
                    strings.iter().map(|value| value.map(flink_url_encode)),
                ))))
            }
            ColumnarValue::Scalar(ScalarValue::Utf8(value)) => Ok(ColumnarValue::Scalar(
                ScalarValue::Utf8(value.as_deref().map(flink_url_encode)),
            )),
            ColumnarValue::Scalar(value) => Err(DataFusionError::Execution(format!(
                "URL_ENCODE expected Utf8 scalar, got {}",
                value.data_type()
            ))),
        }
    }

    fn return_field(&self, input_schema: &Schema) -> Result<FieldRef> {
        let source = self.value.return_field(input_schema)?;
        Ok(Arc::new(Field::new(
            source.name(),
            DataType::Utf8,
            source.is_nullable(),
        )))
    }

    fn children(&self) -> Vec<&Arc<dyn PhysicalExpr>> {
        vec![&self.value]
    }

    fn with_new_children(
        self: Arc<Self>,
        children: Vec<Arc<dyn PhysicalExpr>>,
    ) -> Result<Arc<dyn PhysicalExpr>> {
        Ok(Arc::new(Self {
            value: Arc::clone(&children[0]),
        }))
    }

    fn fmt_sql(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(formatter, "URL_ENCODE(")?;
        self.value.fmt_sql(formatter)?;
        write!(formatter, ")")
    }
}

fn flink_url_encode(value: &str) -> String {
    const HEX: &[u8; 16] = b"0123456789ABCDEF";
    let mut encoded = String::with_capacity(value.len());
    for byte in value.bytes() {
        match byte {
            b'A'..=b'Z' | b'a'..=b'z' | b'0'..=b'9' | b'.' | b'-' | b'*' | b'_' => {
                encoded.push(char::from(byte));
            }
            b' ' => encoded.push('+'),
            _ => {
                encoded.push('%');
                encoded.push(char::from(HEX[usize::from(byte >> 4)]));
                encoded.push(char::from(HEX[usize::from(byte & 0x0f)]));
            }
        }
    }
    encoded
}

pub(crate) fn create(
    value: Arc<dyn PhysicalExpr>,
    schema: &Schema,
) -> Result<Arc<dyn PhysicalExpr>> {
    if value.data_type(schema)? != DataType::Utf8 {
        return Err(DataFusionError::Plan(
            "URL_ENCODE requires Arrow Utf8 input".to_string(),
        ));
    }
    Ok(Arc::new(FlinkUrlEncodeExpr { value }))
}

#[cfg(test)]
mod tests {
    use super::flink_url_encode;

    #[test]
    fn follows_java_form_encoding_rules() {
        assert_eq!(flink_url_encode(""), "");
        assert_eq!(flink_url_encode("a b+c"), "a+b%2Bc");
        assert_eq!(flink_url_encode(".-*_"), ".-*_");
        assert_eq!(flink_url_encode("你好😀"), "%E4%BD%A0%E5%A5%BD%F0%9F%98%80");
    }
}
