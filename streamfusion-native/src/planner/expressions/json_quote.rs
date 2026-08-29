// Copyright 2026 StreamFusion Authors
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0

use std::fmt::Write;
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
struct FlinkJsonQuoteExpr {
    value: Arc<dyn PhysicalExpr>,
}

impl PartialEq for FlinkJsonQuoteExpr {
    fn eq(&self, other: &Self) -> bool {
        self.value.eq(&other.value)
    }
}

impl Hash for FlinkJsonQuoteExpr {
    fn hash<H: Hasher>(&self, state: &mut H) {
        self.value.hash(state);
    }
}

impl std::fmt::Display for FlinkJsonQuoteExpr {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(formatter, "JSON_QUOTE({})", self.value)
    }
}

impl PhysicalExpr for FlinkJsonQuoteExpr {
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
                        DataFusionError::Execution("JSON_QUOTE expected Utf8 input".to_string())
                    })?;
                Ok(ColumnarValue::Array(Arc::new(StringArray::from_iter(
                    strings.iter().map(|value| value.map(flink_json_quote)),
                ))))
            }
            ColumnarValue::Scalar(ScalarValue::Utf8(value)) => Ok(ColumnarValue::Scalar(
                ScalarValue::Utf8(value.as_deref().map(flink_json_quote)),
            )),
            ColumnarValue::Scalar(value) => Err(DataFusionError::Execution(format!(
                "JSON_QUOTE expected Utf8 scalar, got {}",
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
        write!(formatter, "JSON_QUOTE(")?;
        self.value.fmt_sql(formatter)?;
        write!(formatter, ")")
    }
}

fn flink_json_quote(value: &str) -> String {
    let units = value.encode_utf16().collect::<Vec<_>>();
    let mut quoted = String::with_capacity(value.len() + 2);
    quoted.push('"');
    for (index, unit) in units.iter().copied().enumerate() {
        let code_point = if (0xd800..=0xdbff).contains(&unit)
            && units
                .get(index + 1)
                .is_some_and(|next| (0xdc00..=0xdfff).contains(next))
        {
            0x10000 + ((u32::from(unit) - 0xd800) << 10) + (u32::from(units[index + 1]) - 0xdc00)
        } else {
            u32::from(unit)
        };
        match code_point {
            0x00..=0x7f => append_ascii(&mut quoted, code_point as u8),
            _ => write!(quoted, "\\u{code_point:04x}").expect("writing to String cannot fail"),
        }
    }
    quoted.push('"');
    quoted
}

fn append_ascii(output: &mut String, value: u8) {
    match value {
        b'"' => output.push_str("\\\""),
        b'\\' => output.push_str("\\\\"),
        b'/' => output.push_str("\\/"),
        0x08 => output.push_str("\\b"),
        0x0c => output.push_str("\\f"),
        b'\n' => output.push_str("\\n"),
        b'\r' => output.push_str("\\r"),
        b'\t' => output.push_str("\\t"),
        _ => output.push(char::from(value)),
    }
}

pub(crate) fn create(
    value: Arc<dyn PhysicalExpr>,
    schema: &Schema,
) -> Result<Arc<dyn PhysicalExpr>> {
    if value.data_type(schema)? != DataType::Utf8 {
        return Err(DataFusionError::Plan(
            "JSON_QUOTE requires Arrow Utf8 input".to_string(),
        ));
    }
    Ok(Arc::new(FlinkJsonQuoteExpr { value }))
}

#[cfg(test)]
mod tests {
    use super::flink_json_quote;

    #[test]
    fn preserves_flinks_ascii_unicode_and_utf16_rules() {
        assert_eq!(flink_json_quote("null"), "\"null\"");
        assert_eq!(flink_json_quote("\"\\/\n\t"), "\"\\\"\\\\\\/\\n\\t\"");
        assert_eq!(flink_json_quote("≠"), "\"\\u2260\"");
        assert_eq!(flink_json_quote("😀"), "\"\\u1f600\\ude00\"");
    }
}
