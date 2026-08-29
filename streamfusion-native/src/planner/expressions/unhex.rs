// Copyright 2026 StreamFusion Authors
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0

use std::hash::{Hash, Hasher};
use std::sync::Arc;

use arrow::array::{BinaryArray, StringArray};
use arrow::datatypes::{DataType, Field, FieldRef, Schema};
use arrow::record_batch::RecordBatch;
use datafusion::error::{DataFusionError, Result};
use datafusion::logical_expr::ColumnarValue;
use datafusion::physical_expr::PhysicalExpr;
use datafusion::scalar::ScalarValue;

#[derive(Debug, Eq)]
struct FlinkUnhexExpr {
    value: Arc<dyn PhysicalExpr>,
}

impl PartialEq for FlinkUnhexExpr {
    fn eq(&self, other: &Self) -> bool {
        self.value.eq(&other.value)
    }
}

impl Hash for FlinkUnhexExpr {
    fn hash<H: Hasher>(&self, state: &mut H) {
        self.value.hash(state);
    }
}

impl std::fmt::Display for FlinkUnhexExpr {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(formatter, "UNHEX({})", self.value)
    }
}

impl PhysicalExpr for FlinkUnhexExpr {
    fn data_type(&self, _input_schema: &Schema) -> Result<DataType> {
        Ok(DataType::Binary)
    }

    fn nullable(&self, _input_schema: &Schema) -> Result<bool> {
        // Invalid hexadecimal input also produces null.
        Ok(true)
    }

    fn evaluate(&self, batch: &RecordBatch) -> Result<ColumnarValue> {
        match self.value.evaluate(batch)? {
            ColumnarValue::Array(array) => {
                let strings = array
                    .as_any()
                    .downcast_ref::<StringArray>()
                    .ok_or_else(|| {
                        DataFusionError::Execution("UNHEX expected Utf8 input".to_string())
                    })?;
                let decoded = strings
                    .iter()
                    .map(|value| value.and_then(flink_unhex))
                    .collect::<Vec<_>>();
                Ok(ColumnarValue::Array(Arc::new(BinaryArray::from_iter(
                    decoded.iter().map(|value| value.as_deref()),
                ))))
            }
            ColumnarValue::Scalar(ScalarValue::Utf8(value)) => Ok(ColumnarValue::Scalar(
                ScalarValue::Binary(value.as_deref().and_then(flink_unhex)),
            )),
            ColumnarValue::Scalar(value) => Err(DataFusionError::Execution(format!(
                "UNHEX expected Utf8 scalar, got {}",
                value.data_type()
            ))),
        }
    }

    fn return_field(&self, input_schema: &Schema) -> Result<FieldRef> {
        let source = self.value.return_field(input_schema)?;
        Ok(Arc::new(Field::new(source.name(), DataType::Binary, true)))
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
        write!(formatter, "UNHEX(")?;
        self.value.fmt_sql(formatter)?;
        write!(formatter, ")")
    }
}

fn flink_unhex(value: &str) -> Option<Vec<u8>> {
    let bytes = value.as_bytes();
    let mut output = vec![0; bytes.len().div_ceil(2)];
    let mut input_index = bytes.len().saturating_sub(2);
    let mut output_index = output.len();
    while input_index < bytes.len().saturating_sub(1) {
        let left = hex_digit(bytes[input_index])?;
        let right = hex_digit(bytes[input_index + 1])?;
        output_index -= 1;
        output[output_index] = (left << 4) | right;
        if input_index < 2 {
            break;
        }
        input_index -= 2;
    }
    if bytes.len() % 2 == 1 {
        hex_digit(bytes[0])?;
    }
    Some(output)
}

fn hex_digit(value: u8) -> Option<u8> {
    match value {
        b'0'..=b'9' => Some(value - b'0'),
        b'a'..=b'f' => Some(value - b'a' + 10),
        b'A'..=b'F' => Some(value - b'A' + 10),
        _ => None,
    }
}

pub(crate) fn create(
    value: Arc<dyn PhysicalExpr>,
    schema: &Schema,
) -> Result<Arc<dyn PhysicalExpr>> {
    if value.data_type(schema)? != DataType::Utf8 {
        return Err(DataFusionError::Plan(
            "UNHEX requires Arrow Utf8 input".to_string(),
        ));
    }
    Ok(Arc::new(FlinkUnhexExpr { value }))
}

#[cfg(test)]
mod tests {
    use super::flink_unhex;

    #[test]
    fn preserves_flinks_odd_length_and_invalid_input_rules() {
        assert_eq!(flink_unhex(""), Some(Vec::new()));
        assert_eq!(flink_unhex("1"), Some(vec![0]));
        assert_eq!(flink_unhex("146"), Some(vec![0, 0x46]));
        assert_eq!(flink_unhex("466C696E6B"), Some(b"Flink".to_vec()));
        assert_eq!(flink_unhex("z"), None);
        assert_eq!(flink_unhex("1-"), None);
        assert_eq!(flink_unhex("😀"), None);
    }
}
