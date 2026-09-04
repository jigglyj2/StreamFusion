// Copyright 2026 StreamFusion Authors
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0

use std::hash::{Hash, Hasher};
use std::sync::Arc;

use arrow::array::{Int16Array, Int32Array, Int64Array, Int8Array, StringArray};
use arrow::datatypes::{DataType, Field, FieldRef, Schema};
use arrow::record_batch::RecordBatch;
use datafusion::error::{DataFusionError, Result};
use datafusion::logical_expr::ColumnarValue;
use datafusion::physical_expr::PhysicalExpr;
use datafusion::scalar::ScalarValue;

#[derive(Debug, Eq)]
struct FlinkStringHashCodeExpr {
    value: Arc<dyn PhysicalExpr>,
}

impl PartialEq for FlinkStringHashCodeExpr {
    fn eq(&self, other: &Self) -> bool {
        self.value.eq(&other.value)
    }
}

impl Hash for FlinkStringHashCodeExpr {
    fn hash<H: Hasher>(&self, state: &mut H) {
        self.value.hash(state);
    }
}

impl std::fmt::Display for FlinkStringHashCodeExpr {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(formatter, "HASH_CODE({})", self.value)
    }
}

impl PhysicalExpr for FlinkStringHashCodeExpr {
    fn data_type(&self, _input_schema: &Schema) -> Result<DataType> {
        Ok(DataType::Int32)
    }

    fn nullable(&self, input_schema: &Schema) -> Result<bool> {
        self.value.nullable(input_schema)
    }

    fn evaluate(&self, batch: &RecordBatch) -> Result<ColumnarValue> {
        match self.value.evaluate(batch)? {
            ColumnarValue::Array(array) => {
                let hashes = match array.data_type() {
                    DataType::Utf8 => Int32Array::from_iter(
                        array
                            .as_any()
                            .downcast_ref::<StringArray>()
                            .expect("Utf8 array")
                            .iter()
                            .map(|value| value.map(flink_string_hash_code)),
                    ),
                    DataType::Int8 => Int32Array::from_iter(
                        array
                            .as_any()
                            .downcast_ref::<Int8Array>()
                            .expect("Int8 array")
                            .iter()
                            .map(|value| value.map(i32::from)),
                    ),
                    DataType::Int16 => Int32Array::from_iter(
                        array
                            .as_any()
                            .downcast_ref::<Int16Array>()
                            .expect("Int16 array")
                            .iter()
                            .map(|value| value.map(i32::from)),
                    ),
                    DataType::Int32 => Int32Array::from_iter(
                        array
                            .as_any()
                            .downcast_ref::<Int32Array>()
                            .expect("Int32 array")
                            .iter(),
                    ),
                    DataType::Int64 => Int32Array::from_iter(
                        array
                            .as_any()
                            .downcast_ref::<Int64Array>()
                            .expect("Int64 array")
                            .iter()
                            .map(|value| value.map(flink_long_hash_code)),
                    ),
                    other => {
                        return Err(DataFusionError::Execution(format!(
                            "HASH_CODE does not support {other} input"
                        )))
                    }
                };
                Ok(ColumnarValue::Array(Arc::new(hashes)))
            }
            ColumnarValue::Scalar(ScalarValue::Utf8(value)) => Ok(ColumnarValue::Scalar(
                ScalarValue::Int32(value.as_deref().map(flink_string_hash_code)),
            )),
            ColumnarValue::Scalar(ScalarValue::Int8(value)) => Ok(ColumnarValue::Scalar(
                ScalarValue::Int32(value.map(i32::from)),
            )),
            ColumnarValue::Scalar(ScalarValue::Int16(value)) => Ok(ColumnarValue::Scalar(
                ScalarValue::Int32(value.map(i32::from)),
            )),
            ColumnarValue::Scalar(ScalarValue::Int32(value)) => {
                Ok(ColumnarValue::Scalar(ScalarValue::Int32(value)))
            }
            ColumnarValue::Scalar(ScalarValue::Int64(value)) => Ok(ColumnarValue::Scalar(
                ScalarValue::Int32(value.map(flink_long_hash_code)),
            )),
            ColumnarValue::Scalar(value) => Err(DataFusionError::Execution(format!(
                "HASH_CODE expected Utf8 scalar, got {}",
                value.data_type()
            ))),
        }
    }

    fn return_field(&self, input_schema: &Schema) -> Result<FieldRef> {
        let source = self.value.return_field(input_schema)?;
        Ok(Arc::new(Field::new(
            source.name(),
            DataType::Int32,
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
        write!(formatter, "HASH_CODE(")?;
        self.value.fmt_sql(formatter)?;
        write!(formatter, ")")
    }
}

fn flink_string_hash_code(value: &str) -> i32 {
    value
        .encode_utf16()
        .fold(0_i32, |hash, unit| {
            hash.wrapping_mul(31).wrapping_add(i32::from(unit))
        })
        .wrapping_abs()
}

fn flink_long_hash_code(value: i64) -> i32 {
    (value ^ ((value as u64 >> 32) as i64)) as i32
}

pub(crate) fn create(
    value: Arc<dyn PhysicalExpr>,
    schema: &Schema,
) -> Result<Arc<dyn PhysicalExpr>> {
    if !matches!(
        value.data_type(schema)?,
        DataType::Utf8 | DataType::Int8 | DataType::Int16 | DataType::Int32 | DataType::Int64
    ) {
        return Err(DataFusionError::Plan(
            "HASH_CODE requires Arrow Utf8 or signed integral input".to_string(),
        ));
    }
    Ok(Arc::new(FlinkStringHashCodeExpr { value }))
}

#[cfg(test)]
mod tests {
    use super::{flink_long_hash_code, flink_string_hash_code};

    #[test]
    fn matches_java_utf16_hash_and_absolute_value() {
        assert_eq!(flink_string_hash_code(""), 0);
        assert_eq!(flink_string_hash_code("abc"), 96_354);
        assert_eq!(flink_string_hash_code("😀"), 1_772_899);
        assert_eq!(flink_string_hash_code("polygenelubricants"), i32::MIN);
    }

    #[test]
    fn matches_java_long_hash_code() {
        assert_eq!(flink_long_hash_code(0), 0);
        assert_eq!(flink_long_hash_code(1), 1);
        assert_eq!(flink_long_hash_code(i64::MAX), -2_147_483_648);
        assert_eq!(flink_long_hash_code(i64::MIN), -2_147_483_648);
    }
}
