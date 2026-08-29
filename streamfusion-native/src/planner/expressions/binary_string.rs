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
struct FlinkBinaryStringExpr {
    value: Arc<dyn PhysicalExpr>,
}

impl PartialEq for FlinkBinaryStringExpr {
    fn eq(&self, other: &Self) -> bool {
        self.value.eq(&other.value)
    }
}

impl Hash for FlinkBinaryStringExpr {
    fn hash<H: Hasher>(&self, state: &mut H) {
        self.value.hash(state);
    }
}

impl std::fmt::Display for FlinkBinaryStringExpr {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(formatter, "BIN({})", self.value)
    }
}

impl PhysicalExpr for FlinkBinaryStringExpr {
    fn data_type(&self, _input_schema: &Schema) -> Result<DataType> {
        Ok(DataType::Utf8)
    }

    fn nullable(&self, input_schema: &Schema) -> Result<bool> {
        self.value.nullable(input_schema)
    }

    fn evaluate(&self, batch: &RecordBatch) -> Result<ColumnarValue> {
        match self.value.evaluate(batch)? {
            ColumnarValue::Array(array) => {
                let integers = array.as_any().downcast_ref::<Int64Array>().ok_or_else(|| {
                    DataFusionError::Execution("BIN expected Int64 input".to_string())
                })?;
                Ok(ColumnarValue::Array(Arc::new(StringArray::from_iter(
                    integers.iter().map(|value| value.map(binary_string)),
                ))))
            }
            ColumnarValue::Scalar(ScalarValue::Int64(value)) => Ok(ColumnarValue::Scalar(
                ScalarValue::Utf8(value.map(binary_string)),
            )),
            ColumnarValue::Scalar(value) => Err(DataFusionError::Execution(format!(
                "BIN expected Int64 scalar, got {}",
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
        write!(formatter, "BIN(")?;
        self.value.fmt_sql(formatter)?;
        write!(formatter, ")")
    }
}

fn binary_string(value: i64) -> String {
    format!("{value:b}")
}

pub(crate) fn create(
    value: Arc<dyn PhysicalExpr>,
    schema: &Schema,
) -> Result<Arc<dyn PhysicalExpr>> {
    let data_type = value.data_type(schema)?;
    if !matches!(
        data_type,
        DataType::Int8 | DataType::Int16 | DataType::Int32 | DataType::Int64
    ) {
        return Err(DataFusionError::Plan(format!(
            "BIN does not support Arrow type {data_type}"
        )));
    }
    Ok(Arc::new(FlinkBinaryStringExpr {
        value: Arc::new(CastExpr::new(value, DataType::Int64, None)),
    }))
}

#[cfg(test)]
mod tests {
    use super::binary_string;

    #[test]
    fn formats_signed_long_bits_like_flink() {
        assert_eq!(binary_string(0), "0");
        assert_eq!(binary_string(42), "101010");
        assert_eq!(binary_string(-1), "1".repeat(64));
        assert_eq!(binary_string(i64::MIN), format!("1{}", "0".repeat(63)));
    }
}
