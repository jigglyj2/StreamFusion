// Copyright 2026 StreamFusion Authors
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
// http://www.apache.org/licenses/LICENSE-2.0

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
struct FlinkChrExpr {
    value: Arc<dyn PhysicalExpr>,
}

impl PartialEq for FlinkChrExpr {
    fn eq(&self, other: &Self) -> bool {
        self.value.eq(&other.value)
    }
}

impl Hash for FlinkChrExpr {
    fn hash<H: Hasher>(&self, state: &mut H) {
        self.value.hash(state);
    }
}

impl std::fmt::Display for FlinkChrExpr {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(formatter, "CHR({})", self.value)
    }
}

impl PhysicalExpr for FlinkChrExpr {
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
                    DataFusionError::Execution("CHR expected Int64 input".to_string())
                })?;
                Ok(ColumnarValue::Array(Arc::new(StringArray::from_iter(
                    integers.iter().map(|value| value.map(flink_chr)),
                ))))
            }
            ColumnarValue::Scalar(ScalarValue::Int64(value)) => Ok(ColumnarValue::Scalar(
                ScalarValue::Utf8(value.map(flink_chr)),
            )),
            ColumnarValue::Scalar(value) => Err(DataFusionError::Execution(format!(
                "CHR expected Int64 scalar, got {}",
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
        write!(formatter, "CHR(")?;
        self.value.fmt_sql(formatter)?;
        write!(formatter, ")")
    }
}

fn flink_chr(value: i64) -> String {
    if value < 0 {
        String::new()
    } else {
        char::from_u32((value & 0xff) as u32)
            .expect("the low byte is a Unicode scalar")
            .to_string()
    }
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
            "CHR does not support Arrow type {data_type}"
        )));
    }
    Ok(Arc::new(FlinkChrExpr {
        value: Arc::new(CastExpr::new(value, DataType::Int64, None)),
    }))
}
