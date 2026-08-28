// Copyright 2026 StreamFusion Authors
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0

use std::hash::{Hash, Hasher};
use std::sync::Arc;

use arrow::array::{Int16Array, Int32Array};
use arrow::datatypes::{DataType, Field, FieldRef, Schema};
use arrow::record_batch::RecordBatch;
use datafusion::error::{DataFusionError, Result};
use datafusion::logical_expr::ColumnarValue;
use datafusion::physical_expr::PhysicalExpr;
use datafusion::scalar::ScalarValue;

#[derive(Debug, Eq)]
pub(crate) struct Int32ToInt16WrappingExpr {
    operand: Arc<dyn PhysicalExpr>,
}

impl Int32ToInt16WrappingExpr {
    pub(crate) fn new(operand: Arc<dyn PhysicalExpr>) -> Self {
        Self { operand }
    }
}

impl PartialEq for Int32ToInt16WrappingExpr {
    fn eq(&self, other: &Self) -> bool {
        self.operand.eq(&other.operand)
    }
}

impl Hash for Int32ToInt16WrappingExpr {
    fn hash<H: Hasher>(&self, state: &mut H) {
        self.operand.hash(state);
    }
}

impl std::fmt::Display for Int32ToInt16WrappingExpr {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(formatter, "CAST_WRAPPING({} AS Int16)", self.operand)
    }
}

impl PhysicalExpr for Int32ToInt16WrappingExpr {
    fn data_type(&self, _input_schema: &Schema) -> Result<DataType> {
        Ok(DataType::Int16)
    }

    fn nullable(&self, input_schema: &Schema) -> Result<bool> {
        self.operand.nullable(input_schema)
    }

    fn evaluate(&self, batch: &RecordBatch) -> Result<ColumnarValue> {
        match self.operand.evaluate(batch)? {
            ColumnarValue::Array(array) => {
                let integers = array.as_any().downcast_ref::<Int32Array>().ok_or_else(|| {
                    DataFusionError::Execution("wrapping cast expected Int32 input".to_string())
                })?;
                let result = Int16Array::from_iter(
                    integers.iter().map(|value| value.map(|value| value as i16)),
                );
                Ok(ColumnarValue::Array(Arc::new(result)))
            }
            ColumnarValue::Scalar(ScalarValue::Int32(value)) => Ok(ColumnarValue::Scalar(
                ScalarValue::Int16(value.map(|value| value as i16)),
            )),
            ColumnarValue::Scalar(value) => Err(DataFusionError::Execution(format!(
                "wrapping cast expected Int32 scalar, got {}",
                value.data_type()
            ))),
        }
    }

    fn return_field(&self, input_schema: &Schema) -> Result<FieldRef> {
        let source = self.operand.return_field(input_schema)?;
        Ok(Arc::new(Field::new(
            source.name(),
            DataType::Int16,
            source.is_nullable(),
        )))
    }

    fn children(&self) -> Vec<&Arc<dyn PhysicalExpr>> {
        vec![&self.operand]
    }

    fn with_new_children(
        self: Arc<Self>,
        children: Vec<Arc<dyn PhysicalExpr>>,
    ) -> Result<Arc<dyn PhysicalExpr>> {
        Ok(Arc::new(Self::new(Arc::clone(&children[0]))))
    }

    fn fmt_sql(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(formatter, "CAST(")?;
        self.operand.fmt_sql(formatter)?;
        write!(formatter, " AS SMALLINT)")
    }
}
