// Copyright 2026 StreamFusion Authors
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0

use std::hash::{Hash, Hasher};
use std::sync::Arc;

use arrow::array::{Int32Array, Int64Array};
use arrow::datatypes::{DataType, Field, FieldRef, Schema};
use arrow::record_batch::RecordBatch;
use datafusion::error::{DataFusionError, Result};
use datafusion::logical_expr::ColumnarValue;
use datafusion::physical_expr::expressions::CastExpr;
use datafusion::physical_expr::PhysicalExpr;

#[derive(Debug, Eq)]
struct FlinkIntegerTruncateExpr {
    value: Arc<dyn PhysicalExpr>,
    scale: Arc<dyn PhysicalExpr>,
}

impl PartialEq for FlinkIntegerTruncateExpr {
    fn eq(&self, other: &Self) -> bool {
        self.value.eq(&other.value) && self.scale.eq(&other.scale)
    }
}

impl Hash for FlinkIntegerTruncateExpr {
    fn hash<H: Hasher>(&self, state: &mut H) {
        self.value.hash(state);
        self.scale.hash(state);
    }
}

impl std::fmt::Display for FlinkIntegerTruncateExpr {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(formatter, "TRUNCATE({}, {})", self.value, self.scale)
    }
}

impl PhysicalExpr for FlinkIntegerTruncateExpr {
    fn data_type(&self, _input_schema: &Schema) -> Result<DataType> {
        Ok(DataType::Int64)
    }

    fn nullable(&self, input_schema: &Schema) -> Result<bool> {
        Ok(self.value.nullable(input_schema)? || self.scale.nullable(input_schema)?)
    }

    fn evaluate(&self, batch: &RecordBatch) -> Result<ColumnarValue> {
        let values = self.value.evaluate(batch)?.into_array(batch.num_rows())?;
        let scales = self.scale.evaluate(batch)?.into_array(batch.num_rows())?;
        let values = values
            .as_any()
            .downcast_ref::<Int64Array>()
            .ok_or_else(|| {
                DataFusionError::Execution("TRUNCATE expected Int64 input".to_string())
            })?;
        let scales = scales
            .as_any()
            .downcast_ref::<Int32Array>()
            .ok_or_else(|| {
                DataFusionError::Execution("TRUNCATE expected Int32 scale".to_string())
            })?;
        Ok(ColumnarValue::Array(Arc::new(Int64Array::from_iter(
            values.iter().zip(scales.iter()).map(|(value, scale)| {
                value
                    .zip(scale)
                    .map(|(value, scale)| truncate(value, scale))
            }),
        ))))
    }

    fn return_field(&self, input_schema: &Schema) -> Result<FieldRef> {
        let source = self.value.return_field(input_schema)?;
        Ok(Arc::new(Field::new(
            source.name(),
            DataType::Int64,
            self.nullable(input_schema)?,
        )))
    }

    fn children(&self) -> Vec<&Arc<dyn PhysicalExpr>> {
        vec![&self.value, &self.scale]
    }

    fn with_new_children(
        self: Arc<Self>,
        children: Vec<Arc<dyn PhysicalExpr>>,
    ) -> Result<Arc<dyn PhysicalExpr>> {
        Ok(Arc::new(Self {
            value: Arc::clone(&children[0]),
            scale: Arc::clone(&children[1]),
        }))
    }

    fn fmt_sql(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(formatter, "TRUNCATE(")?;
        self.value.fmt_sql(formatter)?;
        write!(formatter, ", ")?;
        self.scale.fmt_sql(formatter)?;
        write!(formatter, ")")
    }
}

fn truncate(value: i64, scale: i32) -> i64 {
    if scale >= 0 {
        return value;
    }
    let mut factor = 1_i64;
    for _ in 0..scale.unsigned_abs() {
        let Some(next) = factor.checked_mul(10) else {
            return 0;
        };
        factor = next;
    }
    value / factor * factor
}

pub(crate) fn create(
    value: Arc<dyn PhysicalExpr>,
    scale: Arc<dyn PhysicalExpr>,
    schema: &Schema,
) -> Result<Arc<dyn PhysicalExpr>> {
    let result_type = value.data_type(schema)?;
    if !matches!(
        result_type,
        DataType::Int8 | DataType::Int16 | DataType::Int32 | DataType::Int64
    ) {
        return Err(DataFusionError::Plan(format!(
            "TRUNCATE does not support Arrow type {result_type}"
        )));
    }
    if scale.data_type(schema)? != DataType::Int32 {
        return Err(DataFusionError::Plan(
            "TRUNCATE requires an Int32 scale".to_string(),
        ));
    }
    let expression = Arc::new(FlinkIntegerTruncateExpr {
        value: Arc::new(CastExpr::new(value, DataType::Int64, None)),
        scale,
    });
    Ok(Arc::new(CastExpr::new(expression, result_type, None)))
}

#[cfg(test)]
mod tests {
    use super::truncate;

    #[test]
    fn truncates_signed_integers_toward_zero() {
        assert_eq!(truncate(12_345, -2), 12_300);
        assert_eq!(truncate(-12_345, -2), -12_300);
        assert_eq!(truncate(12_345, 0), 12_345);
        assert_eq!(truncate(12_345, 4), 12_345);
        assert_eq!(truncate(i64::MAX, i32::MIN), 0);
    }
}
