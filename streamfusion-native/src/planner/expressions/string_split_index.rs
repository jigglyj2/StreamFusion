// Copyright 2026 StreamFusion Authors
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
// http://www.apache.org/licenses/LICENSE-2.0

use std::hash::{Hash, Hasher};
use std::sync::Arc;

use arrow::array::{Array, Int64Array, StringArray};
use arrow::datatypes::{DataType, Field, FieldRef, Schema};
use arrow::record_batch::RecordBatch;
use datafusion::error::{DataFusionError, Result};
use datafusion::logical_expr::ColumnarValue;
use datafusion::physical_expr::expressions::CastExpr;
use datafusion::physical_expr::PhysicalExpr;

#[derive(Debug, Eq)]
struct FlinkSplitIndexExpr {
    value: Arc<dyn PhysicalExpr>,
    delimiter: String,
    index: Arc<dyn PhysicalExpr>,
}

impl PartialEq for FlinkSplitIndexExpr {
    fn eq(&self, other: &Self) -> bool {
        self.value.eq(&other.value)
            && self.delimiter == other.delimiter
            && self.index.eq(&other.index)
    }
}

impl Hash for FlinkSplitIndexExpr {
    fn hash<H: Hasher>(&self, state: &mut H) {
        self.value.hash(state);
        self.delimiter.hash(state);
        self.index.hash(state);
    }
}

impl std::fmt::Display for FlinkSplitIndexExpr {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(
            formatter,
            "SPLIT_INDEX({}, {:?}, {})",
            self.value, self.delimiter, self.index
        )
    }
}

impl PhysicalExpr for FlinkSplitIndexExpr {
    fn data_type(&self, _input_schema: &Schema) -> Result<DataType> {
        Ok(DataType::Utf8)
    }

    fn nullable(&self, _input_schema: &Schema) -> Result<bool> {
        Ok(true)
    }

    fn evaluate(&self, batch: &RecordBatch) -> Result<ColumnarValue> {
        let values = self.value.evaluate(batch)?.into_array(batch.num_rows())?;
        let indices = self.index.evaluate(batch)?.into_array(batch.num_rows())?;
        let values = values
            .as_any()
            .downcast_ref::<StringArray>()
            .ok_or_else(|| {
                DataFusionError::Execution("SPLIT_INDEX expected Utf8 input".to_string())
            })?;
        let indices = indices
            .as_any()
            .downcast_ref::<Int64Array>()
            .ok_or_else(|| {
                DataFusionError::Execution("SPLIT_INDEX expected Int64 index".to_string())
            })?;
        let mut output = Vec::with_capacity(batch.num_rows());
        for row in 0..batch.num_rows() {
            let selected = if values.is_null(row) || indices.is_null(row) {
                None
            } else {
                split_index(values.value(row), &self.delimiter, indices.value(row))
            };
            output.push(selected);
        }
        Ok(ColumnarValue::Array(Arc::new(StringArray::from(output))))
    }

    fn return_field(&self, input_schema: &Schema) -> Result<FieldRef> {
        let source = self.value.return_field(input_schema)?;
        Ok(Arc::new(Field::new(source.name(), DataType::Utf8, true)))
    }

    fn children(&self) -> Vec<&Arc<dyn PhysicalExpr>> {
        vec![&self.value, &self.index]
    }

    fn with_new_children(
        self: Arc<Self>,
        children: Vec<Arc<dyn PhysicalExpr>>,
    ) -> Result<Arc<dyn PhysicalExpr>> {
        Ok(Arc::new(Self {
            value: Arc::clone(&children[0]),
            delimiter: self.delimiter.clone(),
            index: Arc::clone(&children[1]),
        }))
    }

    fn fmt_sql(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(formatter, "SPLIT_INDEX(")?;
        self.value.fmt_sql(formatter)?;
        write!(formatter, ", {:?}, ", self.delimiter)?;
        self.index.fmt_sql(formatter)?;
        write!(formatter, ")")
    }
}

fn split_index<'a>(value: &'a str, delimiter: &str, index: i64) -> Option<&'a str> {
    if value.is_empty() || index < 0 {
        return None;
    }
    let requested = usize::try_from(index).ok()?;
    let mut start = 0;
    for _ in 0..requested {
        let relative = value[start..].find(delimiter)?;
        start += relative + delimiter.len();
    }
    let end = value[start..]
        .find(delimiter)
        .map_or(value.len(), |relative| start + relative);
    Some(&value[start..end])
}

pub(crate) fn create(
    value: Arc<dyn PhysicalExpr>,
    delimiter: &str,
    index: Arc<dyn PhysicalExpr>,
    schema: &Schema,
) -> Result<Arc<dyn PhysicalExpr>> {
    if delimiter.is_empty() {
        return Err(DataFusionError::Plan(
            "SPLIT_INDEX delimiter must be nonempty".to_string(),
        ));
    }
    if value.data_type(schema)? != DataType::Utf8 {
        return Err(DataFusionError::Plan(
            "SPLIT_INDEX requires Arrow Utf8 input".to_string(),
        ));
    }
    Ok(Arc::new(FlinkSplitIndexExpr {
        value,
        delimiter: delimiter.to_string(),
        index: Arc::new(CastExpr::new(index, DataType::Int64, None)),
    }))
}

#[cfg(test)]
mod tests {
    use super::split_index;

    #[test]
    fn selects_one_field_without_materializing_all_fields() {
        assert_eq!(split_index("a/b/c", "/", 0), Some("a"));
        assert_eq!(split_index("a/b/c", "/", 2), Some("c"));
        assert_eq!(split_index("a//c/", "/", 1), Some(""));
        assert_eq!(split_index("a//c/", "/", 3), Some(""));
        assert_eq!(split_index("a/b", "/", 2), None);
        assert_eq!(split_index("", "/", 0), None);
        assert_eq!(split_index("a/b", "/", -1), None);
        assert_eq!(split_index("a界b界c", "界", 1), Some("b"));
    }
}
