// Copyright 2026 StreamFusion Authors
//
// Licensed under the Apache License, Version 2.0

use std::hash::{Hash, Hasher};
use std::sync::Arc;

use arrow::array::{Array, ListArray, UInt32Array};
use arrow::compute::{take, TakeOptions};
use arrow::datatypes::{DataType, Field, FieldRef, Schema};
use arrow::record_batch::RecordBatch;
use datafusion::error::{DataFusionError, Result};
use datafusion::logical_expr::ColumnarValue;
use datafusion::physical_expr::PhysicalExpr;

#[derive(Debug, Eq)]
struct FlinkElementExpr {
    array: Arc<dyn PhysicalExpr>,
    element_type: DataType,
}

impl PartialEq for FlinkElementExpr {
    fn eq(&self, other: &Self) -> bool {
        self.array.eq(&other.array) && self.element_type == other.element_type
    }
}

impl Hash for FlinkElementExpr {
    fn hash<H: Hasher>(&self, state: &mut H) {
        self.array.hash(state);
        self.element_type.hash(state);
    }
}

impl std::fmt::Display for FlinkElementExpr {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(formatter, "ELEMENT({})", self.array)
    }
}

impl PhysicalExpr for FlinkElementExpr {
    fn data_type(&self, _input_schema: &Schema) -> Result<DataType> {
        Ok(self.element_type.clone())
    }

    fn nullable(&self, _input_schema: &Schema) -> Result<bool> {
        Ok(true)
    }

    fn evaluate(&self, batch: &RecordBatch) -> Result<ColumnarValue> {
        let array = self.array.evaluate(batch)?.into_array(batch.num_rows())?;
        let lists = array.as_any().downcast_ref::<ListArray>().ok_or_else(|| {
            DataFusionError::Execution(format!(
                "ELEMENT requires List input, got {}",
                array.data_type()
            ))
        })?;
        let offsets = lists.value_offsets();
        let mut indices = Vec::with_capacity(lists.len());
        for row in 0..lists.len() {
            if lists.is_null(row) || lists.value_length(row) == 0 {
                indices.push(None);
            } else if lists.value_length(row) == 1 {
                indices.push(Some(u32::try_from(offsets[row]).map_err(|_| {
                    DataFusionError::Execution("ELEMENT encountered a negative List offset".into())
                })?));
            } else {
                return Err(DataFusionError::Execution(
                    "ELEMENT requires an array with at most one element".into(),
                ));
            }
        }
        let indices = UInt32Array::from(indices);
        let output = take(
            lists.values().as_ref(),
            &indices,
            Some(TakeOptions { check_bounds: true }),
        )?;
        Ok(ColumnarValue::Array(output))
    }

    fn return_field(&self, input_schema: &Schema) -> Result<FieldRef> {
        let source = self.array.return_field(input_schema)?;
        Ok(Arc::new(Field::new(
            source.name(),
            self.element_type.clone(),
            true,
        )))
    }

    fn children(&self) -> Vec<&Arc<dyn PhysicalExpr>> {
        vec![&self.array]
    }

    fn with_new_children(
        self: Arc<Self>,
        children: Vec<Arc<dyn PhysicalExpr>>,
    ) -> Result<Arc<dyn PhysicalExpr>> {
        Ok(Arc::new(Self {
            array: Arc::clone(&children[0]),
            element_type: self.element_type.clone(),
        }))
    }

    fn fmt_sql(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(formatter, "ELEMENT(")?;
        self.array.fmt_sql(formatter)?;
        write!(formatter, ")")
    }
}

pub(crate) fn create(
    array: Arc<dyn PhysicalExpr>,
    schema: &Schema,
) -> Result<Arc<dyn PhysicalExpr>> {
    let DataType::List(element) = array.data_type(schema)? else {
        return Err(DataFusionError::Plan(
            "ELEMENT requires Arrow List input".into(),
        ));
    };
    Ok(Arc::new(FlinkElementExpr {
        array,
        element_type: element.data_type().clone(),
    }))
}

#[cfg(test)]
mod tests {
    use std::sync::Arc;

    use arrow::array::{Array, Int32Array, ListArray};
    use arrow::datatypes::{Field, Int32Type, Schema};
    use arrow::record_batch::RecordBatch;
    use datafusion::physical_expr::expressions::Column;

    use super::create;

    fn evaluate(rows: Vec<Option<Vec<Option<i32>>>>) -> datafusion::error::Result<Arc<dyn Array>> {
        let arrays = Arc::new(ListArray::from_iter_primitive::<Int32Type, _, _>(rows));
        let schema = Arc::new(Schema::new(vec![Field::new(
            "arrays",
            arrays.data_type().clone(),
            true,
        )]));
        let batch = RecordBatch::try_new(Arc::clone(&schema), vec![arrays])?;
        create(Arc::new(Column::new("arrays", 0)), schema.as_ref())?
            .evaluate(&batch)?
            .into_array(batch.num_rows())
    }

    #[test]
    fn returns_singletons_and_null_for_empty_or_null_arrays() {
        let output = evaluate(vec![Some(vec![Some(7)]), Some(vec![]), None]).unwrap();
        let output = output.as_any().downcast_ref::<Int32Array>().unwrap();
        assert_eq!(output.value(0), 7);
        assert!(output.is_null(1));
        assert!(output.is_null(2));
    }

    #[test]
    fn rejects_arrays_with_more_than_one_element() {
        let error = evaluate(vec![Some(vec![Some(1), Some(2)])]).unwrap_err();
        assert!(error
            .to_string()
            .contains("ELEMENT requires an array with at most one element"));
    }
}
