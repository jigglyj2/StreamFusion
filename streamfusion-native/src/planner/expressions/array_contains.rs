// Copyright 2026 StreamFusion Authors
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
// http://www.apache.org/licenses/LICENSE-2.0

use std::sync::Arc;
use std::{hash::Hash, hash::Hasher};

use arrow::array::{Array, BooleanBuilder, ListArray};
use arrow::datatypes::{DataType, Field, FieldRef, Schema};
use arrow::record_batch::RecordBatch;
use datafusion::common::config::ConfigOptions;
use datafusion::error::{DataFusionError, Result};
use datafusion::logical_expr::ColumnarValue;
use datafusion::physical_expr::expressions::{CaseExpr, IsNullExpr};
use datafusion::physical_expr::{PhysicalExpr, ScalarFunctionExpr};

#[derive(Debug, Eq)]
struct ArrayContainsNullExpr {
    array: Arc<dyn PhysicalExpr>,
}

impl PartialEq for ArrayContainsNullExpr {
    fn eq(&self, other: &Self) -> bool {
        self.array.eq(&other.array)
    }
}

impl Hash for ArrayContainsNullExpr {
    fn hash<H: Hasher>(&self, state: &mut H) {
        self.array.hash(state);
    }
}

impl std::fmt::Display for ArrayContainsNullExpr {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(formatter, "ARRAY_CONTAINS_NULL({})", self.array)
    }
}

impl PhysicalExpr for ArrayContainsNullExpr {
    fn data_type(&self, _input_schema: &Schema) -> Result<DataType> {
        Ok(DataType::Boolean)
    }

    fn nullable(&self, input_schema: &Schema) -> Result<bool> {
        self.array.nullable(input_schema)
    }

    fn evaluate(&self, batch: &RecordBatch) -> Result<ColumnarValue> {
        let array = self.array.evaluate(batch)?.into_array(batch.num_rows())?;
        let lists = array.as_any().downcast_ref::<ListArray>().ok_or_else(|| {
            DataFusionError::Execution(format!(
                "ARRAY_CONTAINS requires List input, got {}",
                array.data_type()
            ))
        })?;
        let values = lists.values();
        let offsets = lists.value_offsets();
        let mut output = BooleanBuilder::with_capacity(lists.len());
        for row in 0..lists.len() {
            if lists.is_null(row) {
                output.append_null();
                continue;
            }
            let start = usize::try_from(offsets[row]).map_err(|_| {
                DataFusionError::Execution(
                    "ARRAY_CONTAINS encountered a negative List offset".into(),
                )
            })?;
            let end = usize::try_from(offsets[row + 1]).map_err(|_| {
                DataFusionError::Execution(
                    "ARRAY_CONTAINS encountered a negative List offset".into(),
                )
            })?;
            output.append_value((start..end).any(|index| values.is_null(index)));
        }
        Ok(ColumnarValue::Array(Arc::new(output.finish())))
    }

    fn return_field(&self, input_schema: &Schema) -> Result<FieldRef> {
        let source = self.array.return_field(input_schema)?;
        Ok(Arc::new(Field::new(
            source.name(),
            DataType::Boolean,
            source.is_nullable(),
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
        }))
    }

    fn fmt_sql(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(formatter, "ARRAY_CONTAINS_NULL(")?;
        self.array.fmt_sql(formatter)?;
        write!(formatter, ")")
    }
}

pub(crate) fn create(
    array: Arc<dyn PhysicalExpr>,
    needle: Arc<dyn PhysicalExpr>,
    needle_nullable: bool,
    schema: &Schema,
) -> Result<Arc<dyn PhysicalExpr>> {
    let contains = Arc::new(ScalarFunctionExpr::try_new(
        datafusion_functions_nested::array_has::array_has_udf(),
        vec![Arc::clone(&array), Arc::clone(&needle)],
        schema,
        Arc::new(ConfigOptions::new()),
    )?) as Arc<dyn PhysicalExpr>;
    if !needle_nullable {
        return Ok(contains);
    }
    let contains_null = Arc::new(ArrayContainsNullExpr { array }) as Arc<dyn PhysicalExpr>;
    Ok(Arc::new(CaseExpr::try_new(
        None,
        vec![(Arc::new(IsNullExpr::new(needle)), contains_null)],
        Some(contains),
    )?))
}

#[cfg(test)]
mod tests {
    use std::sync::Arc;

    use arrow::array::{Array, BooleanArray, Int32Array, ListArray};
    use arrow::datatypes::{Field, Int32Type, Schema};
    use arrow::record_batch::RecordBatch;
    use datafusion::physical_expr::expressions::Column;

    use super::create;

    #[test]
    fn null_needle_searches_for_null_elements_like_flink() {
        let arrays = Arc::new(ListArray::from_iter_primitive::<Int32Type, _, _>(vec![
            Some(vec![Some(1), None]),
            Some(vec![Some(1), Some(2)]),
            None,
        ]));
        let needles = Arc::new(Int32Array::from(vec![None, None, None]));
        let schema = Arc::new(Schema::new(vec![
            Field::new("arrays", arrays.data_type().clone(), true),
            Field::new("needles", needles.data_type().clone(), true),
        ]));
        let batch = RecordBatch::try_new(Arc::clone(&schema), vec![arrays, needles]).unwrap();
        let output = create(
            Arc::new(Column::new("arrays", 0)),
            Arc::new(Column::new("needles", 1)),
            true,
            schema.as_ref(),
        )
        .unwrap()
        .evaluate(&batch)
        .unwrap()
        .into_array(batch.num_rows())
        .unwrap();
        let output = output.as_any().downcast_ref::<BooleanArray>().unwrap();

        assert!(output.value(0));
        assert!(!output.value(1));
        assert!(output.is_null(2));
    }
}
