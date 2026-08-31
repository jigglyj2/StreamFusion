// Copyright 2026 StreamFusion Authors
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
// http://www.apache.org/licenses/LICENSE-2.0

use std::sync::Arc;
use std::{hash::Hash, hash::Hasher};

use arrow::array::{make_array, Array, ListArray, NullBufferBuilder};
use arrow::buffer::{OffsetBuffer, ScalarBuffer};
use arrow::datatypes::{DataType, FieldRef, Schema};
use arrow::record_batch::RecordBatch;
use datafusion::common::config::ConfigOptions;
use datafusion::error::{DataFusionError, Result};
use datafusion::logical_expr::ColumnarValue;
use datafusion::physical_expr::expressions::{CaseExpr, IsNullExpr};
use datafusion::physical_expr::{PhysicalExpr, ScalarFunctionExpr};

#[derive(Debug, Eq)]
struct ArrayRemoveNullExpr {
    array: Arc<dyn PhysicalExpr>,
    data_type: DataType,
}

impl PartialEq for ArrayRemoveNullExpr {
    fn eq(&self, other: &Self) -> bool {
        self.array.eq(&other.array) && self.data_type == other.data_type
    }
}

impl Hash for ArrayRemoveNullExpr {
    fn hash<H: Hasher>(&self, state: &mut H) {
        self.array.hash(state);
        self.data_type.hash(state);
    }
}

impl std::fmt::Display for ArrayRemoveNullExpr {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(formatter, "ARRAY_REMOVE_NULL({})", self.array)
    }
}

impl PhysicalExpr for ArrayRemoveNullExpr {
    fn data_type(&self, _input_schema: &Schema) -> Result<DataType> {
        Ok(self.data_type.clone())
    }

    fn nullable(&self, input_schema: &Schema) -> Result<bool> {
        self.array.nullable(input_schema)
    }

    fn evaluate(&self, batch: &RecordBatch) -> Result<ColumnarValue> {
        let array = self.array.evaluate(batch)?.into_array(batch.num_rows())?;
        let lists = array.as_any().downcast_ref::<ListArray>().ok_or_else(|| {
            DataFusionError::Execution(format!(
                "ARRAY_REMOVE requires List input, got {}",
                array.data_type()
            ))
        })?;
        let values = lists.values();
        let values_data = values.to_data();
        let mut copied =
            arrow::array::MutableArrayData::new(vec![&values_data], false, values.len());
        let source_offsets = lists.value_offsets();
        let mut output_offsets = Vec::with_capacity(lists.len() + 1);
        let mut output_length = 0_i32;
        output_offsets.push(output_length);
        let mut validity = NullBufferBuilder::new(lists.len());
        for row in 0..lists.len() {
            if lists.is_null(row) {
                validity.append_null();
                output_offsets.push(output_length);
                continue;
            }
            validity.append_non_null();
            let start = usize::try_from(source_offsets[row]).map_err(|_| {
                DataFusionError::Execution("ARRAY_REMOVE encountered a negative List offset".into())
            })?;
            let end = usize::try_from(source_offsets[row + 1]).map_err(|_| {
                DataFusionError::Execution("ARRAY_REMOVE encountered a negative List offset".into())
            })?;
            for index in start..end {
                if values.is_valid(index) {
                    copied.try_extend(0, index, index + 1)?;
                    output_length = output_length.checked_add(1).ok_or_else(|| {
                        DataFusionError::Execution(
                            "ARRAY_REMOVE output exceeds Arrow List capacity".into(),
                        )
                    })?;
                }
            }
            output_offsets.push(output_length);
        }
        let DataType::List(element) = lists.data_type() else {
            unreachable!("ListArray must report List data type")
        };
        let output = ListArray::try_new(
            Arc::clone(element),
            OffsetBuffer::new(ScalarBuffer::from(output_offsets)),
            make_array(copied.freeze()),
            validity.finish(),
        )?;
        Ok(ColumnarValue::Array(Arc::new(output)))
    }

    fn return_field(&self, input_schema: &Schema) -> Result<FieldRef> {
        self.array.return_field(input_schema)
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
            data_type: self.data_type.clone(),
        }))
    }

    fn fmt_sql(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(formatter, "ARRAY_REMOVE_NULL(")?;
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
    let data_type = array.data_type(schema)?;
    let removed = Arc::new(ScalarFunctionExpr::try_new(
        datafusion_functions_nested::remove::array_remove_all_udf(),
        vec![Arc::clone(&array), Arc::clone(&needle)],
        schema,
        Arc::new(ConfigOptions::new()),
    )?) as Arc<dyn PhysicalExpr>;
    if !needle_nullable {
        return Ok(removed);
    }
    let remove_null = Arc::new(ArrayRemoveNullExpr { array, data_type }) as Arc<dyn PhysicalExpr>;
    Ok(Arc::new(CaseExpr::try_new(
        None,
        vec![(Arc::new(IsNullExpr::new(needle)), remove_null)],
        Some(removed),
    )?))
}

#[cfg(test)]
mod tests {
    use std::sync::Arc;

    use arrow::array::{Array, Int32Array, ListArray};
    use arrow::datatypes::{Field, Int32Type, Schema};
    use arrow::record_batch::RecordBatch;
    use datafusion::physical_expr::expressions::Column;

    use super::create;

    #[test]
    fn null_needle_removes_only_null_elements_like_flink() {
        let arrays = Arc::new(ListArray::from_iter_primitive::<Int32Type, _, _>(vec![
            Some(vec![Some(1), None, Some(2), None]),
            Some(vec![]),
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
        let output = output.as_any().downcast_ref::<ListArray>().unwrap();

        let first = output.value(0);
        let first = first.as_any().downcast_ref::<Int32Array>().unwrap();
        assert_eq!(first.values(), &[1, 2]);
        assert_eq!(output.value_length(1), 0);
        assert!(output.is_null(2));
    }
}
