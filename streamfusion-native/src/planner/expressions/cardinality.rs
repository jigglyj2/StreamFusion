// Copyright 2026 StreamFusion Authors
//
// Licensed under the Apache License, Version 2.0

use std::hash::{Hash, Hasher};
use std::sync::Arc;

use arrow::array::{Array, Int32Builder, ListArray, MapArray};
use arrow::datatypes::{DataType, Field, FieldRef, Schema};
use arrow::record_batch::RecordBatch;
use datafusion::error::{DataFusionError, Result};
use datafusion::logical_expr::ColumnarValue;
use datafusion::physical_expr::PhysicalExpr;

#[derive(Debug, Eq)]
struct FlinkCardinalityExpr {
    collection: Arc<dyn PhysicalExpr>,
}

impl PartialEq for FlinkCardinalityExpr {
    fn eq(&self, other: &Self) -> bool {
        self.collection.eq(&other.collection)
    }
}

impl Hash for FlinkCardinalityExpr {
    fn hash<H: Hasher>(&self, state: &mut H) {
        self.collection.hash(state);
    }
}

impl std::fmt::Display for FlinkCardinalityExpr {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(formatter, "CARDINALITY({})", self.collection)
    }
}

impl PhysicalExpr for FlinkCardinalityExpr {
    fn data_type(&self, _input_schema: &Schema) -> Result<DataType> {
        Ok(DataType::Int32)
    }

    fn nullable(&self, input_schema: &Schema) -> Result<bool> {
        self.collection.nullable(input_schema)
    }

    fn evaluate(&self, batch: &RecordBatch) -> Result<ColumnarValue> {
        let collection = self
            .collection
            .evaluate(batch)?
            .into_array(batch.num_rows())?;
        let mut output = Int32Builder::with_capacity(batch.num_rows());
        if let Some(array) = collection.as_any().downcast_ref::<ListArray>() {
            for row in 0..array.len() {
                if array.is_null(row) {
                    output.append_null();
                } else {
                    output.append_value(array.value_length(row));
                }
            }
        } else if let Some(map) = collection.as_any().downcast_ref::<MapArray>() {
            for row in 0..map.len() {
                if map.is_null(row) {
                    output.append_null();
                } else {
                    output.append_value(map.value_length(row));
                }
            }
        } else {
            return Err(DataFusionError::Execution(format!(
                "CARDINALITY requires List or Map input, got {}",
                collection.data_type()
            )));
        }
        Ok(ColumnarValue::Array(Arc::new(output.finish())))
    }

    fn return_field(&self, input_schema: &Schema) -> Result<FieldRef> {
        let source = self.collection.return_field(input_schema)?;
        Ok(Arc::new(Field::new(
            source.name(),
            DataType::Int32,
            source.is_nullable(),
        )))
    }

    fn children(&self) -> Vec<&Arc<dyn PhysicalExpr>> {
        vec![&self.collection]
    }

    fn with_new_children(
        self: Arc<Self>,
        children: Vec<Arc<dyn PhysicalExpr>>,
    ) -> Result<Arc<dyn PhysicalExpr>> {
        Ok(Arc::new(Self {
            collection: Arc::clone(&children[0]),
        }))
    }

    fn fmt_sql(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(formatter, "CARDINALITY(")?;
        self.collection.fmt_sql(formatter)?;
        write!(formatter, ")")
    }
}

pub(crate) fn create(
    collection: Arc<dyn PhysicalExpr>,
    schema: &Schema,
) -> Result<Arc<dyn PhysicalExpr>> {
    match collection.data_type(schema)? {
        DataType::List(_) | DataType::Map(_, _) => {
            Ok(Arc::new(FlinkCardinalityExpr { collection }))
        }
        other => Err(DataFusionError::Plan(format!(
            "CARDINALITY requires List or Map input, got {other}"
        ))),
    }
}

#[cfg(test)]
mod tests {
    use std::sync::Arc;

    use arrow::array::{Array, Int32Array, Int32Builder, ListBuilder};
    use arrow::datatypes::{Field, Schema};
    use arrow::record_batch::RecordBatch;
    use datafusion::physical_expr::expressions::Column;

    use super::create;

    #[test]
    fn counts_only_the_outer_dimension_of_nested_arrays() {
        let mut builder = ListBuilder::new(ListBuilder::new(Int32Builder::new()));
        builder.values().values().append_value(1);
        builder.values().values().append_value(2);
        builder.values().append(true);
        builder.values().values().append_value(3);
        builder.values().append(true);
        builder.append(true);
        builder.append(true);
        builder.append(false);
        let arrays = Arc::new(builder.finish());
        let schema = Arc::new(Schema::new(vec![Field::new(
            "arrays",
            arrays.data_type().clone(),
            true,
        )]));
        let batch = RecordBatch::try_new(Arc::clone(&schema), vec![arrays]).unwrap();
        let expression = create(Arc::new(Column::new("arrays", 0)), schema.as_ref()).unwrap();
        let output = expression
            .evaluate(&batch)
            .unwrap()
            .into_array(batch.num_rows())
            .unwrap();
        let output = output.as_any().downcast_ref::<Int32Array>().unwrap();

        assert_eq!(output.value(0), 2);
        assert_eq!(output.value(1), 0);
        assert!(output.is_null(2));
    }
}
