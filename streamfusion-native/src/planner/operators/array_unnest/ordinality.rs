// Copyright 2026 StreamFusion Authors
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0

use std::hash::{Hash, Hasher};
use std::sync::Arc;

use arrow::array::{Array, Int32Array, ListArray};
use arrow::buffer::{OffsetBuffer, ScalarBuffer};
use arrow::datatypes::{DataType, Field, FieldRef, Schema};
use arrow::record_batch::RecordBatch;
use datafusion::error::{DataFusionError, Result};
use datafusion::logical_expr::ColumnarValue;
use datafusion::physical_expr::PhysicalExpr;

#[derive(Debug, Eq)]
struct ArrayOrdinalityExpr {
    array: Arc<dyn PhysicalExpr>,
}

impl PartialEq for ArrayOrdinalityExpr {
    fn eq(&self, other: &Self) -> bool {
        self.array.eq(&other.array)
    }
}

impl Hash for ArrayOrdinalityExpr {
    fn hash<H: Hasher>(&self, state: &mut H) {
        self.array.hash(state);
    }
}

impl std::fmt::Display for ArrayOrdinalityExpr {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(formatter, "ARRAY_ORDINALITY({})", self.array)
    }
}

impl PhysicalExpr for ArrayOrdinalityExpr {
    fn data_type(&self, _input_schema: &Schema) -> Result<DataType> {
        Ok(DataType::List(Arc::new(Field::new(
            "item",
            DataType::Int32,
            false,
        ))))
    }

    fn nullable(&self, input_schema: &Schema) -> Result<bool> {
        self.array.nullable(input_schema)
    }

    fn evaluate(&self, batch: &RecordBatch) -> Result<ColumnarValue> {
        let array = self.array.evaluate(batch)?.into_array(batch.num_rows())?;
        let lists = array.as_any().downcast_ref::<ListArray>().ok_or_else(|| {
            DataFusionError::Execution("array UNNEST ordinality expected List input".to_string())
        })?;
        let offsets = lists.offsets();
        let base = offsets[0];
        let positions = offsets
            .windows(2)
            .flat_map(|window| 1..=(window[1] - window[0]))
            .collect::<Vec<_>>();
        let rebased_offsets = OffsetBuffer::new(ScalarBuffer::from(
            offsets
                .iter()
                .map(|offset| offset - base)
                .collect::<Vec<_>>(),
        ));
        let output = ListArray::new(
            Arc::new(Field::new("item", DataType::Int32, false)),
            rebased_offsets,
            Arc::new(Int32Array::from(positions)),
            lists.nulls().cloned(),
        );
        Ok(ColumnarValue::Array(Arc::new(output)))
    }

    fn return_field(&self, input_schema: &Schema) -> Result<FieldRef> {
        let source = self.array.return_field(input_schema)?;
        Ok(Arc::new(Field::new(
            source.name(),
            self.data_type(input_schema)?,
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
        write!(formatter, "ARRAY_ORDINALITY(")?;
        self.array.fmt_sql(formatter)?;
        write!(formatter, ")")
    }
}

pub(super) fn create(array: Arc<dyn PhysicalExpr>) -> Arc<dyn PhysicalExpr> {
    Arc::new(ArrayOrdinalityExpr { array })
}
