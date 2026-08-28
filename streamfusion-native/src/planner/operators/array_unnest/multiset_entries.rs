// Copyright 2026 StreamFusion Authors
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0

use std::hash::{Hash, Hasher};
use std::sync::Arc;

use arrow::array::{Array, Int32Array, ListArray, MapArray, UInt32Array};
use arrow::buffer::OffsetBuffer;
use arrow::compute::take;
use arrow::datatypes::{DataType, Field, FieldRef, Schema};
use arrow::record_batch::RecordBatch;
use datafusion::error::{DataFusionError, Result};
use datafusion::logical_expr::ColumnarValue;
use datafusion::physical_expr::PhysicalExpr;

#[derive(Debug, Eq)]
struct MultisetEntriesExpr {
    multiset: Arc<dyn PhysicalExpr>,
}

impl PartialEq for MultisetEntriesExpr {
    fn eq(&self, other: &Self) -> bool {
        self.multiset.eq(&other.multiset)
    }
}

impl Hash for MultisetEntriesExpr {
    fn hash<H: Hasher>(&self, state: &mut H) {
        self.multiset.hash(state);
    }
}

impl std::fmt::Display for MultisetEntriesExpr {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(formatter, "MULTISET_ENTRIES({})", self.multiset)
    }
}

impl PhysicalExpr for MultisetEntriesExpr {
    fn data_type(&self, input_schema: &Schema) -> Result<DataType> {
        let DataType::Map(entries, _) = self.multiset.data_type(input_schema)? else {
            return Err(DataFusionError::Plan(
                "multiset UNNEST expected Arrow Map input".to_string(),
            ));
        };
        let DataType::Struct(fields) = entries.data_type() else {
            return Err(DataFusionError::Plan(
                "multiset Arrow map entries must be a struct".to_string(),
            ));
        };
        Ok(DataType::List(Arc::new(Field::new(
            "item",
            fields[0].data_type().clone(),
            fields[0].is_nullable(),
        ))))
    }

    fn nullable(&self, input_schema: &Schema) -> Result<bool> {
        self.multiset.nullable(input_schema)
    }

    fn evaluate(&self, batch: &RecordBatch) -> Result<ColumnarValue> {
        let ColumnarValue::Array(array) = self.multiset.evaluate(batch)? else {
            return Err(DataFusionError::Execution(
                "multiset UNNEST requires a column".to_string(),
            ));
        };
        let maps = array.as_any().downcast_ref::<MapArray>().ok_or_else(|| {
            DataFusionError::Execution("multiset UNNEST expected Arrow Map input".to_string())
        })?;
        let counts = maps
            .entries()
            .column(1)
            .as_any()
            .downcast_ref::<Int32Array>()
            .ok_or_else(|| {
                DataFusionError::Execution("multiset multiplicities must be INT".to_string())
            })?;
        let mut indices = Vec::new();
        let mut lengths = Vec::with_capacity(maps.len());
        for row in 0..maps.len() {
            if maps.is_null(row) {
                lengths.push(0);
                continue;
            }
            let start = maps.value_offsets()[row] as usize;
            let end = maps.value_offsets()[row + 1] as usize;
            let before = indices.len();
            for entry in start..end {
                let count = counts.value(entry).max(0) as usize;
                indices.extend(std::iter::repeat_n(entry as u32, count));
            }
            lengths.push(indices.len() - before);
        }
        let values = take(
            maps.entries().column(0).as_ref(),
            &UInt32Array::from(indices),
            None,
        )?;
        let DataType::List(field) = self.data_type(batch.schema().as_ref())? else {
            unreachable!()
        };
        Ok(ColumnarValue::Array(Arc::new(ListArray::new(
            field,
            OffsetBuffer::from_lengths(lengths),
            values,
            maps.nulls().cloned(),
        ))))
    }

    fn return_field(&self, input_schema: &Schema) -> Result<FieldRef> {
        let source = self.multiset.return_field(input_schema)?;
        Ok(Arc::new(Field::new(
            source.name(),
            self.data_type(input_schema)?,
            source.is_nullable(),
        )))
    }

    fn children(&self) -> Vec<&Arc<dyn PhysicalExpr>> {
        vec![&self.multiset]
    }

    fn with_new_children(
        self: Arc<Self>,
        children: Vec<Arc<dyn PhysicalExpr>>,
    ) -> Result<Arc<dyn PhysicalExpr>> {
        Ok(Arc::new(Self {
            multiset: Arc::clone(&children[0]),
        }))
    }

    fn fmt_sql(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(formatter, "MULTISET_ENTRIES(")?;
        self.multiset.fmt_sql(formatter)?;
        write!(formatter, ")")
    }
}

pub(super) fn create(multiset: Arc<dyn PhysicalExpr>) -> Arc<dyn PhysicalExpr> {
    Arc::new(MultisetEntriesExpr { multiset })
}
