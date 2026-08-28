// Copyright 2026 StreamFusion Authors
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0

use std::hash::{Hash, Hasher};
use std::sync::Arc;

use arrow::array::{Array, ListArray, MapArray};
use arrow::datatypes::{DataType, Field, FieldRef, Schema};
use arrow::record_batch::RecordBatch;
use datafusion::error::{DataFusionError, Result};
use datafusion::logical_expr::ColumnarValue;
use datafusion::physical_expr::PhysicalExpr;

#[derive(Debug, Eq)]
struct MapEntriesExpr {
    map: Arc<dyn PhysicalExpr>,
}

impl PartialEq for MapEntriesExpr {
    fn eq(&self, other: &Self) -> bool {
        self.map.eq(&other.map)
    }
}

impl Hash for MapEntriesExpr {
    fn hash<H: Hasher>(&self, state: &mut H) {
        self.map.hash(state);
    }
}

impl std::fmt::Display for MapEntriesExpr {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(formatter, "MAP_ENTRIES({})", self.map)
    }
}

impl PhysicalExpr for MapEntriesExpr {
    fn data_type(&self, input_schema: &Schema) -> Result<DataType> {
        match self.map.data_type(input_schema)? {
            DataType::Map(entries, _) => Ok(DataType::List(entries)),
            other => Err(DataFusionError::Plan(format!(
                "map UNNEST expected Map input, got {other}"
            ))),
        }
    }

    fn nullable(&self, input_schema: &Schema) -> Result<bool> {
        self.map.nullable(input_schema)
    }

    fn evaluate(&self, batch: &RecordBatch) -> Result<ColumnarValue> {
        let array = self.map.evaluate(batch)?.into_array(batch.num_rows())?;
        let maps = array.as_any().downcast_ref::<MapArray>().ok_or_else(|| {
            DataFusionError::Execution("map UNNEST expected Map input".to_string())
        })?;
        let DataType::Map(entries, _) = maps.data_type() else {
            unreachable!("MapArray always has Map type")
        };
        Ok(ColumnarValue::Array(Arc::new(ListArray::new(
            Arc::clone(entries),
            maps.offsets().clone(),
            Arc::new(maps.entries().clone()),
            maps.nulls().cloned(),
        ))))
    }

    fn return_field(&self, input_schema: &Schema) -> Result<FieldRef> {
        let source = self.map.return_field(input_schema)?;
        Ok(Arc::new(Field::new(
            source.name(),
            self.data_type(input_schema)?,
            source.is_nullable(),
        )))
    }

    fn children(&self) -> Vec<&Arc<dyn PhysicalExpr>> {
        vec![&self.map]
    }

    fn with_new_children(
        self: Arc<Self>,
        children: Vec<Arc<dyn PhysicalExpr>>,
    ) -> Result<Arc<dyn PhysicalExpr>> {
        Ok(Arc::new(Self {
            map: Arc::clone(&children[0]),
        }))
    }

    fn fmt_sql(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(formatter, "MAP_ENTRIES(")?;
        self.map.fmt_sql(formatter)?;
        write!(formatter, ")")
    }
}

pub(super) fn create(map: Arc<dyn PhysicalExpr>) -> Arc<dyn PhysicalExpr> {
    Arc::new(MapEntriesExpr { map })
}
