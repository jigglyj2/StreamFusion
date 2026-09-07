// Copyright 2026 StreamFusion Authors
// Licensed under the Apache License, Version 2.0.

//! Admit the gather after evaluating the predicate once. DataFusion still owns filtering.

use super::*;
use arrow::array::{Array, BooleanArray, RecordBatch};
use arrow::datatypes::{DataType, Schema};
use datafusion::logical_expr::ColumnarValue;
use std::hash::{Hash, Hasher};

#[derive(Debug)]
pub(super) struct GatherAdmission {
    pub(super) inner: Arc<dyn PhysicalExpr>,
    transaction: Arc<Transaction>,
    pool: Arc<dyn MemoryPool>,
    projection: Option<Vec<usize>>,
}
impl GatherAdmission {
    pub(super) fn new(
        inner: Arc<dyn PhysicalExpr>,
        transaction: Arc<Transaction>,
        pool: Arc<dyn MemoryPool>,
        projection: Option<Vec<usize>>,
    ) -> Self {
        Self {
            inner,
            transaction,
            pool,
            projection,
        }
    }
}
impl PartialEq for GatherAdmission {
    fn eq(&self, other: &Self) -> bool {
        self.inner.eq(&other.inner) && Arc::ptr_eq(&self.transaction, &other.transaction)
    }
}
impl Eq for GatherAdmission {}
impl Hash for GatherAdmission {
    fn hash<H: Hasher>(&self, state: &mut H) {
        self.inner.hash(state);
        (Arc::as_ptr(&self.transaction) as usize).hash(state);
    }
}
impl std::fmt::Display for GatherAdmission {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        self.inner.fmt(f)
    }
}
impl PhysicalExpr for GatherAdmission {
    fn fmt_sql(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        self.inner.fmt_sql(f)
    }
    fn data_type(&self, schema: &Schema) -> Result<DataType> {
        self.inner.data_type(schema)
    }
    fn nullable(&self, schema: &Schema) -> Result<bool> {
        self.inner.nullable(schema)
    }
    fn evaluate(&self, batch: &RecordBatch) -> Result<ColumnarValue> {
        let result = self.inner.evaluate(batch)?.into_array(batch.num_rows())?;
        let mask = result
            .as_any()
            .downcast_ref::<BooleanArray>()
            .ok_or_else(|| DataFusionError::Execution("filter predicate must be Boolean".into()))?;
        let selected = mask.true_count();
        self.transaction.prepare(
            batch,
            self.projection.as_deref(),
            &self.pool,
            selected > 0 && selected < batch.num_rows(),
        )?;
        Ok(ColumnarValue::Array(result))
    }
    fn children(&self) -> Vec<&Arc<dyn PhysicalExpr>> {
        vec![&self.inner]
    }
    fn with_new_children(
        self: Arc<Self>,
        mut children: Vec<Arc<dyn PhysicalExpr>>,
    ) -> Result<Arc<dyn PhysicalExpr>> {
        if children.len() != 1 {
            return Err(DataFusionError::Plan(
                "filter admission requires one predicate".into(),
            ));
        }
        Ok(Arc::new(Self::new(
            children.remove(0),
            self.transaction.clone(),
            self.pool.clone(),
            self.projection.clone(),
        )))
    }
}
