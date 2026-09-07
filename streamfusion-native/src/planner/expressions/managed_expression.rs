// Copyright 2026 StreamFusion Authors
// Licensed under the Apache License, Version 2.0.

//! Admission for audited, fixed-width physical kernels. The original expression still
//! evaluates its children and implements SQL semantics, including conditional evaluation.

use arrow::array::{Array, BooleanArray, RecordBatch};
use arrow::datatypes::{DataType, FieldRef, Schema};
use datafusion::common::tree_node::Transformed;
use datafusion::error::{DataFusionError, Result};
use datafusion::execution::memory_pool::{MemoryConsumer, MemoryPool, MemoryReservation};
use datafusion::logical_expr::interval_arithmetic::Interval;
use datafusion::logical_expr::sort_properties::ExprProperties;
#[allow(deprecated)]
use datafusion::logical_expr::statistics::Distribution;
use datafusion::logical_expr::ColumnarValue;
use datafusion::logical_expr::ExpressionPlacement;
use datafusion::physical_expr::PhysicalExpr;
use std::hash::{Hash, Hasher};
use std::sync::Arc;

#[cfg(test)]
mod decimal_tests;
mod materialize;
pub(crate) use materialize::evaluate as evaluate_projection;
mod policy;
#[cfg(test)]
mod tests;
mod validity;

#[derive(Debug)]
struct AdmittedExpression {
    inner: Arc<dyn PhysicalExpr>,
    pool: Arc<dyn MemoryPool>,
    // None denotes the projection-only scalar-to-array boundary, not a kernel policy.
    policy: Option<policy::Policy>,
}
impl PartialEq for AdmittedExpression {
    fn eq(&self, other: &Self) -> bool {
        self.inner.eq(&other.inner)
            && self.policy == other.policy
            && Arc::ptr_eq(&self.pool, &other.pool)
    }
}
impl Eq for AdmittedExpression {}
impl Hash for AdmittedExpression {
    fn hash<H: Hasher>(&self, state: &mut H) {
        self.inner.hash(state);
        self.policy.hash(state);
        (Arc::as_ptr(&self.pool) as *const () as usize).hash(state);
    }
}
impl std::fmt::Display for AdmittedExpression {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        self.inner.fmt(f)
    }
}
impl AdmittedExpression {
    fn reserve(&self, batch: &RecordBatch, selection: bool) -> Result<MemoryReservation> {
        let memory =
            MemoryConsumer::new("native physical expression workspace").register(&self.pool);
        memory.try_grow(
            self.policy
                .unwrap_or(policy::Policy::Forward)
                .workspace(batch, selection)?,
        )?;
        Ok(memory)
    }

    fn retain(&self, result: ColumnarValue, memory: MemoryReservation) -> Result<ColumnarValue> {
        match result {
            // Preserve scalar-vs-array semantics. Projection's later scalar broadcast is
            // a separate admission boundary; do not change conditional kernel behavior.
            ColumnarValue::Scalar(value) => Ok(ColumnarValue::Scalar(value)),
            ColumnarValue::Array(array) => {
                let retained = array.get_array_memory_size();
                if retained > memory.size() {
                    return Err(DataFusionError::ResourcesExhausted(
                        "physical expression result exceeded its admitted workspace".into(),
                    ));
                }
                memory.try_resize(retained)?;
                Ok(ColumnarValue::Array(
                    crate::memory_pool::arrow_lease::datafusion_array_registered(
                        array,
                        memory,
                        crate::memory_pool::buffer_registry(&self.pool),
                    )?,
                ))
            }
        }
    }
}
impl PhysicalExpr for AdmittedExpression {
    fn data_type(&self, schema: &Schema) -> Result<DataType> {
        self.inner.data_type(schema)
    }
    fn nullable(&self, schema: &Schema) -> Result<bool> {
        self.inner.nullable(schema)
    }
    fn return_field(&self, schema: &Schema) -> Result<FieldRef> {
        self.inner.return_field(schema)
    }
    fn evaluate(&self, batch: &RecordBatch) -> Result<ColumnarValue> {
        if self.policy.is_none() {
            return evaluate_projection(&self.inner, batch, &self.pool);
        }
        let memory = self.reserve(batch, false)?;
        let result = self.inner.evaluate(batch)?;
        self.retain(result, memory)
    }
    fn evaluate_selection(
        &self,
        batch: &RecordBatch,
        selection: &BooleanArray,
    ) -> Result<ColumnarValue> {
        // DataFusion's default implementation may gather the whole input and scatter
        // the result. Cover that workspace before delegating, without another gather.
        let partial = selection.len() != batch.num_rows()
            || selection.null_count() != 0
            || selection.has_false();
        let memory = self.reserve(batch, partial)?;
        // A scalar boolean can forward this mask, including a tiny slice of a
        // larger bitmap. Its backing capacity is not bounded by the batch's rows.
        memory.try_grow(selection.get_array_memory_size())?;
        if self.policy.is_none() {
            if let Some(literal) = self
                .inner
                .downcast_ref::<datafusion::physical_expr::expressions::Literal>()
            {
                // Masked evaluation delegates unchanged to DF, which may clone the
                // literal. Normal projection evaluation borrows it instead.
                memory.try_grow(literal.value().size())?;
            }
        }
        let result = self.inner.evaluate_selection(batch, selection)?;
        if self.policy.is_none() {
            if let ColumnarValue::Scalar(value) = result {
                // Keep masked-evaluation credit until this temporary scalar is dropped.
                return materialize::scalar(&value, batch.num_rows(), &self.pool);
            }
        }
        self.retain(result, memory)
    }
    fn children(&self) -> Vec<&Arc<dyn PhysicalExpr>> {
        self.inner.children()
    }
    fn with_new_children(
        self: Arc<Self>,
        children: Vec<Arc<dyn PhysicalExpr>>,
    ) -> Result<Arc<dyn PhysicalExpr>> {
        Ok(Arc::new(Self {
            inner: self.inner.clone().with_new_children(children)?,
            pool: self.pool.clone(),
            policy: self.policy,
        }))
    }
    fn evaluate_bounds(&self, children: &[&Interval]) -> Result<Interval> {
        self.inner.evaluate_bounds(children)
    }
    fn propagate_constraints(
        &self,
        interval: &Interval,
        children: &[&Interval],
    ) -> Result<Option<Vec<Interval>>> {
        self.inner.propagate_constraints(interval, children)
    }
    fn get_properties(&self, children: &[ExprProperties]) -> Result<ExprProperties> {
        self.inner.get_properties(children)
    }
    fn fmt_sql(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        self.inner.fmt_sql(f)
    }
    fn is_volatile_node(&self) -> bool {
        self.inner.is_volatile_node()
    }
    fn placement(&self) -> ExpressionPlacement {
        self.inner.placement()
    }
    fn expression_id(&self) -> Option<u64> {
        self.inner.expression_id()
    }
    fn snapshot_generation(&self) -> u64 {
        self.inner.snapshot_generation()
    }
    fn snapshot(&self) -> Result<Option<Arc<dyn PhysicalExpr>>> {
        Ok(self.inner.snapshot()?.map(|inner| {
            Arc::new(Self {
                inner,
                pool: self.pool.clone(),
                policy: self.policy,
            }) as Arc<dyn PhysicalExpr>
        }))
    }
    #[allow(deprecated)]
    fn evaluate_statistics(&self, children: &[&Distribution]) -> Result<Distribution> {
        self.inner.evaluate_statistics(children)
    }
    #[allow(deprecated)]
    fn propagate_statistics(
        &self,
        parent: &Distribution,
        children: &[&Distribution],
    ) -> Result<Option<Vec<Distribution>>> {
        self.inner.propagate_statistics(parent, children)
    }
}

pub(super) fn install(
    expression: Arc<dyn PhysicalExpr>,
    pool: &Arc<dyn MemoryPool>,
    schema: &Schema,
) -> Result<Transformed<Arc<dyn PhysicalExpr>>> {
    let Some(policy) = policy::Policy::for_expression(expression.as_ref(), schema)? else {
        return Ok(Transformed::no(expression));
    };
    Ok(Transformed::yes(Arc::new(AdmittedExpression {
        inner: expression,
        pool: pool.clone(),
        policy: Some(policy),
    })))
}

/// Install only at projection roots, after filter column remapping. Inner conditional
/// expressions must retain their original scalar-vs-array evaluation semantics.
pub(crate) fn projection(
    expression: Arc<dyn PhysicalExpr>,
    pool: Option<&Arc<dyn MemoryPool>>,
) -> Arc<dyn PhysicalExpr> {
    let Some(pool) = pool else {
        return expression;
    };
    if expression
        .downcast_ref::<datafusion::physical_expr::expressions::Column>()
        .is_some()
    {
        return expression; // Direct references already return shared Arrow arrays.
    }
    Arc::new(AdmittedExpression {
        inner: expression,
        pool: pool.clone(),
        policy: None,
    })
}
