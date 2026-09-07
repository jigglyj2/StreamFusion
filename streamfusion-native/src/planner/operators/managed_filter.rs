// Copyright 2026 StreamFusion Authors
// Licensed under the Apache License, Version 2.0.

//! Admission around DataFusion's unchanged FilterExec. This is a kernel workspace
//! policy, not an operator-pair fusion driver. Input and output remain Arrow streams.

use std::fmt;
use std::sync::Arc;

use datafusion::common::tree_node::TreeNodeRecursion;
use datafusion::common::Statistics;
use datafusion::error::{DataFusionError, Result};
use datafusion::execution::memory_pool::MemoryPool;
use datafusion::execution::{SendableRecordBatchStream, TaskContext};
use datafusion::physical_expr::PhysicalExpr;
use datafusion::physical_plan::execution_plan::CardinalityEffect;
use datafusion::physical_plan::filter::FilterExec;
use datafusion::physical_plan::metrics::MetricsSet;
use datafusion::physical_plan::statistics::{ChildStats, StatisticsArgs};
use datafusion::physical_plan::{
    ChildrenPropertiesMode, DisplayAs, DisplayFormatType, ExecutionPlan, PlanProperties,
    ReplaceChildrenOptions,
};

mod predicate;
mod stream;
use stream::{AdmittedInput, AdmittedOutput, Transaction};
#[cfg(test)]
mod flow_tests;
#[cfg(test)]
mod tests;

#[derive(Debug)]
pub(crate) struct ManagedFilterExec {
    filter: FilterExec,
    input: Arc<dyn ExecutionPlan>,
    pool: Arc<dyn MemoryPool>,
    transaction: Arc<Transaction>,
}

impl ManagedFilterExec {
    pub(crate) fn wrap(
        filter: FilterExec,
        pool: Option<Arc<dyn MemoryPool>>,
    ) -> Result<Arc<dyn ExecutionPlan>> {
        let Some(pool) = pool else {
            return Ok(Arc::new(filter));
        };
        // With batch_size=1, DF forwards every nonempty result without coalescing.
        // Therefore the next input pull cannot overlap a previous buffered result.
        if filter.batch_size() != 1 {
            return Err(DataFusionError::Plan(
                "managed filter requires non-coalescing batch_size=1".into(),
            ));
        }
        let input = filter.input().clone();
        let transaction = Arc::new(Transaction::default());
        let admitted = Arc::new(AdmittedInput::new(
            input.clone(),
            transaction.clone(),
            pool.clone(),
            filter.projection().as_ref().map(|indices| indices.to_vec()),
        ));
        // Install the input admission once during common lowering, never per batch.
        // Its properties are identical to the actual child. The cached DF filter
        // retains its own metric set across all invocations of the task-local plan.
        let predicate = Arc::new(predicate::GatherAdmission::new(
            filter
                .predicate()
                .downcast_ref::<predicate::GatherAdmission>()
                .map_or_else(
                    || filter.predicate().clone(),
                    |predicate| predicate.inner.clone(),
                ),
            transaction.clone(),
            pool.clone(),
            filter.projection().as_ref().map(|indices| indices.to_vec()),
        ));
        let filter = datafusion::physical_plan::filter::FilterExecBuilder::from(&filter)
            .with_input(admitted)
            .with_predicate(predicate)
            .build()?;
        Ok(Arc::new(Self {
            filter,
            input,
            pool,
            transaction,
        }))
    }
}

impl DisplayAs for ManagedFilterExec {
    fn fmt_as(&self, kind: DisplayFormatType, f: &mut fmt::Formatter) -> fmt::Result {
        self.filter.fmt_as(kind, f)
    }
}

impl ExecutionPlan for ManagedFilterExec {
    fn name(&self) -> &str {
        "ManagedFilterExec"
    }
    fn properties(&self) -> &Arc<PlanProperties> {
        self.filter.properties()
    }
    fn children(&self) -> Vec<&Arc<dyn ExecutionPlan>> {
        vec![&self.input]
    }
    fn maintains_input_order(&self) -> Vec<bool> {
        self.filter.maintains_input_order()
    }
    fn apply_expressions(
        &self,
        f: &mut dyn FnMut(&Arc<dyn PhysicalExpr>) -> Result<TreeNodeRecursion>,
    ) -> Result<TreeNodeRecursion> {
        self.filter.apply_expressions(f)
    }
    fn metrics(&self) -> Option<MetricsSet> {
        self.filter.metrics()
    }
    fn child_stats_requests(&self, partition: Option<usize>) -> Vec<ChildStats> {
        self.filter.child_stats_requests(partition)
    }
    fn statistics_from_inputs(
        &self,
        input_stats: &[Arc<Statistics>],
        args: &StatisticsArgs,
    ) -> Result<Arc<Statistics>> {
        self.filter.statistics_from_inputs(input_stats, args)
    }
    fn cardinality_effect(&self) -> CardinalityEffect {
        self.filter.cardinality_effect()
    }
    fn fetch(&self) -> Option<usize> {
        self.filter.fetch()
    }
    fn replace_children(
        self: Arc<Self>,
        children: Vec<Arc<dyn ExecutionPlan>>,
        options: ReplaceChildrenOptions,
    ) -> Result<Arc<dyn ExecutionPlan>> {
        let filter = Arc::new(self.filter.clone()).replace_children(children, options)?;
        Self::wrap(
            filter.downcast_ref::<FilterExec>().unwrap().clone(),
            Some(self.pool.clone()),
        )
    }
    fn with_new_children(
        self: Arc<Self>,
        children: Vec<Arc<dyn ExecutionPlan>>,
    ) -> Result<Arc<dyn ExecutionPlan>> {
        self.replace_children(
            children,
            ReplaceChildrenOptions::new(ChildrenPropertiesMode::Recompute),
        )
    }
    fn execute(
        &self,
        partition: usize,
        context: Arc<TaskContext>,
    ) -> Result<SendableRecordBatchStream> {
        if partition != 0 {
            return Err(DataFusionError::Execution(
                "managed filter requires a task-local single partition".into(),
            ));
        }
        let guard = self.transaction.begin(&self.pool)?;
        let inner = self.filter.execute(partition, context)?;
        Ok(Box::pin(AdmittedOutput::new(inner, guard)))
    }
}
