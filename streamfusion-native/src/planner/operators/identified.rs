// Copyright 2026 StreamFusion Authors
// Licensed under the Apache License, Version 2.0

use std::fmt;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::Arc;

use datafusion::common::tree_node::TreeNodeRecursion;
use datafusion::error::{DataFusionError, Result};
use datafusion::execution::{SendableRecordBatchStream, TaskContext};
use datafusion::physical_expr::PhysicalExpr;
use datafusion::physical_plan::stream::RecordBatchStreamAdapter;
use datafusion::physical_plan::{
    ChildrenPropertiesMode, DisplayAs, DisplayFormatType, ExecutionPlan, PlanProperties,
    ReplaceChildrenOptions,
};
use futures::StreamExt;

/// Transparent boundary retaining the protobuf identity of one StreamFusion physical operator.
///
/// DataFusion operators implementing a single Flink operator may form a small subtree (for
/// example, Calc can lower to FilterExec + ProjectionExec). Nested `IdentifiedExec` nodes delimit
/// child StreamFusion operators so metrics can be attributed to the matching Java plan node
/// without double counting their children.
#[derive(Debug)]
pub(crate) struct IdentifiedExec {
    plan_node_id: u64,
    input: Arc<dyn ExecutionPlan>,
    output_rows: Arc<AtomicU64>,
    output_batches: Arc<AtomicU64>,
}

impl IdentifiedExec {
    pub(crate) fn wrap(plan_node_id: u64, input: Arc<dyn ExecutionPlan>) -> Arc<dyn ExecutionPlan> {
        Arc::new(Self {
            plan_node_id,
            input,
            output_rows: Arc::new(AtomicU64::new(0)),
            output_batches: Arc::new(AtomicU64::new(0)),
        })
    }

    pub(crate) fn plan_node_id(&self) -> u64 {
        self.plan_node_id
    }

    pub(crate) fn input(&self) -> &Arc<dyn ExecutionPlan> {
        &self.input
    }

    pub(crate) fn output_rows(&self) -> u64 {
        self.output_rows.load(Ordering::Relaxed)
    }

    pub(crate) fn output_batches(&self) -> u64 {
        self.output_batches.load(Ordering::Relaxed)
    }

    pub(crate) fn input_batches(&self) -> u64 {
        fn child_batches(plan: &Arc<dyn ExecutionPlan>) -> u64 {
            if let Some(stage) = plan.downcast_ref::<IdentifiedExec>() {
                return stage.output_batches();
            }
            plan.children().into_iter().map(child_batches).sum()
        }
        child_batches(&self.input)
    }

    pub(crate) fn input_rows(&self) -> u64 {
        fn child_rows(plan: &Arc<dyn ExecutionPlan>) -> u64 {
            if let Some(stage) = plan.downcast_ref::<IdentifiedExec>() {
                return stage.output_rows();
            }
            plan.children().into_iter().map(child_rows).sum()
        }
        child_rows(&self.input)
    }
}

impl DisplayAs for IdentifiedExec {
    fn fmt_as(&self, format: DisplayFormatType, f: &mut fmt::Formatter) -> fmt::Result {
        self.input.fmt_as(format, f)
    }
}

impl ExecutionPlan for IdentifiedExec {
    fn name(&self) -> &str {
        self.input.name()
    }

    fn properties(&self) -> &Arc<PlanProperties> {
        self.input.properties()
    }

    fn children(&self) -> Vec<&Arc<dyn ExecutionPlan>> {
        vec![&self.input]
    }

    fn apply_expressions(
        &self,
        _f: &mut dyn FnMut(&Arc<dyn PhysicalExpr>) -> Result<TreeNodeRecursion>,
    ) -> Result<TreeNodeRecursion> {
        Ok(TreeNodeRecursion::Continue)
    }

    fn replace_children(
        self: Arc<Self>,
        mut children: Vec<Arc<dyn ExecutionPlan>>,
        _options: ReplaceChildrenOptions,
    ) -> Result<Arc<dyn ExecutionPlan>> {
        if children.len() != 1 {
            return Err(DataFusionError::Internal(format!(
                "identified plan node {} expected one child, received {}",
                self.plan_node_id,
                children.len()
            )));
        }
        Ok(Self::wrap(self.plan_node_id, children.swap_remove(0)))
    }

    #[allow(deprecated)]
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
        let output_rows = Arc::clone(&self.output_rows);
        let output_batches = Arc::clone(&self.output_batches);
        let stream = self.input.execute(partition, context)?.map(move |result| {
            if let Ok(batch) = &result {
                output_rows.fetch_add(batch.num_rows() as u64, Ordering::Relaxed);
                output_batches.fetch_add(1, Ordering::Relaxed);
            }
            result
        });
        Ok(Box::pin(RecordBatchStreamAdapter::new(
            self.schema(),
            stream,
        )))
    }
}
