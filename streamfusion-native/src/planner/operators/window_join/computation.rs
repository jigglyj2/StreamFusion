// Copyright 2026 StreamFusion Authors
// Licensed under the Apache License, Version 2.0.

//! DataFusion computes a closed inner window. Flink-specific state/timer completion stays
//! with the caller, which acknowledges the window only after this stream reaches EOF.

use super::build_pool::WindowBuildPool;
use super::closing::ClosedWindow;
use super::*;
use crate::memory_pool::{arrow_lease, buffer_registry, buffer_size::batch_bytes};
use crate::planner::operators::reusable_input::ReusableInputExec;
use datafusion::common::JoinType;
use datafusion::execution::{runtime_env::RuntimeEnvBuilder, TaskContext};
use datafusion::physical_plan::joins::{utils::JoinFilter, NestedLoopJoinExec};
use datafusion::physical_plan::{ExecutionPlan, RecordBatchStream, SendableRecordBatchStream};
use futures::Stream;
use std::pin::Pin;
use std::task::{Context, Poll};

mod admission;

/// Shared resource installation calls this before opening state, so unsupported kernels
/// are not first discovered after a window has accumulated input.
pub(super) fn validate(plan: &proto::WindowJoin) -> Result<Option<JoinFilter>> {
    let filter = super::planning::filter(plan)?;
    for schema in [plan.left_schema.as_ref(), plan.right_schema.as_ref()] {
        let schema = arrow_schema(
            schema.ok_or_else(|| DataFusionError::Plan("window join schema missing".into()))?,
        )?;
        if schema
            .fields()
            .iter()
            .any(|field| !admission::scalar_payload(field.data_type()))
        {
            return Err(DataFusionError::Plan(
                "native window join bounded output currently requires scalar payload columns"
                    .into(),
            ));
        }
    }
    admission::predicate_row_bytes(filter.as_ref(), 0)?;
    Ok(filter)
}

pub(super) struct ClosedWindowStream {
    // Free DataFusion's raw buffers before returning their workspace, including on cancellation.
    stream: Option<SendableRecordBatchStream>,
    plan: Option<Arc<dyn ExecutionPlan>>,
    schema: SchemaRef,
    workspace: HostMemoryReservation,
    registry: Arc<arrow_lease::Registry>,
    workspace_bound: usize,
    output_bound: usize,
    completed: bool,
}

impl ClosedWindowStream {
    pub(super) fn try_new(
        closed: ClosedWindow,
        filter: Option<JoinFilter>,
        context: Arc<TaskContext>,
        owner: &HostMemoryReservation,
    ) -> Result<Self> {
        let registry = buffer_registry(context.memory_pool()).unwrap_or_default();
        let [left, right] = closed.inputs;
        if left
            .schema()
            .fields()
            .iter()
            .chain(right.schema().fields())
            .any(|field| !admission::scalar_payload(field.data_type()))
        {
            return Err(DataFusionError::Plan(
                "native window join bounded output currently requires scalar payload columns"
                    .into(),
            ));
        }
        let fields = left.num_columns().saturating_add(right.num_columns());
        // NLJ processes one complete right input (or a small left range). The physical
        // candidate can exceed the returned batch size, but never depends on left*right.
        // Cover decode-independent gather/filter buffers, coalescer queues and Arrow padding.
        let rows = context.session_config().batch_size().max(right.num_rows());
        let row_width = closed.max_row_bytes[0]
            .saturating_add(closed.max_row_bytes[1])
            .saturating_add(fields.saturating_mul(16))
            .saturating_add(32);
        let predicate_width = admission::predicate_row_bytes(filter.as_ref(), row_width)?;
        let output_bound = rows
            .saturating_mul(row_width)
            .saturating_mul(2)
            .saturating_add(fields.saturating_mul(4096));
        let workspace_bound = output_bound
            .saturating_mul(8)
            .saturating_add(rows.saturating_mul(predicate_width).saturating_mul(4));
        let mut workspace = owner.sibling("DataFusion window join candidate and output workspace");
        workspace.resize(workspace_bound)?;

        // Register existing input owners for any views forwarded by upstream kernels. C Data
        // ownership remains attached; this does not reserve or copy their buffers again.
        let left = arrow_lease::borrowed_batch(left, Some(registry.clone()))?;
        let right = arrow_lease::borrowed_batch(right, Some(registry.clone()))?;
        let build_pool = Arc::new(WindowBuildPool::new(left.clone()));
        let runtime = RuntimeEnvBuilder::from_runtime_env(&context.runtime_env())
            .with_memory_pool(build_pool)
            .build_arc()?;
        let context = Arc::new(TaskContext::new(
            context.task_id(),
            context.session_id(),
            context.session_config().clone(),
            context.scalar_functions().clone(),
            context.higher_order_functions().clone(),
            context.aggregate_functions().clone(),
            context.window_functions().clone(),
            runtime,
        ));
        let input = |batch: RecordBatch| -> Result<Arc<dyn ExecutionPlan>> {
            let source = Arc::new(ReusableInputExec::new(batch.schema()));
            source.replace_batch(batch)?;
            Ok(source)
        };
        let plan: Arc<dyn ExecutionPlan> = Arc::new(NestedLoopJoinExec::try_new(
            input(left)?,
            input(right)?,
            filter,
            &JoinType::Inner,
            None,
        )?);
        let schema = plan.schema();
        let stream = plan.execute(0, context)?;
        Ok(Self {
            stream: Some(stream),
            plan: Some(plan),
            schema,
            workspace,
            registry,
            workspace_bound,
            output_bound,
            completed: false,
        })
    }

    pub(super) fn completed(&self) -> bool {
        self.completed
    }

    fn close(&mut self) {
        self.stream = None;
        self.plan = None;
        // HostMemoryReservation owns cleanup; errors from explicit release must propagate.
    }

    fn fail(&mut self, error: DataFusionError) -> Poll<Option<Result<RecordBatch>>> {
        self.close();
        // Retained output has independent leases and remains valid on every terminal path.
        let cleanup = self.workspace.resize(0);
        Poll::Ready(Some(Err(cleanup.err().unwrap_or(error))))
    }
}

impl Stream for ClosedWindowStream {
    type Item = Result<RecordBatch>;
    fn poll_next(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Option<Self::Item>> {
        let this = self.get_mut();
        if this.stream.is_none() {
            return Poll::Ready(None);
        }
        // Output transfers consume workspace credit. Refill before the next kernel poll;
        // a downstream consumer retaining many batches must compete for the same Flink budget.
        if let Err(error) = this.workspace.resize(this.workspace_bound) {
            return this.fail(error);
        }
        match this.stream.as_mut().unwrap().as_mut().poll_next(cx) {
            Poll::Pending => Poll::Pending,
            Poll::Ready(Some(Ok(batch))) => {
                let size = match batch_bytes(&batch) {
                    Ok(size) => size,
                    Err(error) => {
                        drop(batch);
                        return this.fail(error);
                    }
                };
                if size > this.output_bound {
                    drop(batch);
                    return this.fail(DataFusionError::ResourcesExhausted(
                        "DataFusion window join output exceeds its admitted candidate bound".into(),
                    ));
                }
                match arrow_lease::host_edge_batch(batch, &mut this.workspace, &this.registry) {
                    Ok(batch) => Poll::Ready(Some(Ok(batch))),
                    Err(error) => this.fail(error),
                }
            }
            Poll::Ready(Some(Err(error))) => this.fail(error),
            Poll::Ready(None) => {
                this.close();
                if let Err(error) = this.workspace.resize(0) {
                    return this.fail(error);
                }
                this.completed = true;
                Poll::Ready(None)
            }
        }
    }
}

impl RecordBatchStream for ClosedWindowStream {
    fn schema(&self) -> SchemaRef {
        self.schema.clone()
    }
}

#[cfg(test)]
mod tests;
