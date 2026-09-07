// Copyright 2026 StreamFusion Authors
// Licensed under the Apache License, Version 2.0.

use super::*;
use crate::memory_pool::{tests_support::TestBroker, FlinkMemoryPool};
use arrow::array::{ArrayRef, BooleanArray, RecordBatch, StringArray};
use datafusion::datasource::memory::MemorySourceConfig;
use datafusion::physical_expr::expressions::Column;
use datafusion::physical_plan::filter::FilterExecBuilder;
use datafusion::physical_plan::stream::RecordBatchStreamAdapter;
use datafusion::prelude::SessionContext;
use futures::StreamExt;
use std::task::{Context, Poll};

#[derive(Debug)]
struct PauseAfterFirst(Arc<dyn ExecutionPlan>);
impl DisplayAs for PauseAfterFirst {
    fn fmt_as(&self, _: DisplayFormatType, f: &mut fmt::Formatter) -> fmt::Result {
        write!(f, "PauseAfterFirst")
    }
}
impl ExecutionPlan for PauseAfterFirst {
    fn name(&self) -> &str {
        "PauseAfterFirst"
    }
    fn properties(&self) -> &Arc<PlanProperties> {
        self.0.properties()
    }
    fn children(&self) -> Vec<&Arc<dyn ExecutionPlan>> {
        vec![&self.0]
    }
    fn apply_expressions(
        &self,
        _: &mut dyn FnMut(&Arc<dyn PhysicalExpr>) -> Result<TreeNodeRecursion>,
    ) -> Result<TreeNodeRecursion> {
        Ok(TreeNodeRecursion::Continue)
    }
    fn with_new_children(
        self: Arc<Self>,
        children: Vec<Arc<dyn ExecutionPlan>>,
    ) -> Result<Arc<dyn ExecutionPlan>> {
        Ok(Arc::new(Self(children[0].clone())))
    }
    fn execute(
        &self,
        partition: usize,
        context: Arc<TaskContext>,
    ) -> Result<SendableRecordBatchStream> {
        let mut inner = self.0.execute(partition, context)?;
        let mut polls = 0;
        let input = futures::stream::poll_fn(move |cx| {
            polls += 1;
            if polls == 2 {
                cx.waker().wake_by_ref();
                Poll::Pending
            } else {
                inner.as_mut().poll_next(cx)
            }
        });
        Ok(Box::pin(RecordBatchStreamAdapter::new(
            self.schema(),
            input,
        )))
    }
}

#[test]
fn empty_result_releases_workspace_before_pending_and_cancellation_allows_reuse() {
    let runtime = tokio::runtime::Builder::new_current_thread()
        .build()
        .unwrap();
    let input = RecordBatch::try_from_iter(vec![
        (
            "keep",
            Arc::new(BooleanArray::from(vec![false; 4096])) as ArrayRef,
        ),
        (
            "text",
            Arc::new(StringArray::from(vec!["x".repeat(128); 4096])) as ArrayRef,
        ),
    ])
    .unwrap();
    let source =
        MemorySourceConfig::try_new_exec(&[vec![input.clone()]], input.schema(), None).unwrap();
    let filter = FilterExecBuilder::new(
        Arc::new(Column::new("keep", 0)),
        Arc::new(PauseAfterFirst(source)),
    )
    .with_batch_size(1)
    .build()
    .unwrap();
    let broker = Arc::new(TestBroker::new(8 << 20));
    let pool = Arc::new(FlinkMemoryPool::new(broker.clone(), 8 << 20));
    let plan = ManagedFilterExec::wrap(filter, Some(pool)).unwrap();
    let context = SessionContext::new().task_ctx();
    for cancel in [true, false] {
        let mut stream = plan.execute(0, context.clone()).unwrap();
        let mut cx = Context::from_waker(futures::task::noop_waker_ref());
        assert!(matches!(stream.as_mut().poll_next(&mut cx), Poll::Pending));
        // Rejected batches and bounded stream controls require no payload reservation.
        assert_eq!(broker.reserved(), 0);
        if !cancel {
            assert!(runtime.block_on(stream.next()).is_none());
        }
        drop(stream);
        assert_eq!(broker.reserved(), 0);
    }
}
