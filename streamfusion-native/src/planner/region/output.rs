// Copyright 2026 StreamFusion Authors
// Licensed under the Apache License, Version 2.0.

use super::PhysicalRegion;
use arrow::datatypes::SchemaRef;
use arrow::record_batch::RecordBatch;
use datafusion::error::{DataFusionError, Result};
use datafusion::execution::TaskContext;
use futures::{stream::SelectAll, Stream, StreamExt};
use std::pin::Pin;
use std::sync::{atomic::Ordering, Arc};
use std::task::{Context, Poll};

type TaggedStream = Pin<Box<dyn Stream<Item = Result<RegionBatch>> + Send>>;
type Completion = Box<dyn FnOnce(bool) + Send>;

pub(crate) struct RegionBatch {
    pub(crate) port: usize,
    pub(crate) batch: RecordBatch,
}

/// Multiple schemas cannot be exported through one Arrow C Stream. The native boundary
/// consumes tagged batches and exports each batch with the schema of its declared output.
/// Polling all roots cooperatively prevents a fast consumer from filling shared queues.
pub(crate) struct RegionOutput {
    streams: Option<SelectAll<TaggedStream>>,
    owner: Arc<PhysicalRegion>,
    completion: Option<Completion>,
}
impl PhysicalRegion {
    pub(crate) fn output_schemas(&self) -> Vec<SchemaRef> {
        self.outputs.iter().map(|plan| plan.schema()).collect()
    }
    pub(crate) fn start(
        self: &Arc<Self>,
        task: Arc<TaskContext>,
        completion: Completion,
    ) -> Result<RegionOutput> {
        if !Arc::ptr_eq(&self.pool, task.memory_pool()) {
            return Err(DataFusionError::Plan(
                "region execution must use its lowering memory pool".into(),
            ));
        }
        self.invocation.compare_exchange(0, 1, Ordering::AcqRel, Ordering::Acquire).map_err(|_| DataFusionError::Execution("native region invocation is active or failed; failed execution requires recovery".into()))?;
        let mut output = RegionOutput {
            streams: Some(SelectAll::new()),
            owner: self.clone(),
            completion: Some(completion),
        };
        for shared in &self.shared {
            shared.arm()?;
        }
        for (port, plan) in self.outputs.iter().enumerate() {
            let stream = plan.execute(0, task.clone())?;
            output.streams.as_mut().unwrap().push(Box::pin(
                stream.map(move |result| result.map(|batch| RegionBatch { port, batch })),
            ));
        }
        Ok(output)
    }
}
impl RegionOutput {
    pub(crate) fn schemas(&self) -> Vec<SchemaRef> {
        self.owner.output_schemas()
    }
    fn finish(&mut self, successful: bool) {
        let Some(streams) = self.streams.take() else {
            return;
        };
        drop(streams);
        for shared in self.owner.shared.iter().rev() {
            shared.clear();
        }
        self.owner
            .invocation
            .store(if successful { 0 } else { 2 }, Ordering::Release);
        if let Some(completion) = self.completion.take() {
            completion(successful);
        }
    }
}
impl Stream for RegionOutput {
    type Item = Result<RegionBatch>;
    fn poll_next(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Option<Self::Item>> {
        let this = self.get_mut();
        let Some(streams) = this.streams.as_mut() else {
            return Poll::Ready(None);
        };
        let polled = std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| {
            Pin::new(streams).poll_next(cx)
        }));
        match polled {
            Ok(Poll::Ready(None)) => {
                if this.owner.shared.iter().all(|shared| shared.completed()) {
                    this.finish(true);
                    Poll::Ready(None)
                } else {
                    this.finish(false);
                    Poll::Ready(Some(Err(DataFusionError::Execution(
                        "native region outputs ended before their shared producers drained".into(),
                    ))))
                }
            }
            Ok(Poll::Ready(Some(Err(error)))) => {
                this.finish(false);
                Poll::Ready(Some(Err(error)))
            }
            Err(_) => {
                this.finish(false);
                Poll::Ready(Some(Err(DataFusionError::Execution(
                    "native region producer panicked".into(),
                ))))
            }
            Ok(other) => other,
        }
    }
}
impl Drop for RegionOutput {
    fn drop(&mut self) {
        self.finish(false);
    }
}
