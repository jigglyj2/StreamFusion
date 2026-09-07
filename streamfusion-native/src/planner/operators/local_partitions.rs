// Copyright 2026 StreamFusion Authors
// Licensed under the Apache License, Version 2.0

use std::fmt;
use std::pin::Pin;
use std::sync::Arc;
use std::task::{Context, Poll};

use arrow::datatypes::SchemaRef;
use arrow::record_batch::RecordBatch;
use datafusion::common::tree_node::TreeNodeRecursion;
use datafusion::error::{DataFusionError, Result};
use datafusion::execution::memory_pool::{MemoryConsumer, MemoryReservation};
use datafusion::execution::TaskContext;
use datafusion::physical_expr::{EquivalenceProperties, PhysicalExpr};
use datafusion::physical_plan::{
    DisplayAs, DisplayFormatType, ExecutionPlan, ExecutionPlanProperties, Partitioning,
    PlanProperties, RecordBatchStream, SendableRecordBatchStream,
};
use futures::Stream;

/// Flink distributes tasks; a task-local native stage must expose one stream to its parent.
/// Apply this structural adapter to every lowered node, not to an operator-pair whitelist.
pub(in crate::planner) fn single_partition(
    input: Arc<dyn ExecutionPlan>,
) -> Arc<dyn ExecutionPlan> {
    if input.output_partitioning().partition_count() == 1 {
        return input;
    }
    // Per-partition ordering is not global ordering. Do not carry it across the merge.
    let properties = Arc::new(PlanProperties::new(
        EquivalenceProperties::new(input.schema()),
        Partitioning::UnknownPartitioning(1),
        input.properties().emission_type,
        input.properties().boundedness,
    ));
    Arc::new(LocalPartitionsExec { input, properties })
}

#[derive(Debug)]
struct LocalPartitionsExec {
    input: Arc<dyn ExecutionPlan>,
    properties: Arc<PlanProperties>,
}
impl DisplayAs for LocalPartitionsExec {
    fn fmt_as(&self, _: DisplayFormatType, f: &mut fmt::Formatter) -> fmt::Result {
        write!(f, "StreamFusionLocalPartitionsExec")
    }
}
impl ExecutionPlan for LocalPartitionsExec {
    fn name(&self) -> &str {
        "StreamFusionLocalPartitionsExec"
    }
    fn properties(&self) -> &Arc<PlanProperties> {
        &self.properties
    }
    fn children(&self) -> Vec<&Arc<dyn ExecutionPlan>> {
        vec![&self.input]
    }
    fn apply_expressions(
        &self,
        _: &mut dyn FnMut(&Arc<dyn PhysicalExpr>) -> Result<TreeNodeRecursion>,
    ) -> Result<TreeNodeRecursion> {
        Ok(TreeNodeRecursion::Continue)
    }
    fn with_new_children(
        self: Arc<Self>,
        mut children: Vec<Arc<dyn ExecutionPlan>>,
    ) -> Result<Arc<dyn ExecutionPlan>> {
        if children.len() != 1 {
            return Err(DataFusionError::Plan(
                "Local partition adapter requires one child".into(),
            ));
        }
        Ok(single_partition(children.remove(0)))
    }
    fn execute(
        &self,
        partition: usize,
        context: Arc<TaskContext>,
    ) -> Result<SendableRecordBatchStream> {
        if partition != 0 {
            return Err(DataFusionError::Execution(
                "Local partition adapter has only partition zero".into(),
            ));
        }
        let count = self.input.output_partitioning().partition_count();
        let bytes = count
            .checked_mul(2048)
            .and_then(|bytes| bytes.checked_add(1024))
            .ok_or_else(|| {
                DataFusionError::ResourcesExhausted(
                    "local partition stream allowance overflow".into(),
                )
            })?;
        let reservation = MemoryConsumer::new("native local partition stream controls")
            .register(context.memory_pool());
        reservation.try_grow(bytes)?;
        let mut streams = Vec::with_capacity(count);
        for index in 0..count {
            streams.push(Some(self.input.execute(index, context.clone())?));
        }
        Ok(Box::pin(LocalPartitionStream {
            schema: self.schema(),
            streams,
            remaining: count,
            next: 0,
            _reservation: reservation,
        }))
    }
}

/// Like DataFusion's interleaving stream, poll local children without spawning background
/// workers or buffering batches in channels. Flink control events and cancellation must not
/// race a worker retaining mutable state after the owning invocation has returned.
struct LocalPartitionStream {
    schema: SchemaRef,
    streams: Vec<Option<SendableRecordBatchStream>>,
    remaining: usize,
    next: usize,
    _reservation: MemoryReservation,
}
impl RecordBatchStream for LocalPartitionStream {
    fn schema(&self) -> SchemaRef {
        self.schema.clone()
    }
}
impl Stream for LocalPartitionStream {
    type Item = Result<RecordBatch>;
    fn poll_next(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Option<Self::Item>> {
        if self.remaining == 0 {
            return Poll::Ready(None);
        }
        for _ in 0..self.streams.len() {
            let index = self.next;
            self.next = (index + 1) % self.streams.len();
            let Some(stream) = self.streams[index].as_mut() else {
                continue;
            };
            match stream.as_mut().poll_next(cx) {
                Poll::Ready(Some(Err(error))) => {
                    self.streams.clear();
                    self.remaining = 0;
                    return Poll::Ready(Some(Err(error)));
                }
                Poll::Ready(Some(Ok(batch))) => return Poll::Ready(Some(Ok(batch))),
                Poll::Ready(None) => {
                    self.streams[index] = None;
                    self.remaining -= 1;
                }
                Poll::Pending => {}
            }
        }
        if self.remaining == 0 {
            Poll::Ready(None)
        } else {
            Poll::Pending
        }
    }
}

#[cfg(test)]
mod tests;
