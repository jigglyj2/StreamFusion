// Copyright 2026 StreamFusion Authors
// Licensed under the Apache License, Version 2.0

use std::fmt;
use std::pin::Pin;
use std::sync::{Arc, Mutex};
use std::task::{Context, Poll};

use arrow::datatypes::SchemaRef;
use arrow::record_batch::RecordBatch;
use datafusion::common::tree_node::TreeNodeRecursion;
use datafusion::error::{DataFusionError, Result};
use datafusion::execution::{SendableRecordBatchStream, TaskContext};
use datafusion::physical_expr::{EquivalenceProperties, PhysicalExpr};
use datafusion::physical_plan::execution_plan::{Boundedness, EmissionType};
use datafusion::physical_plan::{
    DisplayAs, DisplayFormatType, ExecutionPlan, Partitioning, PlanProperties, RecordBatchStream,
};
use futures::Stream;

use super::RegularJoinProcessor;
use crate::planner::persistent::PersistentOperatorFactory;
use crate::proto;

// Bound the number of equality keys whose decoded history and dirty encodings coexist.
// These are internal zero-copy Arrow batches, not additional Flink/JNI input events.
const STATE_BATCH_ROWS: usize = 1024;

pub(crate) struct RegularJoinFactory(pub(crate) Arc<Mutex<RegularJoinProcessor>>);
impl PersistentOperatorFactory for RegularJoinFactory {
    fn supports_owned_envelope(&self) -> bool {
        // region_input selects only the planned SQL fields and explicit RowKind.
        // Regular SQL joins do not use StreamRecord timestamps or arrival ordinals.
        true
    }
    fn snapshot(&self, key_group: u32) -> Result<crate::state::SnapshotBytes> {
        self.0
            .lock()
            .map_err(|_| poisoned())?
            .snapshot_key_group(key_group)
    }
    fn restore(&self, key_group: u32, bytes: &[u8]) -> Result<()> {
        self.0
            .lock()
            .map_err(|_| poisoned())?
            .restore_key_group(key_group, bytes)
    }
    fn checkpoint(&self, directory: &std::path::Path) -> Result<()> {
        self.0.lock().map_err(|_| poisoned())?.checkpoint(directory)
    }
    fn build(
        &self,
        node: &proto::Operator,
        children: Vec<Arc<dyn ExecutionPlan>>,
    ) -> Result<Arc<dyn ExecutionPlan>> {
        if !matches!(
            node.operator,
            Some(proto::operator::Operator::RegularJoin(_))
        ) {
            return Err(DataFusionError::Plan(
                "regular join binding requires a RegularJoin node".into(),
            ));
        }
        Ok(Arc::new(RegularJoinExec::new(self.0.clone(), children)?))
    }
}

/// A persistent, two-input physical node. Flink delivers one input event at a time; the driver
/// installs that batch at its input port and an empty batch at the other port before executing
/// the tree. Child plans and parent plans exchange shared RecordBatch streams, never JNI calls.
pub(crate) struct RegularJoinExec {
    processor: Arc<Mutex<RegularJoinProcessor>>,
    inputs: Vec<Arc<dyn ExecutionPlan>>,
    properties: Arc<PlanProperties>,
}

impl RegularJoinExec {
    pub(crate) fn new(
        processor: Arc<Mutex<RegularJoinProcessor>>,
        inputs: Vec<Arc<dyn ExecutionPlan>>,
    ) -> Result<Self> {
        if inputs.len() != 2 {
            return Err(DataFusionError::Plan(
                "regular join ExecutionPlan requires two children".into(),
            ));
        }
        let schema = {
            let join = processor.lock().map_err(|_| poisoned())?;
            if join.plan.bounded_final_output
                || join.fused_output_calcs.is_some()
                || join.fused_output_projection.is_some()
            {
                return Err(DataFusionError::Plan("streaming RegularJoinExec requires a bare streaming processor; adjacent operators belong in the ExecutionPlan tree".into()));
            }
            join.output_schema.clone()
        };
        let properties = Arc::new(PlanProperties::new(
            EquivalenceProperties::new(schema),
            Partitioning::UnknownPartitioning(1),
            EmissionType::Incremental,
            Boundedness::Bounded,
        ));
        Ok(Self {
            processor,
            inputs,
            properties,
        })
    }
}

impl fmt::Debug for RegularJoinExec {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        f.debug_struct("RegularJoinExec")
            .field("inputs", &self.inputs)
            .finish_non_exhaustive()
    }
}

impl DisplayAs for RegularJoinExec {
    fn fmt_as(&self, _: DisplayFormatType, f: &mut fmt::Formatter) -> fmt::Result {
        write!(f, "StreamFusionRegularJoinExec")
    }
}

impl ExecutionPlan for RegularJoinExec {
    fn name(&self) -> &str {
        "StreamFusionRegularJoinExec"
    }
    fn properties(&self) -> &Arc<PlanProperties> {
        &self.properties
    }
    fn children(&self) -> Vec<&Arc<dyn ExecutionPlan>> {
        self.inputs.iter().collect()
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
        Ok(Arc::new(Self::new(self.processor.clone(), children)?))
    }
    fn execute(
        &self,
        partition: usize,
        context: Arc<TaskContext>,
    ) -> Result<SendableRecordBatchStream> {
        if partition != 0 {
            return Err(DataFusionError::Execution(
                "a Flink join subtask has one native partition".into(),
            ));
        }
        {
            let mut join = self.processor.lock().map_err(|_| poisoned())?;
            join.require_idle_stream()?;
            join.streaming_region_active = true;
        }
        let mut stream = JoinPlanStream {
            processor: self.processor.clone(),
            schema: self.schema(),
            inputs: Vec::new(),
            side: 0,
            processing_input: false,
            pending_input: None,
            complete: false,
            terminal: false,
        };
        // A failing child constructor drops the guard and poisons the invocation just like a
        // failing downstream kernel: no partial input/output invocation can be checkpointed.
        for input in &self.inputs {
            stream.inputs.push(input.execute(0, context.clone())?);
        }
        Ok(Box::pin(stream))
    }
}

struct JoinPlanStream {
    processor: Arc<Mutex<RegularJoinProcessor>>,
    schema: SchemaRef,
    inputs: Vec<SendableRecordBatchStream>,
    side: usize,
    processing_input: bool,
    pending_input: Option<(RecordBatch, usize)>,
    complete: bool,
    terminal: bool,
}

impl JoinPlanStream {
    fn poll_batch(&mut self, cx: &mut Context<'_>) -> Poll<Result<Option<RecordBatch>>> {
        loop {
            if self.processing_input {
                let output = self
                    .processor
                    .lock()
                    .map_err(|_| poisoned())?
                    .next_native_output()?;
                if let Some(output) = output {
                    let batch =
                        crate::memory_pool::arrow_lease::host_batch(output.batch, output._memory)?;
                    return Poll::Ready(Ok(Some(batch)));
                }
                self.processing_input = false;
            }
            if let Some((batch, offset)) = self.pending_input.take() {
                let rows = batch.num_rows().min(STATE_BATCH_ROWS);
                let slice = batch.slice(0, rows);
                if rows < batch.num_rows() {
                    self.pending_input =
                        Some((batch.slice(rows, batch.num_rows() - rows), offset + rows));
                }
                self.processor
                    .lock()
                    .map_err(|_| poisoned())?
                    .begin_region_input(self.side, slice, offset)?;
                self.processing_input = true;
                continue;
            }
            if self.side == self.inputs.len() {
                self.complete = true;
                self.processor
                    .lock()
                    .map_err(|_| poisoned())?
                    .streaming_region_active = false;
                return Poll::Ready(Ok(None));
            }
            match self.inputs[self.side].as_mut().poll_next(cx) {
                Poll::Pending => return Poll::Pending,
                Poll::Ready(None) => self.side += 1,
                Poll::Ready(Some(Err(error))) => return Poll::Ready(Err(error)),
                Poll::Ready(Some(Ok(batch))) if batch.num_rows() == 0 => {}
                Poll::Ready(Some(Ok(batch))) => {
                    self.pending_input = Some((batch, 0));
                }
            }
        }
    }
}

impl Stream for JoinPlanStream {
    type Item = Result<RecordBatch>;
    fn poll_next(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Option<Self::Item>> {
        if self.terminal {
            return Poll::Ready(None);
        }
        match self.poll_batch(cx) {
            Poll::Pending => Poll::Pending,
            Poll::Ready(Ok(Some(batch))) => Poll::Ready(Some(Ok(batch))),
            Poll::Ready(result) => {
                self.terminal = true;
                Poll::Ready(result.err().map(Err))
            }
        }
    }
}

impl RecordBatchStream for JoinPlanStream {
    fn schema(&self) -> SchemaRef {
        self.schema.clone()
    }
}

impl Drop for JoinPlanStream {
    fn drop(&mut self) {
        if !self.complete {
            if let Ok(mut join) = self.processor.lock() {
                join.cancel_streaming_batch();
                join.streaming_failed = true;
                join.streaming_region_active = false;
            }
        }
    }
}

fn poisoned() -> DataFusionError {
    DataFusionError::Execution("regular join ExecutionPlan state lock is poisoned".into())
}

#[cfg(test)]
mod tests;
