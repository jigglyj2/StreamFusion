// Copyright 2026 StreamFusion Authors
// Licensed under the Apache License, Version 2.0

//! Common DataFusion adapter for synchronous unary persistent kernels with bounded output pulls.
//! Children may be any native ExecutionPlan; this is not a fusion registry. Unary kernels may
//! retain an admitted input cursor while draining bounded output. Multi-input kernels keep their
//! own physical operators under the same recursive region driver.
//! Invocation EOF is not Flink endInput or a mini-batch flush.

use std::fmt;
use std::pin::Pin;
use std::sync::{Arc, Mutex};
use std::task::{Context, Poll};

use super::control::{ControlEvent, ControlEvents};
use arrow::datatypes::SchemaRef;
use arrow::record_batch::RecordBatch;
use datafusion::common::tree_node::TreeNodeRecursion;
use datafusion::error::{DataFusionError, Result};
use datafusion::execution::memory_pool::{MemoryConsumer, MemoryReservation};
use datafusion::execution::{SendableRecordBatchStream, TaskContext};
use datafusion::physical_expr::{EquivalenceProperties, PhysicalExpr};
use datafusion::physical_plan::execution_plan::{Boundedness, EmissionType};
use datafusion::physical_plan::{
    DisplayAs, DisplayFormatType, ExecutionPlan, ExecutionPlanProperties, Partitioning,
    PlanProperties, RecordBatchStream,
};
use futures::Stream;

#[derive(Default)]
pub(crate) enum InvocationState {
    #[default]
    Idle,
    Active,
    Failed,
}
impl InvocationState {
    pub(crate) fn require_idle(&self, name: &str) -> Result<()> {
        if matches!(self, Self::Idle) {
            Ok(())
        } else {
            Err(DataFusionError::Execution(format!(
                "{name} native invocation is active or failed; a failed invocation requires recovery"
            )))
        }
    }
}

pub(crate) trait UnaryBatchProcessor: Send + 'static {
    const NAME: &'static str;
    fn invocation(&mut self) -> &mut InvocationState;
    fn prepare_output_schema(&mut self, input: SchemaRef) -> Result<SchemaRef>;

    /// Output allocations must already retain their admission. Never transfer ownership to
    /// Java here: the next consumer is another native stage.
    fn process_batch(&mut self, input: RecordBatch) -> Result<RecordBatch>;

    /// A kernel that pauses input computation to flush a bounded output chunk retains its
    /// admitted cursor itself. Drain that cursor before polling another input or any control.
    /// The default is constant false, so ordinary one-output kernels need no extra lock/pull.
    fn has_pending_output(&self) -> bool {
        false
    }

    fn poll_pending_output(&mut self) -> Result<Option<RecordBatch>> {
        Ok(None)
    }

    /// Called only for an explicit control invocation, after the child is fully drained.
    /// Return one admitted Arrow output at a time, then None. Cancellation requires recovery.
    fn poll_control(&mut self, _event: ControlEvent) -> Result<Option<RecordBatch>> {
        Err(DataFusionError::Plan(format!(
            "{} has no native control binding",
            Self::NAME
        )))
    }
}

pub(crate) struct UnaryExec<P: UnaryBatchProcessor> {
    processor: Arc<Mutex<P>>,
    input: Arc<dyn ExecutionPlan>,
    properties: Arc<PlanProperties>,
    node_id: u64,
}
impl<P: UnaryBatchProcessor> UnaryExec<P> {
    pub(crate) fn new(processor: Arc<Mutex<P>>, input: Arc<dyn ExecutionPlan>) -> Result<Self> {
        if input.output_partitioning().partition_count() != 1 {
            return Err(DataFusionError::Plan(format!(
                "{} requires a single task-local input partition; normalize children during lowering",
                P::NAME
            )));
        }
        let schema = {
            let mut processor = processor.lock().map_err(|_| poisoned::<P>())?;
            processor.invocation().require_idle(P::NAME)?;
            processor.prepare_output_schema(input.schema())?
        };
        Ok(Self {
            node_id: 0,
            processor,
            input,
            properties: Arc::new(PlanProperties::new(
                EquivalenceProperties::new(schema),
                Partitioning::UnknownPartitioning(1),
                EmissionType::Incremental,
                // Each invocation is finite, not the lifetime of its persistent state.
                Boundedness::Bounded,
            )),
        })
    }

    pub(crate) fn with_node_id(mut self, node_id: u64) -> Self {
        self.node_id = node_id;
        self
    }
}
impl<P: UnaryBatchProcessor> fmt::Debug for UnaryExec<P> {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        f.debug_struct(P::NAME).field("input", &self.input).finish()
    }
}
impl<P: UnaryBatchProcessor> DisplayAs for UnaryExec<P> {
    fn fmt_as(&self, _: DisplayFormatType, f: &mut fmt::Formatter) -> fmt::Result {
        f.write_str(P::NAME)
    }
}
impl<P: UnaryBatchProcessor> ExecutionPlan for UnaryExec<P> {
    fn name(&self) -> &str {
        P::NAME
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
            return Err(DataFusionError::Plan(format!(
                "{} requires one child",
                P::NAME
            )));
        }
        Ok(Arc::new(
            Self::new(self.processor.clone(), children.remove(0))?.with_node_id(self.node_id),
        ))
    }
    fn execute(
        &self,
        partition: usize,
        context: Arc<TaskContext>,
    ) -> Result<SendableRecordBatchStream> {
        if partition != 0 {
            return Err(DataFusionError::Execution(format!(
                "{} has one task-local native partition",
                P::NAME
            )));
        }
        // Admit controls before opening children or claiming mutable state.
        let memory = MemoryConsumer::new("native persistent unary stream controls")
            .register(context.memory_pool());
        memory.try_grow(std::mem::size_of::<UnaryStream<P>>() + 1024)?;
        let control = ControlEvents::for_stage(&context, self.node_id)?;
        {
            let mut processor = self.processor.lock().map_err(|_| poisoned::<P>())?;
            let state = processor.invocation();
            state.require_idle(P::NAME)?;
            *state = InvocationState::Active;
        }
        // Arm teardown before child construction, including panic unwinding. Do not hold the
        // processor lock while opening/polling children: duplicate owners must error, not deadlock.
        let mut stream = UnaryStream {
            input: None,
            processor: self.processor.clone(),
            schema: self.schema(),
            terminal: false,
            control,
            child_ended: false,
            pending_output: false,
            _memory: memory,
        };
        match self.input.execute(0, context) {
            Ok(input) => stream.input = Some(input),
            Err(error) => {
                // This processor has not consumed input. Child/context guards independently
                // protect any child state affected by the failed construction.
                stream.finish(true)?;
                return Err(error);
            }
        }
        Ok(Box::pin(stream))
    }
}

struct UnaryStream<P: UnaryBatchProcessor> {
    input: Option<SendableRecordBatchStream>,
    processor: Arc<Mutex<P>>,
    schema: SchemaRef,
    terminal: bool,
    control: Option<ControlEvent>,
    child_ended: bool,
    pending_output: bool,
    // Drop after children and processor references, never before their control storage.
    _memory: MemoryReservation,
}
impl<P: UnaryBatchProcessor> UnaryStream<P> {
    fn finish(&mut self, successful: bool) -> Result<()> {
        // Tear down upstream work before releasing this invocation's exclusion.
        self.input = None;
        self.terminal = true;
        *self
            .processor
            .lock()
            .map_err(|_| poisoned::<P>())?
            .invocation() = if successful {
            InvocationState::Idle
        } else {
            InvocationState::Failed
        };
        Ok(())
    }
}
impl<P: UnaryBatchProcessor> Stream for UnaryStream<P> {
    type Item = Result<RecordBatch>;
    fn poll_next(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Option<Self::Item>> {
        if self.terminal {
            return Poll::Ready(None);
        }
        if self.pending_output {
            let result =
                self.processor
                    .lock()
                    .map_err(|_| poisoned::<P>())
                    .and_then(|mut processor| {
                        let output = processor.poll_pending_output()?;
                        let pending = processor.has_pending_output();
                        if output.is_none() && pending {
                            return Err(DataFusionError::Execution(format!(
                                "{} pending cursor made no progress",
                                P::NAME
                            )));
                        }
                        Ok((output, pending))
                    });
            match result {
                Ok((Some(batch), pending)) if batch.schema() == self.schema => {
                    self.pending_output = pending;
                    return Poll::Ready(Some(Ok(batch)));
                }
                Ok((None, _)) => self.pending_output = false,
                Ok((Some(_), _)) => {
                    let _ = self.finish(false);
                    return Poll::Ready(Some(Err(DataFusionError::Execution(format!(
                        "{} pending output differs from its native schema",
                        P::NAME
                    )))));
                }
                Err(error) => {
                    let _ = self.finish(false);
                    return Poll::Ready(Some(Err(error)));
                }
            }
        }
        if self.child_ended {
            return self.poll_control();
        }
        match self
            .input
            .as_mut()
            .expect("constructed child")
            .as_mut()
            .poll_next(cx)
        {
            Poll::Pending => Poll::Pending,
            Poll::Ready(None) => {
                self.input = None;
                self.child_ended = true;
                self.poll_control()
            }
            Poll::Ready(Some(input)) => {
                let result = input.and_then(|batch| {
                    let mut processor = self.processor.lock().map_err(|_| poisoned::<P>())?;
                    let output = processor.process_batch(batch)?;
                    Ok((output, processor.has_pending_output()))
                });
                let result = result.map(|(output, pending)| {
                    self.pending_output = pending;
                    output
                });
                if result.is_err() {
                    // Preserve the kernel/child error even if its failure poisoned the lock.
                    let _ = self.finish(false);
                }
                Poll::Ready(Some(result))
            }
        }
    }
}
impl<P: UnaryBatchProcessor> UnaryStream<P> {
    fn poll_control(&mut self) -> Poll<Option<Result<RecordBatch>>> {
        let Some(event) = self.control else {
            return Poll::Ready(self.finish(true).err().map(Err));
        };
        let result = self
            .processor
            .lock()
            .map_err(|_| poisoned::<P>())
            .and_then(|mut processor| processor.poll_control(event));
        match result {
            Ok(Some(batch)) if batch.schema() == self.schema => Poll::Ready(Some(Ok(batch))),
            Ok(Some(_)) => {
                let _ = self.finish(false);
                Poll::Ready(Some(Err(DataFusionError::Execution(format!(
                    "{} control output differs from its native schema",
                    P::NAME
                )))))
            }
            Ok(None) => Poll::Ready(self.finish(true).err().map(Err)),
            Err(error) => {
                let _ = self.finish(false);
                Poll::Ready(Some(Err(error)))
            }
        }
    }
}
impl<P: UnaryBatchProcessor> RecordBatchStream for UnaryStream<P> {
    fn schema(&self) -> SchemaRef {
        self.schema.clone()
    }
}
impl<P: UnaryBatchProcessor> Drop for UnaryStream<P> {
    fn drop(&mut self) {
        if !self.terminal {
            let _ = self.finish(false);
        }
    }
}
fn poisoned<P: UnaryBatchProcessor>() -> DataFusionError {
    DataFusionError::Execution(format!("{} native state lock is poisoned", P::NAME))
}

#[cfg(test)]
mod tests;
