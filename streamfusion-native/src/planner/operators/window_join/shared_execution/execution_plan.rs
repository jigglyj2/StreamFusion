// Copyright 2026 StreamFusion Authors
// Licensed under the Apache License, Version 2.0.

use super::super::computation::ClosedWindowStream;
use super::*;
use crate::planner::persistent::control::ControlEvents;
use datafusion::common::tree_node::TreeNodeRecursion;
use datafusion::execution::TaskContext;
use datafusion::physical_expr::{EquivalenceProperties, PhysicalExpr};
use datafusion::physical_plan::execution_plan::{Boundedness, EmissionType};
use datafusion::physical_plan::{
    DisplayAs, DisplayFormatType, ExecutionPlanProperties, Partitioning, PlanProperties,
    RecordBatchStream, SendableRecordBatchStream,
};
use futures::Stream;
use std::fmt;
use std::pin::Pin;
use std::task::{Context, Poll};

pub(super) struct WindowJoinExec {
    owner: Arc<Mutex<SharedWindowJoin>>,
    node_id: u64,
    children: Vec<Arc<dyn ExecutionPlan>>,
    properties: Arc<PlanProperties>,
}
impl WindowJoinExec {
    pub(super) fn new(
        owner: Arc<Mutex<SharedWindowJoin>>,
        node_id: u64,
        children: Vec<Arc<dyn ExecutionPlan>>,
    ) -> Result<Self> {
        if children.len() != 2
            || children
                .iter()
                .any(|input| input.output_partitioning().partition_count() != 1)
        {
            return Err(invalid(
                "shared window join requires two single-partition native children",
            ));
        }
        let schema = {
            let state = owner.lock().map_err(|_| poisoned())?;
            state.invocation.require_idle(NAME)?;
            for (side, input) in children.iter().enumerate() {
                state.validate_input(side, &input.schema())?;
            }
            state.output_schema.clone()
        };
        Ok(Self {
            owner,
            node_id,
            children,
            properties: Arc::new(PlanProperties::new(
                EquivalenceProperties::new(schema),
                Partitioning::UnknownPartitioning(1),
                EmissionType::Incremental,
                Boundedness::Bounded,
            )),
        })
    }
}
impl fmt::Debug for WindowJoinExec {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct(NAME)
            .field("children", &self.children)
            .finish()
    }
}
impl DisplayAs for WindowJoinExec {
    fn fmt_as(&self, _: DisplayFormatType, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "{NAME}")
    }
}
impl ExecutionPlan for WindowJoinExec {
    fn name(&self) -> &str {
        NAME
    }
    fn properties(&self) -> &Arc<PlanProperties> {
        &self.properties
    }
    fn children(&self) -> Vec<&Arc<dyn ExecutionPlan>> {
        self.children.iter().collect()
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
        Ok(Arc::new(Self::new(
            self.owner.clone(),
            self.node_id,
            children,
        )?))
    }
    fn execute(
        &self,
        partition: usize,
        context: Arc<TaskContext>,
    ) -> Result<SendableRecordBatchStream> {
        if partition != 0 {
            return Err(invalid(
                "shared window join has one native subtask partition",
            ));
        }
        let event = ControlEvents::for_stage(&context, self.node_id)?;
        {
            let mut owner = self.owner.lock().map_err(|_| poisoned())?;
            owner.invocation.require_idle(NAME)?;
            owner.invocation = InvocationState::Active;
        }
        let mut invocation = JoinInvocation {
            compute: None,
            owner: self.owner.clone(),
            context,
            inputs: Vec::new(),
            side: 0,
            schema: self.schema(),
            event,
            control_started: false,
            complete: false,
            terminal: false,
        };
        for input in &self.children {
            invocation
                .inputs
                .push(input.execute(0, invocation.context.clone())?);
        }
        Ok(Box::pin(invocation))
    }
}

struct JoinInvocation {
    compute: Option<ClosedWindowStream>,
    owner: Arc<Mutex<SharedWindowJoin>>,
    context: Arc<TaskContext>,
    inputs: Vec<SendableRecordBatchStream>,
    side: usize,
    schema: SchemaRef,
    event: Option<ControlEvent>,
    control_started: bool,
    complete: bool,
    terminal: bool,
}
impl JoinInvocation {
    fn poll_batch(&mut self, cx: &mut Context<'_>) -> Poll<Result<Option<RecordBatch>>> {
        while self.side < self.inputs.len() {
            match self.inputs[self.side].as_mut().poll_next(cx) {
                Poll::Pending => return Poll::Pending,
                Poll::Ready(None) => self.side += 1,
                Poll::Ready(Some(Err(error))) => return Poll::Ready(Err(error)),
                Poll::Ready(Some(Ok(batch))) if batch.num_rows() == 0 => {}
                Poll::Ready(Some(Ok(batch))) => self
                    .owner
                    .lock()
                    .map_err(|_| poisoned())?
                    .ingest(self.side, batch)?,
            }
        }
        if !self.control_started {
            match self.event {
                Some(ControlEvent::Watermark(watermark)) => self
                    .owner
                    .lock()
                    .map_err(|_| poisoned())?
                    .kernel
                    .begin_watermark(watermark)?,
                None | Some(ControlEvent::BeforeCheckpoint(_) | ControlEvent::EndInput) => {}
                _ => {
                    return Poll::Ready(Err(invalid(
                        "shared window join does not support processing-time controls",
                    )))
                }
            }
            self.control_started = true;
        }
        loop {
            if let Some(compute) = &mut self.compute {
                match Pin::new(&mut *compute).poll_next(cx) {
                    Poll::Pending => return Poll::Pending,
                    Poll::Ready(Some(Err(error))) => return Poll::Ready(Err(error)),
                    Poll::Ready(Some(Ok(batch))) => {
                        return Poll::Ready(
                            self.owner
                                .lock()
                                .map_err(|_| poisoned())?
                                .envelope(batch)
                                .map(Some),
                        )
                    }
                    Poll::Ready(None) => {
                        if !compute.completed() {
                            return Poll::Ready(Err(invalid(
                                "window join compute ended without successful completion",
                            )));
                        }
                        self.compute = None;
                        self.owner
                            .lock()
                            .map_err(|_| poisoned())?
                            .kernel
                            .finish_closed_window()?;
                    }
                }
            }
            if matches!(self.event, Some(ControlEvent::Watermark(_))) {
                let mut owner = self.owner.lock().map_err(|_| poisoned())?;
                if let Some(closed) = owner.kernel.next_closed_window()? {
                    self.compute = Some(ClosedWindowStream::try_new(
                        closed,
                        owner.filter.clone(),
                        self.context.clone(),
                        &owner.kernel.state_memory(),
                    )?);
                    continue;
                }
            }
            self.owner.lock().map_err(|_| poisoned())?.invocation = InvocationState::Idle;
            self.complete = true;
            return Poll::Ready(Ok(None));
        }
    }
}
impl Stream for JoinInvocation {
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
                if result.is_err() {
                    self.compute = None;
                    self.inputs.clear();
                    if let Ok(mut owner) = self.owner.lock() {
                        owner.invocation = InvocationState::Failed;
                    }
                }
                Poll::Ready(result.err().map(Err))
            }
        }
    }
}
impl RecordBatchStream for JoinInvocation {
    fn schema(&self) -> SchemaRef {
        self.schema.clone()
    }
}
impl Drop for JoinInvocation {
    fn drop(&mut self) {
        if !self.complete {
            if let Ok(mut owner) = self.owner.lock() {
                owner.invocation = InvocationState::Failed;
            }
        }
    }
}
