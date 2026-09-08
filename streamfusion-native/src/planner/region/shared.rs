// Copyright 2026 StreamFusion Authors
// Licensed under the Apache License, Version 2.0.

use crate::execution_context::fanout;
use datafusion::common::tree_node::TreeNodeRecursion;
use datafusion::error::{DataFusionError, Result};
use datafusion::execution::TaskContext;
use datafusion::physical_expr::PhysicalExpr;
use datafusion::physical_plan::{
    DisplayAs, DisplayFormatType, ExecutionPlan, PlanProperties, SendableRecordBatchStream,
};
use std::fmt;
use std::sync::{
    atomic::{AtomicU8, Ordering},
    Arc, Mutex,
};

/// Only reused edges need a slot. Ordinary native edges remain direct DataFusion children.
/// Each invocation arms the slot once; the first reader creates the producer stream and
/// all remaining readers share it. The driver releases unused readers on every terminal path.
pub(super) struct SharedStage {
    input: Arc<dyn ExecutionPlan>,
    consumers: usize,
    invocation: Mutex<Option<Readers>>,
}
struct Readers {
    completion: Arc<AtomicU8>,
    streams: Option<Vec<Option<SendableRecordBatchStream>>>,
}
impl SharedStage {
    pub(super) fn new(input: Arc<dyn ExecutionPlan>, consumers: usize) -> Arc<Self> {
        Arc::new(Self {
            input,
            consumers,
            invocation: Mutex::new(None),
        })
    }
    pub(super) fn arm(&self) -> Result<()> {
        let mut invocation = self.invocation.lock().map_err(|_| poisoned())?;
        if invocation.is_some() {
            return Err(DataFusionError::Execution(
                "shared stage invocation is already armed".into(),
            ));
        }
        *invocation = Some(Readers {
            streams: None,
            completion: Arc::new(AtomicU8::new(0)),
        });
        Ok(())
    }
    pub(super) fn completed(&self) -> bool {
        self.invocation
            .lock()
            .ok()
            .and_then(|invocation| {
                invocation
                    .as_ref()
                    .map(|readers| readers.completion.load(Ordering::Acquire) == 1)
            })
            .unwrap_or(false)
    }
    pub(super) fn clear(&self) {
        // Drop outside the slot lock: reader cancellation cascades upstream through fan-out.
        let old = self
            .invocation
            .lock()
            .unwrap_or_else(|e| e.into_inner())
            .take();
        drop(old);
    }
    pub(super) fn reader(self: &Arc<Self>, index: usize) -> Arc<dyn ExecutionPlan> {
        Arc::new(ReadExec {
            shared: self.clone(),
            index,
        })
    }
    fn take(&self, index: usize, task: Arc<TaskContext>) -> Result<SendableRecordBatchStream> {
        let mut invocation = self.invocation.lock().map_err(|_| poisoned())?;
        let readers = invocation.as_mut().ok_or_else(|| {
            DataFusionError::Execution("shared stage has no active invocation".into())
        })?;
        if readers.streams.is_none() {
            let source = self.input.execute(0, task)?;
            let completion = readers.completion.clone();
            readers.streams = Some(
                fanout::split(
                    source,
                    self.consumers,
                    Box::new(move |success| {
                        completion.store(if success { 1 } else { 2 }, Ordering::Release);
                    }),
                )
                .into_iter()
                .map(Some)
                .collect(),
            );
        }
        readers
            .streams
            .as_mut()
            .unwrap()
            .get_mut(index)
            .and_then(Option::take)
            .ok_or_else(|| {
                DataFusionError::Execution("shared stage reader was executed more than once".into())
            })
    }
}
fn poisoned() -> DataFusionError {
    DataFusionError::Execution("shared stage invocation lock poisoned".into())
}

struct ReadExec {
    shared: Arc<SharedStage>,
    index: usize,
}
impl fmt::Debug for ReadExec {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "SharedNativeRead({})", self.index)
    }
}
impl DisplayAs for ReadExec {
    fn fmt_as(&self, _: DisplayFormatType, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        fmt::Debug::fmt(self, f)
    }
}
impl ExecutionPlan for ReadExec {
    fn name(&self) -> &str {
        "SharedNativeRead"
    }
    fn properties(&self) -> &Arc<PlanProperties> {
        self.shared.input.properties()
    }
    fn children(&self) -> Vec<&Arc<dyn ExecutionPlan>> {
        vec![&self.shared.input]
    }
    fn apply_expressions(
        &self,
        _: &mut dyn FnMut(&Arc<dyn PhysicalExpr>) -> Result<TreeNodeRecursion>,
    ) -> Result<TreeNodeRecursion> {
        Ok(TreeNodeRecursion::Continue)
    }
    fn with_new_children(
        self: Arc<Self>,
        _: Vec<Arc<dyn ExecutionPlan>>,
    ) -> Result<Arc<dyn ExecutionPlan>> {
        Err(DataFusionError::Plan(
            "shared native reads are bound after physical planning and cannot be rewritten".into(),
        ))
    }
    fn execute(
        &self,
        partition: usize,
        task: Arc<TaskContext>,
    ) -> Result<SendableRecordBatchStream> {
        if partition != 0 {
            return Err(DataFusionError::Execution(
                "shared native read requires task-local partition zero".into(),
            ));
        }
        self.shared.take(self.index, task)
    }
}
