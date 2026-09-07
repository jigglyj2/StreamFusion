// Copyright 2026 StreamFusion Authors
// Licensed under the Apache License, Version 2.0.

use super::*;
use arrow::array::RecordBatch;
use arrow::datatypes::SchemaRef;
use datafusion::execution::memory_pool::{MemoryConsumer, MemoryReservation};
use datafusion::physical_plan::stream::RecordBatchStreamAdapter;
use datafusion::physical_plan::RecordBatchStream;
use futures::Stream;
use std::pin::Pin;
use std::sync::Mutex;
use std::task::{Context, Poll};

#[derive(Debug, Default)]
struct State {
    active: bool,
    workspace: Option<MemoryReservation>,
}

#[derive(Debug, Default)]
pub(super) struct Transaction(Mutex<State>);

impl Transaction {
    pub(super) fn begin(self: &Arc<Self>, pool: &Arc<dyn MemoryPool>) -> Result<Guard> {
        let mut state = self.0.lock().map_err(|_| invalid("poisoned transaction"))?;
        if state.active {
            return Err(invalid("concurrent invocation"));
        }
        state.active = true;
        Ok(Guard {
            transaction: self.clone(),
            registry: crate::memory_pool::buffer_registry(pool),
        })
    }

    fn discard_empty_result(&self) -> Result<()> {
        // A new input pull proves DF consumed/discarded the previous empty result.
        // Release before polling upstream, including when that poll returns Pending.
        self.0
            .lock()
            .map_err(|_| invalid("poisoned transaction"))?
            .workspace = None;
        Ok(())
    }

    pub(super) fn prepare(
        &self,
        batch: &RecordBatch,
        projection: Option<&[usize]>,
        pool: &Arc<dyn MemoryPool>,
        gather: bool,
    ) -> Result<()> {
        let mut state = self.0.lock().map_err(|_| invalid("poisoned transaction"))?;
        if !state.active {
            return Err(invalid("input without active invocation"));
        }
        if !gather {
            state.workspace = None;
            return Ok(());
        }
        let bytes = workspace(batch, projection)?;
        let memory = MemoryConsumer::new("native filter gather workspace").register(pool);
        memory.try_grow(bytes)?;
        state.workspace = Some(memory);
        Ok(())
    }

    fn finish(
        &self,
        batch: RecordBatch,
        registry: Option<Arc<crate::memory_pool::arrow_lease::Registry>>,
    ) -> Result<RecordBatch> {
        let Some(memory) = self
            .0
            .lock()
            .map_err(|_| invalid("poisoned transaction"))?
            .workspace
            .take()
        else {
            return Ok(batch); // All-pass output retains the original input owners.
        };
        if crate::memory_pool::buffer_size::batch_bytes(&batch)? > memory.size() {
            drop(batch);
            return Err(invalid("gather output exceeded its admitted workspace"));
        }
        memory.try_resize(crate::memory_pool::buffer_size::batch_bytes(&batch)?)?;
        crate::memory_pool::arrow_lease::datafusion_batch_registered(batch, memory, registry)
    }
}

fn invalid(message: &str) -> DataFusionError {
    DataFusionError::Execution(format!("native filter admission: {message}"))
}

/// Filtering selects a subsequence: output payload cannot exceed projected input
/// storage. Reserve overlap, validity, selection indices, and descriptor scratch.
/// Predicate-specific workspaces remain owned by the expression admission hook.
fn workspace(batch: &RecordBatch, projection: Option<&[usize]>) -> Result<usize> {
    let indices: Vec<_> =
        projection.map_or_else(|| (0..batch.num_columns()).collect(), |p| p.to_vec());
    let mut bytes = batch
        .num_rows()
        .checked_mul(8)
        .ok_or_else(|| invalid("gather size overflow"))?;
    for index in indices {
        // Logical buffer spans bound a gather without multiplying a shared IPC allocation
        // by the schema width. Factor two covers output capacity rounding, not input copies.
        let data = batch.column(index).to_data();
        bytes = bytes
            .checked_add(
                data.get_slice_memory_size()?
                    .checked_mul(2)
                    .ok_or_else(|| invalid("gather size overflow"))?,
            )
            .and_then(|n| n.checked_add(1024))
            .ok_or_else(|| invalid("gather size overflow"))?;
    }
    Ok(bytes)
}

pub(super) struct Guard {
    transaction: Arc<Transaction>,
    registry: Option<Arc<crate::memory_pool::arrow_lease::Registry>>,
}
impl Drop for Guard {
    fn drop(&mut self) {
        let mut state = self
            .transaction
            .0
            .lock()
            .unwrap_or_else(|error| error.into_inner());
        state.workspace = None;
        state.active = false;
    }
}

#[derive(Debug)]
pub(super) struct AdmittedInput {
    input: Arc<dyn ExecutionPlan>,
    transaction: Arc<Transaction>,
}

impl AdmittedInput {
    pub(super) fn new(
        input: Arc<dyn ExecutionPlan>,
        transaction: Arc<Transaction>,
        _pool: Arc<dyn MemoryPool>,
        _projection: Option<Vec<usize>>,
    ) -> Self {
        Self { input, transaction }
    }
}

impl DisplayAs for AdmittedInput {
    fn fmt_as(&self, kind: DisplayFormatType, f: &mut fmt::Formatter) -> fmt::Result {
        self.input.fmt_as(kind, f)
    }
}
impl ExecutionPlan for AdmittedInput {
    fn name(&self) -> &str {
        "FilterAdmissionInput"
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
    fn with_new_children(
        self: Arc<Self>,
        _children: Vec<Arc<dyn ExecutionPlan>>,
    ) -> Result<Arc<dyn ExecutionPlan>> {
        Err(invalid(
            "internal admission input is rebuilt by its filter owner",
        ))
    }
    fn execute(
        &self,
        partition: usize,
        context: Arc<TaskContext>,
    ) -> Result<SendableRecordBatchStream> {
        let transaction = self.transaction.clone();
        let mut inner = self.input.execute(partition, context)?;
        let input = futures::stream::poll_fn(move |cx| {
            if let Err(error) = transaction.discard_empty_result() {
                return Poll::Ready(Some(Err(error)));
            }
            inner.as_mut().poll_next(cx)
        });
        Ok(Box::pin(RecordBatchStreamAdapter::new(
            self.schema(),
            input,
        )))
    }
}

pub(super) struct AdmittedOutput {
    // Always destroy DF's potentially retained inputs/output before returning scratch credit.
    inner: Option<SendableRecordBatchStream>,
    schema: SchemaRef,
    guard: Option<Guard>,
}
impl AdmittedOutput {
    pub(super) fn new(inner: SendableRecordBatchStream, guard: Guard) -> Self {
        Self {
            schema: inner.schema(),
            inner: Some(inner),
            guard: Some(guard),
        }
    }
    fn close(&mut self) {
        self.inner = None;
        self.guard = None;
    }
}
impl RecordBatchStream for AdmittedOutput {
    fn schema(&self) -> SchemaRef {
        self.schema.clone()
    }
}
impl Stream for AdmittedOutput {
    type Item = Result<RecordBatch>;
    fn poll_next(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Option<Self::Item>> {
        let Some(inner) = self.inner.as_mut() else {
            return Poll::Ready(None);
        };
        match inner.as_mut().poll_next(cx) {
            Poll::Ready(Some(Ok(batch))) => {
                let guard = self.guard.as_ref().unwrap();
                let result = guard.transaction.finish(batch, guard.registry.clone());
                if result.is_err() {
                    self.close();
                }
                Poll::Ready(Some(result))
            }
            Poll::Ready(other) => {
                self.close();
                Poll::Ready(other)
            }
            Poll::Pending => Poll::Pending,
        }
    }
}
