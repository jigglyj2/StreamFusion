// Copyright 2026 StreamFusion Authors
// Licensed under the Apache License, Version 2.0.

//! Port-tagged C Data at a region edge. One pull drives all native exits cooperatively;
//! schemas are negotiated once per output per invocation, including outputs arriving late.
use crate::exchange::{
    managed_routing::AccountedFrames, output_bindings::OutputBindings,
    prepared_router::PreparedRouter,
};
use crate::planner::region::RegionBatch;
use crate::{execution_context::NativeExecutionContext, planner::region::RegionOutput};
use arrow::datatypes::SchemaRef;
use arrow::ffi::{FFI_ArrowArray, FFI_ArrowSchema};
use arrow::record_batch::RecordBatch;
use datafusion::error::{DataFusionError, Result};
use datafusion::execution::memory_pool::MemoryReservation;
use datafusion::physical_plan::RecordBatchStream;
use futures::{Stream, StreamExt};
use std::collections::{HashMap, VecDeque};
use std::pin::Pin;
use std::sync::{
    atomic::{AtomicI64, Ordering},
    Arc, Mutex, OnceLock,
};

pub(super) struct Output {
    stream: Option<Pin<Box<dyn Stream<Item = Result<RegionBatch>> + Send>>>,
    schemas: Vec<SchemaRef>,
    emitted_schema: Vec<bool>,
    failed: bool,
    bindings: Option<Arc<OutputBindings>>,
    pending: VecDeque<Pending>,
    _inputs: Vec<MemoryReservation>,
    context: Arc<NativeExecutionContext>,
}
impl Output {
    pub(super) fn new(
        context: Arc<NativeExecutionContext>,
        stream: RegionOutput,
        inputs: Vec<MemoryReservation>,
    ) -> Result<Self> {
        let bindings = context.exchange_output_bindings()?;
        let schemas = stream.schemas();
        let emitted_schema = vec![false; schemas.len()];
        Ok(Self {
            stream: Some(Box::pin(stream)),
            schemas,
            emitted_schema,
            failed: false,
            bindings,
            pending: VecDeque::new(),
            _inputs: inputs,
            context,
        })
    }
    /// One edge driver for a tree or DAG. The underlying streams keep their own completion
    /// and cancellation contracts; tagging a tree's sole exit never changes execution.
    pub(super) fn start(
        context: Arc<NativeExecutionContext>,
        batches: Vec<arrow::record_batch::RecordBatch>,
        events: Option<&[(u64, crate::planner::persistent::control::ControlEvent)]>,
        inputs: Vec<MemoryReservation>,
    ) -> Result<Self> {
        if context.protocol_version() < crate::RECORD_POLICY_PLAN_PROTOCOL_VERSION {
            return Err(DataFusionError::Plan(
                "port-tagged output requires an owned-envelope plan".into(),
            ));
        }
        if context.region_input_count().is_some() {
            let stream = match events {
                Some(events) => context.start_region_control(batches, events)?,
                None => context.start_region(batches)?,
            };
            return Self::new(context, stream, inputs);
        }
        let stream = match events {
            Some(events) => context.start_control(batches, events)?,
            None => context.start(batches)?,
        };
        let schema = stream.schema();
        let bindings = context.exchange_output_bindings()?;
        Ok(Self {
            stream: Some(Box::pin(
                stream.map(|result| result.map(|batch| RegionBatch { port: 0, batch })),
            )),
            schemas: vec![schema],
            emitted_schema: vec![false],
            failed: false,
            bindings,
            pending: VecDeque::new(),
            _inputs: inputs,
            context,
        })
    }

    /// Caller supplies empty C descriptors for Arrow outputs. Frame outputs leave them untouched.
    pub(super) unsafe fn next_event(
        &mut self,
        array: *mut FFI_ArrowArray,
        schema: *mut FFI_ArrowSchema,
    ) -> Result<Event> {
        let result = std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| unsafe {
            self.next_inner(array, schema)
        }));
        match result {
            Ok(Ok(event)) => Ok(event),
            other => {
                self.failed = true;
                self.pending.clear();
                drop(self.stream.take());
                match other {
                    Ok(Err(error)) => Err(error),
                    _ => Err(DataFusionError::Execution(
                        "panic while exporting native region output".into(),
                    )),
                }
            }
        }
    }
    unsafe fn next_inner(
        &mut self,
        array: *mut FFI_ArrowArray,
        schema: *mut FFI_ArrowSchema,
    ) -> Result<Event> {
        if self.failed {
            return Err(DataFusionError::Execution(
                "native region output has failed".into(),
            ));
        }
        while self.pending.is_empty() {
            let Some(stream) = &mut self.stream else {
                return Ok(Event::End);
            };
            let Some(next) = self.context.runtime().block_on(stream.next()) else {
                drop(self.stream.take());
                return Ok(Event::End);
            };
            let next = next?;
            let expected = self.schemas.get(next.port).ok_or_else(|| {
                DataFusionError::Execution("native region returned an unknown output port".into())
            })?;
            if next.batch.schema() != *expected {
                return Err(DataFusionError::Execution(
                    "native region output schema changed during invocation".into(),
                ));
            }
            let batch = crate::memory_pool::arrow_lease::edge_batch(
                next.batch,
                self.context.reservation("native output batch ownership"),
                crate::memory_pool::buffer_registry(self.context.task_context().memory_pool()),
            )?;
            if let Some(bindings) = &self.bindings {
                let outputs = &bindings.ports[next.port];
                if outputs.arrow {
                    self.pending
                        .push_back(Pending::Arrow(next.port, batch.clone()));
                }
                for (id, router) in &outputs.frames {
                    self.pending
                        .push_back(Pending::Frames(*id, router.clone(), batch.clone()));
                }
            } else {
                self.pending.push_back(Pending::Arrow(next.port, batch));
            }
        }
        match self.pending.pop_front().unwrap() {
            Pending::Frames(id, router, batch) => {
                let rows = batch.num_rows();
                Ok(Event::Frames(id, rows, router.route_owned(batch)?))
            }
            Pending::Arrow(port, batch) => {
                if array.is_null() || schema.is_null() {
                    return Err(DataFusionError::Execution(
                        "native region output C Data addresses must be non-null".into(),
                    ));
                }
                let rows = batch.num_rows();
                let output = self.context.reservation("native region Arrow output");
                let exported_schema = if !self.emitted_schema[port] {
                    Some(crate::memory_pool::c_data::schema(
                        &self.schemas[port],
                        output.new_empty(),
                    )?)
                } else {
                    None
                };
                let exported_array = crate::memory_pool::c_data::array(batch, output)?;
                unsafe {
                    array.write(exported_array);
                    if let Some(exported) = exported_schema {
                        schema.write(exported);
                        self.emitted_schema[port] = true;
                    }
                }
                Ok(Event::Arrow(port, rows))
            }
        }
    }
}

enum Pending {
    Arrow(usize, RecordBatch),
    Frames(usize, Arc<PreparedRouter>, RecordBatch),
}
pub(super) enum Event {
    End,
    Arrow(usize, usize),
    Frames(usize, usize, AccountedFrames),
}

struct Slot {
    output: Option<Output>,
    closed: bool,
}
type Entry = Arc<Mutex<Slot>>;
static NEXT: AtomicI64 = AtomicI64::new(1);
static OUTPUTS: OnceLock<Mutex<HashMap<i64, Entry>>> = OnceLock::new();
fn outputs() -> &'static Mutex<HashMap<i64, Entry>> {
    OUTPUTS.get_or_init(|| Mutex::new(HashMap::new()))
}
fn poisoned() -> DataFusionError {
    DataFusionError::Execution("native region output registry lock poisoned".into())
}
pub(super) fn register(output: Output) -> Result<i64> {
    let handle = NEXT.fetch_add(1, Ordering::Relaxed);
    if handle <= 0 {
        return Err(DataFusionError::Execution(
            "native region output handle overflow".into(),
        ));
    }
    outputs().lock().map_err(|_| poisoned())?.insert(
        handle,
        Arc::new(Mutex::new(Slot {
            output: Some(output),
            closed: false,
        })),
    );
    Ok(handle)
}
pub(super) unsafe fn next(
    handle: i64,
    array: *mut FFI_ArrowArray,
    schema: *mut FFI_ArrowSchema,
) -> Result<i32> {
    with_output(handle, |output| unsafe {
        match output.next_event(array, schema)? {
            Event::End => Ok(-1),
            Event::Arrow(port, _) => {
                i32::try_from(port).map_err(|e| DataFusionError::External(Box::new(e)))
            }
            Event::Frames(_, _, _) => Err(DataFusionError::Execution(
                "framed outputs require the tagged transport API".into(),
            )),
        }
    })
}

pub(super) fn with_output<T>(
    handle: i64,
    action: impl FnOnce(&mut Output) -> Result<T>,
) -> Result<T> {
    let entry = outputs()
        .lock()
        .map_err(|_| poisoned())?
        .get(&handle)
        .cloned()
        .ok_or_else(|| {
            DataFusionError::Execution("native region output is missing or closed".into())
        })?;
    let mut output = {
        let mut slot = entry.lock().map_err(|_| poisoned())?;
        slot.output.take().ok_or_else(|| {
            DataFusionError::Execution("native region output is busy or closed".into())
        })?
    };
    let result = action(&mut output);
    let mut slot = entry.lock().unwrap_or_else(|e| e.into_inner());
    if !slot.closed {
        slot.output = Some(output);
    } else {
        drop(slot);
        drop(output);
    }
    result
}

pub(super) fn close(handle: i64) -> Result<()> {
    let entry = outputs().lock().map_err(|_| poisoned())?.remove(&handle);
    if let Some(entry) = entry {
        let output = {
            let mut slot = entry.lock().unwrap_or_else(|e| e.into_inner());
            slot.closed = true;
            slot.output.take()
        };
        drop(output);
    }
    Ok(())
}

#[cfg(test)]
mod tests;
