// Copyright 2026 StreamFusion Authors
// Licensed under the Apache License, Version 2.0.

//! Port-tagged C Data at a region edge. One pull drives all native exits cooperatively;
//! schemas are negotiated once per output per invocation, including outputs arriving late.
use crate::{execution_context::NativeExecutionContext, planner::region::RegionOutput};
use arrow::datatypes::SchemaRef;
use arrow::ffi::{FFI_ArrowArray, FFI_ArrowSchema};
use datafusion::error::{DataFusionError, Result};
use datafusion::execution::memory_pool::MemoryReservation;
use futures::StreamExt;
use std::collections::HashMap;
use std::sync::{
    atomic::{AtomicI64, Ordering},
    Arc, Mutex, OnceLock,
};

pub(super) struct Output {
    stream: Option<RegionOutput>,
    schemas: Vec<SchemaRef>,
    emitted_schema: Vec<bool>,
    failed: bool,
    _inputs: Vec<MemoryReservation>,
    context: Arc<NativeExecutionContext>,
}
impl Output {
    pub(super) fn new(
        context: Arc<NativeExecutionContext>,
        stream: RegionOutput,
        inputs: Vec<MemoryReservation>,
    ) -> Self {
        let schemas = stream.schemas();
        let emitted_schema = vec![false; schemas.len()];
        Self {
            stream: Some(stream),
            schemas,
            emitted_schema,
            failed: false,
            _inputs: inputs,
            context,
        }
    }
    /// Caller supplies fresh empty C descriptors. On failure, all native cleanup finishes
    /// before JNI installs a Java exception; no output descriptor is partially published.
    pub(super) unsafe fn next(
        &mut self,
        array: *mut FFI_ArrowArray,
        schema: *mut FFI_ArrowSchema,
    ) -> Result<i32> {
        let result = std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| unsafe {
            self.next_inner(array, schema)
        }));
        match result {
            Ok(Ok(port)) => Ok(port),
            other => {
                self.failed = true;
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
    ) -> Result<i32> {
        if self.failed {
            return Err(DataFusionError::Execution(
                "native region output has failed".into(),
            ));
        }
        if array.is_null() || schema.is_null() {
            return Err(DataFusionError::Execution(
                "native region output C Data addresses must be non-null".into(),
            ));
        }
        let Some(stream) = &mut self.stream else {
            return Ok(-1);
        };
        let Some(next) = self.context.runtime().block_on(stream.next()) else {
            drop(self.stream.take());
            return Ok(-1);
        };
        let next = next?;
        let port = i32::try_from(next.port).map_err(|_| {
            DataFusionError::Execution(
                "native region output port exceeds Java integer range".into(),
            )
        })?;
        let expected = self.schemas.get(next.port).ok_or_else(|| {
            DataFusionError::Execution("native region returned an unknown output port".into())
        })?;
        if next.batch.schema() != *expected {
            return Err(DataFusionError::Execution(
                "native region output schema changed during invocation".into(),
            ));
        }
        let output = self.context.reservation("native region Arrow output");
        let batch = crate::memory_pool::arrow_lease::edge_batch(
            next.batch,
            output.new_empty(),
            crate::memory_pool::buffer_registry(self.context.task_context().memory_pool()),
        )?;
        let exported_schema = if !self.emitted_schema[next.port] {
            Some(crate::memory_pool::c_data::schema(
                expected,
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
                self.emitted_schema[next.port] = true;
            }
        }
        Ok(port)
    }
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
    let result = unsafe { output.next(array, schema) };
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
