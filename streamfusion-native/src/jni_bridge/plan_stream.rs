// Copyright 2026 StreamFusion Authors
// Licensed under the Apache License, Version 2.0

use arrow::ffi::{FFI_ArrowArray, FFI_ArrowSchema};
use arrow::ffi_stream::FFI_ArrowArrayStream;
use arrow::record_batch::RecordBatch;
use datafusion::execution::memory_pool::MemoryReservation;
use jni::errors::ThrowRuntimeExAndDefault;
use jni::objects::{JByteArray, JClass, JLongArray};
use jni::sys::jlong;
use jni::EnvUnowned;

use super::common::{export_plan_stream, import_input, throw};
use crate::execution_context;

/// Negotiate once at the plan edge. A replayable local buffer needs RowKinds too;
/// Java must not infer data-plane requirements from keyed checkpoint ownership.
#[unsafe(no_mangle)]
pub extern "system" fn Java_tech_streamfusion_nativebridge_NativeExecutionContext_readInputEnvelopeRequirement<
    'a,
>(
    mut env: EnvUnowned<'a>,
    _class: JClass<'a>,
    handle: jlong,
) -> jni::sys::jboolean {
    env.with_env(|env| -> jni::errors::Result<_> {
        let context = execution_context::get(handle).map_err(|error| throw(env, error))?;
        Ok(context.requires_input_envelope())
    })
    .resolve::<ThrowRuntimeExAndDefault>()
}

#[unsafe(no_mangle)]
pub extern "system" fn Java_tech_streamfusion_nativebridge_NativeExecutionContext_readControlCapabilities<
    'a,
>(
    mut env: EnvUnowned<'a>,
    _class: JClass<'a>,
    handle: jlong,
) -> jni::sys::jbyteArray {
    env.with_env(|env| -> jni::errors::Result<_> {
        let (bytes, memory) = execution_context::get(handle)
            .and_then(|context| context.control_capabilities())
            .map_err(|error| throw(env, error))?;
        let result = env
            .byte_array_from_slice(&bytes)
            .map(|array| array.into_raw());
        drop(bytes);
        drop(memory);
        result
    })
    .resolve::<ThrowRuntimeExAndDefault>()
}
use crate::planner::persistent::control::{ControlEvent, ControlEvents};

#[unsafe(no_mangle)]
pub extern "system" fn Java_tech_streamfusion_nativebridge_NativeExecutionContext_nativeStreamEdgeVersion(
    _env: EnvUnowned<'_>,
    _class: JClass<'_>,
) -> jni::sys::jint {
    1
}

/// Plan-level edge: input arity is supplied by Java's protobuf tree, never an operator name.
#[unsafe(no_mangle)]
pub extern "system" fn Java_tech_streamfusion_nativebridge_NativeExecutionContext_executeArrowStreamInputs<
    'caller,
>(
    mut unowned_env: EnvUnowned<'caller>,
    _class: JClass<'caller>,
    handle: jlong,
    input_arrays: JLongArray<'caller>,
    input_schemas: JLongArray<'caller>,
    output_stream: jlong,
) {
    unowned_env
        .with_env(|env| {
            execute_edge(
                env,
                handle,
                input_arrays,
                input_schemas,
                None,
                output_stream,
            )
        })
        .resolve::<ThrowRuntimeExAndDefault>();
}

#[unsafe(no_mangle)]
pub extern "system" fn Java_tech_streamfusion_nativebridge_NativeExecutionContext_nativePlanProtocolVersion(
    _env: EnvUnowned<'_>,
    _class: JClass<'_>,
) -> jni::sys::jint {
    crate::RECORD_POLICY_PLAN_PROTOCOL_VERSION as jni::sys::jint
}

#[unsafe(no_mangle)]
pub extern "system" fn Java_tech_streamfusion_nativebridge_NativeExecutionContext_nativeControlEdgeVersion(
    _env: EnvUnowned<'_>,
    _class: JClass<'_>,
) -> jni::sys::jint {
    2
}

#[unsafe(no_mangle)]
pub extern "system" fn Java_tech_streamfusion_nativebridge_NativeExecutionContext_executeArrowControlStreamInputs<
    'a,
>(
    mut env: EnvUnowned<'a>,
    _class: JClass<'a>,
    handle: jlong,
    arrays: JLongArray<'a>,
    schemas: JLongArray<'a>,
    request: JByteArray<'a>,
    output: jlong,
) {
    env.with_env(|env| execute_edge(env, handle, arrays, schemas, Some(request), output))
        .resolve::<ThrowRuntimeExAndDefault>();
}

fn execute_edge<'a>(
    env: &mut jni::Env<'a>,
    handle: jlong,
    input_arrays: JLongArray<'a>,
    input_schemas: JLongArray<'a>,
    control_payload: Option<JByteArray<'a>>,
    output_stream: jlong,
) -> jni::errors::Result<()> {
    let context = execution_context::get(handle).map_err(|error| throw(env, error))?;
    let count = input_arrays.len(env)?;
    if input_schemas.len(env)? != count || output_stream == 0 {
        return Err(throw(env, "Invalid native plan Arrow edge addresses"));
    }
    let controls = context.reservation("native plan multi-input edge controls");
    let events = if let Some(payload) = control_payload {
        controls
            .try_grow(
                payload
                    .len(env)?
                    .checked_mul(32)
                    .and_then(|n| n.checked_add(4096))
                    .ok_or_else(|| throw(env, "native control decode allowance overflow"))?,
            )
            .map_err(|error| throw(env, error))?;
        let bytes = env.convert_byte_array(payload)?;
        let decoded = ControlEvents::decode(&bytes);
        drop(bytes);
        match decoded {
            Ok(events) => Some(events),
            Err(error) => {
                drop(controls);
                return Err(throw(env, error));
            }
        }
    } else {
        None
    };
    let mut arrays = vec![0; count];
    let mut schemas = vec![0; count];
    input_arrays.get_region(env, 0, &mut arrays)?;
    input_schemas.get_region(env, 0, &mut schemas)?;
    // Finish native cleanup (including callbacks into Java) before installing a Java
    // exception. Otherwise a release callback can observe/clear the pending exception.
    unsafe {
        execute_inputs(
            &context,
            &arrays,
            &schemas,
            controls,
            events.as_deref(),
            None,
            output_stream as *mut FFI_ArrowArrayStream,
        )
    }
    .map_err(|error| throw(env, error))?;
    Ok(())
}

pub(super) unsafe fn execute_inputs(
    context: &std::sync::Arc<execution_context::NativeExecutionContext>,
    arrays: &[jlong],
    schemas: &[jlong],
    controls: MemoryReservation,
    events: Option<&[(u64, ControlEvent)]>,
    decoded: Option<(usize, RecordBatch)>,
    output: *mut FFI_ArrowArrayStream,
) -> datafusion::error::Result<()> {
    let (batches, reservations) =
        unsafe { import_inputs(context, arrays, schemas, controls, decoded) }?;
    let stream = match events {
        Some(events) => context.start_control(batches, events)?,
        None => context.start(batches)?,
    };
    unsafe { export_plan_stream(context, stream, reservations, output) }
}

/// Shared C Data import for single-output trees and port-tagged regions.
/// Inactive ports still use the negotiated schema; their Flink channels remain distinct.
pub(super) unsafe fn import_inputs(
    context: &std::sync::Arc<execution_context::NativeExecutionContext>,
    arrays: &[jlong],
    schemas: &[jlong],
    controls: MemoryReservation,
    decoded: Option<(usize, RecordBatch)>,
) -> datafusion::error::Result<(Vec<RecordBatch>, Vec<MemoryReservation>)> {
    use datafusion::error::DataFusionError;
    if arrays.len() != schemas.len() {
        return Err(DataFusionError::Execution(
            "native C Data input arity mismatch".into(),
        ));
    }
    let clocks = context.clock_input_bindings();
    let count = arrays.len().checked_sub(clocks.len()).ok_or_else(|| {
        DataFusionError::Execution("missing native clock input descriptors".into())
    })?;
    if clocks.iter().any(|&(_, port)| port >= count)
        || arrays[count..].iter().any(|&address| address == 0)
        || schemas[count..].iter().any(|&address| address == 0)
    {
        return Err(DataFusionError::Execution(
            "native clock inputs require explicit C Data descriptors".into(),
        ));
    }
    let mut batches = Vec::with_capacity(count);
    let mut reservations = Vec::with_capacity(count + 1);
    let mut row_offset = 0usize;
    for index in 0..count {
        if arrays[index] == 0 {
            if schemas[index] != 0 {
                return Err(DataFusionError::Execution(
                    "inactive native input must use its cached schema".into(),
                ));
            }
            let empty = RecordBatch::new_empty(context.input_schema(index)?);
            batches.push(super::common::prepare_input(context, empty, row_offset)?);
            continue;
        }
        let (batch, reservation) = unsafe {
            import_input(
                context,
                arrays[index] as *mut FFI_ArrowArray,
                schemas[index] as *mut FFI_ArrowSchema,
                index,
                row_offset,
            )
        }?;
        row_offset = row_offset.checked_add(batch.num_rows()).ok_or_else(|| {
            DataFusionError::Execution("native plan input ordinal overflow".into())
        })?;
        batches.push(batch);
        reservations.push(reservation);
    }
    if let Some((index, batch)) = decoded {
        let expected = batches
            .get(index)
            .ok_or_else(|| DataFusionError::Execution("exchange input port out of range".into()))?;
        if expected.schema() != batch.schema() {
            return Err(DataFusionError::Execution(
                "exchange input schema changed after negotiation".into(),
            ));
        }
        batches[index] = batch;
    }
    for (slot, &(_, port)) in clocks.iter().enumerate() {
        let clock = unsafe {
            super::common::import_record_batch(
                arrays[count + slot] as *mut FFI_ArrowArray,
                schemas[count + slot] as *mut FFI_ArrowSchema,
            )
        }?;
        let clock = crate::memory_pool::arrow_lease::borrowed_batch(
            clock,
            crate::memory_pool::buffer_registry(context.task_context().memory_pool()),
        )?;
        // An IPC port was replaced above by its one native decode before attaching clocks.
        // Clone only RecordBatch/array descriptors; producer buffers remain shared.
        batches[port] = crate::planner::operators::envelope::processing_time::attach(
            batches[port].clone(),
            clock,
        )?;
    }
    reservations.push(controls);
    Ok((batches, reservations))
}

#[cfg(test)]
mod clock_tests;
