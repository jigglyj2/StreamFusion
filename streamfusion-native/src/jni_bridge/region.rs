// Copyright 2026 StreamFusion Authors
// Licensed under the Apache License, Version 2.0.

use super::{common::throw, region_output};
use crate::execution_context;
use arrow::ffi::{FFI_ArrowArray, FFI_ArrowSchema};
use jni::errors::ThrowRuntimeExAndDefault;
use jni::objects::{JByteArray, JClass, JLongArray, JObject};
use jni::sys::{jint, jlong};
use jni::EnvUnowned;

#[unsafe(no_mangle)]
pub extern "system" fn Java_tech_streamfusion_nativebridge_NativeRegionStream_createContext<'a>(
    mut env: EnvUnowned<'a>,
    _: JClass<'a>,
    plan: JByteArray<'a>,
    state: JByteArray<'a>,
    task: JByteArray<'a>,
    manager: JObject<'a>,
    limit: jlong,
) -> jlong {
    env.with_env(|env| -> jni::errors::Result<_> {
        if limit <= 0 {
            return Err(throw(env, "native memory limit must be positive"));
        }
        // Java admits these JNI copies before entry, just as for the tree contract.
        let plan = env.convert_byte_array(plan)?;
        let state = if state.is_null() {
            None
        } else {
            Some(env.convert_byte_array(state)?)
        };
        let task = if task.is_null() {
            None
        } else {
            Some(env.convert_byte_array(task)?)
        };
        let vm = env.get_java_vm()?;
        let manager = env.new_global_ref(manager)?;
        execution_context::register_region_with_resources(
            &plan,
            state.as_deref(),
            task.as_deref(),
            vm,
            manager,
            limit as usize,
        )
        .map_err(|error| throw(env, error))
    })
    .resolve::<ThrowRuntimeExAndDefault>()
}

#[unsafe(no_mangle)]
pub extern "system" fn Java_tech_streamfusion_nativebridge_NativeRegionStream_open<'a>(
    mut env: EnvUnowned<'a>,
    _: JClass<'a>,
    handle: jlong,
    input_arrays: JLongArray<'a>,
    input_schemas: JLongArray<'a>,
    control: JByteArray<'a>,
) -> jlong {
    env.with_env(|env| -> jni::errors::Result<_> {
        let context = execution_context::get(handle).map_err(|error| throw(env, error))?;
        let count = input_arrays.len(env)?;
        let expected = context
            .region_input_count()
            .map_err(|error| throw(env, error))?
            + context.clock_input_bindings().len();
        if count != expected || count != input_schemas.len(env)? {
            return Err(throw(env, "native region input arity mismatch"));
        }
        let memory = context.reservation("native region edge controls");
        let events = if control.is_null() {
            None
        } else {
            memory
                .try_grow(
                    control
                        .len(env)?
                        .checked_mul(32)
                        .and_then(|n| n.checked_add(4096))
                        .ok_or_else(|| throw(env, "native region control admission overflow"))?,
                )
                .map_err(|error| throw(env, error))?;
            let bytes = env.convert_byte_array(control)?;
            let decoded = crate::planner::persistent::control::ControlEvents::decode(&bytes);
            drop(bytes);
            match decoded {
                Ok(events) => Some(events),
                Err(error) => {
                    drop(memory);
                    return Err(throw(env, error));
                }
            }
        };
        let mut arrays = vec![0; count];
        let mut schemas = vec![0; count];
        input_arrays.get_region(env, 0, &mut arrays)?;
        input_schemas.get_region(env, 0, &mut schemas)?;
        // Drop failed imports/streams before installing a pending Java exception.
        let result = (|| -> datafusion::error::Result<_> {
            let (batches, reservations) = unsafe {
                super::plan_stream::import_inputs(&context, &arrays, &schemas, memory, None)
            }?;
            let stream = match events.as_deref() {
                Some(events) => context.start_region_control(batches, events)?,
                None => context.start_region(batches)?,
            };
            region_output::register(region_output::Output::new(
                context.clone(),
                stream,
                reservations,
            ))
        })();
        result.map_err(|error| throw(env, error))
    })
    .resolve::<ThrowRuntimeExAndDefault>()
}

#[unsafe(no_mangle)]
pub extern "system" fn Java_tech_streamfusion_nativebridge_NativeRegionStream_nextBatch<'a>(
    mut env: EnvUnowned<'a>,
    _: JClass<'a>,
    handle: jlong,
    array: jlong,
    schema: jlong,
) -> jint {
    env.with_env(|env| -> jni::errors::Result<_> {
        unsafe {
            region_output::next(
                handle,
                array as *mut FFI_ArrowArray,
                schema as *mut FFI_ArrowSchema,
            )
        }
        .map_err(|error| throw(env, error))
    })
    .resolve::<ThrowRuntimeExAndDefault>()
}
#[unsafe(no_mangle)]
pub extern "system" fn Java_tech_streamfusion_nativebridge_NativeRegionStream_release<'a>(
    mut env: EnvUnowned<'a>,
    _: JClass<'a>,
    handle: jlong,
) {
    env.with_env(|env| -> jni::errors::Result<_> {
        region_output::close(handle).map_err(|error| throw(env, error))
    })
    .resolve::<ThrowRuntimeExAndDefault>();
}

#[unsafe(no_mangle)]
pub extern "system" fn Java_tech_streamfusion_nativebridge_NativeRegionStream_nativeEdgeVersion(
    _: EnvUnowned<'_>,
    _: JClass<'_>,
) -> jint {
    2
}
