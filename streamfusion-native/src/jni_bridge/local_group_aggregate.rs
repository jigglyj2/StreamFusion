// Copyright 2026 StreamFusion Authors
// Licensed under the Apache License, Version 2.0

use std::sync::Arc;

use arrow::ffi::{FFI_ArrowArray, FFI_ArrowSchema};
use jni::errors::ThrowRuntimeExAndDefault;
use jni::objects::{JByteArray, JClass, JObject};
use jni::sys::jlong;
use jni::EnvUnowned;

use super::common::{export_record_batch, import_record_batch};
use crate::memory_pool::{HostMemoryReservation, JvmMemoryReservationBroker};
use crate::planner::operators::local_group_aggregate::LocalGroupAggregateProcessor;

#[unsafe(no_mangle)]
pub extern "system" fn Java_tech_streamfusion_nativebridge_NativeLocalGroupAggregateBridge_createHandle<
    'caller,
>(
    mut unowned_env: EnvUnowned<'caller>,
    _class: JClass<'caller>,
    serialized_plan: JByteArray<'caller>,
    memory_manager: JObject<'caller>,
    memory_limit: jlong,
) -> jlong {
    unowned_env
        .with_env(|env| -> jni::errors::Result<_> {
            let plan = env.convert_byte_array(serialized_plan)?;
            if memory_limit <= 0 {
                return Err(throw(
                    env,
                    "local group aggregate native memory limit must be positive",
                ));
            }
            let broker = Arc::new(JvmMemoryReservationBroker::new(
                env.get_java_vm()?,
                env.new_global_ref(memory_manager)?,
            ));
            let processor = LocalGroupAggregateProcessor::new(
                &plan,
                HostMemoryReservation::new(broker, "native local group aggregate bundle"),
            )
            .map_err(|error| throw(env, error))?;
            Ok(Box::into_raw(Box::new(processor)) as jlong)
        })
        .resolve::<ThrowRuntimeExAndDefault>()
}

#[unsafe(no_mangle)]
pub extern "system" fn Java_tech_streamfusion_nativebridge_NativeLocalGroupAggregateBridge_processArrowBatch<
    'caller,
>(
    mut unowned_env: EnvUnowned<'caller>,
    _class: JClass<'caller>,
    handle: jlong,
    input_array_address: jlong,
    input_schema_address: jlong,
    output_array_address: jlong,
    output_schema_address: jlong,
) -> jlong {
    unowned_env
        .with_env(|env| -> jni::errors::Result<_> {
            let rows = (|| -> datafusion::error::Result<_> {
                unsafe {
                    let input = import_record_batch(
                        input_array_address as *mut FFI_ArrowArray,
                        input_schema_address as *mut FFI_ArrowSchema,
                    )?;
                    let output = processor(handle)?.process_arrow(input)?;
                    export_record_batch(
                        output,
                        output_array_address as *mut FFI_ArrowArray,
                        output_schema_address as *mut FFI_ArrowSchema,
                    )
                }
            })()
            .map_err(|error| throw(env, error))?;
            Ok(rows as jlong)
        })
        .resolve::<ThrowRuntimeExAndDefault>()
}

#[unsafe(no_mangle)]
pub extern "system" fn Java_tech_streamfusion_nativebridge_NativeLocalGroupAggregateBridge_finishBundleNative<
    'caller,
>(
    mut unowned_env: EnvUnowned<'caller>,
    _class: JClass<'caller>,
    handle: jlong,
    output_array_address: jlong,
    output_schema_address: jlong,
) -> jlong {
    unowned_env
        .with_env(|env| -> jni::errors::Result<_> {
            let rows = (|| -> datafusion::error::Result<_> {
                unsafe {
                    let output = processor(handle)?.finish_bundle()?;
                    export_record_batch(
                        output,
                        output_array_address as *mut FFI_ArrowArray,
                        output_schema_address as *mut FFI_ArrowSchema,
                    )
                }
            })()
            .map_err(|error| throw(env, error))?;
            Ok(rows as jlong)
        })
        .resolve::<ThrowRuntimeExAndDefault>()
}

#[unsafe(no_mangle)]
pub extern "system" fn Java_tech_streamfusion_nativebridge_NativeLocalGroupAggregateBridge_pendingElementCountNative<
    'caller,
>(
    mut unowned_env: EnvUnowned<'caller>,
    _class: JClass<'caller>,
    handle: jlong,
) -> jlong {
    unowned_env
        .with_env(|env| -> jni::errors::Result<_> {
            Ok(unsafe { processor(handle) }
                .map(|processor| processor.pending_element_count() as jlong)
                .map_err(|error| throw(env, error))?)
        })
        .resolve::<ThrowRuntimeExAndDefault>()
}

#[unsafe(no_mangle)]
pub extern "system" fn Java_tech_streamfusion_nativebridge_NativeLocalGroupAggregateBridge_pendingKeyCountNative<
    'caller,
>(
    mut unowned_env: EnvUnowned<'caller>,
    _class: JClass<'caller>,
    handle: jlong,
) -> jlong {
    unowned_env
        .with_env(|env| -> jni::errors::Result<_> {
            Ok(unsafe { processor(handle) }
                .map(|processor| processor.pending_key_count() as jlong)
                .map_err(|error| throw(env, error))?)
        })
        .resolve::<ThrowRuntimeExAndDefault>()
}

#[unsafe(no_mangle)]
pub extern "system" fn Java_tech_streamfusion_nativebridge_NativeLocalGroupAggregateBridge_destroyHandle<
    'caller,
>(
    mut unowned_env: EnvUnowned<'caller>,
    _class: JClass<'caller>,
    handle: jlong,
) {
    unowned_env
        .with_env(|_env| -> jni::errors::Result<_> {
            if handle != 0 {
                unsafe {
                    drop(Box::from_raw(handle as *mut LocalGroupAggregateProcessor));
                }
            }
            Ok(())
        })
        .resolve::<ThrowRuntimeExAndDefault>()
}

unsafe fn processor<'a>(
    handle: jlong,
) -> datafusion::error::Result<&'a mut LocalGroupAggregateProcessor> {
    unsafe { super::common::processor_mut(handle, "local group aggregate") }
}

fn throw(env: &mut jni::Env<'_>, error: impl std::fmt::Display) -> jni::errors::Error {
    super::common::throw(env, error)
}
