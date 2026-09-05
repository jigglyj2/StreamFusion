// Copyright 2026 StreamFusion Authors
// Licensed under the Apache License, Version 2.0

use std::sync::Arc;

use arrow::ffi::{FFI_ArrowArray, FFI_ArrowSchema};
use datafusion::error::DataFusionError;
use jni::errors::ThrowRuntimeExAndDefault;
use jni::objects::{JByteArray, JClass, JLongArray, JObject, JString};
use jni::sys::{jbyteArray, jint, jlong, jlongArray};
use jni::EnvUnowned;

use super::common::{export_record_batch, import_record_batch};
use crate::memory_pool::{HostMemoryReservation, JvmMemoryReservationBroker};
use crate::planner::operators::regular_join::RegularJoinProcessor;

#[unsafe(no_mangle)]
pub extern "system" fn Java_tech_streamfusion_nativebridge_NativeRegularJoinBridge_createHandle<
    'caller,
>(
    mut unowned_env: EnvUnowned<'caller>,
    _class: JClass<'caller>,
    plan: JByteArray<'caller>,
    max_parallelism: jint,
    first_group: jint,
    last_group: jint,
    manager: JObject<'caller>,
    limit: jlong,
) -> jlong {
    unowned_env
        .with_env(|env| -> jni::errors::Result<_> {
            if limit <= 0 {
                return Err(throw(
                    env,
                    "regular join native memory limit must be positive",
                ));
            }
            let plan = env.convert_byte_array(plan)?;
            let broker = Arc::new(JvmMemoryReservationBroker::new(
                env.get_java_vm()?,
                env.new_global_ref(manager)?,
            ));
            let value = (|| -> datafusion::error::Result<_> {
                RegularJoinProcessor::new(
                    &plan,
                    non_negative(max_parallelism, "max parallelism")?,
                    non_negative(first_group, "first key group")?,
                    non_negative(last_group, "last key group")?,
                    HostMemoryReservation::new(broker, "native regular join keyed state"),
                )
            })()
            .map_err(|error| throw(env, error))?;
            Ok(Box::into_raw(Box::new(value)) as jlong)
        })
        .resolve::<ThrowRuntimeExAndDefault>()
}

#[unsafe(no_mangle)]
pub extern "system" fn Java_tech_streamfusion_nativebridge_NativeRegularJoinBridge_createRocksHandle<
    'caller,
>(
    mut unowned_env: EnvUnowned<'caller>,
    _class: JClass<'caller>,
    plan: JByteArray<'caller>,
    max_parallelism: jint,
    first_group: jint,
    last_group: jint,
    plugin: JString<'caller>,
    database: JString<'caller>,
    manager: JObject<'caller>,
    limit: jlong,
) -> jlong {
    unowned_env
        .with_env(|env| -> jni::errors::Result<_> {
            let plan = env.convert_byte_array(plan)?;
            let limit = usize::try_from(limit)
                .map_err(|_| throw(env, "regular join RocksDB limit must fit usize"))?;
            let broker = Arc::new(JvmMemoryReservationBroker::new(
                env.get_java_vm()?,
                env.new_global_ref(manager)?,
            ));
            let value = (|| -> datafusion::error::Result<_> {
                RegularJoinProcessor::new_rocksdb(
                    &plan,
                    non_negative(max_parallelism, "max parallelism")?,
                    non_negative(first_group, "first key group")?,
                    non_negative(last_group, "last key group")?,
                    std::path::Path::new(&plugin.to_string()),
                    std::path::Path::new(&database.to_string()),
                    limit,
                    HostMemoryReservation::new(broker, "native RocksDB regular join scratch"),
                )
            })()
            .map_err(|error| throw(env, error))?;
            Ok(Box::into_raw(Box::new(value)) as jlong)
        })
        .resolve::<ThrowRuntimeExAndDefault>()
}

#[unsafe(no_mangle)]
pub extern "system" fn Java_tech_streamfusion_nativebridge_NativeRegularJoinBridge_processArrowBatch<
    'caller,
>(
    mut unowned_env: EnvUnowned<'caller>,
    _class: JClass<'caller>,
    handle: jlong,
    side: jint,
    input_array: jlong,
    input_schema: jlong,
    output_array: jlong,
    output_schema: jlong,
) -> jlong {
    unowned_env
        .with_env(|env| -> jni::errors::Result<_> {
            let rows = (|| -> datafusion::error::Result<_> {
                unsafe {
                    let input = import_record_batch(
                        input_array as *mut FFI_ArrowArray,
                        input_schema as *mut FFI_ArrowSchema,
                    )?;
                    let output = processor(handle)?.process_arrow(
                        usize::try_from(side).map_err(|_| {
                            DataFusionError::Execution("regular join side is negative".to_string())
                        })?,
                        input,
                    )?;
                    export_record_batch(
                        output,
                        output_array as *mut FFI_ArrowArray,
                        output_schema as *mut FFI_ArrowSchema,
                    )
                }
            })()
            .map_err(|error| throw(env, error))?;
            Ok(rows as jlong)
        })
        .resolve::<ThrowRuntimeExAndDefault>()
}

#[unsafe(no_mangle)]
pub extern "system" fn Java_tech_streamfusion_nativebridge_NativeRegularJoinBridge_processBoundedFrame<
    'caller,
>(
    mut unowned_env: EnvUnowned<'caller>,
    _class: JClass<'caller>,
    handle: jlong,
    side: jint,
    key_group: jint,
    exchange_plan: JByteArray<'caller>,
    payload: JByteArray<'caller>,
    payload_offset: jint,
    payload_length: jint,
    metadata_length: jint,
) -> jlong {
    unowned_env
        .with_env(|env| -> jni::errors::Result<_> {
            let array_length = payload.len(env)?;
            let payload_offset = usize::try_from(payload_offset)
                .map_err(|_| throw(env, "regular join exchange payload offset was negative"))?;
            let payload_length = usize::try_from(payload_length)
                .map_err(|_| throw(env, "regular join exchange payload length was negative"))?;
            let metadata_length = usize::try_from(metadata_length)
                .map_err(|_| throw(env, "regular join exchange metadata length was negative"))?;
            if payload_offset > array_length
                || payload_length > array_length - payload_offset
                || metadata_length > payload_length
            {
                return Err(throw(
                    env,
                    "regular join exchange payload range exceeded its Java array",
                ));
            }
            let exchange_plan = env.convert_byte_array(exchange_plan)?;
            let mut payload_bytes = vec![0u8; payload_length];
            let signed = unsafe {
                std::slice::from_raw_parts_mut(
                    payload_bytes.as_mut_ptr().cast::<jni::sys::jbyte>(),
                    payload_length,
                )
            };
            payload.get_region(env, payload_offset as i32, signed)?;
            let rows = unsafe { processor(handle) }
                .and_then(|value| {
                    value.process_bounded_exchange_frame(
                        usize::try_from(side).map_err(|_| {
                            DataFusionError::Execution("regular join side is negative".to_string())
                        })?,
                        non_negative(key_group, "key group")?,
                        &exchange_plan,
                        payload_bytes,
                        metadata_length,
                    )
                })
                .map_err(|error| throw(env, error))?;
            Ok(rows as jlong)
        })
        .resolve::<ThrowRuntimeExAndDefault>()
}

#[unsafe(no_mangle)]
pub extern "system" fn Java_tech_streamfusion_nativebridge_NativeRegularJoinBridge_finishBoundedOutput<
    'caller,
>(
    mut unowned_env: EnvUnowned<'caller>,
    _class: JClass<'caller>,
    handle: jlong,
    output_array: jlong,
    output_schema: jlong,
) -> jlong {
    unowned_env
        .with_env(|env| -> jni::errors::Result<_> {
            let rows = (|| -> datafusion::error::Result<_> {
                unsafe {
                    let output = processor(handle)?.finish_bounded_output()?;
                    export_record_batch(
                        output,
                        output_array as *mut FFI_ArrowArray,
                        output_schema as *mut FFI_ArrowSchema,
                    )
                }
            })()
            .map_err(|error| throw(env, error))?;
            Ok(rows as jlong)
        })
        .resolve::<ThrowRuntimeExAndDefault>()
}

#[unsafe(no_mangle)]
pub extern "system" fn Java_tech_streamfusion_nativebridge_NativeRegularJoinBridge_nativeStatistics<
    'caller,
>(
    mut unowned_env: EnvUnowned<'caller>,
    _class: JClass<'caller>,
    handle: jlong,
) -> jlongArray {
    unowned_env
        .with_env(|env| -> jni::errors::Result<_> {
            let values = unsafe { processor(handle) }
                .map(|value| value.statistics())
                .map_err(|error| throw(env, error))?;
            let values = values.map(|value| value.min(i64::MAX as u64) as jlong);
            let output: JLongArray<'_> = env.new_long_array(values.len())?;
            output.set_region(env, 0, &values)?;
            Ok(output.into_raw())
        })
        .resolve::<ThrowRuntimeExAndDefault>()
}

#[unsafe(no_mangle)]
pub extern "system" fn Java_tech_streamfusion_nativebridge_NativeRegularJoinBridge_snapshotKeyGroup<
    'caller,
>(
    mut unowned_env: EnvUnowned<'caller>,
    _class: JClass<'caller>,
    handle: jlong,
    group: jint,
) -> jbyteArray {
    unowned_env
        .with_env(|env| -> jni::errors::Result<_> {
            let bytes = unsafe { processor(handle) }
                .and_then(|value| value.snapshot_key_group(non_negative(group, "key group")?))
                .map_err(|error| throw(env, error))?;
            Ok(env.byte_array_from_slice(&bytes)?.into_raw())
        })
        .resolve::<ThrowRuntimeExAndDefault>()
}

#[unsafe(no_mangle)]
pub extern "system" fn Java_tech_streamfusion_nativebridge_NativeRegularJoinBridge_restoreKeyGroup<
    'caller,
>(
    mut unowned_env: EnvUnowned<'caller>,
    _class: JClass<'caller>,
    handle: jlong,
    group: jint,
    bytes: JByteArray<'caller>,
) {
    unowned_env
        .with_env(|env| -> jni::errors::Result<_> {
            let bytes = env.convert_byte_array(bytes)?;
            unsafe { processor(handle) }
                .and_then(|value| {
                    value.restore_key_group(non_negative(group, "key group")?, &bytes)
                })
                .map_err(|error| throw(env, error))
        })
        .resolve::<ThrowRuntimeExAndDefault>()
}

#[unsafe(no_mangle)]
pub extern "system" fn Java_tech_streamfusion_nativebridge_NativeRegularJoinBridge_checkpointRocksHandle<
    'caller,
>(
    mut unowned_env: EnvUnowned<'caller>,
    _class: JClass<'caller>,
    handle: jlong,
    directory: JString<'caller>,
) {
    unowned_env
        .with_env(|env| -> jni::errors::Result<_> {
            unsafe { processor(handle) }
                .and_then(|value| value.checkpoint(std::path::Path::new(&directory.to_string())))
                .map_err(|error| throw(env, error))
        })
        .resolve::<ThrowRuntimeExAndDefault>()
}

#[unsafe(no_mangle)]
pub extern "system" fn Java_tech_streamfusion_nativebridge_NativeRegularJoinBridge_importRocksCheckpointHandle<
    'caller,
>(
    mut unowned_env: EnvUnowned<'caller>,
    _class: JClass<'caller>,
    target: jlong,
    plugin: JString<'caller>,
    checkpoint: JString<'caller>,
    first: jint,
    last: jint,
    limit: jlong,
) {
    unowned_env
        .with_env(|env| -> jni::errors::Result<_> {
            let limit = usize::try_from(limit)
                .map_err(|_| throw(env, "regular join restore limit must fit usize"))?;
            (|| -> datafusion::error::Result<()> {
                use crate::state::{KeyedState, RocksPluginKeyedState};
                let source = RocksPluginKeyedState::open(
                    std::path::Path::new(&plugin.to_string()),
                    std::path::Path::new(&checkpoint.to_string()),
                    non_negative(first, "first key group")?,
                    non_negative(last, "last key group")?,
                    limit,
                )?;
                let target = unsafe { processor(target) }?;
                for group in first..=last {
                    let group = non_negative(group, "key group")?;
                    target.restore_key_group(group, &source.snapshot_key_group(group)?)?;
                }
                Ok(())
            })()
            .map_err(|error| throw(env, error))
        })
        .resolve::<ThrowRuntimeExAndDefault>()
}

#[unsafe(no_mangle)]
pub extern "system" fn Java_tech_streamfusion_nativebridge_NativeRegularJoinBridge_destroyHandle<
    'caller,
>(
    mut unowned_env: EnvUnowned<'caller>,
    _class: JClass<'caller>,
    handle: jlong,
) {
    unowned_env
        .with_env(|_env| -> jni::errors::Result<_> {
            if handle != 0 {
                unsafe { drop(Box::from_raw(handle as *mut RegularJoinProcessor)) };
            }
            Ok(())
        })
        .resolve::<ThrowRuntimeExAndDefault>()
}

fn non_negative(value: jint, description: &str) -> datafusion::error::Result<u32> {
    super::common::non_negative(value, "regular join", description)
}

unsafe fn processor<'a>(handle: jlong) -> datafusion::error::Result<&'a mut RegularJoinProcessor> {
    unsafe { super::common::processor_mut(handle, "regular join") }
}

fn throw(env: &mut jni::Env<'_>, error: impl std::fmt::Display) -> jni::errors::Error {
    super::common::throw(env, error)
}
