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
use crate::planner::operators::multi_join::MultiJoinProcessor;

#[unsafe(no_mangle)]
pub extern "system" fn Java_tech_streamfusion_nativebridge_NativeMultiJoinBridge_createHandle<
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
                    "multi-join native memory limit must be positive",
                ));
            }
            let plan = env.convert_byte_array(plan)?;
            let broker = Arc::new(JvmMemoryReservationBroker::new(
                env.get_java_vm()?,
                env.new_global_ref(manager)?,
            ));
            let value = (|| -> datafusion::error::Result<_> {
                MultiJoinProcessor::new(
                    &plan,
                    non_negative(max_parallelism, "max parallelism")?,
                    non_negative(first_group, "first key group")?,
                    non_negative(last_group, "last key group")?,
                    HostMemoryReservation::new(broker, "native multi-join keyed state"),
                )
            })()
            .map_err(|error| throw(env, error))?;
            Ok(Box::into_raw(Box::new(value)) as jlong)
        })
        .resolve::<ThrowRuntimeExAndDefault>()
}

#[unsafe(no_mangle)]
pub extern "system" fn Java_tech_streamfusion_nativebridge_NativeMultiJoinBridge_createRocksHandle<
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
                .map_err(|_| throw(env, "multi-join RocksDB limit must fit usize"))?;
            let broker = Arc::new(JvmMemoryReservationBroker::new(
                env.get_java_vm()?,
                env.new_global_ref(manager)?,
            ));
            let value = (|| -> datafusion::error::Result<_> {
                MultiJoinProcessor::new_rocksdb(
                    &plan,
                    non_negative(max_parallelism, "max parallelism")?,
                    non_negative(first_group, "first key group")?,
                    non_negative(last_group, "last key group")?,
                    std::path::Path::new(&plugin.to_string()),
                    std::path::Path::new(&database.to_string()),
                    limit,
                    HostMemoryReservation::new(broker, "native RocksDB multi-join scratch"),
                )
            })()
            .map_err(|error| throw(env, error))?;
            Ok(Box::into_raw(Box::new(value)) as jlong)
        })
        .resolve::<ThrowRuntimeExAndDefault>()
}

#[unsafe(no_mangle)]
pub extern "system" fn Java_tech_streamfusion_nativebridge_NativeMultiJoinBridge_processArrowBatch<
    'caller,
>(
    mut unowned_env: EnvUnowned<'caller>,
    _class: JClass<'caller>,
    handle: jlong,
    input: jint,
    input_array: jlong,
    input_schema: jlong,
    output_array: jlong,
    output_schema: jlong,
) -> jlong {
    unowned_env
        .with_env(|env| -> jni::errors::Result<_> {
            let rows = (|| -> datafusion::error::Result<_> {
                unsafe {
                    let input_batch = import_record_batch(
                        input_array as *mut FFI_ArrowArray,
                        input_schema as *mut FFI_ArrowSchema,
                    )?;
                    let output = processor(handle)?.process_arrow(
                        usize::try_from(input).map_err(|_| {
                            DataFusionError::Execution("multi-join input is negative".to_string())
                        })?,
                        input_batch,
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
pub extern "system" fn Java_tech_streamfusion_nativebridge_NativeMultiJoinBridge_nativeStatistics<
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
pub extern "system" fn Java_tech_streamfusion_nativebridge_NativeMultiJoinBridge_snapshotKeyGroup<
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
pub extern "system" fn Java_tech_streamfusion_nativebridge_NativeMultiJoinBridge_restoreKeyGroup<
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
pub extern "system" fn Java_tech_streamfusion_nativebridge_NativeMultiJoinBridge_checkpointRocksHandle<
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
pub extern "system" fn Java_tech_streamfusion_nativebridge_NativeMultiJoinBridge_importRocksCheckpointHandle<
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
                .map_err(|_| throw(env, "multi-join restore limit must fit usize"))?;
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
pub extern "system" fn Java_tech_streamfusion_nativebridge_NativeMultiJoinBridge_destroyHandle<
    'caller,
>(
    mut unowned_env: EnvUnowned<'caller>,
    _class: JClass<'caller>,
    handle: jlong,
) {
    unowned_env
        .with_env(|_env| -> jni::errors::Result<_> {
            if handle != 0 {
                unsafe { drop(Box::from_raw(handle as *mut MultiJoinProcessor)) };
            }
            Ok(())
        })
        .resolve::<ThrowRuntimeExAndDefault>()
}

fn non_negative(value: jint, description: &str) -> datafusion::error::Result<u32> {
    super::common::non_negative(value, "multi-join", description)
}

unsafe fn processor<'a>(handle: jlong) -> datafusion::error::Result<&'a mut MultiJoinProcessor> {
    unsafe { super::common::processor_mut(handle, "multi-join") }
}

fn throw(env: &mut jni::Env<'_>, error: impl std::fmt::Display) -> jni::errors::Error {
    super::common::throw(env, error)
}
