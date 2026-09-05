// Copyright 2026 StreamFusion Authors
// Licensed under the Apache License, Version 2.0

use std::sync::Arc;

use arrow::ffi::{FFI_ArrowArray, FFI_ArrowSchema};
use jni::errors::ThrowRuntimeExAndDefault;
use jni::objects::{JByteArray, JClass, JLongArray, JObject, JString};
use jni::sys::{jbyteArray, jint, jlong, jlongArray};
use jni::EnvUnowned;

use super::common::{export_record_batch, import_record_batch};
use crate::memory_pool::{HostMemoryReservation, JvmMemoryReservationBroker};
use crate::planner::operators::session_window_table_function::SessionWindowTableFunctionProcessor;

#[unsafe(no_mangle)]
pub extern "system" fn Java_tech_streamfusion_nativebridge_NativeSessionWindowTableFunctionBridge_createHandle<
    'caller,
>(
    mut unowned_env: EnvUnowned<'caller>,
    _class: JClass<'caller>,
    serialized_plan: JByteArray<'caller>,
    max_parallelism: jint,
    first_key_group: jint,
    last_key_group: jint,
    memory_manager: JObject<'caller>,
    memory_limit: jlong,
) -> jlong {
    unowned_env
        .with_env(|env| -> jni::errors::Result<_> {
            if memory_limit <= 0 {
                return Err(throw(
                    env,
                    "session Window TVF native memory limit must be positive",
                ));
            }
            let plan = env.convert_byte_array(serialized_plan)?;
            let broker = Arc::new(JvmMemoryReservationBroker::new(
                env.get_java_vm()?,
                env.new_global_ref(memory_manager)?,
            ));
            let value = (|| -> datafusion::error::Result<_> {
                SessionWindowTableFunctionProcessor::new(
                    &plan,
                    non_negative(max_parallelism, "max parallelism")?,
                    non_negative(first_key_group, "first key group")?,
                    non_negative(last_key_group, "last key group")?,
                    HostMemoryReservation::new(broker, "native session Window TVF keyed state"),
                )
            })()
            .map_err(|error| throw(env, error))?;
            Ok(Box::into_raw(Box::new(value)) as jlong)
        })
        .resolve::<ThrowRuntimeExAndDefault>()
}

#[unsafe(no_mangle)]
pub extern "system" fn Java_tech_streamfusion_nativebridge_NativeSessionWindowTableFunctionBridge_createRocksHandle<
    'caller,
>(
    mut unowned_env: EnvUnowned<'caller>,
    _class: JClass<'caller>,
    serialized_plan: JByteArray<'caller>,
    max_parallelism: jint,
    first_key_group: jint,
    last_key_group: jint,
    plugin_path: JString<'caller>,
    database_path: JString<'caller>,
    memory_manager: JObject<'caller>,
    memory_limit: jlong,
) -> jlong {
    unowned_env
        .with_env(|env| -> jni::errors::Result<_> {
            let plan = env.convert_byte_array(serialized_plan)?;
            let limit = usize::try_from(memory_limit).map_err(|_| {
                throw(
                    env,
                    "session Window TVF RocksDB memory limit must fit usize",
                )
            })?;
            let broker = Arc::new(JvmMemoryReservationBroker::new(
                env.get_java_vm()?,
                env.new_global_ref(memory_manager)?,
            ));
            let value = (|| -> datafusion::error::Result<_> {
                SessionWindowTableFunctionProcessor::new_rocksdb(
                    &plan,
                    non_negative(max_parallelism, "max parallelism")?,
                    non_negative(first_key_group, "first key group")?,
                    non_negative(last_key_group, "last key group")?,
                    std::path::Path::new(&plugin_path.to_string()),
                    std::path::Path::new(&database_path.to_string()),
                    limit,
                    HostMemoryReservation::new(
                        broker,
                        "native RocksDB session Window TVF scratch and timers",
                    ),
                )
            })()
            .map_err(|error| throw(env, error))?;
            Ok(Box::into_raw(Box::new(value)) as jlong)
        })
        .resolve::<ThrowRuntimeExAndDefault>()
}

#[unsafe(no_mangle)]
pub extern "system" fn Java_tech_streamfusion_nativebridge_NativeSessionWindowTableFunctionBridge_processArrowBatch<
    'caller,
>(
    mut unowned_env: EnvUnowned<'caller>,
    _class: JClass<'caller>,
    handle: jlong,
    input_array: jlong,
    input_schema: jlong,
    output_array: jlong,
    output_schema: jlong,
    processing_time: jlong,
) -> jlong {
    unowned_env
        .with_env(|env| -> jni::errors::Result<_> {
            let rows = (|| -> datafusion::error::Result<_> {
                unsafe {
                    let input = import_record_batch(
                        input_array as *mut FFI_ArrowArray,
                        input_schema as *mut FFI_ArrowSchema,
                    )?;
                    let output = processor(handle)?.process_arrow(input, processing_time)?;
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
pub extern "system" fn Java_tech_streamfusion_nativebridge_NativeSessionWindowTableFunctionBridge_advanceTime<
    'caller,
>(
    mut unowned_env: EnvUnowned<'caller>,
    _class: JClass<'caller>,
    handle: jlong,
    processing: jint,
    timestamp: jlong,
    output_array: jlong,
    output_schema: jlong,
) -> jlong {
    unowned_env
        .with_env(|env| -> jni::errors::Result<_> {
            let rows = (|| -> datafusion::error::Result<_> {
                let output = unsafe {
                    if processing != 0 {
                        processor(handle)?.advance_processing_time(timestamp)?
                    } else {
                        processor(handle)?.advance_event_time(timestamp)?
                    }
                };
                unsafe {
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
pub extern "system" fn Java_tech_streamfusion_nativebridge_NativeSessionWindowTableFunctionBridge_nextProcessingTimer<
    'caller,
>(
    mut unowned_env: EnvUnowned<'caller>,
    _class: JClass<'caller>,
    handle: jlong,
) -> jlong {
    unowned_env
        .with_env(|env| -> jni::errors::Result<_> {
            unsafe { processor(handle) }
                .map(|value| value.next_processing_time_timer())
                .map_err(|error| throw(env, error))
        })
        .resolve::<ThrowRuntimeExAndDefault>()
}

#[unsafe(no_mangle)]
pub extern "system" fn Java_tech_streamfusion_nativebridge_NativeSessionWindowTableFunctionBridge_lateRecordsDropped<
    'caller,
>(
    mut unowned_env: EnvUnowned<'caller>,
    _class: JClass<'caller>,
    handle: jlong,
) -> jlong {
    counter(&mut unowned_env, handle, |value| {
        value.late_records_dropped()
    })
}

#[unsafe(no_mangle)]
pub extern "system" fn Java_tech_streamfusion_nativebridge_NativeSessionWindowTableFunctionBridge_nullRowtimesDropped<
    'caller,
>(
    mut unowned_env: EnvUnowned<'caller>,
    _class: JClass<'caller>,
    handle: jlong,
) -> jlong {
    counter(&mut unowned_env, handle, |value| {
        value.null_rowtimes_dropped()
    })
}

#[unsafe(no_mangle)]
pub extern "system" fn Java_tech_streamfusion_nativebridge_NativeSessionWindowTableFunctionBridge_nativeStatistics<
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
pub extern "system" fn Java_tech_streamfusion_nativebridge_NativeSessionWindowTableFunctionBridge_snapshotKeyGroup<
    'caller,
>(
    mut unowned_env: EnvUnowned<'caller>,
    _class: JClass<'caller>,
    handle: jlong,
    key_group: jint,
) -> jbyteArray {
    unowned_env
        .with_env(|env| -> jni::errors::Result<_> {
            let bytes = unsafe { processor(handle) }
                .and_then(|value| value.snapshot_key_group(non_negative(key_group, "key group")?))
                .map_err(|error| throw(env, error))?;
            Ok(env.byte_array_from_slice(&bytes)?.into_raw())
        })
        .resolve::<ThrowRuntimeExAndDefault>()
}

#[unsafe(no_mangle)]
pub extern "system" fn Java_tech_streamfusion_nativebridge_NativeSessionWindowTableFunctionBridge_restoreKeyGroup<
    'caller,
>(
    mut unowned_env: EnvUnowned<'caller>,
    _class: JClass<'caller>,
    handle: jlong,
    key_group: jint,
    bytes: JByteArray<'caller>,
) {
    unowned_env
        .with_env(|env| -> jni::errors::Result<_> {
            let bytes = env.convert_byte_array(bytes)?;
            unsafe { processor(handle) }
                .and_then(|value| {
                    value.restore_key_group(non_negative(key_group, "key group")?, &bytes)
                })
                .map_err(|error| throw(env, error))
        })
        .resolve::<ThrowRuntimeExAndDefault>()
}

#[unsafe(no_mangle)]
pub extern "system" fn Java_tech_streamfusion_nativebridge_NativeSessionWindowTableFunctionBridge_checkpointRocksHandle<
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
pub extern "system" fn Java_tech_streamfusion_nativebridge_NativeSessionWindowTableFunctionBridge_importRocksCheckpointHandle<
    'caller,
>(
    mut unowned_env: EnvUnowned<'caller>,
    _class: JClass<'caller>,
    target_handle: jlong,
    plugin_path: JString<'caller>,
    checkpoint_path: JString<'caller>,
    first_key_group: jint,
    last_key_group: jint,
    memory_limit: jlong,
) {
    unowned_env
        .with_env(|env| -> jni::errors::Result<_> {
            let limit = usize::try_from(memory_limit).map_err(|_| {
                throw(
                    env,
                    "session Window TVF restore memory limit must fit usize",
                )
            })?;
            (|| -> datafusion::error::Result<()> {
                use crate::state::{KeyedState, RocksPluginKeyedState};
                let source = RocksPluginKeyedState::open(
                    std::path::Path::new(&plugin_path.to_string()),
                    std::path::Path::new(&checkpoint_path.to_string()),
                    non_negative(first_key_group, "first key group")?,
                    non_negative(last_key_group, "last key group")?,
                    limit,
                )?;
                let target = unsafe { processor(target_handle) }?;
                for group in first_key_group..=last_key_group {
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
pub extern "system" fn Java_tech_streamfusion_nativebridge_NativeSessionWindowTableFunctionBridge_destroyHandle<
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
                    drop(Box::from_raw(
                        handle as *mut SessionWindowTableFunctionProcessor,
                    ))
                };
            }
            Ok(())
        })
        .resolve::<ThrowRuntimeExAndDefault>()
}

fn counter<'caller>(
    unowned_env: &mut EnvUnowned<'caller>,
    handle: jlong,
    get: impl FnOnce(&SessionWindowTableFunctionProcessor) -> u64,
) -> jlong {
    unowned_env
        .with_env(|env| -> jni::errors::Result<_> {
            unsafe { processor(handle) }
                .map(|value| get(value).min(i64::MAX as u64) as jlong)
                .map_err(|error| throw(env, error))
        })
        .resolve::<ThrowRuntimeExAndDefault>()
}

fn non_negative(value: jint, description: &str) -> datafusion::error::Result<u32> {
    super::common::non_negative(value, "session window table function", description)
}

unsafe fn processor<'a>(
    handle: jlong,
) -> datafusion::error::Result<&'a mut SessionWindowTableFunctionProcessor> {
    unsafe { super::common::processor_mut(handle, "session window table function") }
}

fn throw(env: &mut jni::Env<'_>, error: impl std::fmt::Display) -> jni::errors::Error {
    super::common::throw(env, error)
}
