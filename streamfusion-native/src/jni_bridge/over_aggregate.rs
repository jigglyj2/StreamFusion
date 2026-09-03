// Copyright 2026 StreamFusion Authors
// Licensed under the Apache License, Version 2.0

use std::sync::Arc;

use arrow::ffi::{FFI_ArrowArray, FFI_ArrowSchema};
use jni::errors::ThrowRuntimeExAndDefault;
use jni::jni_str;
use jni::objects::{JByteArray, JClass, JLongArray, JObject, JString};
use jni::strings::JNIString;
use jni::sys::{jbyteArray, jint, jlong, jlongArray};
use jni::EnvUnowned;

use super::common::{export_record_batch, import_record_batch};
use crate::memory_pool::{HostMemoryReservation, JvmMemoryReservationBroker};
use crate::planner::operators::over_aggregate::OverAggregateProcessor;

#[unsafe(no_mangle)]
pub extern "system" fn Java_tech_streamfusion_nativebridge_NativeOverAggregateBridge_createHandle<
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
                    "OVER aggregate native memory limit must be positive",
                ));
            }
            let plan = env.convert_byte_array(serialized_plan)?;
            let broker = Arc::new(JvmMemoryReservationBroker::new(
                env.get_java_vm()?,
                env.new_global_ref(memory_manager)?,
            ));
            let processor = (|| -> datafusion::error::Result<_> {
                OverAggregateProcessor::new(
                    &plan,
                    non_negative(max_parallelism, "max parallelism")?,
                    non_negative(first_key_group, "first key group")?,
                    non_negative(last_key_group, "last key group")?,
                    HostMemoryReservation::new(broker, "native OVER aggregate keyed state"),
                )
            })()
            .map_err(|error| throw(env, error))?;
            Ok(Box::into_raw(Box::new(processor)) as jlong)
        })
        .resolve::<ThrowRuntimeExAndDefault>()
}

#[unsafe(no_mangle)]
pub extern "system" fn Java_tech_streamfusion_nativebridge_NativeOverAggregateBridge_createRocksHandle<
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
            let memory_limit = usize::try_from(memory_limit)
                .map_err(|_| throw(env, "RocksDB OVER memory limit must fit usize"))?;
            let broker = Arc::new(JvmMemoryReservationBroker::new(
                env.get_java_vm()?,
                env.new_global_ref(memory_manager)?,
            ));
            let processor = (|| -> datafusion::error::Result<_> {
                OverAggregateProcessor::new_rocksdb(
                    &plan,
                    non_negative(max_parallelism, "max parallelism")?,
                    non_negative(first_key_group, "first key group")?,
                    non_negative(last_key_group, "last key group")?,
                    std::path::Path::new(&plugin_path.to_string()),
                    std::path::Path::new(&database_path.to_string()),
                    memory_limit,
                    HostMemoryReservation::new(
                        broker,
                        "native RocksDB OVER aggregate batch scratch and output",
                    ),
                )
            })()
            .map_err(|error| throw(env, error))?;
            Ok(Box::into_raw(Box::new(processor)) as jlong)
        })
        .resolve::<ThrowRuntimeExAndDefault>()
}

#[unsafe(no_mangle)]
pub extern "system" fn Java_tech_streamfusion_nativebridge_NativeOverAggregateBridge_processArrowBatch<
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
            let rows = unsafe {
                let input = import_record_batch(
                    input_array_address as *mut FFI_ArrowArray,
                    input_schema_address as *mut FFI_ArrowSchema,
                )
                .map_err(|error| throw(env, error))?;
                let output = processor(handle)
                    .and_then(|processor| processor.process_arrow(input))
                    .map_err(|error| throw(env, error))?;
                export_record_batch(
                    output,
                    output_array_address as *mut FFI_ArrowArray,
                    output_schema_address as *mut FFI_ArrowSchema,
                )
                .map_err(|error| throw(env, error))?
            };
            Ok(rows as jlong)
        })
        .resolve::<ThrowRuntimeExAndDefault>()
}

#[unsafe(no_mangle)]
pub extern "system" fn Java_tech_streamfusion_nativebridge_NativeOverAggregateBridge_statistics0<
    'caller,
>(
    mut unowned_env: EnvUnowned<'caller>,
    _class: JClass<'caller>,
    handle: jlong,
) -> jlongArray {
    unowned_env
        .with_env(|env| -> jni::errors::Result<_> {
            let values = unsafe { processor(handle) }
                .map(|processor| {
                    processor
                        .statistics()
                        .map(|value| value.min(i64::MAX as u64) as jlong)
                })
                .map_err(|error| throw(env, error))?;
            let output: JLongArray<'_> = env.new_long_array(values.len())?;
            output.set_region(env, 0, &values)?;
            Ok(output.into_raw())
        })
        .resolve::<ThrowRuntimeExAndDefault>()
}

#[unsafe(no_mangle)]
pub extern "system" fn Java_tech_streamfusion_nativebridge_NativeOverAggregateBridge_advanceEventTime0<
    'caller,
>(
    mut unowned_env: EnvUnowned<'caller>,
    _class: JClass<'caller>,
    handle: jlong,
    watermark: jlong,
    output_array_address: jlong,
    output_schema_address: jlong,
) -> jlong {
    unowned_env
        .with_env(|env| -> jni::errors::Result<_> {
            let rows = (|| -> datafusion::error::Result<_> {
                let output = unsafe { processor(handle) }?.advance_event_time(watermark)?;
                unsafe {
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
pub extern "system" fn Java_tech_streamfusion_nativebridge_NativeOverAggregateBridge_lateRecordsDropped0<
    'caller,
>(
    mut unowned_env: EnvUnowned<'caller>,
    _class: JClass<'caller>,
    handle: jlong,
) -> jlong {
    unowned_env
        .with_env(|env| -> jni::errors::Result<_> {
            unsafe { processor(handle) }
                .and_then(|processor| {
                    i64::try_from(processor.late_records_dropped()).map_err(|_| {
                        DataFusionError::Execution(
                            "OVER late record count exceeds Java long".to_string(),
                        )
                    })
                })
                .map_err(|error| throw(env, error))
        })
        .resolve::<ThrowRuntimeExAndDefault>()
}

#[unsafe(no_mangle)]
pub extern "system" fn Java_tech_streamfusion_nativebridge_NativeOverAggregateBridge_snapshotKeyGroup<
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
                .and_then(|processor| {
                    processor.snapshot_key_group(non_negative(key_group, "key group")?)
                })
                .map_err(|error| throw(env, error))?;
            Ok(env.byte_array_from_slice(&bytes)?.into_raw())
        })
        .resolve::<ThrowRuntimeExAndDefault>()
}

#[unsafe(no_mangle)]
pub extern "system" fn Java_tech_streamfusion_nativebridge_NativeOverAggregateBridge_restoreKeyGroup<
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
                .and_then(|processor| {
                    processor.restore_key_group(non_negative(key_group, "key group")?, &bytes)
                })
                .map_err(|error| throw(env, error))?;
            Ok(())
        })
        .resolve::<ThrowRuntimeExAndDefault>()
}

#[unsafe(no_mangle)]
pub extern "system" fn Java_tech_streamfusion_nativebridge_NativeOverAggregateBridge_checkpointRocksHandle<
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
                .and_then(|processor| {
                    processor.checkpoint(std::path::Path::new(&directory.to_string()))
                })
                .map_err(|error| throw(env, error))?;
            Ok(())
        })
        .resolve::<ThrowRuntimeExAndDefault>()
}

#[unsafe(no_mangle)]
pub extern "system" fn Java_tech_streamfusion_nativebridge_NativeOverAggregateBridge_importRocksCheckpointHandle<
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
            let memory_limit = usize::try_from(memory_limit)
                .map_err(|_| throw(env, "RocksDB OVER restore memory limit must fit usize"))?;
            (|| -> datafusion::error::Result<()> {
                use crate::state::{KeyedState, RocksPluginKeyedState};
                let source = RocksPluginKeyedState::open(
                    std::path::Path::new(&plugin_path.to_string()),
                    std::path::Path::new(&checkpoint_path.to_string()),
                    non_negative(first_key_group, "first key group")?,
                    non_negative(last_key_group, "last key group")?,
                    memory_limit,
                )?;
                let target = unsafe { processor(target_handle) }?;
                for group in first_key_group..=last_key_group {
                    let group = non_negative(group, "key group")?;
                    target.restore_key_group(group, &source.snapshot_key_group(group)?)?;
                }
                Ok(())
            })()
            .map_err(|error| throw(env, error))?;
            Ok(())
        })
        .resolve::<ThrowRuntimeExAndDefault>()
}

#[unsafe(no_mangle)]
pub extern "system" fn Java_tech_streamfusion_nativebridge_NativeOverAggregateBridge_destroyHandle<
    'caller,
>(
    mut unowned_env: EnvUnowned<'caller>,
    _class: JClass<'caller>,
    handle: jlong,
) {
    unowned_env
        .with_env(|_env| -> jni::errors::Result<_> {
            if handle != 0 {
                unsafe { drop(Box::from_raw(handle as *mut OverAggregateProcessor)) };
            }
            Ok(())
        })
        .resolve::<ThrowRuntimeExAndDefault>()
}

fn non_negative(value: jint, description: &str) -> datafusion::error::Result<u32> {
    u32::try_from(value).map_err(|_| {
        DataFusionError::Execution(format!("OVER aggregate {description} must be non-negative"))
    })
}

unsafe fn processor<'a>(
    handle: jlong,
) -> datafusion::error::Result<&'a mut OverAggregateProcessor> {
    if handle == 0 {
        return Err(DataFusionError::Execution(
            "OVER aggregate native handle is closed".to_string(),
        ));
    }
    unsafe { (handle as *mut OverAggregateProcessor).as_mut() }.ok_or_else(|| {
        DataFusionError::Execution("OVER aggregate native handle is invalid".to_string())
    })
}

fn throw(env: &mut jni::Env<'_>, error: impl std::fmt::Display) -> jni::errors::Error {
    let _ = env.throw_new(
        jni_str!("java/lang/IllegalStateException"),
        JNIString::new(error.to_string()),
    );
    jni::errors::Error::JavaException
}

use datafusion::error::DataFusionError;
