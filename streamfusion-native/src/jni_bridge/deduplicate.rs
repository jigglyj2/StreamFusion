// Copyright 2026 StreamFusion Authors
// Licensed under the Apache License, Version 2.0

use std::sync::{Arc, MutexGuard};

use arrow::ffi::{FFI_ArrowArray, FFI_ArrowSchema};
use jni::errors::ThrowRuntimeExAndDefault;
use jni::objects::{JByteArray, JClass, JLongArray, JObject, JString};
use jni::sys::{jbyteArray, jint, jlong, jlongArray};
use jni::EnvUnowned;

use super::common::{export_record_batch, import_record_batch};
use crate::memory_pool::{HostMemoryReservation, JvmMemoryReservationBroker};
use crate::planner::operators::deduplicate::region::DeduplicateHandle;
use crate::planner::operators::deduplicate::DeduplicateProcessor;

#[unsafe(no_mangle)]
pub extern "system" fn Java_tech_streamfusion_nativebridge_NativeDeduplicateBridge_createHandle<
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
                    "deduplicate native memory limit must be positive",
                ));
            }
            let broker = Arc::new(JvmMemoryReservationBroker::new(
                env.get_java_vm()?,
                env.new_global_ref(memory_manager)?,
            ));
            let mut plan_memory =
                HostMemoryReservation::new(broker.clone(), "deduplicate JNI plan copy");
            plan_memory
                .resize(serialized_plan.len(env)?)
                .map_err(|error| throw(env, error))?;
            let plan = env.convert_byte_array(serialized_plan)?;
            let processor = (|| -> datafusion::error::Result<_> {
                DeduplicateHandle::new(
                    &plan,
                    plan_memory.sibling("deduplicate native region"),
                    |bare| {
                        DeduplicateProcessor::new(
                            bare,
                            non_negative(max_parallelism, "max parallelism")?,
                            non_negative(first_key_group, "first key group")?,
                            non_negative(last_key_group, "last key group")?,
                            HostMemoryReservation::new(broker, "native deduplicate keyed state"),
                        )
                    },
                )
            })()
            .map_err(|error| throw(env, error))?;
            Ok(Box::into_raw(Box::new(processor)) as jlong)
        })
        .resolve::<ThrowRuntimeExAndDefault>()
}

#[unsafe(no_mangle)]
pub extern "system" fn Java_tech_streamfusion_nativebridge_NativeDeduplicateBridge_createRocksHandle<
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
            let plugin_path = plugin_path.to_string();
            let database_path = database_path.to_string();
            let memory_limit = usize::try_from(memory_limit).map_err(|_| {
                throw(
                    env,
                    "RocksDB native memory limit must be positive and fit usize",
                )
            })?;
            let broker = Arc::new(JvmMemoryReservationBroker::new(
                env.get_java_vm()?,
                env.new_global_ref(memory_manager)?,
            ));
            let mut plan_memory =
                HostMemoryReservation::new(broker.clone(), "RocksDB deduplicate JNI plan copy");
            plan_memory
                .resize(serialized_plan.len(env)?)
                .map_err(|error| throw(env, error))?;
            let plan = env.convert_byte_array(serialized_plan)?;
            let processor = (|| -> datafusion::error::Result<_> {
                DeduplicateHandle::new(
                    &plan,
                    plan_memory.sibling("RocksDB deduplicate native region"),
                    |bare| {
                        DeduplicateProcessor::new_rocksdb(
                            bare,
                            non_negative(max_parallelism, "max parallelism")?,
                            non_negative(first_key_group, "first key group")?,
                            non_negative(last_key_group, "last key group")?,
                            std::path::Path::new(&plugin_path),
                            std::path::Path::new(&database_path),
                            memory_limit,
                            HostMemoryReservation::new(
                                broker,
                                "native RocksDB deduplicate scratch",
                            ),
                        )
                    },
                )
            })()
            .map_err(|error| throw(env, error))?;
            Ok(Box::into_raw(Box::new(processor)) as jlong)
        })
        .resolve::<ThrowRuntimeExAndDefault>()
}

#[unsafe(no_mangle)]
pub extern "system" fn Java_tech_streamfusion_nativebridge_NativeDeduplicateBridge_processArrowBatch<
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
                    let output = processor(handle)?.process_selection(input)?;
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
pub extern "system" fn Java_tech_streamfusion_nativebridge_NativeDeduplicateBridge_processOutputArrowBatch<
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
                    let output = native_handle(handle)?.process_arrow(input)?;
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
pub extern "system" fn Java_tech_streamfusion_nativebridge_NativeDeduplicateBridge_snapshotKeyGroup<
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
pub extern "system" fn Java_tech_streamfusion_nativebridge_NativeDeduplicateBridge_restoreKeyGroup<
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
            let mut restore_input = unsafe { processor(handle) }
                .map(|processor| processor.state_memory())
                .map_err(|error| throw(env, error))?;
            restore_input
                .resize(bytes.len(env)?)
                .map_err(|error| throw(env, error))?;
            let bytes = env.convert_byte_array(bytes)?;
            unsafe { processor(handle) }
                .and_then(|mut processor| {
                    processor.restore_key_group(non_negative(key_group, "key group")?, &bytes)
                })
                .map_err(|error| throw(env, error))?;
            Ok(())
        })
        .resolve::<ThrowRuntimeExAndDefault>()
}

#[unsafe(no_mangle)]
pub extern "system" fn Java_tech_streamfusion_nativebridge_NativeDeduplicateBridge_checkpointRocksHandle<
    'caller,
>(
    mut unowned_env: EnvUnowned<'caller>,
    _class: JClass<'caller>,
    handle: jlong,
    directory: JString<'caller>,
) {
    unowned_env
        .with_env(|env| -> jni::errors::Result<_> {
            let directory = directory.to_string();
            unsafe { processor(handle) }
                .and_then(|processor| processor.checkpoint(std::path::Path::new(&directory)))
                .map_err(|error| throw(env, error))?;
            Ok(())
        })
        .resolve::<ThrowRuntimeExAndDefault>()
}

#[unsafe(no_mangle)]
pub extern "system" fn Java_tech_streamfusion_nativebridge_NativeDeduplicateBridge_importRocksCheckpointHandle<
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
            let plugin_path = plugin_path.to_string();
            let checkpoint_path = checkpoint_path.to_string();
            let memory_limit = usize::try_from(memory_limit).map_err(|_| {
                throw(
                    env,
                    "RocksDB restore memory limit must be positive and fit usize",
                )
            })?;
            (|| -> datafusion::error::Result<()> {
                use crate::state::{KeyedState, RocksPluginKeyedState};
                let source = RocksPluginKeyedState::open(
                    std::path::Path::new(&plugin_path),
                    std::path::Path::new(&checkpoint_path),
                    non_negative(first_key_group, "first key group")?,
                    non_negative(last_key_group, "last key group")?,
                    memory_limit,
                )?;
                let mut target = unsafe { processor(target_handle) }?;
                for key_group in first_key_group..=last_key_group {
                    let key_group = non_negative(key_group, "key group")?;
                    let snapshot = source.snapshot_key_group(key_group, &target.state_memory())?;
                    target.restore_key_group(key_group, &snapshot)?;
                }
                Ok(())
            })()
            .map_err(|error| throw(env, error))?;
            Ok(())
        })
        .resolve::<ThrowRuntimeExAndDefault>()
}

#[unsafe(no_mangle)]
pub extern "system" fn Java_tech_streamfusion_nativebridge_NativeDeduplicateBridge_destroyHandle<
    'caller,
>(
    mut unowned_env: EnvUnowned<'caller>,
    _class: JClass<'caller>,
    handle: jlong,
) {
    unowned_env
        .with_env(|_env| -> jni::errors::Result<_> {
            if handle == 0 {
                return Ok(());
            }
            unsafe {
                drop(Box::from_raw(handle as *mut DeduplicateHandle));
            }
            Ok(())
        })
        .resolve::<ThrowRuntimeExAndDefault>()
}

fn non_negative(value: jint, description: &str) -> datafusion::error::Result<u32> {
    super::common::non_negative(value, "deduplicate", description)
}

unsafe fn native_handle<'a>(handle: jlong) -> datafusion::error::Result<&'a mut DeduplicateHandle> {
    unsafe { super::common::processor_mut(handle, "deduplicate") }
}

unsafe fn processor<'a>(
    handle: jlong,
) -> datafusion::error::Result<MutexGuard<'a, DeduplicateProcessor>> {
    let handle = unsafe { native_handle(handle) }?;
    handle.context.require_idle()?;
    handle.processor.lock().map_err(|_| {
        datafusion::error::DataFusionError::Execution("deduplicate state lock is poisoned".into())
    })
}

#[unsafe(no_mangle)]
pub extern "system" fn Java_tech_streamfusion_nativebridge_NativeDeduplicateBridge_nativeMetricSnapshot<
    'caller,
>(
    mut env: EnvUnowned<'caller>,
    _class: JClass<'caller>,
    handle: jlong,
) -> jlongArray {
    env.with_env(|env| -> jni::errors::Result<_> {
        let values = unsafe { native_handle(handle) }
            .map_err(|error| throw(env, error))?
            .metrics()
            .map_err(|error| throw(env, error))?;
        let output: JLongArray<'_> = env.new_long_array(values.len())?;
        output.set_region(env, 0, &values)?;
        Ok(output.into_raw())
    })
    .resolve::<ThrowRuntimeExAndDefault>()
}

fn throw(env: &mut jni::Env<'_>, error: impl std::fmt::Display) -> jni::errors::Error {
    super::common::throw(env, error)
}
