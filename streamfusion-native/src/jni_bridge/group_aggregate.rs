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
use prost::Message;

use super::common::{export_record_batch, import_record_batch};
use crate::memory_pool::{HostMemoryReservation, JvmMemoryReservationBroker};
use crate::planner::operators::global_group_aggregate::GlobalGroupAggregateProcessor;
use crate::planner::operators::group_aggregate::GroupAggregateProcessor;
use crate::planner::operators::incremental_group_aggregate::IncrementalGroupAggregateProcessor;
use crate::proto;

enum AggregateProcessor {
    Group(GroupAggregateProcessor),
    Global(GlobalGroupAggregateProcessor),
    Incremental(IncrementalGroupAggregateProcessor),
}

impl AggregateProcessor {
    fn new(
        plan: &[u8],
        max_parallelism: u32,
        first_key_group: u32,
        last_key_group: u32,
        reservation: HostMemoryReservation,
    ) -> datafusion::error::Result<Self> {
        match aggregate_kind(plan)? {
            AggregateKind::Global => Ok(Self::Global(GlobalGroupAggregateProcessor::new(
                plan,
                max_parallelism,
                first_key_group,
                last_key_group,
                reservation,
            )?)),
            AggregateKind::Incremental => {
                Ok(Self::Incremental(IncrementalGroupAggregateProcessor::new(
                    plan,
                    max_parallelism,
                    first_key_group,
                    last_key_group,
                    reservation,
                )?))
            }
            AggregateKind::Group => Ok(Self::Group(GroupAggregateProcessor::new(
                plan,
                max_parallelism,
                first_key_group,
                last_key_group,
                reservation,
            )?)),
        }
    }

    #[allow(clippy::too_many_arguments)]
    fn new_rocksdb(
        plan: &[u8],
        max_parallelism: u32,
        first_key_group: u32,
        last_key_group: u32,
        plugin_path: &std::path::Path,
        database_path: &std::path::Path,
        memory_limit: usize,
        reservation: HostMemoryReservation,
    ) -> datafusion::error::Result<Self> {
        match aggregate_kind(plan)? {
            AggregateKind::Global => Ok(Self::Global(GlobalGroupAggregateProcessor::new_rocksdb(
                plan,
                max_parallelism,
                first_key_group,
                last_key_group,
                plugin_path,
                database_path,
                memory_limit,
                reservation,
            )?)),
            AggregateKind::Incremental => Ok(Self::Incremental(
                IncrementalGroupAggregateProcessor::new_rocksdb(
                    plan,
                    max_parallelism,
                    first_key_group,
                    last_key_group,
                    plugin_path,
                    database_path,
                    memory_limit,
                    reservation,
                )?,
            )),
            AggregateKind::Group => Ok(Self::Group(GroupAggregateProcessor::new_rocksdb(
                plan,
                max_parallelism,
                first_key_group,
                last_key_group,
                plugin_path,
                database_path,
                memory_limit,
                reservation,
            )?)),
        }
    }

    fn process_arrow(
        &mut self,
        batch: arrow::record_batch::RecordBatch,
    ) -> datafusion::error::Result<arrow::record_batch::RecordBatch> {
        match self {
            Self::Group(processor) => processor.process_arrow(batch),
            Self::Global(processor) => processor.process_arrow(batch),
            Self::Incremental(processor) => processor.process_arrow(batch),
        }
    }

    fn finish_bundle(&mut self) -> datafusion::error::Result<arrow::record_batch::RecordBatch> {
        match self {
            Self::Group(processor) => processor.finish_bundle(),
            Self::Global(processor) => processor.finish_bundle(),
            Self::Incremental(processor) => processor.finish_bundle(),
        }
    }

    fn pending_element_count(&self) -> usize {
        match self {
            Self::Group(processor) => processor.pending_element_count(),
            Self::Global(processor) => processor.pending_element_count(),
            Self::Incremental(processor) => processor.pending_element_count(),
        }
    }

    fn pending_key_count(&self) -> usize {
        match self {
            Self::Group(processor) => processor.pending_key_count(),
            Self::Global(processor) => processor.pending_key_count(),
            Self::Incremental(processor) => processor.pending_key_count(),
        }
    }

    fn statistics(&self) -> [u64; 2] {
        match self {
            Self::Group(processor) => processor.statistics(),
            Self::Global(processor) => processor.statistics(),
            Self::Incremental(processor) => processor.statistics(),
        }
    }

    fn snapshot_key_group(&self, key_group: u32) -> datafusion::error::Result<Vec<u8>> {
        match self {
            Self::Group(processor) => processor.snapshot_key_group(key_group),
            Self::Global(processor) => processor.snapshot_key_group(key_group),
            Self::Incremental(processor) => processor.snapshot_key_group(key_group),
        }
    }

    fn restore_key_group(&mut self, key_group: u32, bytes: &[u8]) -> datafusion::error::Result<()> {
        match self {
            Self::Group(processor) => processor.restore_key_group(key_group, bytes),
            Self::Global(processor) => processor.restore_key_group(key_group, bytes),
            Self::Incremental(processor) => processor.restore_key_group(key_group, bytes),
        }
    }

    fn checkpoint(&self, directory: &std::path::Path) -> datafusion::error::Result<()> {
        match self {
            Self::Group(processor) => processor.checkpoint(directory),
            Self::Global(processor) => processor.checkpoint(directory),
            Self::Incremental(processor) => processor.checkpoint(directory),
        }
    }
}

enum AggregateKind {
    Group,
    Global,
    Incremental,
}

fn aggregate_kind(plan: &[u8]) -> datafusion::error::Result<AggregateKind> {
    let native = proto::NativePlan::decode(plan).map_err(|error| {
        datafusion::error::DataFusionError::Plan(format!("invalid native plan: {error}"))
    })?;
    match native.root.and_then(|operator| operator.operator) {
        Some(proto::operator::Operator::GroupAggregate(_)) => Ok(AggregateKind::Group),
        Some(proto::operator::Operator::GlobalGroupAggregate(_)) => Ok(AggregateKind::Global),
        Some(proto::operator::Operator::IncrementalGroupAggregate(_)) => {
            Ok(AggregateKind::Incremental)
        }
        _ => Err(datafusion::error::DataFusionError::Plan(
            "native aggregate handle requires a group, global, or incremental aggregate root"
                .to_string(),
        )),
    }
}

#[unsafe(no_mangle)]
pub extern "system" fn Java_tech_streamfusion_nativebridge_NativeGroupAggregateBridge_createHandle<
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
            let plan = env.convert_byte_array(serialized_plan)?;
            if memory_limit <= 0 {
                return Err(throw(
                    env,
                    "group aggregate native memory limit must be positive",
                ));
            }
            let broker = Arc::new(JvmMemoryReservationBroker::new(
                env.get_java_vm()?,
                env.new_global_ref(memory_manager)?,
            ));
            let processor = (|| -> datafusion::error::Result<_> {
                AggregateProcessor::new(
                    &plan,
                    non_negative(max_parallelism, "max parallelism")?,
                    non_negative(first_key_group, "first key group")?,
                    non_negative(last_key_group, "last key group")?,
                    HostMemoryReservation::new(broker, "native group aggregate keyed state"),
                )
            })()
            .map_err(|error| throw(env, error))?;
            Ok(Box::into_raw(Box::new(processor)) as jlong)
        })
        .resolve::<ThrowRuntimeExAndDefault>()
}

#[unsafe(no_mangle)]
pub extern "system" fn Java_tech_streamfusion_nativebridge_NativeGroupAggregateBridge_createRocksHandle<
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
            let processor = (|| -> datafusion::error::Result<_> {
                AggregateProcessor::new_rocksdb(
                    &plan,
                    non_negative(max_parallelism, "max parallelism")?,
                    non_negative(first_key_group, "first key group")?,
                    non_negative(last_key_group, "last key group")?,
                    std::path::Path::new(&plugin_path),
                    std::path::Path::new(&database_path),
                    memory_limit,
                    HostMemoryReservation::new(
                        broker,
                        "native RocksDB group aggregate batch scratch and output",
                    ),
                )
            })()
            .map_err(|error| throw(env, error))?;
            Ok(Box::into_raw(Box::new(processor)) as jlong)
        })
        .resolve::<ThrowRuntimeExAndDefault>()
}

#[unsafe(no_mangle)]
pub extern "system" fn Java_tech_streamfusion_nativebridge_NativeGroupAggregateBridge_processArrowBatch<
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
pub extern "system" fn Java_tech_streamfusion_nativebridge_NativeGroupAggregateBridge_finishBundleNative<
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
pub extern "system" fn Java_tech_streamfusion_nativebridge_NativeGroupAggregateBridge_pendingElementCountNative<
    'caller,
>(
    mut unowned_env: EnvUnowned<'caller>,
    _class: JClass<'caller>,
    handle: jlong,
) -> jlong {
    unowned_env
        .with_env(|env| -> jni::errors::Result<_> {
            let count = unsafe { processor(handle) }
                .map(|processor| processor.pending_element_count())
                .map_err(|error| throw(env, error))?;
            Ok(count as jlong)
        })
        .resolve::<ThrowRuntimeExAndDefault>()
}

#[unsafe(no_mangle)]
pub extern "system" fn Java_tech_streamfusion_nativebridge_NativeGroupAggregateBridge_pendingKeyCountNative<
    'caller,
>(
    mut unowned_env: EnvUnowned<'caller>,
    _class: JClass<'caller>,
    handle: jlong,
) -> jlong {
    unowned_env
        .with_env(|env| -> jni::errors::Result<_> {
            let count = unsafe { processor(handle) }
                .map(|processor| processor.pending_key_count())
                .map_err(|error| throw(env, error))?;
            Ok(count as jlong)
        })
        .resolve::<ThrowRuntimeExAndDefault>()
}

#[unsafe(no_mangle)]
pub extern "system" fn Java_tech_streamfusion_nativebridge_NativeGroupAggregateBridge_nativeStatistics<
    'caller,
>(
    mut unowned_env: EnvUnowned<'caller>,
    _class: JClass<'caller>,
    handle: jlong,
) -> jlongArray {
    unowned_env
        .with_env(|env| -> jni::errors::Result<_> {
            let values = unsafe { processor(handle) }
                .map(|processor| processor.statistics())
                .map_err(|error| throw(env, error))?
                .map(|value| value.min(i64::MAX as u64) as jlong);
            let output: JLongArray<'_> = env.new_long_array(values.len())?;
            output.set_region(env, 0, &values)?;
            Ok(output.into_raw())
        })
        .resolve::<ThrowRuntimeExAndDefault>()
}

#[unsafe(no_mangle)]
pub extern "system" fn Java_tech_streamfusion_nativebridge_NativeGroupAggregateBridge_snapshotKeyGroup<
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
pub extern "system" fn Java_tech_streamfusion_nativebridge_NativeGroupAggregateBridge_restoreKeyGroup<
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
pub extern "system" fn Java_tech_streamfusion_nativebridge_NativeGroupAggregateBridge_checkpointRocksHandle<
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
pub extern "system" fn Java_tech_streamfusion_nativebridge_NativeGroupAggregateBridge_importRocksCheckpointHandle<
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
                let target = unsafe { processor(target_handle) }?;
                for key_group in first_key_group..=last_key_group {
                    let key_group = non_negative(key_group, "key group")?;
                    target.restore_key_group(key_group, &source.snapshot_key_group(key_group)?)?;
                }
                Ok(())
            })()
            .map_err(|error| throw(env, error))?;
            Ok(())
        })
        .resolve::<ThrowRuntimeExAndDefault>()
}

#[unsafe(no_mangle)]
pub extern "system" fn Java_tech_streamfusion_nativebridge_NativeGroupAggregateBridge_destroyHandle<
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
                    drop(Box::from_raw(handle as *mut AggregateProcessor));
                }
            }
            Ok(())
        })
        .resolve::<ThrowRuntimeExAndDefault>()
}

fn non_negative(value: jint, description: &str) -> datafusion::error::Result<u32> {
    u32::try_from(value).map_err(|_| {
        datafusion::error::DataFusionError::Execution(format!(
            "group aggregate {description} must be non-negative"
        ))
    })
}

unsafe fn processor<'a>(handle: jlong) -> datafusion::error::Result<&'a mut AggregateProcessor> {
    if handle == 0 {
        return Err(datafusion::error::DataFusionError::Execution(
            "group aggregate native handle is closed".to_string(),
        ));
    }
    unsafe { (handle as *mut AggregateProcessor).as_mut() }.ok_or_else(|| {
        datafusion::error::DataFusionError::Execution(
            "group aggregate native handle is invalid".to_string(),
        )
    })
}

fn throw(env: &mut jni::Env<'_>, error: impl std::fmt::Display) -> jni::errors::Error {
    let _ = env.throw_new(
        jni_str!("java/lang/IllegalStateException"),
        JNIString::new(error.to_string()),
    );
    jni::errors::Error::JavaException
}
