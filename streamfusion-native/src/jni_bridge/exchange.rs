// Copyright 2026 StreamFusion Authors
// Licensed under the Apache License, Version 2.0.

use arrow::array::{Array, StructArray};
use std::mem::size_of;
use std::sync::Arc;

use arrow::ffi::{FFI_ArrowArray, FFI_ArrowSchema};
use datafusion::error::{DataFusionError, Result};
use jni::errors::ThrowRuntimeExAndDefault;
use jni::jni_str;
use jni::objects::{JByteArray, JClass, JObject};
use jni::strings::JNIString;
use jni::sys::{jbyteArray, jint, jlong};
use jni::EnvUnowned;

use super::common::import_record_batch;
use crate::exchange::{decode_exchange_plan, exchange_key_fields, frame_hash_exchange_batch};
use crate::memory_pool::{
    HostMemoryReservation, JvmMemoryReservationBroker, MemoryReservationBroker,
};

struct AccountedBytes {
    bytes: Vec<u8>,
    _reservation: HostMemoryReservation,
}

#[unsafe(no_mangle)]
pub extern "system" fn Java_tech_streamfusion_nativebridge_NativeExchangeBridge_routeArrowBatch<
    'caller,
>(
    mut unowned_env: EnvUnowned<'caller>,
    _class: JClass<'caller>,
    serialized_plan: JByteArray<'caller>,
    input_array_address: jlong,
    input_schema_address: jlong,
    memory_manager: JObject<'caller>,
) -> jbyteArray {
    unowned_env
        .with_env(|env| -> jni::errors::Result<_> {
            let plan_bytes = env.convert_byte_array(serialized_plan)?;
            let broker: Arc<dyn MemoryReservationBroker> =
                Arc::new(JvmMemoryReservationBroker::new(
                    env.get_java_vm()?,
                    env.new_global_ref(memory_manager)?,
                ));
            let routed = unsafe {
                route(
                    &plan_bytes,
                    input_array_address as *mut FFI_ArrowArray,
                    input_schema_address as *mut FFI_ArrowSchema,
                    broker,
                )
            };
            let encoded = routed.map_err(|error| {
                let _ = env.throw_new(
                    jni_str!("java/lang/IllegalStateException"),
                    JNIString::new(error.to_string()),
                );
                jni::errors::Error::JavaException
            })?;
            env.byte_array_from_slice(&encoded.bytes)
                .map(|array| array.into_raw())
        })
        .resolve::<ThrowRuntimeExAndDefault>()
}

#[unsafe(no_mangle)]
pub extern "system" fn Java_tech_streamfusion_nativebridge_NativeExchangeBridge_decodeArrowBatch<
    'caller,
>(
    mut unowned_env: EnvUnowned<'caller>,
    _class: JClass<'caller>,
    serialized_plan: JByteArray<'caller>,
    payload: JByteArray<'caller>,
    metadata_length: jint,
    output_array_address: jlong,
    output_schema_address: jlong,
    memory_manager: JObject<'caller>,
) -> jlong {
    unowned_env
        .with_env(|env| -> jni::errors::Result<_> {
            let broker: Arc<dyn MemoryReservationBroker> =
                Arc::new(JvmMemoryReservationBroker::new(
                    env.get_java_vm()?,
                    env.new_global_ref(memory_manager)?,
                ));
            let mut reservation = HostMemoryReservation::new(broker, "native exchange decode");
            let plan_size = serialized_plan.len(env)?;
            let payload_size = payload.len(env)?;
            let input_size = plan_size.checked_add(payload_size).ok_or_else(|| {
                let _ = env.throw_new(
                    jni_str!("java/lang/IllegalStateException"),
                    JNIString::new("native exchange decode accounting overflowed usize"),
                );
                jni::errors::Error::JavaException
            })?;
            reservation.try_grow(input_size).map_err(|error| {
                let _ = env.throw_new(
                    jni_str!("java/lang/IllegalStateException"),
                    JNIString::new(error.to_string()),
                );
                jni::errors::Error::JavaException
            })?;
            let plan_bytes = env.convert_byte_array(serialized_plan)?;
            let payload = env.convert_byte_array(payload)?;
            let rows = unsafe {
                decode(
                    &plan_bytes,
                    payload,
                    usize::try_from(metadata_length).map_err(|_| {
                        let _ = env.throw_new(
                            jni_str!("java/lang/IllegalArgumentException"),
                            JNIString::new("exchange IPC metadata length was negative"),
                        );
                        jni::errors::Error::JavaException
                    })?,
                    output_array_address as *mut FFI_ArrowArray,
                    output_schema_address as *mut FFI_ArrowSchema,
                    &mut reservation,
                )
            }
            .map_err(|error| {
                let _ = env.throw_new(
                    jni_str!("java/lang/IllegalStateException"),
                    JNIString::new(error.to_string()),
                );
                jni::errors::Error::JavaException
            })?;
            Ok(rows as jlong)
        })
        .resolve::<ThrowRuntimeExAndDefault>()
}

unsafe fn route(
    plan_bytes: &[u8],
    input_array_address: *mut FFI_ArrowArray,
    input_schema_address: *mut FFI_ArrowSchema,
    broker: Arc<dyn MemoryReservationBroker>,
) -> Result<AccountedBytes> {
    let plan = decode_exchange_plan(plan_bytes)?;
    let keys = exchange_key_fields(&plan)?;
    let batch = unsafe { import_record_batch(input_array_address, input_schema_address) }?;

    // Routing materializes at most one copy of the input columns across the key-group
    // batches. Reserve that working set before asking Arrow to allocate it.
    let mut reservation = HostMemoryReservation::new(broker, "native exchange buffers");
    reservation.try_grow(batch.get_array_memory_size())?;
    let frames = frame_hash_exchange_batch(batch, &keys, plan.max_parallelism)?;
    let frame_bytes = frames.iter().try_fold(
        frames
            .capacity()
            .saturating_mul(size_of::<crate::exchange::RoutedFrame>()),
        |bytes, routed| {
            bytes
                .checked_add(routed.frame().metadata.capacity())
                .and_then(|bytes| bytes.checked_add(routed.frame().body.capacity()))
                .ok_or_else(|| {
                    DataFusionError::ResourcesExhausted(
                        "native exchange frame accounting overflowed usize".to_string(),
                    )
                })
        },
    )?;
    reservation.resize(frame_bytes)?;

    let encoded_size = encode_frames_size(&frames)?;
    reservation.try_grow(encoded_size)?;
    let bytes = encode_frames_with_capacity(&frames, encoded_size)?;
    drop(frames);
    reservation.resize(bytes.capacity())?;
    Ok(AccountedBytes {
        bytes,
        _reservation: reservation,
    })
}

unsafe fn decode(
    plan_bytes: &[u8],
    payload: Vec<u8>,
    metadata_length: usize,
    output_array_address: *mut FFI_ArrowArray,
    output_schema_address: *mut FFI_ArrowSchema,
    reservation: &mut HostMemoryReservation,
) -> Result<usize> {
    if output_array_address.is_null() || output_schema_address.is_null() {
        return Err(DataFusionError::Execution(
            "Arrow C Data exchange output address was null".to_string(),
        ));
    }
    let plan = decode_exchange_plan(plan_bytes)?;
    let schema = crate::planner::arrow_schema(
        plan.schema
            .as_ref()
            .ok_or_else(|| DataFusionError::Plan("exchange schema is required".to_string()))?,
    )?;
    let batch =
        crate::exchange::IpcBatchFrame::decode_contiguous(payload, metadata_length, schema)?;
    reservation.resize(
        plan_bytes
            .len()
            .checked_add(batch.get_array_memory_size())
            .ok_or_else(|| {
                DataFusionError::ResourcesExhausted(
                    "native exchange decoded batch accounting overflowed usize".to_string(),
                )
            })?,
    )?;
    let rows = batch.num_rows();
    let output_data = StructArray::from(batch).to_data();
    let output_array = FFI_ArrowArray::new(&output_data);
    let output_schema = FFI_ArrowSchema::try_from(output_data.data_type())?;
    unsafe {
        std::ptr::write(output_array_address, output_array);
        std::ptr::write(output_schema_address, output_schema);
    }
    Ok(rows)
}

#[cfg(test)]
fn encode_frames(frames: &[crate::exchange::RoutedFrame]) -> Result<Vec<u8>> {
    let capacity = encode_frames_size(frames)?;
    encode_frames_with_capacity(frames, capacity)
}

fn encode_frames_size(frames: &[crate::exchange::RoutedFrame]) -> Result<usize> {
    frames.iter().try_fold(4usize, |bytes, routed| {
        bytes
            .checked_add(12)
            .and_then(|bytes| bytes.checked_add(routed.frame().metadata.len()))
            .and_then(|bytes| bytes.checked_add(routed.frame().body.len()))
            .ok_or_else(|| {
                DataFusionError::ResourcesExhausted(
                    "native exchange JNI output accounting overflowed usize".to_string(),
                )
            })
    })
}

fn encode_frames_with_capacity(
    frames: &[crate::exchange::RoutedFrame],
    capacity: usize,
) -> Result<Vec<u8>> {
    let mut bytes = Vec::with_capacity(capacity);
    write_u32(&mut bytes, frames.len(), "frame count")?;
    for routed in frames {
        bytes.extend_from_slice(&routed.key_group().to_le_bytes());
        write_u32(
            &mut bytes,
            routed.frame().metadata.len(),
            "IPC metadata length",
        )?;
        write_u32(&mut bytes, routed.frame().body.len(), "IPC body length")?;
        bytes.extend_from_slice(&routed.frame().metadata);
        bytes.extend_from_slice(&routed.frame().body);
    }
    Ok(bytes)
}

fn write_u32(bytes: &mut Vec<u8>, value: usize, label: &str) -> Result<()> {
    let value = u32::try_from(value).map_err(|_| {
        DataFusionError::Execution(format!("exchange {label} exceeds the JNI frame limit"))
    })?;
    bytes.extend_from_slice(&value.to_le_bytes());
    Ok(())
}

#[cfg(test)]
mod tests {
    use std::sync::Arc;

    use arrow::array::{ArrayRef, Int32Array};
    use arrow::record_batch::RecordBatch;

    use super::*;
    use crate::exchange::{frame_hash_exchange_batch, KeyField};

    #[test]
    fn encodes_destination_and_arrow_frame_lengths_for_java() {
        let batch = RecordBatch::try_from_iter(vec![(
            "key",
            Arc::new(Int32Array::from(vec![1, 42])) as ArrayRef,
        )])
        .unwrap();
        let frames = frame_hash_exchange_batch(batch, &[(0, KeyField::Integer)], 128).unwrap();

        let encoded = encode_frames(&frames).unwrap();

        assert_eq!(u32::from_le_bytes(encoded[0..4].try_into().unwrap()), 2);
        assert_eq!(
            u32::from_le_bytes(encoded[4..8].try_into().unwrap()),
            frames[0].key_group()
        );
        let metadata_len = u32::from_le_bytes(encoded[8..12].try_into().unwrap()) as usize;
        let body_len = u32::from_le_bytes(encoded[12..16].try_into().unwrap()) as usize;
        let second = 16 + metadata_len + body_len;
        let second_metadata_len =
            u32::from_le_bytes(encoded[second + 4..second + 8].try_into().unwrap()) as usize;
        let second_body_len =
            u32::from_le_bytes(encoded[second + 8..second + 12].try_into().unwrap()) as usize;
        assert_eq!(
            encoded.len(),
            second + 12 + second_metadata_len + second_body_len
        );
    }
}
