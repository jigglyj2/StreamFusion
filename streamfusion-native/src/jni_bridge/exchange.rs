// Copyright 2026 StreamFusion Authors
// Licensed under the Apache License, Version 2.0.

use arrow::array::{Array, StructArray};
use arrow::ffi::{FFI_ArrowArray, FFI_ArrowSchema};
use datafusion::error::{DataFusionError, Result};
use jni::errors::ThrowRuntimeExAndDefault;
use jni::jni_str;
use jni::objects::{JByteArray, JClass};
use jni::strings::JNIString;
use jni::sys::{jbyteArray, jlong};
use jni::EnvUnowned;

use super::common::import_record_batch;
use crate::exchange::{decode_exchange_plan, exchange_key_fields, frame_hash_exchange_batch};

#[unsafe(no_mangle)]
pub extern "system" fn Java_tech_streamfusion_nativebridge_NativeExchangeBridge_routeArrowBatch<
    'caller,
>(
    mut unowned_env: EnvUnowned<'caller>,
    _class: JClass<'caller>,
    serialized_plan: JByteArray<'caller>,
    input_array_address: jlong,
    input_schema_address: jlong,
) -> jbyteArray {
    unowned_env
        .with_env(|env| -> jni::errors::Result<_> {
            let plan_bytes = env.convert_byte_array(serialized_plan)?;
            let routed = unsafe {
                route(
                    &plan_bytes,
                    input_array_address as *mut FFI_ArrowArray,
                    input_schema_address as *mut FFI_ArrowSchema,
                )
            };
            let encoded = routed.map_err(|error| {
                let _ = env.throw_new(
                    jni_str!("java/lang/IllegalStateException"),
                    JNIString::new(error.to_string()),
                );
                jni::errors::Error::JavaException
            })?;
            Ok(env.byte_array_from_slice(&encoded)?.into_raw())
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
    metadata: JByteArray<'caller>,
    body: JByteArray<'caller>,
    output_array_address: jlong,
    output_schema_address: jlong,
) -> jlong {
    unowned_env
        .with_env(|env| -> jni::errors::Result<_> {
            let plan_bytes = env.convert_byte_array(serialized_plan)?;
            let metadata = env.convert_byte_array(metadata)?;
            let body = env.convert_byte_array(body)?;
            let rows = unsafe {
                decode(
                    &plan_bytes,
                    metadata,
                    body,
                    output_array_address as *mut FFI_ArrowArray,
                    output_schema_address as *mut FFI_ArrowSchema,
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
) -> Result<Vec<u8>> {
    let plan = decode_exchange_plan(plan_bytes)?;
    let keys = exchange_key_fields(&plan)?;
    let batch = unsafe { import_record_batch(input_array_address, input_schema_address) }?;
    let frames = frame_hash_exchange_batch(batch, &keys, plan.max_parallelism)?;
    encode_frames(&frames)
}

unsafe fn decode(
    plan_bytes: &[u8],
    metadata: Vec<u8>,
    body: Vec<u8>,
    output_array_address: *mut FFI_ArrowArray,
    output_schema_address: *mut FFI_ArrowSchema,
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
    let batch = crate::exchange::IpcBatchFrame { metadata, body }.decode(schema)?;
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

fn encode_frames(frames: &[crate::exchange::RoutedFrame]) -> Result<Vec<u8>> {
    let mut bytes = Vec::new();
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
