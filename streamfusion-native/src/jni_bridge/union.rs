// Copyright 2026 StreamFusion Authors
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0

use arrow::ffi::{FFI_ArrowArray, FFI_ArrowSchema};
use jni::errors::ThrowRuntimeExAndDefault;
use jni::jni_str;
use jni::objects::{JClass, JLongArray};
use jni::strings::JNIString;
use jni::sys::jlong;
use jni::EnvUnowned;

use super::common::{execute_and_export, import_input};
use crate::execution_context::{self, NativeExecutionContext};

#[unsafe(no_mangle)]
pub extern "system" fn Java_tech_streamfusion_nativebridge_NativeUnionBridge_executeArrowBatches<
    'caller,
>(
    mut unowned_env: EnvUnowned<'caller>,
    _class: JClass<'caller>,
    execution_context: jlong,
    input_array_addresses: JLongArray<'caller>,
    input_schema_addresses: JLongArray<'caller>,
    output_array_address: jlong,
    output_schema_address: jlong,
) -> jlong {
    unowned_env
        .with_env(|env| -> jni::errors::Result<_> {
            let context = execution_context::get(execution_context).map_err(|error| {
                let _ = env.throw_new(
                    jni_str!("java/lang/IllegalStateException"),
                    JNIString::new(error.to_string()),
                );
                jni::errors::Error::JavaException
            })?;
            let input_count = input_array_addresses.len(env)?;
            if input_schema_addresses.len(env)? != input_count {
                let _ = env.throw_new(
                    jni_str!("java/lang/IllegalArgumentException"),
                    JNIString::new("Arrow input array and schema address counts differ"),
                );
                return Err(jni::errors::Error::JavaException);
            }
            let address_reservation = context.reservation("native UNION JNI addresses");
            let address_bytes = input_count
                .checked_mul(std::mem::size_of::<jlong>() * 2)
                .ok_or_else(|| {
                    let _ = env.throw_new(
                        jni_str!("java/lang/IllegalStateException"),
                        JNIString::new("native UNION address accounting overflowed usize"),
                    );
                    jni::errors::Error::JavaException
                })?;
            address_reservation
                .try_grow(address_bytes)
                .map_err(|error| {
                    let _ = env.throw_new(
                        jni_str!("java/lang/IllegalStateException"),
                        JNIString::new(error.to_string()),
                    );
                    jni::errors::Error::JavaException
                })?;
            let mut arrays = vec![0; input_count];
            let mut schemas = vec![0; input_count];
            input_array_addresses.get_region(env, 0, &mut arrays)?;
            input_schema_addresses.get_region(env, 0, &mut schemas)?;
            let rows = unsafe {
                execute_arrow_inputs(
                    &context,
                    &arrays,
                    &schemas,
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

unsafe fn execute_arrow_inputs(
    context: &NativeExecutionContext,
    input_array_addresses: &[jlong],
    input_schema_addresses: &[jlong],
    output_array_address: *mut FFI_ArrowArray,
    output_schema_address: *mut FFI_ArrowSchema,
) -> datafusion::error::Result<usize> {
    if input_array_addresses.len() < 2 {
        return Err(datafusion::error::DataFusionError::Execution(
            "native UNION ALL requires at least two Arrow inputs".to_string(),
        ));
    }
    let mut row_offset = 0;
    let mut inputs = Vec::with_capacity(input_array_addresses.len());
    let mut input_reservations = Vec::with_capacity(input_array_addresses.len());
    for (input_index, (&array_address, &schema_address)) in input_array_addresses
        .iter()
        .zip(input_schema_addresses.iter())
        .enumerate()
    {
        let (batch, reservation) = unsafe {
            import_input(
                context,
                array_address as *mut FFI_ArrowArray,
                schema_address as *mut FFI_ArrowSchema,
                input_index,
                row_offset,
            )
        }?;
        row_offset += batch.num_rows();
        input_reservations.push(reservation);
        inputs.push(batch);
    }
    context.execute_plan(inputs, |plan| unsafe {
        execute_and_export(context, plan, output_array_address, output_schema_address)
    })
}
