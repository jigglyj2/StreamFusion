// Copyright 2026 StreamFusion Authors
// Licensed under the Apache License, Version 2.0.

//! Decode network IPC once at the native-plan edge, retaining its buffers inside Rust.
use super::common::throw;
use arrow::ffi_stream::FFI_ArrowArrayStream;
use datafusion::error::{DataFusionError, Result};
use jni::errors::ThrowRuntimeExAndDefault;
use jni::objects::{JByteArray, JClass, JLongArray};
use jni::sys::{jint, jlong};
use jni::EnvUnowned;

#[unsafe(no_mangle)]
pub extern "system" fn Java_tech_streamfusion_nativebridge_NativeExecutionContext_executeExchangeStreamInputs<
    'a,
>(
    mut env: EnvUnowned<'a>,
    _class: JClass<'a>,
    handle: jlong,
    port: jint,
    plan: JByteArray<'a>,
    payload: JByteArray<'a>,
    offset: jint,
    length: jint,
    metadata: jint,
    input_arrays: JLongArray<'a>,
    input_schemas: JLongArray<'a>,
    output: jlong,
) -> jlong {
    env.with_env(|env| -> jni::errors::Result<_> {
        let context = crate::execution_context::get(handle).map_err(|e| throw(env, e))?;
        if context.plan().protocol_version < crate::RECORD_POLICY_PLAN_PROTOCOL_VERSION {
            return Err(throw(
                env,
                "direct IPC plan input requires owned record-envelope protocol 3",
            ));
        }
        let count = input_arrays.len(env)?;
        let source_len = payload.len(env)?;
        if port < 0
            || port as usize >= count
            || count != input_schemas.len(env)?
            || output == 0
            || offset < 0
            || length < 0
            || metadata < 0
            || metadata > length
            || offset as usize > source_len
            || length as usize > source_len - offset as usize
        {
            return Err(throw(env, "invalid native IPC plan input range or address"));
        }
        let memory = context.reservation("native exchange input payload");
        memory
            .try_grow(length as usize)
            .map_err(|e| throw(env, e))?;
        let plan = env.convert_byte_array(plan)?;
        let mut bytes = vec![0u8; length as usize];
        // JNI copies the network range once; the IPC decoder borrows its backing allocation.
        payload.get_region(env, offset, unsafe {
            std::slice::from_raw_parts_mut(bytes.as_mut_ptr().cast(), bytes.len())
        })?;
        let decoded: Result<_> = (|| {
            let mut batch = super::exchange::decode_batch(&plan, bytes, metadata as usize)?;
            // Routing has already selected the Flink subtask. This edge uses SQL keys natively.
            if batch
                .schema()
                .fields()
                .last()
                .is_some_and(|f| f.name() == "__streamfusion_key")
            {
                batch = batch.project(&(0..batch.num_columns() - 1).collect::<Vec<_>>())?;
            }
            let rows = batch.num_rows();
            let batch = crate::memory_pool::arrow_lease::datafusion_batch_registered(
                batch,
                memory,
                crate::memory_pool::buffer_registry(context.task_context().memory_pool()),
            )?;
            let ordinal = context.reservation("native exchange input ordinal");
            ordinal.try_grow(rows.checked_mul(4).ok_or_else(|| {
                DataFusionError::ResourcesExhausted("exchange ordinal overflow".into())
            })?)?;
            Ok((
                super::common::prepare_input(&context, batch, 0)?,
                ordinal,
                rows,
            ))
        })();
        let (batch, ordinal, rows) = decoded.map_err(|e| throw(env, e))?;
        let mut arrays = vec![0; count];
        let mut schemas = vec![0; count];
        input_arrays.get_region(env, 0, &mut arrays)?;
        input_schemas.get_region(env, 0, &mut schemas)?;
        unsafe {
            super::plan_stream::execute_inputs(
                &context,
                &arrays,
                &schemas,
                ordinal,
                None,
                Some((port as usize, batch)),
                output as *mut FFI_ArrowArrayStream,
            )
        }
        .map_err(|e| throw(env, e))?;
        Ok(rows as jlong)
    })
    .resolve::<ThrowRuntimeExAndDefault>()
}
