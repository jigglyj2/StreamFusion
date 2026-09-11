// Copyright 2026 StreamFusion Authors
// Licensed under the Apache License, Version 2.0.

//! Flink owns checkpoint streams. Transfer the existing canonical frame in bounded chunks;
//! never retain another whole-key-group Java array alongside the native snapshot/restore bytes.
use crate::execution_context;
use jni::errors::ThrowRuntimeExAndDefault;
use jni::objects::{JClass, JObject, JValue};
use jni::sys::{jint, jlong};
use jni::{jni_sig, jni_str, EnvUnowned};

const CHUNK_BYTES: usize = 64 << 10;

#[unsafe(no_mangle)]
pub extern "system" fn Java_tech_streamfusion_nativebridge_NativePlanState_streamEdgeVersion(
    _env: EnvUnowned<'_>,
    _class: JClass<'_>,
) -> jint {
    1
}

#[unsafe(no_mangle)]
pub extern "system" fn Java_tech_streamfusion_nativebridge_NativePlanState_writeSnapshot<'a>(
    mut env: EnvUnowned<'a>,
    _class: JClass<'a>,
    handle: jlong,
    id: jlong,
    group: jint,
    output: JObject<'a>,
) -> jlong {
    env.with_env(|env| -> jni::errors::Result<_> {
        let context = execution_context::get(handle).map_err(|error| io_error(env, error))?;
        let bytes = context
            .snapshot_state(id as u64, group as u32)
            .map_err(|error| io_error(env, error))?;
        let length = jint::try_from(bytes.len()).map_err(|error| io_error(env, error))?;
        let reservation = context.reservation("canonical snapshot JVM transport");
        let size = bytes.len().min(CHUNK_BYTES);
        reservation
            .try_grow(size)
            .map_err(|error| io_error(env, error))?;
        let chunk = env.new_byte_array(size)?;
        env.call_method(
            &output,
            jni_str!("writeInt"),
            jni_sig!("(I)V"),
            &[JValue::Int(length)],
        )?;
        for bytes in bytes.chunks(CHUNK_BYTES) {
            // Java bytes and Rust u8 have the same one-byte representation; the view is borrowed.
            let signed =
                unsafe { std::slice::from_raw_parts(bytes.as_ptr().cast::<i8>(), bytes.len()) };
            chunk.set_region(env, 0, signed)?;
            env.call_method(
                &output,
                jni_str!("write"),
                jni_sig!("([BII)V"),
                &[
                    JValue::Object(chunk.as_ref()),
                    JValue::Int(0),
                    JValue::Int(bytes.len() as jint),
                ],
            )?;
        }
        Ok(4 + jlong::from(length))
    })
    .resolve::<ThrowRuntimeExAndDefault>()
}

#[unsafe(no_mangle)]
pub extern "system" fn Java_tech_streamfusion_nativebridge_NativePlanState_restoreStream<'a>(
    mut env: EnvUnowned<'a>,
    _class: JClass<'a>,
    handle: jlong,
    id: jlong,
    group: jint,
    input: JObject<'a>,
    length: jint,
) {
    env.with_env(|env| -> jni::errors::Result<_> {
        let length = usize::try_from(length).map_err(|error| io_error(env, error))?;
        let context = execution_context::get(handle).map_err(|error| io_error(env, error))?;
        let reservation = context.reservation("canonical restore bytes and JVM transport");
        let size = length.min(CHUNK_BYTES);
        reservation
            .try_grow(length.saturating_add(size))
            .map_err(|error| io_error(env, error))?;
        let chunk = env.new_byte_array(size)?;
        // Admit both buffers before allocating or reading. A truncated Flink stream cannot mutate state.
        let mut bytes = vec![0u8; length];
        for bytes in bytes.chunks_mut(CHUNK_BYTES) {
            env.call_method(
                &input,
                jni_str!("readFully"),
                jni_sig!("([BII)V"),
                &[
                    JValue::Object(chunk.as_ref()),
                    JValue::Int(0),
                    JValue::Int(bytes.len() as jint),
                ],
            )?;
            let signed = unsafe {
                std::slice::from_raw_parts_mut(bytes.as_mut_ptr().cast::<i8>(), bytes.len())
            };
            chunk.get_region(env, 0, signed)?;
        }
        context
            .restore_state(id as u64, group as u32, &bytes)
            .map_err(|error| io_error(env, error))
    })
    .resolve::<ThrowRuntimeExAndDefault>()
}

fn io_error(env: &mut jni::Env<'_>, error: impl std::fmt::Display) -> jni::errors::Error {
    let _ = env.throw_new(
        jni_str!("java/io/IOException"),
        jni::strings::JNIString::new(error.to_string()),
    );
    jni::errors::Error::JavaException
}
