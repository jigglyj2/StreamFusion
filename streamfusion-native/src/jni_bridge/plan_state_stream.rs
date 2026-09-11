// Copyright 2026 StreamFusion Authors
// Licensed under the Apache License, Version 2.0.

//! Flink owns checkpoint streams. Transfer the existing canonical frame in bounded chunks;
//! production restore stages to admitted native spill files instead of retaining whole groups.
use crate::execution_context;
use jni::errors::ThrowRuntimeExAndDefault;
use jni::objects::{JByteArray, JClass, JObject, JValue};
use jni::sys::{jint, jlong};
use jni::{jni_sig, jni_str, EnvUnowned};

const CHUNK_BYTES: usize = 64 << 10;

#[unsafe(no_mangle)]
pub extern "system" fn Java_tech_streamfusion_nativebridge_NativePlanState_streamEdgeVersion(
    _env: EnvUnowned<'_>,
    _class: JClass<'_>,
) -> jint {
    2
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
        let reservation = context.reservation("canonical snapshot buffered transport");
        reservation
            .try_grow(CHUNK_BYTES * 2)
            .map_err(|error| io_error(env, error))?;
        let chunk = env.new_byte_array(CHUNK_BYTES)?;
        let mut buffered = Vec::with_capacity(CHUNK_BYTES);
        let mut flush = |bytes: &[u8]| -> datafusion::error::Result<()> {
            let signed =
                unsafe { std::slice::from_raw_parts(bytes.as_ptr().cast::<i8>(), bytes.len()) };
            chunk
                .set_region(env, 0, signed)
                .map_err(|error| datafusion::error::DataFusionError::External(Box::new(error)))?;
            env.call_method(
                &output,
                jni_str!("write"),
                jni_sig!("([BII)V"),
                &[
                    JValue::Object(chunk.as_ref()),
                    JValue::Int(0),
                    JValue::Int(bytes.len() as jint),
                ],
            )
            .map_err(|error| datafusion::error::DataFusionError::External(Box::new(error)))?;
            Ok(())
        };
        let result = context
            .write_snapshot_state(id as u64, group as u32, &mut |mut part: &[u8]| {
                // Coalesce entry framing and small state values. JNI is crossed per transport chunk,
                // never once per keyed entry. The backend's borrowed page remains alive until copied.
                while !part.is_empty() {
                    let count = part.len().min(CHUNK_BYTES - buffered.len());
                    buffered.extend_from_slice(&part[..count]);
                    part = &part[count..];
                    if buffered.len() == CHUNK_BYTES {
                        flush(&buffered)?;
                        buffered.clear();
                    }
                }
                Ok(())
            })
            .and_then(|bytes| {
                if !buffered.is_empty() {
                    flush(&buffered)?;
                }
                Ok(bytes as jlong)
            });
        result.map_err(|error| {
            if env.exception_check() {
                jni::errors::Error::JavaException
            } else {
                io_error(env, error)
            }
        })
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
    length: jlong,
) {
    env.with_env(|env| -> jni::errors::Result<_> {
        let length = u64::try_from(length).map_err(|error| io_error(env, error))?;
        let context = execution_context::get(handle).map_err(|error| io_error(env, error))?;
        let reservation = context.reservation("canonical restore JVM transport");
        let size = length.min(CHUNK_BYTES as u64) as usize;
        reservation
            .try_grow(size)
            .map_err(|error| io_error(env, error))?;
        let chunk = env.new_byte_array(size)?;
        let mut reader = JavaInput {
            env,
            input,
            chunk,
            size,
        };
        let result = context.restore_state_reader(id as u64, group as u32, length, &mut reader);
        drop(reader);
        result.map_err(|error| {
            if env.exception_check() {
                jni::errors::Error::JavaException
            } else {
                io_error(env, error)
            }
        })
    })
    .resolve::<ThrowRuntimeExAndDefault>()
}

/// One reusable Java byte array. Calls follow transport chunks, never state keys or sort rows.
struct JavaInput<'env, 'local> {
    env: &'env mut jni::Env<'local>,
    input: JObject<'local>,
    chunk: JByteArray<'local>,
    size: usize,
}
impl std::io::Read for JavaInput<'_, '_> {
    fn read(&mut self, bytes: &mut [u8]) -> std::io::Result<usize> {
        let count = bytes.len().min(self.size);
        if count == 0 {
            return Ok(0);
        }
        let result = (|| -> jni::errors::Result<()> {
            self.env.call_method(
                &self.input,
                jni_str!("readFully"),
                jni_sig!("([BII)V"),
                &[
                    JValue::Object(self.chunk.as_ref()),
                    JValue::Int(0),
                    JValue::Int(count as jint),
                ],
            )?;
            let signed =
                unsafe { std::slice::from_raw_parts_mut(bytes.as_mut_ptr().cast::<i8>(), count) };
            self.chunk.get_region(self.env, 0, signed)
        })();
        result
            .map(|()| count)
            .map_err(|error| std::io::Error::other(error.to_string()))
    }
}

fn io_error(env: &mut jni::Env<'_>, error: impl std::fmt::Display) -> jni::errors::Error {
    let _ = env.throw_new(
        jni_str!("java/io/IOException"),
        jni::strings::JNIString::new(error.to_string()),
    );
    jni::errors::Error::JavaException
}
