// Copyright 2026 StreamFusion Authors
// Licensed under the Apache License, Version 2.0

use crate::execution_context;
use jni::errors::ThrowRuntimeExAndDefault;
use jni::objects::{JByteArray, JClass, JObject, JString};
use jni::sys::{jbyteArray, jint, jlong};
use jni::EnvUnowned;

#[unsafe(no_mangle)]
pub extern "system" fn Java_tech_streamfusion_nativebridge_NativePlanState_create<'a>(
    mut env: EnvUnowned<'a>,
    _class: JClass<'a>,
    plan: JByteArray<'a>,
    bindings: JByteArray<'a>,
    manager: JObject<'a>,
    limit: jlong,
) -> jlong {
    env.with_env(|env| -> jni::errors::Result<_> {
        if limit <= 0 {
            return Err(throw(env, "Native memory limit must be positive"));
        }
        // The Java context reserves both JNI copies before entering this constructor.
        let plan = env.convert_byte_array(plan)?;
        let bindings = env.convert_byte_array(bindings)?;
        let vm = env.get_java_vm()?;
        let manager = env.new_global_ref(manager)?;
        execution_context::register_with_state(&plan, Some(&bindings), vm, manager, limit as usize)
            .map_err(|error| throw(env, error))
    })
    .resolve::<ThrowRuntimeExAndDefault>()
}

#[unsafe(no_mangle)]
pub extern "system" fn Java_tech_streamfusion_nativebridge_NativePlanState_snapshot<'a>(
    mut env: EnvUnowned<'a>,
    _class: JClass<'a>,
    handle: jlong,
    id: jlong,
    group: jint,
) -> jbyteArray {
    env.with_env(|env| -> jni::errors::Result<_> {
        let context = execution_context::get(handle).map_err(|error| throw(env, error))?;
        let bytes = context
            .snapshot_state(id as u64, group as u32)
            .map_err(|error| throw(env, error))?;
        env.byte_array_from_slice(&bytes)
            .map(|array| array.into_raw())
    })
    .resolve::<ThrowRuntimeExAndDefault>()
}

#[unsafe(no_mangle)]
pub extern "system" fn Java_tech_streamfusion_nativebridge_NativePlanState_restore<'a>(
    mut env: EnvUnowned<'a>,
    _class: JClass<'a>,
    handle: jlong,
    id: jlong,
    group: jint,
    bytes: JByteArray<'a>,
) {
    env.with_env(|env| -> jni::errors::Result<_> {
        let context = execution_context::get(handle).map_err(|error| throw(env, error))?;
        let reservation = context.reservation("native region restore JNI bytes");
        reservation
            .try_grow(bytes.len(env)?)
            .map_err(|error| throw(env, error))?;
        let bytes = env.convert_byte_array(bytes)?;
        let result = context.restore_state(id as u64, group as u32, &bytes);
        drop(bytes);
        drop(reservation);
        result.map_err(|error| throw(env, error))
    })
    .resolve::<ThrowRuntimeExAndDefault>()
}

#[unsafe(no_mangle)]
pub extern "system" fn Java_tech_streamfusion_nativebridge_NativePlanState_checkpoint<'a>(
    mut env: EnvUnowned<'a>,
    _class: JClass<'a>,
    handle: jlong,
    id: jlong,
    directory: JString<'a>,
) {
    env.with_env(|env| -> jni::errors::Result<_> {
        let directory = directory.try_to_string(env)?;
        execution_context::get(handle)
            .and_then(|context| {
                context.checkpoint_state(id as u64, std::path::Path::new(&directory))
            })
            .map_err(|error| throw(env, error))
    })
    .resolve::<ThrowRuntimeExAndDefault>()
}

#[unsafe(no_mangle)]
pub extern "system" fn Java_tech_streamfusion_nativebridge_NativePlanState_importCheckpoint<'a>(
    mut env: EnvUnowned<'a>,
    _class: JClass<'a>,
    handle: jlong,
    id: jlong,
    plugin: JString<'a>,
    directory: JString<'a>,
    first: jint,
    last: jint,
    limit: jlong,
) {
    env.with_env(|env| -> jni::errors::Result<_> {
        if first < 0 || last < first || limit <= 0 {
            return Err(throw(
                env,
                "invalid checkpoint import range or memory lease",
            ));
        }
        let limit = usize::try_from(limit).map_err(|error| throw(env, error))?;
        let plugin = plugin.try_to_string(env)?;
        let directory = directory.try_to_string(env)?;
        execution_context::get(handle)
            .and_then(|context| {
                context.import_state_checkpoint(
                    id as u64,
                    std::path::Path::new(&plugin),
                    std::path::Path::new(&directory),
                    first as u32,
                    last as u32,
                    limit,
                )
            })
            .map_err(|error| throw(env, error))
    })
    .resolve::<ThrowRuntimeExAndDefault>()
}

fn throw(env: &mut jni::Env<'_>, error: impl std::fmt::Display) -> jni::errors::Error {
    let _ = env.throw_new(
        jni::jni_str!("java/lang/IllegalStateException"),
        jni::strings::JNIString::new(error.to_string()),
    );
    jni::errors::Error::JavaException
}
