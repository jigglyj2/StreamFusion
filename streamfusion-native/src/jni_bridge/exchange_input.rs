// Copyright 2026 StreamFusion Authors
// Licensed under the Apache License, Version 2.0.

use super::common::throw;
use jni::errors::ThrowRuntimeExAndDefault;
use jni::objects::{JByteArray, JClass};
use jni::sys::{jint, jlong};
use jni::EnvUnowned;

#[unsafe(no_mangle)]
pub extern "system" fn Java_tech_streamfusion_nativebridge_NativeExchangeInputs_edgeVersion(
    _env: EnvUnowned<'_>,
    _class: JClass<'_>,
) -> jint {
    1
}

#[unsafe(no_mangle)]
pub extern "system" fn Java_tech_streamfusion_nativebridge_NativeExchangeInputs_prepare<'a>(
    mut env: EnvUnowned<'a>,
    _class: JClass<'a>,
    handle: jlong,
    port: jint,
    plan: JByteArray<'a>,
) {
    env.with_env(|env| -> jni::errors::Result<_> {
        let port = usize::try_from(port).map_err(|error| throw(env, error))?;
        let context = crate::execution_context::get(handle).map_err(|error| throw(env, error))?;
        // One task-lifetime reservation covers the protobuf copy, decoded fields, and schema.
        let memory = context.reservation("native exchange input plan and schema");
        memory
            .try_grow(plan.len(env)?.saturating_mul(16).saturating_add(65536))
            .map_err(|error| throw(env, error))?;
        let bytes = env.convert_byte_array(plan)?;
        context
            .bind_exchange_input(port, &bytes, memory)
            .map_err(|error| throw(env, error))
    })
    .resolve::<ThrowRuntimeExAndDefault>()
}
