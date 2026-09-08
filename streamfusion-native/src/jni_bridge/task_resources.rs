// Copyright 2026 StreamFusion Authors
// Licensed under the Apache License, Version 2.0.

use crate::execution_context;
use jni::errors::ThrowRuntimeExAndDefault;
use jni::objects::{JByteArray, JClass, JObject};
use jni::sys::jlong;
use jni::EnvUnowned;

#[unsafe(no_mangle)]
pub extern "system" fn Java_tech_streamfusion_nativebridge_NativeTaskResources_create<'a>(
    mut env: EnvUnowned<'a>,
    _class: JClass<'a>,
    plan: JByteArray<'a>,
    state_bindings: JByteArray<'a>,
    task_bindings: JByteArray<'a>,
    manager: JObject<'a>,
    limit: jlong,
) -> jlong {
    env.with_env(|env| -> jni::errors::Result<_> {
        if limit <= 0 {
            return Err(throw(env, "Native memory limit must be positive"));
        }
        // Java admits all JNI copies before entry. They drop before that credit is returned.
        let plan = env.convert_byte_array(plan)?;
        let state = if state_bindings.is_null() {
            None
        } else {
            Some(env.convert_byte_array(state_bindings)?)
        };
        let task = env.convert_byte_array(task_bindings)?;
        let vm = env.get_java_vm()?;
        let manager = env.new_global_ref(manager)?;
        execution_context::register_with_resources(
            &plan,
            state.as_deref(),
            Some(&task),
            vm,
            manager,
            limit as usize,
        )
        .map_err(|error| throw(env, error))
    })
    .resolve::<ThrowRuntimeExAndDefault>()
}
fn throw(env: &mut jni::Env<'_>, error: impl std::fmt::Display) -> jni::errors::Error {
    let _ = env.throw_new(
        jni::jni_str!("java/lang/IllegalArgumentException"),
        jni::strings::JNIString::new(error.to_string()),
    );
    jni::errors::Error::JavaException
}
