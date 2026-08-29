// Copyright 2026 StreamFusion Authors
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
// http://www.apache.org/licenses/LICENSE-2.0

use jni::errors::ThrowRuntimeExAndDefault;
use jni::jni_str;
use jni::objects::{JByteArray, JClass, JObject};
use jni::strings::JNIString;
use jni::sys::jlong;
use jni::EnvUnowned;

use crate::decode_plan;
use crate::execution_context;

#[unsafe(no_mangle)]
pub extern "system" fn Java_tech_streamfusion_nativebridge_NativeExecutionContext_createExecutionContext<
    'caller,
>(
    mut unowned_env: EnvUnowned<'caller>,
    _class: JClass<'caller>,
    serialized_plan: JByteArray<'caller>,
    memory_manager: JObject<'caller>,
    memory_limit: jlong,
) -> jlong {
    unowned_env
        .with_env(|env| -> jni::errors::Result<_> {
            if memory_limit <= 0 {
                let _ = env.throw_new(
                    jni_str!("java/lang/IllegalArgumentException"),
                    JNIString::new("Native memory limit must be positive"),
                );
                return Err(jni::errors::Error::JavaException);
            }
            let plan = env.convert_byte_array(serialized_plan)?;
            let plan = decode_plan(&plan).map_err(|error| {
                let _ = env.throw_new(
                    jni_str!("java/lang/IllegalArgumentException"),
                    JNIString::new(error.to_string()),
                );
                jni::errors::Error::JavaException
            })?;
            let java_vm = env.get_java_vm()?;
            let memory_manager = env.new_global_ref(memory_manager)?;
            execution_context::register(plan, java_vm, memory_manager, memory_limit as usize)
                .map(|handle| handle as jlong)
                .map_err(|error| {
                    let _ = env.throw_new(
                        jni_str!("java/lang/IllegalStateException"),
                        JNIString::new(error.to_string()),
                    );
                    jni::errors::Error::JavaException
                })
        })
        .resolve::<ThrowRuntimeExAndDefault>()
}

#[unsafe(no_mangle)]
pub extern "system" fn Java_tech_streamfusion_nativebridge_NativeExecutionContext_closeExecutionContext<
    'caller,
>(
    mut unowned_env: EnvUnowned<'caller>,
    _class: JClass<'caller>,
    handle: jlong,
) {
    unowned_env
        .with_env(|env| -> jni::errors::Result<()> {
            execution_context::close(handle).map_err(|error| {
                let _ = env.throw_new(
                    jni_str!("java/lang/IllegalStateException"),
                    JNIString::new(error.to_string()),
                );
                jni::errors::Error::JavaException
            })
        })
        .resolve::<ThrowRuntimeExAndDefault>()
}
