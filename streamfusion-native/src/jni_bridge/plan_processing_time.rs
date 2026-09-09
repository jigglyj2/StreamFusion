// Copyright 2026 StreamFusion Authors
// Licensed under the Apache License, Version 2.0.

use super::common::throw;
use crate::execution_context;
use jni::errors::ThrowRuntimeExAndDefault;
use jni::objects::JClass;
use jni::sys::{jlong, jlongArray};
use jni::EnvUnowned;

#[unsafe(no_mangle)]
pub extern "system" fn Java_tech_streamfusion_nativebridge_NativeExecutionContext_readProcessingTimeDeadlines<
    'a,
>(
    mut env: EnvUnowned<'a>,
    _class: JClass<'a>,
    handle: jlong,
) -> jlongArray {
    env.with_env(|env| -> jni::errors::Result<_> {
        let values = execution_context::get(handle)
            .and_then(|context| context.processing_time_deadlines())
            .map_err(|error| throw(env, error))?;
        let result = (|| {
            let output = env.new_long_array(values.len())?;
            output.set_region(env, 0, &values)?;
            Ok(output.into_raw())
        })();
        drop(values);
        result
    })
    .resolve::<ThrowRuntimeExAndDefault>()
}
