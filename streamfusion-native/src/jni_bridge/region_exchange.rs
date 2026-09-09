// Copyright 2026 StreamFusion Authors
// Licensed under the Apache License, Version 2.0.

use super::{common::throw, region_output};
use jni::errors::ThrowRuntimeExAndDefault;
use jni::objects::{JByteArray, JClass, JLongArray};
use jni::sys::{jint, jlong, jlongArray};
use jni::EnvUnowned;

#[unsafe(no_mangle)]
pub extern "system" fn Java_tech_streamfusion_nativebridge_NativeRegionStream_openExchangeInputs<
    'a,
>(
    mut env: EnvUnowned<'a>,
    _: JClass<'a>,
    handle: jlong,
    port: jint,
    plan: JByteArray<'a>,
    payload: JByteArray<'a>,
    offset: jint,
    length: jint,
    metadata: jint,
    arrays: JLongArray<'a>,
    schemas: JLongArray<'a>,
) -> jlongArray {
    env.with_env(|env| -> jni::errors::Result<_> {
        let context = crate::execution_context::get(handle).map_err(|e| throw(env, e))?;
        let expected = context.region_input_count().map_err(|e| throw(env, e))?
            + context.clock_input_bindings().len();
        if arrays.len(env)? != expected || schemas.len(env)? != expected {
            return Err(throw(env, "native region input arity mismatch"));
        }
        // Allocate the small response before consuming any producer-owned C Data handles.
        let response = env.new_long_array(2)?;
        let (batches, reservations, rows) = super::plan_exchange::prepare_exchange(
            env, &context, port, plan, payload, offset, length, metadata, arrays, schemas,
        )?;
        let opened: datafusion::error::Result<_> = (|| {
            let stream = context.start_region(batches)?;
            region_output::register(region_output::Output::new(
                context.clone(),
                stream,
                reservations,
            ))
        })();
        let output = opened.map_err(|e| throw(env, e))?;
        if let Err(error) = response.set_region(env, 0, &[output, rows as jlong]) {
            // A failed response must not orphan a running invocation in the handle registry.
            let _ = region_output::close(output);
            return Err(error);
        }
        Ok(response.into_raw())
    })
    .resolve::<ThrowRuntimeExAndDefault>()
}
