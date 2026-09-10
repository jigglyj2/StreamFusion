// Copyright 2026 StreamFusion Authors
// Licensed under the Apache License, Version 2.0.

use super::common::throw;
use crate::execution_context;
use jni::errors::ThrowRuntimeExAndDefault;
use jni::objects::JClass;
use jni::sys::{jboolean, jint, jlong, jlongArray};
use jni::EnvUnowned;

#[unsafe(no_mangle)]
pub extern "system" fn Java_tech_streamfusion_nativebridge_NativeExecutionContext_invocationSnapshotEdgeVersion(
    _env: EnvUnowned<'_>,
    _class: JClass<'_>,
) -> jint {
    1
}

#[unsafe(no_mangle)]
pub extern "system" fn Java_tech_streamfusion_nativebridge_NativeExecutionContext_readInvocationSnapshot<
    'a,
>(
    mut env: EnvUnowned<'a>,
    _class: JClass<'a>,
    handle: jlong,
    gauges: jboolean,
    deadlines: jboolean,
) -> jlongArray {
    env.with_env(|env| -> jni::errors::Result<_> {
        let context = execution_context::get(handle).map_err(|error| throw(env, error))?;
        let metrics = context
            .metric_snapshot()
            .map_err(|error| throw(env, error))?;
        let gauge_values = if gauges {
            Some(
                context
                    .gauge_snapshot()
                    .map_err(|error| throw(env, error))?,
            )
        } else {
            None
        };
        let deadlines = if deadlines {
            context
                .processing_time_deadlines()
                .map_err(|error| throw(env, error))?
        } else {
            Vec::new()
        };
        let gauge_slice = gauge_values
            .as_ref()
            .map_or(&[][..], |(values, _)| values.as_slice());
        // These per-stage scalar descriptors fit the admitted execution-context control allowance.
        // Copy each section straight into the single Java result; do not build a second Rust vector.
        let count = 4 + metrics.len() + gauge_slice.len() + deadlines.len();
        let output = env.new_long_array(count)?;
        output.set_region(
            env,
            0,
            &[
                1,
                metrics.len() as i64,
                gauge_slice.len() as i64,
                deadlines.len() as i64,
            ],
        )?;
        output.set_region(env, 4, &metrics)?;
        output.set_region(env, (4 + metrics.len()) as i32, gauge_slice)?;
        output.set_region(
            env,
            (4 + metrics.len() + gauge_slice.len()) as i32,
            &deadlines,
        )?;
        Ok(output.into_raw())
    })
    .resolve::<ThrowRuntimeExAndDefault>()
}
