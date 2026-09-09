// Copyright 2026 StreamFusion Authors
// Licensed under the Apache License, Version 2.0.

use arrow::ffi_stream::FFI_ArrowArrayStream;
use jni::errors::ThrowRuntimeExAndDefault;
use jni::objects::{JByteArray, JClass, JLongArray, JObject};
use jni::sys::{jboolean, jint, jlong};
use jni::EnvUnowned;

use super::common::throw;
use crate::execution_context::{self, lookup_resources::LookupSource};

#[unsafe(no_mangle)]
pub extern "system" fn Java_tech_streamfusion_nativebridge_NativeLookupResources_edgeVersion(
    _env: EnvUnowned<'_>,
    _class: JClass<'_>,
) -> jint {
    1
}

#[unsafe(no_mangle)]
pub extern "system" fn Java_tech_streamfusion_nativebridge_NativeLookupResources_create<'a>(
    mut env: EnvUnowned<'a>,
    _class: JClass<'a>,
    plan: JByteArray<'a>,
    state: JByteArray<'a>,
    task: JByteArray<'a>,
    ids: JLongArray<'a>,
    streams: JLongArray<'a>,
    manager: JObject<'a>,
    limit: jlong,
    region: jboolean,
) -> jlong {
    env.with_env(|env| -> jni::errors::Result<_> {
        let count = ids.len(env)?;
        if limit <= 0 || count == 0 || count != streams.len(env)? {
            return Err(throw(env, "Invalid native lookup resource arguments"));
        }
        // The Java constructor admits these JNI copies before entry. Source callbacks own
        // their producer buffers separately; cleanup completes before a Java exception is set.
        let plan = env.convert_byte_array(plan)?;
        let state = if state.is_null() {
            None
        } else {
            Some(env.convert_byte_array(state)?)
        };
        let task = if task.is_null() {
            None
        } else {
            Some(env.convert_byte_array(task)?)
        };
        let mut node_ids = vec![0; count];
        let mut addresses = vec![0; count];
        ids.get_region(env, 0, &mut node_ids)?;
        streams.get_region(env, 0, &mut addresses)?;
        if node_ids.iter().any(|&id| id <= 0) {
            return Err(throw(
                env,
                "Lookup snapshot identities must be positive Java longs",
            ));
        }
        let sources = node_ids
            .iter()
            .zip(&addresses)
            .map(|(&id, &address)| LookupSource {
                node_id: id as u64,
                stream: address as *mut FFI_ArrowArrayStream,
            })
            .collect::<Vec<_>>();
        let vm = env.get_java_vm()?;
        let manager = env.new_global_ref(manager)?;
        unsafe {
            execution_context::register_with_lookup_sources(
                &plan,
                state.as_deref(),
                task.as_deref(),
                vm,
                manager,
                limit as usize,
                region,
                &sources,
            )
        }
        .map_err(|error| throw(env, error))
    })
    .resolve::<ThrowRuntimeExAndDefault>()
}
