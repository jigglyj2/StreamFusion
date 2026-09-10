// Copyright 2026 StreamFusion Authors
// Licensed under the Apache License, Version 2.0.

use super::common::throw;
use crate::exchange::{decode_exchange_plan, exchange_key_fields, KeyField};
use crate::memory_pool::{
    HostMemoryReservation, JvmMemoryReservationBroker, MemoryReservationBroker,
};
use arrow::ffi::{FFI_ArrowArray, FFI_ArrowSchema};
use datafusion::error::{DataFusionError, Result};
use jni::errors::ThrowRuntimeExAndDefault;
use jni::objects::{JByteArray, JClass, JObject};
use jni::sys::{jbyteArray, jint, jlong};
use jni::EnvUnowned;
use std::collections::HashMap;
use std::sync::atomic::{AtomicI64, Ordering};
use std::sync::{Arc, Mutex, OnceLock};

struct Router {
    plan: crate::proto::NativeExchangePlan,
    keys: Vec<(usize, KeyField)>,
    broker: Arc<dyn MemoryReservationBroker>,
    _memory: HostMemoryReservation,
}
static ROUTERS: OnceLock<Mutex<HashMap<i64, Arc<Router>>>> = OnceLock::new();
static NEXT: AtomicI64 = AtomicI64::new(1);
fn registry() -> &'static Mutex<HashMap<i64, Arc<Router>>> {
    ROUTERS.get_or_init(Default::default)
}
fn invalid() -> DataFusionError {
    DataFusionError::Execution("Native exchange router is closed or invalid".into())
}
fn get(handle: i64) -> Result<Arc<Router>> {
    registry()
        .lock()
        .map_err(|_| invalid())?
        .get(&handle)
        .cloned()
        .ok_or_else(invalid)
}

#[unsafe(no_mangle)]
pub extern "system" fn Java_tech_streamfusion_nativebridge_NativeExchangeRouter_edgeVersion(
    _env: EnvUnowned<'_>,
    _class: JClass<'_>,
) -> jint {
    1
}

#[unsafe(no_mangle)]
pub extern "system" fn Java_tech_streamfusion_nativebridge_NativeExchangeRouter_create<'a>(
    mut env: EnvUnowned<'a>,
    _class: JClass<'a>,
    plan: JByteArray<'a>,
    manager: JObject<'a>,
) -> jlong {
    env.with_env(|env| -> jni::errors::Result<_> {
        let broker: Arc<dyn MemoryReservationBroker> = Arc::new(JvmMemoryReservationBroker::new(
            env.get_java_vm()?,
            env.new_global_ref(manager)?,
        ));
        let mut memory =
            HostMemoryReservation::new(broker.clone(), "native exchange plan and schema");
        memory
            .resize(plan.len(env)?.saturating_mul(16).saturating_add(65536))
            .map_err(|e| throw(env, e))?;
        let bytes = env.convert_byte_array(plan)?;
        let plan = decode_exchange_plan(&bytes).map_err(|e| throw(env, e))?;
        let keys = exchange_key_fields(&plan).map_err(|e| throw(env, e))?;
        let router = Arc::new(Router {
            plan,
            keys,
            broker,
            _memory: memory,
        });
        let handle = NEXT
            .fetch_update(Ordering::Relaxed, Ordering::Relaxed, |id| id.checked_add(1))
            .map_err(|_| throw(env, "native exchange router handles exhausted"))?;
        registry()
            .lock()
            .map_err(|_| throw(env, invalid()))?
            .insert(handle, router);
        Ok(handle)
    })
    .resolve::<ThrowRuntimeExAndDefault>()
}

#[unsafe(no_mangle)]
pub extern "system" fn Java_tech_streamfusion_nativebridge_NativeExchangeRouter_route<'a>(
    mut env: EnvUnowned<'a>,
    _class: JClass<'a>,
    handle: jlong,
    array: jlong,
    schema: jlong,
) -> jbyteArray {
    env.with_env(|env| -> jni::errors::Result<_> {
        let router = get(handle).map_err(|e| throw(env, e))?;
        let frames = unsafe {
            super::exchange::route_prepared(
                &router.plan,
                &router.keys,
                array as *mut FFI_ArrowArray,
                schema as *mut FFI_ArrowSchema,
                router.broker.clone(),
            )
        }
        .map_err(|e| throw(env, e))?;
        super::exchange::export_frames(env, &frames.frames)
    })
    .resolve::<ThrowRuntimeExAndDefault>()
}

#[unsafe(no_mangle)]
pub extern "system" fn Java_tech_streamfusion_nativebridge_NativeExchangeRouter_closeRouter<'a>(
    mut env: EnvUnowned<'a>,
    _class: JClass<'a>,
    handle: jlong,
) {
    env.with_env(|env| -> jni::errors::Result<_> {
        let router = registry()
            .lock()
            .map_err(|_| throw(env, invalid()))?
            .remove(&handle)
            .ok_or_else(|| throw(env, invalid()))?;
        drop(router); // Release Flink credit after leaving the registry lock.
        Ok(())
    })
    .resolve::<ThrowRuntimeExAndDefault>()
}
