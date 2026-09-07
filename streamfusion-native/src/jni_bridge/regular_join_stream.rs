// Copyright 2026 StreamFusion Authors
// Licensed under the Apache License, Version 2.0

use arrow::ffi::{FFI_ArrowArray, FFI_ArrowSchema};
use arrow::ffi_stream::FFI_ArrowArrayStream;
use datafusion::error::DataFusionError;
use jni::{
    errors::ThrowRuntimeExAndDefault,
    objects::{JClass, JLongArray},
    sys::{jint, jlong, jlongArray},
    EnvUnowned,
};

#[unsafe(no_mangle)]
pub extern "system" fn Java_tech_streamfusion_nativebridge_NativeRegularJoinBridge_processArrowStream<
    'caller,
>(
    mut env: EnvUnowned<'caller>,
    _class: JClass<'caller>,
    handle: jlong,
    side: jint,
    input_array: jlong,
    input_schema: jlong,
    output_stream: jlong,
) {
    env.with_env(|env| -> jni::errors::Result<()> {
        (|| -> datafusion::error::Result<()> {
            if output_stream == 0 {
                return Err(DataFusionError::Execution(
                    "regular join output stream is null".into(),
                ));
            }
            let handle = unsafe { super::regular_join::native_handle(handle) }?;
            let region = handle.region.as_ref().ok_or_else(|| {
                DataFusionError::Execution(
                    "bounded regular join cannot use a streaming input invocation".into(),
                )
            })?;
            let input = unsafe {
                super::common::import_record_batch(
                    input_array as *mut FFI_ArrowArray,
                    input_schema as *mut FFI_ArrowSchema,
                )
            }?;
            let side = usize::try_from(side)
                .map_err(|_| DataFusionError::Execution("regular join side is negative".into()))?;
            let reader = region.start(side, input)?;
            unsafe {
                std::ptr::write(
                    output_stream as *mut FFI_ArrowArrayStream,
                    FFI_ArrowArrayStream::new(Box::new(reader)),
                );
            }
            Ok(())
        })()
        .map_err(|error| super::common::throw(env, error))
    })
    .resolve::<ThrowRuntimeExAndDefault>();
}

#[unsafe(no_mangle)]
pub extern "system" fn Java_tech_streamfusion_nativebridge_NativeRegularJoinBridge_nativeMetricSnapshot<
    'caller,
>(
    mut env: EnvUnowned<'caller>,
    _class: JClass<'caller>,
    handle: jlong,
) -> jlongArray {
    env.with_env(|env| -> jni::errors::Result<_> {
        let handle = unsafe { super::regular_join::native_handle(handle) }
            .map_err(|error| super::common::throw(env, error))?;
        let values = handle
            .region
            .as_ref()
            .map(|region| region.metrics())
            .unwrap_or_default();
        let output: JLongArray<'_> = env.new_long_array(values.len())?;
        output.set_region(env, 0, &values)?;
        Ok(output.into_raw())
    })
    .resolve::<ThrowRuntimeExAndDefault>()
}
