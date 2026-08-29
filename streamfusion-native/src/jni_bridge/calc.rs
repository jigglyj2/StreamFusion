// Copyright 2026 StreamFusion Authors
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0

use arrow::ffi::{FFI_ArrowArray, FFI_ArrowSchema};
use datafusion::datasource::memory::MemorySourceConfig;
use jni::errors::ThrowRuntimeExAndDefault;
use jni::jni_str;
use jni::objects::JClass;
use jni::strings::JNIString;
use jni::sys::jlong;
use jni::EnvUnowned;

use super::common::{execute_and_export, import_input};
use crate::execution_context::{self, NativeExecutionContext};
use crate::planner::create_plan_from_decoded;

#[unsafe(no_mangle)]
pub extern "system" fn Java_tech_streamfusion_nativebridge_NativeCalcBridge_executeArrowBatch<
    'caller,
>(
    mut unowned_env: EnvUnowned<'caller>,
    _class: JClass<'caller>,
    execution_context: jlong,
    input_array_address: jlong,
    input_schema_address: jlong,
    output_array_address: jlong,
    output_schema_address: jlong,
) -> jlong {
    unowned_env
        .with_env(|env| -> jni::errors::Result<_> {
            let context = execution_context::get(execution_context).map_err(|error| {
                let _ = env.throw_new(
                    jni_str!("java/lang/IllegalStateException"),
                    JNIString::new(error.to_string()),
                );
                jni::errors::Error::JavaException
            })?;
            let rows = unsafe {
                execute_arrow(
                    &context,
                    input_array_address as *mut FFI_ArrowArray,
                    input_schema_address as *mut FFI_ArrowSchema,
                    output_array_address as *mut FFI_ArrowArray,
                    output_schema_address as *mut FFI_ArrowSchema,
                )
            }
            .map_err(|error| {
                let _ = env.throw_new(
                    jni_str!("java/lang/IllegalStateException"),
                    JNIString::new(error.to_string()),
                );
                jni::errors::Error::JavaException
            })?;
            Ok(rows as jlong)
        })
        .resolve::<ThrowRuntimeExAndDefault>()
}

unsafe fn execute_arrow(
    context: &NativeExecutionContext,
    input_array_address: *mut FFI_ArrowArray,
    input_schema_address: *mut FFI_ArrowSchema,
    output_array_address: *mut FFI_ArrowArray,
    output_schema_address: *mut FFI_ArrowSchema,
) -> datafusion::error::Result<usize> {
    let (input_batch, _input_reservation) =
        unsafe { import_input(context, input_array_address, input_schema_address, 0) }?;
    let schema = input_batch.schema();
    let source = MemorySourceConfig::try_new_exec(&[vec![input_batch]], schema, None)?;
    let plan = create_plan_from_decoded(context.plan(), vec![source])?;
    unsafe { execute_and_export(context, plan, output_array_address, output_schema_address) }
}
