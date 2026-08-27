// Copyright 2026 StreamFusion Authors
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0

use arrow::array::{Array, RecordBatch, StructArray};
use arrow::ffi::{from_ffi, FFI_ArrowArray, FFI_ArrowSchema};
use datafusion::datasource::memory::MemorySourceConfig;
use datafusion::physical_plan::collect;
use datafusion::prelude::SessionContext;
use jni::errors::ThrowRuntimeExAndDefault;
use jni::jni_str;
use jni::objects::{JByteArray, JClass};
use jni::strings::JNIString;
use jni::sys::jlong;
use jni::EnvUnowned;

use crate::planner::create_plan;

#[unsafe(no_mangle)]
pub extern "system" fn Java_tech_streamfusion_nativebridge_NativeCalcBridge_executeArrowBatch<
    'caller,
>(
    mut unowned_env: EnvUnowned<'caller>,
    _class: JClass<'caller>,
    serialized_plan: JByteArray<'caller>,
    input_array_address: jlong,
    input_schema_address: jlong,
    output_array_address: jlong,
    output_schema_address: jlong,
) -> jlong {
    unowned_env
        .with_env(|env| -> jni::errors::Result<_> {
            let plan = env.convert_byte_array(serialized_plan)?;
            let rows = unsafe {
                execute_arrow(
                    &plan,
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
    plan: &[u8],
    input_array_address: *mut FFI_ArrowArray,
    input_schema_address: *mut FFI_ArrowSchema,
    output_array_address: *mut FFI_ArrowArray,
    output_schema_address: *mut FFI_ArrowSchema,
) -> datafusion::error::Result<usize> {
    if input_array_address.is_null()
        || input_schema_address.is_null()
        || output_array_address.is_null()
        || output_schema_address.is_null()
    {
        return Err(datafusion::error::DataFusionError::Execution(
            "Arrow C Data address was null".to_string(),
        ));
    }

    let ffi_array = unsafe { FFI_ArrowArray::from_raw(input_array_address) };
    let ffi_schema = unsafe { FFI_ArrowSchema::from_raw(input_schema_address) };
    let input_data = unsafe { from_ffi(ffi_array, &ffi_schema) }?;
    let input_batch = RecordBatch::from(StructArray::from(input_data));
    let schema = input_batch.schema();
    let source = MemorySourceConfig::try_new_exec(&[vec![input_batch]], schema, None)?;
    let plan = create_plan(plan, source)?;
    let output_schema = plan.schema();
    let runtime = tokio::runtime::Builder::new_current_thread()
        .build()
        .map_err(|error| datafusion::error::DataFusionError::External(Box::new(error)))?;
    let batches = runtime.block_on(collect(plan, SessionContext::new().task_ctx()))?;
    let output_batch = if batches.is_empty() {
        RecordBatch::new_empty(output_schema)
    } else {
        arrow::compute::concat_batches(&output_schema, batches.iter())?
    };
    let rows = output_batch.num_rows();
    let output_struct = StructArray::from(output_batch);
    let output_data = output_struct.to_data();
    let output_array = FFI_ArrowArray::new(&output_data);
    let output_schema = FFI_ArrowSchema::try_from(output_data.data_type())?;
    unsafe {
        std::ptr::write(output_array_address, output_array);
        std::ptr::write(output_schema_address, output_schema);
    }
    Ok(rows)
}
