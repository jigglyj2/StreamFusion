// Copyright 2026 StreamFusion Authors
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0

use std::sync::Arc;

use arrow::array::{Int32Array, RecordBatch};
use arrow::datatypes::{DataType, Field, Schema};
use datafusion::datasource::memory::MemorySourceConfig;
use datafusion::physical_plan::collect;
use datafusion::prelude::SessionContext;
use jni::errors::ThrowRuntimeExAndDefault;
use jni::jni_str;
use jni::objects::{JByteArray, JClass, JIntArray};
use jni::strings::JNIString;
use jni::sys::jintArray;
use jni::EnvUnowned;

use crate::planner::create_plan;

#[unsafe(no_mangle)]
pub extern "system" fn Java_tech_streamfusion_nativebridge_NativeCalcBridge_executeIntIdentity<
    'caller,
>(
    mut unowned_env: EnvUnowned<'caller>,
    _class: JClass<'caller>,
    serialized_plan: JByteArray<'caller>,
    input: JIntArray<'caller>,
) -> jintArray {
    unowned_env
        .with_env(|env| -> jni::errors::Result<_> {
            let plan = env.convert_byte_array(serialized_plan)?;
            let mut values = vec![0; input.len(env)?];
            input.get_region(env, 0, &mut values)?;
            let result = execute(&plan, values).map_err(|error| {
                let _ = env.throw_new(
                    jni_str!("java/lang/IllegalStateException"),
                    JNIString::new(error.to_string()),
                );
                jni::errors::Error::JavaException
            })?;
            let output = env.new_int_array(result.len())?;
            output.set_region(env, 0, &result)?;
            Ok(output.into_raw())
        })
        .resolve::<ThrowRuntimeExAndDefault>()
}

fn execute(plan: &[u8], values: Vec<i32>) -> datafusion::error::Result<Vec<i32>> {
    let schema = Arc::new(Schema::new(vec![Field::new("id", DataType::Int32, false)]));
    let batch = RecordBatch::try_new(schema.clone(), vec![Arc::new(Int32Array::from(values))])?;
    let source = MemorySourceConfig::try_new_exec(&[vec![batch]], schema, None)?;
    let plan = create_plan(plan, source)?;
    let runtime = tokio::runtime::Builder::new_current_thread()
        .build()
        .map_err(|error| datafusion::error::DataFusionError::External(Box::new(error)))?;
    let batches = runtime.block_on(collect(plan, SessionContext::new().task_ctx()))?;
    let mut output = Vec::new();
    for batch in batches {
        let values = batch
            .column(0)
            .as_any()
            .downcast_ref::<Int32Array>()
            .ok_or_else(|| {
                datafusion::error::DataFusionError::Execution(
                    "identity calc returned a non-INT column".to_string(),
                )
            })?;
        output.extend(values.values().iter().copied());
    }
    Ok(output)
}
