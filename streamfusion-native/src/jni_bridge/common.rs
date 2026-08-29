// Copyright 2026 StreamFusion Authors
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0

use std::sync::Arc;

use arrow::array::{Array, Int32Array, RecordBatch, StructArray};
use arrow::datatypes::{DataType, Field, Schema};
use arrow::ffi::{from_ffi, FFI_ArrowArray, FFI_ArrowSchema};
use datafusion::physical_plan::{collect, ExecutionPlan};
use datafusion::prelude::SessionContext;

pub(super) unsafe fn import_input(
    input_array_address: *mut FFI_ArrowArray,
    input_schema_address: *mut FFI_ArrowSchema,
    row_offset: usize,
) -> datafusion::error::Result<RecordBatch> {
    let input_batch = unsafe { import_record_batch(input_array_address, input_schema_address) }?;
    let row_count = input_batch.num_rows();
    let mut fields = input_batch
        .schema()
        .fields()
        .iter()
        .cloned()
        .collect::<Vec<_>>();
    fields.push(Arc::new(Field::new(
        "__streamfusion_input_row",
        DataType::Int32,
        false,
    )));
    let mut columns = input_batch.columns().to_vec();
    columns.push(Arc::new(Int32Array::from_iter_values(
        (row_offset..row_offset + row_count).map(|index| index as i32),
    )));
    Ok(RecordBatch::try_new(
        Arc::new(Schema::new(fields)),
        columns,
    )?)
}

pub(super) unsafe fn import_record_batch(
    input_array_address: *mut FFI_ArrowArray,
    input_schema_address: *mut FFI_ArrowSchema,
) -> datafusion::error::Result<RecordBatch> {
    if input_array_address.is_null() || input_schema_address.is_null() {
        return Err(datafusion::error::DataFusionError::Execution(
            "Arrow C Data input address was null".to_string(),
        ));
    }
    let ffi_array = unsafe { FFI_ArrowArray::from_raw(input_array_address) };
    let ffi_schema = unsafe { FFI_ArrowSchema::from_raw(input_schema_address) };
    let input_data = unsafe { from_ffi(ffi_array, &ffi_schema) }?;
    Ok(RecordBatch::from(StructArray::from(input_data)))
}

pub(super) unsafe fn execute_and_export(
    plan: Arc<dyn ExecutionPlan>,
    output_array_address: *mut FFI_ArrowArray,
    output_schema_address: *mut FFI_ArrowSchema,
) -> datafusion::error::Result<usize> {
    if output_array_address.is_null() || output_schema_address.is_null() {
        return Err(datafusion::error::DataFusionError::Execution(
            "Arrow C Data output address was null".to_string(),
        ));
    }
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
    let output_data = StructArray::from(output_batch).to_data();
    let output_array = FFI_ArrowArray::new(&output_data);
    let output_schema = FFI_ArrowSchema::try_from(output_data.data_type())?;
    unsafe {
        std::ptr::write(output_array_address, output_array);
        std::ptr::write(output_schema_address, output_schema);
    }
    Ok(rows)
}
