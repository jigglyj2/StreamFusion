// Copyright 2026 StreamFusion Authors
// Licensed under the Apache License, Version 2.0.

use super::*;

pub(super) unsafe extern "C" fn scan_key_group(
    handle: *mut c_void,
    input_array: *mut FFI_ArrowArray,
    input_schema: *mut FFI_ArrowSchema,
    output_array: *mut FFI_ArrowArray,
    output_schema: *mut FFI_ArrowSchema,
) -> i32 {
    unsafe {
        scan(
            handle,
            input_array,
            input_schema,
            output_array,
            output_schema,
            None,
        )
    }
}

pub(super) unsafe extern "C" fn scan_key_group_admitted(
    handle: *mut c_void,
    input_array: *mut FFI_ArrowArray,
    input_schema: *mut FFI_ArrowSchema,
    output_array: *mut FFI_ArrowArray,
    output_schema: *mut FFI_ArrowSchema,
    admission: *const StateMemoryAdmission,
) -> i32 {
    if admission.is_null() {
        return fail("RocksDB admitted scan requires host memory admission");
    }
    unsafe {
        scan(
            handle,
            input_array,
            input_schema,
            output_array,
            output_schema,
            Some(&*admission),
        )
    }
}

unsafe fn scan(
    handle: *mut c_void,
    input_array: *mut FFI_ArrowArray,
    input_schema: *mut FFI_ArrowSchema,
    output_array: *mut FFI_ArrowArray,
    output_schema: *mut FFI_ArrowSchema,
    admission: Option<&StateMemoryAdmission>,
) -> i32 {
    operation(|| {
        let input = unsafe { import_batch(input_array, input_schema) }?;
        if input.num_rows() != 1 {
            return Err("state scan requires one request".to_string());
        }
        let groups = column::<UInt32Array>(&input, 0, "key_group")?;
        let after = column::<BinaryArray>(&input, 1, "after")?;
        let rows = column::<UInt32Array>(&input, 2, "max_rows")?;
        let bytes = column::<UInt64Array>(&input, 3, "max_bytes")?;
        let start = column::<BinaryArray>(&input, 4, "start")?;
        let end = column::<BinaryArray>(&input, 5, "end")?;
        if groups.is_null(0) || rows.is_null(0) || bytes.is_null(0) || start.is_null(0) {
            return Err("state scan bounds must be non-null".to_string());
        }
        let state = backend(handle)?;
        let group = groups.value(0);
        let lower = start.value(0);
        let upper = (!end.is_null(0)).then(|| end.value(0));
        let after = (!after.is_null(0)).then(|| after.value(0));
        let max_rows = rows.value(0) as usize;
        let max_bytes = usize::try_from(bytes.value(0)).map_err(|error| error.to_string())?;
        let page = if let Some(admission) = admission {
            state.scan_range_page_admitted(
                group,
                lower,
                upper,
                after,
                max_rows,
                max_bytes,
                |bytes| {
                    if unsafe { (admission.try_grow)(admission.context, bytes) } == STATE_BACKEND_OK
                    {
                        Ok(())
                    } else {
                        Err(std::io::Error::new(
                            std::io::ErrorKind::OutOfMemory,
                            "Flink denied RocksDB scan page buffers",
                        ))
                    }
                },
            )
        } else {
            state.scan_range_page(group, lower, upper, after, max_rows, max_bytes)
        }
        .map_err(|error| error.to_string())?;
        let (keys, values): (Vec<_>, Vec<_>) = page
            .entries
            .into_iter()
            .map(|(key, value)| (Some(key), Some(value)))
            .unzip();
        let output = RecordBatch::try_new(
            Arc::new(
                Schema::new(vec![
                    Field::new("key", DataType::BinaryView, false),
                    Field::new("value", DataType::BinaryView, false),
                ])
                .with_metadata(std::collections::HashMap::from([(
                    streamfusion_state_abi::STATE_SCAN_COMPLETE_METADATA.to_string(),
                    page.complete.to_string(),
                )])),
            ),
            vec![
                Arc::new(streamfusion_state_abi::owned_binary_views(keys)?),
                Arc::new(streamfusion_state_abi::owned_binary_views(values)?),
            ],
        )
        .map_err(|error| error.to_string())?;
        unsafe { export_batch(output, output_array, output_schema) }
    })
}
