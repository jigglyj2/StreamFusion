// Copyright 2026 StreamFusion Authors
// Licensed under the Apache License, Version 2.0

use std::cell::RefCell;
use std::ffi::{c_char, c_void, CString};
use std::path::Path;
use std::ptr;
use std::sync::Arc;

use arrow::array::{
    Array, BinaryArray, BinaryViewArray, RecordBatch, StructArray, UInt32Array, UInt64Array,
};
use arrow::datatypes::{DataType, Field, Schema};
use arrow::ffi::{from_ffi, FFI_ArrowArray, FFI_ArrowSchema};
use streamfusion_state_abi::{
    StateBackendApiV1, StateBackendOpenOptions, StateMemoryAdmission, STATE_BACKEND_ABI_VERSION,
    STATE_BACKEND_OK,
};

use crate::RocksStateBackend;

#[cfg(test)]
mod open_tests;

thread_local! {
    static LAST_ERROR: RefCell<CString> = RefCell::new(CString::new("no RocksDB state error").unwrap());
}

static API: StateBackendApiV1 = StateBackendApiV1 {
    abi_version: STATE_BACKEND_ABI_VERSION,
    open,
    close,
    get_batch,
    write_batch,
    snapshot_key_group,
    restore_key_group,
    checkpoint,
    scan_key_group,
    last_error,
};

#[unsafe(no_mangle)]
pub unsafe extern "C" fn streamfusion_state_backend_init(
    requested_version: u32,
    output: *mut *const StateBackendApiV1,
) -> i32 {
    if requested_version != STATE_BACKEND_ABI_VERSION || output.is_null() {
        return fail("unsupported or invalid StreamFusion state backend ABI request");
    }
    unsafe { ptr::write(output, &API) };
    STATE_BACKEND_OK
}

unsafe extern "C" fn open(
    options: *const StateBackendOpenOptions,
    output: *mut *mut c_void,
) -> i32 {
    if !output.is_null() {
        unsafe { ptr::write(output, ptr::null_mut()) };
    }
    operation(|| {
        if options.is_null() || output.is_null() {
            return Err("RocksDB open received a null address".to_string());
        }
        if unsafe { (*options).struct_size } != std::mem::size_of::<StateBackendOpenOptions>() {
            return Err("RocksDB open received an unsupported options size".to_string());
        }
        let options = unsafe { &*options };
        if options.path.is_null() || options.path_len == 0 {
            return Err("RocksDB open requires a non-empty database path".to_string());
        }
        let path = std::str::from_utf8(unsafe {
            std::slice::from_raw_parts(options.path, options.path_len)
        })
        .map_err(|error| error.to_string())?;
        let log_directory = if options.log_directory_len == 0 {
            None
        } else {
            if options.log_directory.is_null() {
                return Err("RocksDB log directory has a null address".to_string());
            }
            Some(Path::new(
                std::str::from_utf8(unsafe {
                    std::slice::from_raw_parts(options.log_directory, options.log_directory_len)
                })
                .map_err(|error| error.to_string())?,
            ))
        };
        let backend = RocksStateBackend::open_configured(
            Path::new(path),
            options.first_key_group,
            options.last_key_group,
            options.memory_limit,
            [options.memory_scope_high, options.memory_scope_low],
            log_directory,
        )
        .map_err(|error| error.to_string())?;
        unsafe { ptr::write(output, Box::into_raw(Box::new(backend)).cast()) };
        Ok(())
    })
}

unsafe extern "C" fn close(handle: *mut c_void) {
    if !handle.is_null() {
        unsafe { drop(Box::from_raw(handle.cast::<RocksStateBackend>())) };
    }
}

unsafe extern "C" fn scan_key_group(
    handle: *mut c_void,
    input_array: *mut FFI_ArrowArray,
    input_schema: *mut FFI_ArrowSchema,
    output_array: *mut FFI_ArrowArray,
    output_schema: *mut FFI_ArrowSchema,
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
        let page = backend(handle)?
            .scan_range_page(
                groups.value(0),
                start.value(0),
                (!end.is_null(0)).then(|| end.value(0)),
                (!after.is_null(0)).then(|| after.value(0)),
                rows.value(0) as usize,
                usize::try_from(bytes.value(0)).map_err(|error| error.to_string())?,
            )
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

unsafe extern "C" fn get_batch(
    handle: *mut c_void,
    input_array: *mut FFI_ArrowArray,
    input_schema: *mut FFI_ArrowSchema,
    output_array: *mut FFI_ArrowArray,
    output_schema: *mut FFI_ArrowSchema,
    admission: *const StateMemoryAdmission,
) -> i32 {
    operation(|| {
        if admission.is_null() {
            return Err("RocksDB batch read requires host memory admission".to_string());
        }
        let admission = unsafe { &*admission };
        let input = unsafe { import_batch(input_array, input_schema) }?;
        let key_groups = column::<UInt32Array>(&input, 0, "key_group")?;
        let keys = column::<BinaryArray>(&input, 1, "key")?;
        let values = backend(handle)?
            .get_batch_refs_admitted(
                (0..input.num_rows()).map(|row| (key_groups.value(row), keys.value(row))),
                |bytes| {
                    if unsafe { (admission.try_grow)(admission.context, bytes) } == STATE_BACKEND_OK
                    {
                        Ok(())
                    } else {
                        Err(std::io::Error::new(
                            std::io::ErrorKind::OutOfMemory,
                            "Flink denied RocksDB batch read buffers",
                        ))
                    }
                },
            )
            .map_err(|e| e.to_string())?;
        let values = streamfusion_state_abi::owned_binary_views(values)?;
        let output = RecordBatch::try_new(
            Arc::new(Schema::new(vec![Field::new(
                "value",
                DataType::BinaryView,
                true,
            )])),
            vec![Arc::new(values)],
        )
        .map_err(|error| error.to_string())?;
        unsafe { export_batch(output, output_array, output_schema) }
    })
}

unsafe extern "C" fn write_batch(
    handle: *mut c_void,
    input_array: *mut FFI_ArrowArray,
    input_schema: *mut FFI_ArrowSchema,
    output_array: *mut FFI_ArrowArray,
    output_schema: *mut FFI_ArrowSchema,
) -> i32 {
    operation(|| {
        let input = unsafe { import_batch(input_array, input_schema) }?;
        let key_groups = column::<UInt32Array>(&input, 0, "key_group")?;
        let keys = column::<BinaryViewArray>(&input, 1, "key")?;
        let values = column::<BinaryViewArray>(&input, 2, "value")?;
        backend(handle)?
            .write_batch_refs((0..input.num_rows()).map(|row| {
                (
                    key_groups.value(row),
                    keys.value(row),
                    (!values.is_null(row)).then(|| values.value(row)),
                )
            }))
            .map_err(|error| error.to_string())?;
        unsafe { export_empty(output_array, output_schema) }
    })
}

unsafe extern "C" fn snapshot_key_group(
    handle: *mut c_void,
    input_array: *mut FFI_ArrowArray,
    input_schema: *mut FFI_ArrowSchema,
    output_array: *mut FFI_ArrowArray,
    output_schema: *mut FFI_ArrowSchema,
    admission: *const StateMemoryAdmission,
) -> i32 {
    operation(|| {
        if admission.is_null() {
            return Err("RocksDB snapshot requires host memory admission".to_string());
        }
        let admission = unsafe { &*admission };
        let input = unsafe { import_batch(input_array, input_schema) }?;
        let key_group = one_key_group(&input)?;
        let state = backend(handle)?
            .snapshot_key_group_admitted(key_group, |bytes| {
                if unsafe { (admission.try_grow)(admission.context, bytes) } == STATE_BACKEND_OK {
                    Ok(())
                } else {
                    Err(std::io::Error::new(
                        std::io::ErrorKind::OutOfMemory,
                        "Flink denied canonical snapshot",
                    ))
                }
            })
            .map_err(|error| error.to_string())?;
        let output = RecordBatch::try_new(
            Arc::new(Schema::new(vec![Field::new(
                "state",
                DataType::BinaryView,
                false,
            )])),
            vec![Arc::new(streamfusion_state_abi::owned_binary_views([
                Some(state),
            ])?)],
        )
        .map_err(|error| error.to_string())?;
        unsafe { export_batch(output, output_array, output_schema) }
    })
}

unsafe extern "C" fn restore_key_group(
    handle: *mut c_void,
    input_array: *mut FFI_ArrowArray,
    input_schema: *mut FFI_ArrowSchema,
    output_array: *mut FFI_ArrowArray,
    output_schema: *mut FFI_ArrowSchema,
) -> i32 {
    operation(|| {
        let input = unsafe { import_batch(input_array, input_schema) }?;
        let key_group = one_key_group(&input)?;
        let state = column::<BinaryArray>(&input, 1, "state")?;
        backend(handle)?
            .restore_key_group(key_group, state.value(0))
            .map_err(|error| error.to_string())?;
        unsafe { export_empty(output_array, output_schema) }
    })
}

unsafe extern "C" fn checkpoint(
    handle: *mut c_void,
    input_array: *mut FFI_ArrowArray,
    input_schema: *mut FFI_ArrowSchema,
    output_array: *mut FFI_ArrowArray,
    output_schema: *mut FFI_ArrowSchema,
) -> i32 {
    operation(|| {
        let input = unsafe { import_batch(input_array, input_schema) }?;
        if input.num_rows() != 1 {
            return Err("RocksDB checkpoint requires exactly one destination path".to_string());
        }
        let paths = column::<BinaryArray>(&input, 0, "checkpoint path")?;
        let path = std::str::from_utf8(paths.value(0)).map_err(|error| error.to_string())?;
        backend(handle)?
            .checkpoint(Path::new(path))
            .map_err(|error| error.to_string())?;
        unsafe { export_empty(output_array, output_schema) }
    })
}

fn backend(handle: *mut c_void) -> Result<&'static RocksStateBackend, String> {
    if handle.is_null() {
        Err("RocksDB state handle is null".to_string())
    } else {
        unsafe { handle.cast::<RocksStateBackend>().as_ref() }
            .ok_or_else(|| "RocksDB state handle is invalid".to_string())
    }
}

fn one_key_group(batch: &RecordBatch) -> Result<u32, String> {
    if batch.num_rows() != 1 {
        return Err("state snapshot operation requires exactly one key group".to_string());
    }
    Ok(column::<UInt32Array>(batch, 0, "key_group")?.value(0))
}

fn column<'a, T: Array + 'static>(
    batch: &'a RecordBatch,
    index: usize,
    description: &str,
) -> Result<&'a T, String> {
    batch
        .columns()
        .get(index)
        .and_then(|array| array.as_any().downcast_ref())
        .ok_or_else(|| format!("RocksDB state {description} column has the wrong Arrow type"))
}

unsafe fn import_batch(
    array: *mut FFI_ArrowArray,
    schema: *mut FFI_ArrowSchema,
) -> Result<RecordBatch, String> {
    if array.is_null() || schema.is_null() {
        return Err("RocksDB state Arrow input is null".to_string());
    }
    let array = unsafe { FFI_ArrowArray::from_raw(array) };
    let schema = unsafe { FFI_ArrowSchema::from_raw(schema) };
    let data = unsafe { from_ffi(array, &schema) }.map_err(|error| error.to_string())?;
    Ok(RecordBatch::from(StructArray::from(data)))
}

unsafe fn export_empty(
    array: *mut FFI_ArrowArray,
    schema: *mut FFI_ArrowSchema,
) -> Result<(), String> {
    unsafe {
        export_batch(
            RecordBatch::new_empty(Arc::new(Schema::empty())),
            array,
            schema,
        )
    }
}

unsafe fn export_batch(
    batch: RecordBatch,
    array: *mut FFI_ArrowArray,
    schema: *mut FFI_ArrowSchema,
) -> Result<(), String> {
    if array.is_null() || schema.is_null() {
        return Err("RocksDB state Arrow output is null".to_string());
    }
    let exported_schema =
        FFI_ArrowSchema::try_from(batch.schema().as_ref()).map_err(|error| error.to_string())?;
    let data = StructArray::from(batch).to_data();
    unsafe {
        ptr::write(array, FFI_ArrowArray::new(&data));
        ptr::write(schema, exported_schema);
    }
    Ok(())
}

fn operation(action: impl FnOnce() -> Result<(), String>) -> i32 {
    match std::panic::catch_unwind(std::panic::AssertUnwindSafe(action)) {
        Ok(Ok(())) => STATE_BACKEND_OK,
        Ok(Err(error)) => fail(&error),
        Err(_) => fail("RocksDB state backend panicked"),
    }
}

fn fail(message: &str) -> i32 {
    LAST_ERROR.with(|error| {
        *error.borrow_mut() = CString::new(message.replace('\0', "�")).unwrap();
    });
    -1
}

unsafe extern "C" fn last_error() -> *const c_char {
    LAST_ERROR.with(|error| error.borrow().as_ptr())
}
