// Copyright 2026 StreamFusion Authors
// Licensed under the Apache License, Version 2.0.

//! Arrow C Stream export with panic containment and producer-owned buffer release.

use arrow::datatypes::SchemaRef;
use arrow::ffi::{FFI_ArrowArray, FFI_ArrowSchema};
use arrow::ffi_stream::FFI_ArrowArrayStream;
use arrow::record_batch::RecordBatchReader;
use datafusion::error::Result;
use datafusion::execution::memory_pool::MemoryReservation;
use std::ffi::{c_char, c_int};
use std::fmt::Write;
use std::panic::{catch_unwind, AssertUnwindSafe};

pub(crate) fn export(
    reader: impl RecordBatchReader + Send + 'static,
    memory: MemoryReservation,
) -> Result<FFI_ArrowArrayStream> {
    let schema = reader.schema();
    let private = Box::new(State {
        reader: Box::new(reader),
        schema,
        error: ErrorText::default(),
        failed: false,
        finished: false,
        memory,
    });
    Ok(FFI_ArrowArrayStream {
        get_schema: Some(get_schema),
        get_next: Some(get_next),
        get_last_error: Some(get_last_error),
        release: Some(release),
        private_data: Box::into_raw(private).cast(),
    })
}

struct State {
    // Drop this borrowed schema reference before the reader/context that accounts it.
    schema: SchemaRef,
    reader: Box<dyn RecordBatchReader + Send>,
    error: ErrorText,
    failed: bool,
    finished: bool,
    // Reader, schema and bounded diagnostic storage die before this credit is returned.
    memory: MemoryReservation,
}
impl State {
    fn fail(&mut self, error: impl std::fmt::Display) -> c_int {
        self.error = ErrorText::default();
        let _ = write!(&mut self.error, "{error}");
        self.failed = true;
        // Never poll again. Keep the reader/context/schema admission alive until stream
        // release, when dropping an unfinished reader cancels its native invocation.
        1 // C Stream specifies zero for success, nonzero for failure; detail is get_last_error.
    }
}
unsafe extern "C" fn get_schema(
    stream: *mut FFI_ArrowArrayStream,
    out: *mut FFI_ArrowSchema,
) -> c_int {
    let state = unsafe { &mut *(*stream).private_data.cast::<State>() };
    if state.failed {
        return 1;
    }
    if out.is_null() {
        return state.fail("Arrow C Stream schema output is null");
    }
    match catch_unwind(AssertUnwindSafe(|| {
        super::c_data::schema(&state.schema, state.memory.new_empty())
    })) {
        Ok(Ok(schema)) => {
            unsafe {
                out.write(schema);
            }
            0
        }
        Ok(Err(error)) => state.fail(error),
        Err(_) => state.fail("panic while exporting native Arrow schema"),
    }
}
unsafe extern "C" fn get_next(
    stream: *mut FFI_ArrowArrayStream,
    out: *mut FFI_ArrowArray,
) -> c_int {
    let state = unsafe { &mut *(*stream).private_data.cast::<State>() };
    if state.failed {
        return 1;
    }
    if out.is_null() {
        return state.fail("Arrow C Stream array output is null");
    }
    let result = catch_unwind(AssertUnwindSafe(|| -> Result<FFI_ArrowArray> {
        if state.finished {
            return Ok(FFI_ArrowArray::empty());
        }
        match state.reader.next() {
            Some(Ok(batch)) => super::c_data::array(batch, state.memory.new_empty()),
            Some(Err(error)) => Err(error.into()),
            None => {
                state.finished = true;
                Ok(FFI_ArrowArray::empty())
            }
        }
    }));
    match result {
        Ok(Ok(array)) => {
            unsafe {
                out.write(array);
            }
            0
        }
        Ok(Err(error)) => state.fail(error),
        Err(_) => state.fail("panic while exporting native Arrow array"),
    }
}
unsafe extern "C" fn get_last_error(stream: *mut FFI_ArrowArrayStream) -> *const c_char {
    let state = unsafe { &*(*stream).private_data.cast::<State>() };
    if state.error.length == 0 {
        std::ptr::null()
    } else {
        state.error.bytes.as_ptr().cast()
    }
}
unsafe extern "C" fn release(stream: *mut FFI_ArrowArrayStream) {
    let Some(stream) = (unsafe { stream.as_mut() }) else {
        return;
    };
    if stream.release.is_none() {
        return;
    }
    let state = unsafe { Box::from_raw(stream.private_data.cast::<State>()) };
    stream.release = None;
    stream.private_data = std::ptr::null_mut();
    stream.get_next = None;
    stream.get_schema = None;
    stream.get_last_error = None;
    drop(state);
}

// A denied allocation must not allocate an unbounded diagnostic string outside its budget.
struct ErrorText {
    bytes: [u8; 2048],
    length: usize,
}
impl Default for ErrorText {
    fn default() -> Self {
        Self {
            bytes: [0; 2048],
            length: 0,
        }
    }
}
impl std::fmt::Write for ErrorText {
    fn write_str(&mut self, value: &str) -> std::fmt::Result {
        let mut count = value.len().min(self.bytes.len() - self.length - 1);
        while !value.is_char_boundary(count) {
            count -= 1;
        }
        for byte in value.bytes().take(count) {
            self.bytes[self.length] = if byte == 0 { b'?' } else { byte };
            self.length += 1;
        }
        Ok(())
    }
}

#[cfg(test)]
mod tests;
