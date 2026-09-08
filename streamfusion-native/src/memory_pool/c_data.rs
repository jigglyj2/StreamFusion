// Copyright 2026 StreamFusion Authors
// Licensed under the Apache License, Version 2.0.

//! Arrow owns C Data descriptors. Only compatibility buffer copies need extra reservations.

use arrow::array::{Array, ArrayData, RecordBatch, StructArray};
use arrow::datatypes::Schema;
use arrow::ffi::{FFI_ArrowArray, FFI_ArrowSchema};
use datafusion::error::{DataFusionError, Result};
use datafusion::execution::memory_pool::MemoryReservation;
use std::ffi::c_void;
use std::sync::Arc;

pub(crate) fn array(batch: RecordBatch, memory: MemoryReservation) -> Result<FFI_ArrowArray> {
    let data = StructArray::from(batch).to_data();
    // Arrow may rebase a validity bitmap whose offset differs from its array offset.
    // Admit that actual transport compatibility copy before FFI_ArrowArray::new does it.
    memory.try_grow(alignment_bytes(&data)?)?;
    let mut output = FFI_ArrowArray::new(&data);
    if memory.size() != 0 {
        unsafe {
            retain_array(&mut output, &Arc::new(memory));
        }
    }
    Ok(output)
}

pub(crate) fn schema(schema: &Schema, _memory: MemoryReservation) -> Result<FFI_ArrowSchema> {
    Ok(FFI_ArrowSchema::try_from(schema)?)
}

fn alignment_bytes(data: &ArrayData) -> Result<usize> {
    let mut bytes = 0usize;
    if let Some(nulls) = data.nulls() {
        if data.offset() != nulls.offset() {
            bytes = data
                .offset()
                .checked_add(nulls.len())
                .and_then(|n| n.checked_add(7))
                .map(|n| n / 8)
                .and_then(|n| n.checked_add(64))
                .ok_or_else(overflow)?;
        }
    }
    for child in data.child_data() {
        bytes = bytes
            .checked_add(alignment_bytes(child)?)
            .ok_or_else(overflow)?;
    }
    Ok(bytes)
}

struct ArrayOwner {
    private: *mut c_void,
    release: unsafe extern "C" fn(*mut FFI_ArrowArray),
    _memory: Arc<MemoryReservation>,
}
// Only freshly exported, producer-owned trees reach these functions. Wrap every child
// (including dictionaries) so moving a child out before releasing its parent keeps credit.
unsafe fn retain_array(array: &mut FFI_ArrowArray, memory: &Arc<MemoryReservation>) {
    for index in 0..array.n_children as usize {
        unsafe {
            retain_array(&mut **array.children.add(index), memory);
        }
    }
    if !array.dictionary.is_null() {
        unsafe {
            retain_array(&mut *array.dictionary, memory);
        }
    }
    let owner = Box::new(ArrayOwner {
        private: array.private_data,
        release: array.release.expect("fresh Arrow export"),
        _memory: memory.clone(),
    });
    array.private_data = Box::into_raw(owner).cast();
    array.release = Some(release_array);
}
unsafe extern "C" fn release_array(array: *mut FFI_ArrowArray) {
    let Some(array) = (unsafe { array.as_mut() }) else {
        return;
    };
    if array.release.is_none() {
        return;
    }
    let owner = unsafe { Box::from_raw(array.private_data.cast::<ArrayOwner>()) };
    array.private_data = owner.private;
    array.release = Some(owner.release);
    unsafe {
        (owner.release)(array);
    }
    array.release = None;
    array.private_data = std::ptr::null_mut();
    // Original descriptors, child boxes and payload owners are gone before credit returns.
    drop(owner);
}
fn overflow() -> DataFusionError {
    DataFusionError::ResourcesExhausted("native C Data descriptor admission overflow".into())
}

#[cfg(test)]
mod tests;
