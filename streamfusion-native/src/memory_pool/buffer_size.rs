// Copyright 2026 StreamFusion Authors
// Licensed under the Apache License, Version 2.0.

//! Allocation-sized accounting for Arrow buffers. Array memory estimates count a shared
//! IPC allocation once per column; reservations must count its backing storage once.

use arrow::array::{ArrayData, ArrayRef, RecordBatch};
use arrow::buffer::Buffer;
use datafusion::error::{DataFusionError, Result};
use std::collections::BTreeMap;

pub(crate) fn batch_bytes(batch: &RecordBatch) -> Result<usize> {
    arrays_bytes(batch.columns())
}

pub(crate) fn arrays_bytes(arrays: &[ArrayRef]) -> Result<usize> {
    fn buffer(buffer: &Buffer, allocations: &mut BTreeMap<usize, usize>) -> Result<()> {
        let bytes = buffer.capacity().max(
            buffer
                .ptr_offset()
                .checked_add(buffer.len())
                .ok_or_else(overflow)?,
        );
        let size = allocations
            .entry(buffer.data_ptr().as_ptr() as usize)
            .or_default();
        *size = (*size).max(bytes);
        Ok(())
    }
    fn visit(data: &ArrayData, allocations: &mut BTreeMap<usize, usize>) -> Result<()> {
        for value in data.buffers() {
            buffer(value, allocations)?;
        }
        if let Some(nulls) = data.nulls() {
            buffer(nulls.inner().inner(), allocations)?;
        }
        for child in data.child_data() {
            visit(child, allocations)?;
        }
        Ok(())
    }
    let mut allocations = BTreeMap::new();
    for array in arrays {
        visit(&array.to_data(), &mut allocations)?;
    }
    allocations.values().try_fold(0usize, |total, bytes| {
        total.checked_add(*bytes).ok_or_else(overflow)
    })
}

fn overflow() -> DataFusionError {
    DataFusionError::ResourcesExhausted("Arrow buffer size overflow".into())
}
