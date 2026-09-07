// Copyright 2026 StreamFusion Authors
// Licensed under the Apache License, Version 2.0

//! Conservative Arrow take workspace sizing. Selection skew, nested offsets, retained view
//! buffers, and Arrow's initial list capacity matter; average input row width alone is unsafe.

use arrow::array::{Array, ArrayRef, AsArray};
use arrow::datatypes::DataType;
use datafusion::error::{DataFusionError, Result};

fn overflow() -> DataFusionError {
    DataFusionError::ResourcesExhausted("Arrow selection workspace size overflowed".into())
}

pub(crate) fn add(a: usize, b: usize) -> Result<usize> {
    a.checked_add(b).ok_or_else(overflow)
}

pub(crate) fn multiply(a: usize, b: usize) -> Result<usize> {
    a.checked_mul(b).ok_or_else(overflow)
}

/// Includes capacity growth, temporary gather indices, and bitmap/alignment overhead. Call
/// once per distinct selected input row, then multiply by its repetition count.
pub(crate) fn row_allowance(sources: &[ArrayRef], row: usize) -> Result<usize> {
    multiply(
        sources.iter().try_fold(0, |bytes, source| {
            add(bytes, row_bytes(source.as_ref(), row)?)
        })?,
        4,
    )
}

pub(crate) fn fixed_allowance(sources: &[ArrayRef]) -> Result<usize> {
    sources.iter().try_fold(4096, |bytes, source| {
        add(bytes, retained_bytes(source.as_ref())?)
    })
}

fn row_bytes(array: &dyn Array, row: usize) -> Result<usize> {
    if row >= array.len() {
        return Err(DataFusionError::Execution(
            "Arrow selection row is out of bounds".into(),
        ));
    }
    if let Some(width) = array.data_type().primitive_width() {
        return add(width, 16);
    }
    let payload = match array.data_type() {
        DataType::Null | DataType::Boolean => 1,
        DataType::Utf8 => add(4, array.as_string::<i32>().value_length(row) as usize)?,
        DataType::LargeUtf8 => add(8, array.as_string::<i64>().value_length(row) as usize)?,
        DataType::Binary => add(4, array.as_binary::<i32>().value_length(row) as usize)?,
        DataType::LargeBinary => add(8, array.as_binary::<i64>().value_length(row) as usize)?,
        DataType::BinaryView | DataType::Utf8View => 16,
        DataType::ListView(_) => 8,
        DataType::LargeListView(_) => 16,
        DataType::Dictionary(key, _) => key.primitive_width().unwrap_or(8),
        DataType::FixedSizeBinary(width) => usize::try_from(*width).map_err(|_| overflow())?,
        DataType::Struct(_) => array
            .as_struct()
            .columns()
            .iter()
            .try_fold(0, |bytes, child| {
                add(bytes, row_bytes(child.as_ref(), row)?)
            })?,
        DataType::List(_) => {
            let list = array.as_list::<i32>();
            list_bytes(
                array,
                list.values().as_ref(),
                list.value_offsets()[row] as usize,
                list.value_offsets()[row + 1] as usize,
                4,
            )?
        }
        DataType::LargeList(_) => {
            let list = array.as_list::<i64>();
            list_bytes(
                array,
                list.values().as_ref(),
                list.value_offsets()[row] as usize,
                list.value_offsets()[row + 1] as usize,
                8,
            )?
        }
        DataType::Map(_, _) => {
            let map = array.as_map();
            list_bytes(
                array,
                map.entries(),
                map.value_offsets()[row] as usize,
                map.value_offsets()[row + 1] as usize,
                4,
            )?
        }
        DataType::FixedSizeList(_, width) => {
            let list = array.as_fixed_size_list();
            let start = list.value_offset(row) as usize;
            range_bytes(list.values().as_ref(), start, add(start, *width as usize)?)?
        }
        // These physical encodings are not Flink SQL boundary types. Keep a conservative
        // fallback instead of silently using a scalar-width estimate for their child data.
        _ => add(array.get_array_memory_size(), multiply(array.len(), 16)?)?,
    };
    add(payload, 16)
}

fn range_bytes(array: &dyn Array, start: usize, end: usize) -> Result<usize> {
    if start > end || end > array.len() {
        return Err(DataFusionError::Execution(
            "Arrow selection child range is out of bounds".into(),
        ));
    }
    if let Some(width) = array.data_type().primitive_width() {
        return multiply(end - start, add(width, 16)?);
    }
    let length = end - start;
    let (width, payload) = match array.data_type() {
        DataType::Null | DataType::Boolean => (1, 0),
        DataType::Utf8 => (
            4,
            (array.as_string::<i32>().value_offsets()[end]
                - array.as_string::<i32>().value_offsets()[start]) as usize,
        ),
        DataType::LargeUtf8 => (
            8,
            (array.as_string::<i64>().value_offsets()[end]
                - array.as_string::<i64>().value_offsets()[start]) as usize,
        ),
        DataType::Binary => (
            4,
            (array.as_binary::<i32>().value_offsets()[end]
                - array.as_binary::<i32>().value_offsets()[start]) as usize,
        ),
        DataType::LargeBinary => (
            8,
            (array.as_binary::<i64>().value_offsets()[end]
                - array.as_binary::<i64>().value_offsets()[start]) as usize,
        ),
        DataType::Utf8View | DataType::BinaryView | DataType::LargeListView(_) => (16, 0),
        DataType::ListView(_) => (8, 0),
        DataType::Dictionary(key, _) => (key.primitive_width().unwrap_or(8), 0),
        _ => return (start..end).try_fold(0, |bytes, row| add(bytes, row_bytes(array, row)?)),
    };
    add(multiply(length, add(width, 16)?)?, payload)
}

fn list_bytes(
    parent: &dyn Array,
    child: &dyn Array,
    start: usize,
    end: usize,
    offset: usize,
) -> Result<usize> {
    // take_list initializes MutableArrayData from child.len / parent.len even when the
    // selected list is short or null. Include that capacity as well as the selected payload.
    let initial = parent.get_array_memory_size().div_ceil(parent.len().max(1));
    add(offset, range_bytes(child, start, end)?.max(initial))
}

fn retained_bytes(array: &dyn Array) -> Result<usize> {
    let children = match array.data_type() {
        DataType::Utf8View
        | DataType::BinaryView
        | DataType::Dictionary(_, _)
        | DataType::ListView(_)
        | DataType::LargeListView(_) => array.get_array_memory_size(),
        DataType::Struct(_) => array
            .as_struct()
            .columns()
            .iter()
            .try_fold(0, |bytes, child| {
                add(bytes, retained_bytes(child.as_ref())?)
            })?,
        DataType::List(_) => retained_bytes(array.as_list::<i32>().values().as_ref())?,
        DataType::LargeList(_) => retained_bytes(array.as_list::<i64>().values().as_ref())?,
        DataType::FixedSizeList(_, _) => {
            retained_bytes(array.as_fixed_size_list().values().as_ref())?
        }
        DataType::Map(_, _) => retained_bytes(array.as_map().entries())?,
        _ => 0,
    };
    // Per-node arrays, builder descriptors, null/offset buffers, and alignment padding.
    add(children, 2048)
}

#[cfg(test)]
mod tests;
