// Copyright 2026 StreamFusion Authors
// Licensed under the Apache License, Version 2.0.

//! Coarse batch/frame admission for Arrow take and uncompressed Arrow IPC. Input buffers
//! remain owned by the caller's Arrow lease; these allowances cover new allocations only.
use super::*;
use arrow::array::ArrayData;
use arrow::datatypes::DataType;

pub(super) fn overflow() -> ArrowError {
    ArrowError::MemoryError("native exchange workspace size overflow".into())
}

pub(super) fn allowance(
    batch: &RecordBatch,
    keys: &[(usize, crate::exchange::KeyField)],
    max_parallelism: u32,
    parallelism: u32,
    preserve_key_groups: bool,
    columns: usize,
) -> arrow::error::Result<(usize, usize)> {
    if parallelism == 0 || parallelism > max_parallelism || max_parallelism > 32768 {
        return Err(ArrowError::InvalidArgumentError(
            "invalid Flink exchange parallelism".into(),
        ));
    }
    if columns > batch.num_columns() {
        return Err(ArrowError::InvalidArgumentError(
            "exchange transport columns exceed input schema".into(),
        ));
    }
    let destinations = if preserve_key_groups {
        max_parallelism
    } else {
        parallelism
    } as usize;
    let nonempty = batch.num_rows().min(destinations);
    // Vec index growth (<2x rows, with a four-index minimum per occupied bucket),
    // bucket headers, and simultaneously live selection/frame descriptors.
    let routing = destinations
        .checked_mul(size_of::<Vec<u32>>())
        .and_then(|bytes| bytes.checked_add(batch.num_rows().checked_mul(8)?))
        .and_then(|bytes| {
            bytes.checked_add(nonempty.checked_mul(
                16 + 2 * size_of::<crate::exchange::RoutedBatch>() + 2 * size_of::<RoutedFrame>(),
            )?)
        })
        .and_then(|bytes| bytes.checked_add(4096))
        .ok_or_else(overflow)?;
    let key_bytes = keys.iter().try_fold(0usize, |bytes, &(column, kind)| {
        let array = batch.columns().get(column).ok_or_else(|| {
            ArrowError::InvalidArgumentError(
                "exchange key column is outside the input schema".into(),
            )
        })?;
        let extra = match kind {
            crate::exchange::KeyField::PreencodedBinaryRow => 0, // borrowed directly
            crate::exchange::KeyField::Nested => nested_key_bytes(&array.to_data())?,
            crate::exchange::KeyField::String | crate::exchange::KeyField::Binary => array
                .to_data()
                .buffers()
                .iter()
                .try_fold(32usize, |n, buffer| {
                    n.checked_add(buffer.len()).ok_or_else(overflow)
                })?,
            _ => 32,
        };
        bytes.checked_add(extra).ok_or_else(overflow)
    })?;
    let routing = routing
        .checked_add(key_bytes.checked_mul(2).ok_or_else(overflow)?)
        .ok_or_else(overflow)?;
    // A frame may contain every row. Count each transmitted buffer occurrence, not
    // allocation capacity: shared IPC backing allocations are not copied once per column,
    // but repeated logical columns really do have separate buffers on the wire.
    let output = batch.columns()[..columns]
        .iter()
        .try_fold(0usize, |bytes, array| {
            bytes
                .checked_add(array_workspace(&array.to_data(), false)?)
                .ok_or_else(overflow)
        })?;
    // Gather output, geometrically growing IPC body, and offset/validity rebasing scratch.
    // The allowance is reused per frame; already encoded frames are retained separately.
    let frame = output
        .checked_mul(4)
        .and_then(|bytes| bytes.checked_add(8192))
        .ok_or_else(overflow)?;
    Ok((routing, frame))
}

fn array_workspace(data: &ArrayData, child: bool) -> arrow::error::Result<usize> {
    // These encodings can expand repeated child positions during take, or need dictionary
    // channel state. They are not part of the Flink exchange transport schema contract.
    if matches!(
        data.data_type(),
        DataType::Dictionary(_, _)
            | DataType::RunEndEncoded(_, _)
            | DataType::Union(_, _)
            | DataType::ListView(_)
            | DataType::LargeListView(_)
    ) {
        return Err(ArrowError::NotYetImplemented(format!(
            "unsupported native exchange transport array {}",
            data.data_type()
        )));
    }
    // Arrow synthesizes validity even for all-valid arrays; each buffer has <=64 bytes
    // padding. Per-node headroom includes Arrow descriptors and FlatBuffer metadata.
    let mut bytes = data
        .len()
        .div_ceil(8)
        .checked_add(1024)
        .ok_or_else(overflow)?;
    for buffer in data.buffers() {
        bytes = bytes
            .checked_add(buffer.len())
            .and_then(|n| n.checked_add(64))
            .ok_or_else(overflow)?;
    }
    if child {
        // Nested take builds child selections, including children of boolean/empty structs.
        bytes = bytes
            .checked_add(data.len().checked_mul(8).ok_or_else(overflow)?)
            .ok_or_else(overflow)?;
    }
    for child in data.child_data() {
        bytes = bytes
            .checked_add(array_workspace(child, true)?)
            .ok_or_else(overflow)?;
    }
    Ok(bytes)
}

fn nested_key_bytes(data: &ArrayData) -> arrow::error::Result<usize> {
    // A single nested key may span the whole child array. Flink uses fixed slots, null
    // words, and eight-byte padding rather than Arrow's compact bool/string offsets.
    let mut bytes = data
        .len()
        .checked_mul(24)
        .and_then(|n| n.checked_add(32))
        .ok_or_else(overflow)?;
    for buffer in data.buffers() {
        bytes = bytes.checked_add(buffer.len()).ok_or_else(overflow)?;
    }
    for child in data.child_data() {
        bytes = bytes
            .checked_add(nested_key_bytes(child)?)
            .ok_or_else(overflow)?;
    }
    Ok(bytes)
}
