// Copyright 2026 StreamFusion Authors
// Licensed under the Apache License, Version 2.0

//! Native buffer ownership, independent of the operator that produced or retains a batch.
//! This attaches an already admitted allowance; it is not allocation admission for kernels.

use std::panic::AssertUnwindSafe;
use std::ptr::NonNull;
use std::sync::{Arc, OnceLock};

use arrow::array::{make_array, ArrayData, RecordBatch};
use arrow::buffer::{BooleanBuffer, Buffer, NullBuffer};
use datafusion::error::{DataFusionError, Result};
use datafusion::execution::memory_pool::MemoryReservation;

use super::HostMemoryReservation;

mod registry;
pub(crate) use registry::Registry;
mod edge;
pub(crate) use edge::edge_batch;

/// One conservative batch allowance stays charged until its last retained buffer disappears.
/// Original allocations remain owned by BufferOwner; wrapping copies only Arrow descriptors.
enum Reservation {
    /// The C Data producer retains its own budget until its release callback runs.
    External,
    Host(HostMemoryReservation),
    DataFusion(MemoryReservation),
}

struct BufferOwner {
    // Unregister before freeing the address; remove metadata before returning its credit.
    _registration: OnceLock<registry::Registration>,
    // Drop the original allocation before returning its admission to Flink.
    _buffer: Buffer,
    _upstream: Option<Arc<BufferOwner>>,
    _reservation: Arc<AssertUnwindSafe<Reservation>>,
}

/// Register imported producer-owned buffers so forwarding them does not reserve their payload
/// again at the output edge. The original C Data release owner stays attached to every buffer.
pub(crate) fn borrowed_batch(
    batch: RecordBatch,
    registry: Option<Arc<Registry>>,
) -> Result<RecordBatch> {
    retain_batch(batch, Reservation::External, registry.as_ref())
}

pub(crate) fn host_batch(batch: RecordBatch, memory: HostMemoryReservation) -> Result<RecordBatch> {
    let admission = require_admitted(&batch, memory.size());
    if let Err(error) = admission {
        drop(batch);
        return Err(error);
    }
    let registry = memory.broker.lease_registry();
    retain_batch(batch, Reservation::Host(memory), registry.as_ref())
}

pub(crate) fn datafusion_batch(
    batch: RecordBatch,
    memory: MemoryReservation,
) -> Result<RecordBatch> {
    datafusion_batch_registered(batch, memory, None)
}

pub(crate) fn datafusion_batch_registered(
    batch: RecordBatch,
    memory: MemoryReservation,
    registry: Option<Arc<Registry>>,
) -> Result<RecordBatch> {
    let admission = require_admitted(&batch, memory.size());
    if let Err(error) = admission {
        drop(batch);
        return Err(error);
    }
    retain_batch(batch, Reservation::DataFusion(memory), registry.as_ref())
}

fn require_admitted(batch: &RecordBatch, size: usize) -> Result<()> {
    if size < super::buffer_size::batch_bytes(&batch)? {
        return Err(DataFusionError::Internal(
            "Arrow output lease is smaller than its buffers".into(),
        ));
    }
    Ok(())
}

/// Expression result ownership uses the same buffer leases as batches. No payload copy.
#[cfg(test)]
pub(crate) fn datafusion_array(
    array: arrow::array::ArrayRef,
    memory: MemoryReservation,
) -> Result<arrow::array::ArrayRef> {
    datafusion_array_registered(array, memory, None)
}

pub(crate) fn datafusion_array_registered(
    array: arrow::array::ArrayRef,
    memory: MemoryReservation,
    registry: Option<Arc<Registry>>,
) -> Result<arrow::array::ArrayRef> {
    let admission =
        super::buffer_size::arrays_bytes(std::slice::from_ref(&array)).and_then(|bytes| {
            if memory.size() < bytes {
                Err(DataFusionError::Internal(
                    "Arrow expression lease is smaller than its buffers".into(),
                ))
            } else {
                Ok(())
            }
        });
    if let Err(error) = admission {
        // A release callback may make credit available to another task immediately.
        // Free the producer's payload before returning its credit on all denial paths.
        drop(array);
        return Err(error);
    }
    let memory = Arc::new(AssertUnwindSafe(Reservation::DataFusion(memory)));
    Ok(make_array(retain_data(
        array.to_data(),
        &memory,
        registry.as_ref(),
    )?))
}

fn retain_batch(
    batch: RecordBatch,
    memory: Reservation,
    registry: Option<&Arc<Registry>>,
) -> Result<RecordBatch> {
    let required = super::buffer_size::batch_bytes(&batch)?;
    let admitted = match &memory {
        Reservation::External => required,
        Reservation::Host(memory) => memory.size(),
        Reservation::DataFusion(memory) => memory.size(),
    };
    if admitted < required {
        drop(batch);
        return Err(DataFusionError::Internal(
            "Arrow output lease is smaller than its buffers".into(),
        ));
    }
    let memory = Arc::new(AssertUnwindSafe(memory));
    let columns = batch
        .columns()
        .iter()
        .map(|array| retain_data(array.to_data(), &memory, registry).map(make_array))
        .collect::<Result<Vec<_>>>()?;
    Ok(RecordBatch::try_new_with_options(
        batch.schema(),
        columns,
        &arrow::array::RecordBatchOptions::new().with_row_count(Some(batch.num_rows())),
    )?)
}

fn retain_data(
    data: ArrayData,
    memory: &Arc<AssertUnwindSafe<Reservation>>,
    registry: Option<&Arc<Registry>>,
) -> Result<ArrayData> {
    let buffers = data
        .buffers()
        .iter()
        .map(|buffer| retain_buffer(buffer.clone(), memory, registry, None))
        .collect();
    let nulls = data.nulls().map(|nulls| {
        // Retain the bitmap's bit offset as well as the ArrayData's independent value offset.
        let bits = nulls.inner();
        let buffer = BooleanBuffer::new(
            retain_buffer(bits.inner().clone(), memory, registry, None),
            bits.offset(),
            bits.len(),
        );
        // SAFETY: contents, bit offset, and length are unchanged. Reuse the known null count
        // instead of rescanning the bitmap during handoff.
        unsafe { NullBuffer::new_unchecked(buffer, nulls.null_count()) }
    });
    let children = data
        .child_data()
        .iter()
        .cloned()
        .map(|child| retain_data(child, memory, registry))
        .collect::<Result<Vec<_>>>()?;
    // SAFETY: no type, value, length, offset, or validity changes. Only allocation owners
    // change. Full validation here would rescan strings/offsets during zero-copy handoff.
    Ok(unsafe {
        data.into_builder()
            .buffers(buffers)
            .nulls(nulls)
            .child_data(children)
            .build_unchecked()
    })
}

fn retain_buffer(
    buffer: Buffer,
    memory: &Arc<AssertUnwindSafe<Reservation>>,
    registry: Option<&Arc<Registry>>,
    upstream: Option<Arc<BufferOwner>>,
) -> Buffer {
    let pointer =
        NonNull::new(buffer.as_ptr() as *mut u8).expect("Arrow buffers have non-null pointers");
    let len = buffer.len();
    let owner = Arc::new(BufferOwner {
        _registration: OnceLock::new(),
        _buffer: buffer,
        _upstream: upstream,
        _reservation: memory.clone(),
    });
    if let Some(registry) = registry.filter(|_| len > 0) {
        owner
            ._registration
            .set(registry.register(&owner))
            .expect("fresh buffer registration");
    }
    // SAFETY: the owner retains the original immutable Buffer for exactly this pointer and
    // length. Slices, ArrayData clones, and Arrow C Data releases share that same allocation.
    unsafe { Buffer::from_custom_allocation(pointer, len, owner) }
}

#[cfg(test)]
mod registry_tests;
#[cfg(test)]
pub(super) mod tests;
#[cfg(test)]
mod transaction_tests;
