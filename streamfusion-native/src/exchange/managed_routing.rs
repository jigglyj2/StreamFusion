// Copyright 2026 StreamFusion Authors
// Licensed under the Apache License, Version 2.0.

//! Native RecordBatch routing independent of the JVM/C Data adapter.
use super::frame_hash_exchange_batch_projected;
use crate::memory_pool::{HostMemoryReservation, MemoryReservationBroker};
use arrow::record_batch::RecordBatch;
use datafusion::error::{DataFusionError, Result};
use std::mem::size_of;
use std::sync::Arc;

pub(crate) struct AccountedFrames {
    pub(crate) frames: Vec<crate::exchange::RoutedFrame>,
    _reservation: HostMemoryReservation,
}

pub(crate) fn route_record_batch(
    plan: &crate::proto::NativeExchangePlan,
    keys: &[(usize, crate::exchange::KeyField)],
    batch: RecordBatch,
    broker: Arc<dyn MemoryReservationBroker>,
) -> Result<AccountedFrames> {
    // Reserve an input-sized working allowance before routing. Contiguous selections share
    // Arrow buffers; scattered selections gather only transmitted columns.
    let mut reservation = HostMemoryReservation::new(broker, "native exchange buffers");
    reservation.try_grow(batch.get_array_memory_size())?;
    let mut transport_column_count = plan
        .schema
        .as_ref()
        .ok_or_else(|| DataFusionError::Plan("exchange schema is required".to_string()))?
        .fields
        .len();
    if plan.transport_routing_key {
        transport_column_count = transport_column_count.saturating_add(1);
    }
    let frames = frame_hash_exchange_batch_projected(
        batch,
        keys,
        plan.max_parallelism,
        plan.parallelism,
        plan.preserve_key_groups,
        transport_column_count,
    )?;
    let frame_bytes = frames.iter().try_fold(
        frames
            .capacity()
            .saturating_mul(size_of::<crate::exchange::RoutedFrame>()),
        |bytes, routed| {
            bytes
                .checked_add(routed.frame().metadata.capacity())
                .and_then(|bytes| bytes.checked_add(routed.frame().body.capacity()))
                .ok_or_else(|| {
                    DataFusionError::ResourcesExhausted(
                        "native exchange frame accounting overflowed usize".to_string(),
                    )
                })
        },
    )?;
    reservation.resize(frame_bytes)?;

    Ok(AccountedFrames {
        frames,
        _reservation: reservation,
    })
}
