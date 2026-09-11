// Copyright 2026 StreamFusion Authors
// Licensed under the Apache License, Version 2.0.

//! Native RecordBatch routing independent of the JVM/C Data adapter.
use super::exchange_framer::{frame_hash_exchange_batch_accounted, FrameMemory};
use super::RoutedFrame;
use arrow::error::ArrowError;
mod generated_keys;
#[cfg(test)]
mod tests;
mod workspace;
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
    let generated = plan.transport_routing_key
        && plan
            .metadata_columns
            .as_ref()
            .is_some_and(|metadata| metadata.routing_key_index.is_none());
    let (batch, _key_memory) = if generated {
        if plan
            .schema
            .as_ref()
            .is_none_or(|schema| schema.fields.len() != batch.num_columns())
        {
            return Err(DataFusionError::Execution(
                "native routing-key generation requires exactly the declared input columns".into(),
            ));
        }
        let (batch, memory) = generated_keys::append(batch, keys, broker.clone())?;
        (batch, Some(memory))
    } else {
        (batch, None)
    };
    let generated_key = [(
        batch.num_columns() - 1,
        crate::exchange::KeyField::PreencodedBinaryRow,
    )];
    let keys = if generated { &generated_key[..] } else { keys };
    let mut transport_column_count = plan
        .schema
        .as_ref()
        .ok_or_else(|| DataFusionError::Plan("exchange schema is required".to_string()))?
        .fields
        .len();
    if plan.transport_routing_key {
        transport_column_count = transport_column_count.saturating_add(1);
    }
    let (routing_bytes, frame_workspace) = workspace::allowance(
        &batch,
        keys,
        plan.max_parallelism,
        plan.parallelism,
        plan.preserve_key_groups,
        transport_column_count,
    )?;
    let mut memory = RoutingMemory {
        reservation: HostMemoryReservation::new(broker, "native exchange buffers"),
        routing_bytes,
        frame_workspace,
        retained_bytes: 0,
    };
    memory.reservation.try_grow(routing_bytes)?;
    let frames = frame_hash_exchange_batch_accounted(
        batch,
        keys,
        plan.max_parallelism,
        plan.parallelism,
        plan.preserve_key_groups,
        transport_column_count,
        &mut memory,
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
    memory.reservation.resize(frame_bytes)?;

    Ok(AccountedFrames {
        frames,
        _reservation: memory.reservation,
    })
}

struct RoutingMemory {
    reservation: HostMemoryReservation,
    routing_bytes: usize,
    frame_workspace: usize,
    retained_bytes: usize,
}

impl FrameMemory for RoutingMemory {
    fn before_frame(&mut self) -> arrow::error::Result<()> {
        let bytes = self
            .routing_bytes
            .checked_add(self.retained_bytes)
            .and_then(|bytes| bytes.checked_add(self.frame_workspace))
            .ok_or_else(workspace::overflow)?;
        if bytes <= self.reservation.size() {
            return Ok(());
        }
        // Retained frames grow in batch-sized increments, so a batch with many key groups
        // does not issue one JNI reservation per tiny frame. Keep the credit until export.
        let additional = (bytes - self.reservation.size()).max(self.frame_workspace / 4);
        self.reservation
            .try_grow(additional)
            .map_err(|error| ArrowError::ExternalError(Box::new(error)))
    }

    fn retain_frame(&mut self, frame: &RoutedFrame) -> arrow::error::Result<()> {
        let bytes = frame
            .frame()
            .metadata
            .capacity()
            .checked_add(frame.frame().body.capacity())
            .ok_or_else(workspace::overflow)?;
        // This is a guard on the preflight bound, not admission after allocating the payload.
        if bytes > self.frame_workspace {
            return Err(ArrowError::ExternalError(Box::new(
                DataFusionError::Internal(
                    "exchange IPC frame exceeded its preflight workspace".into(),
                ),
            )));
        }
        self.retained_bytes = self
            .retained_bytes
            .checked_add(bytes)
            .ok_or_else(workspace::overflow)?;
        Ok(())
    }
}
