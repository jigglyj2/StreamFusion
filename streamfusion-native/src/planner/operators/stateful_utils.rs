// Copyright 2026 StreamFusion Authors
// Licensed under the Apache License, Version 2.0

use std::collections::BTreeSet;

use arrow::array::{Array, BinaryArray};
use arrow::datatypes::SchemaRef;
use arrow::record_batch::RecordBatch;
use datafusion::error::{DataFusionError, Result};

use crate::exchange::{encode_binary_row, KeyField};
use crate::memory_pool::HostMemoryReservation;
use crate::state::{
    KeyedState, NativeTimerService, StateKey, StateKeyRef, StateMutation, TimerDomain,
};

pub(super) fn timer_statistics(
    state_read_batches: u64,
    state_write_batches: u64,
    timer_registrations: u64,
    timer_deletions: u64,
    timers_fired: u64,
    timers: &NativeTimerService,
) -> [u64; 7] {
    [
        state_read_batches,
        state_write_batches,
        timer_registrations,
        timer_deletions,
        timers_fired,
        timers.timer_count(TimerDomain::EventTime) as u64,
        timers.timer_count(TimerDomain::ProcessingTime) as u64,
    ]
}

pub(super) fn restore_timer_state(
    state: &mut dyn KeyedState,
    timers: &mut NativeTimerService,
    key_group: u32,
    bytes: &[u8],
    timer_state_key: &[u8],
    state_read_batches: &mut u64,
) -> Result<()> {
    state.restore_key_group(key_group, bytes)?;
    let timer = state.get_batch(&[StateKeyRef {
        key_group,
        key: timer_state_key,
    }])?;
    *state_read_batches = state_read_batches.saturating_add(1);
    if let Some(bytes) = timer.into_iter().next().flatten() {
        timers.restore_key_group(key_group, bytes.as_ref())?;
    }
    Ok(())
}

pub(super) fn append_timer_mutations(
    timers: &NativeTimerService,
    mutations: &mut Vec<StateMutation>,
    key_groups: BTreeSet<u32>,
    timer_state_key: &[u8],
) -> Result<()> {
    for key_group in key_groups {
        mutations.push(StateMutation {
            key: StateKey {
                key_group,
                key: timer_state_key.to_vec(),
            },
            value: Some(timers.snapshot_key_group(key_group)?),
        });
    }
    Ok(())
}

pub(super) fn group_key(
    batch: &RecordBatch,
    row: usize,
    preencoded_key_index: Option<usize>,
    key_fields: &[(usize, KeyField)],
    operator_name: &str,
) -> Result<Vec<u8>> {
    match preencoded_key_index {
        Some(index) => Ok(batch
            .column(index)
            .as_any()
            .downcast_ref::<BinaryArray>()
            .ok_or_else(|| {
                DataFusionError::Execution(format!(
                    "{operator_name} preencoded key is not Arrow Binary"
                ))
            })?
            .value(row)
            .to_vec()),
        None if key_fields.is_empty() => Ok(Vec::new()),
        None => Ok(encode_binary_row(batch, row, key_fields)?),
    }
}

pub(super) fn metadata_index(schema: &SchemaRef, name: &str) -> Option<usize> {
    schema
        .fields()
        .iter()
        .position(|field| field.name() == name)
}

pub(super) fn finish_output(
    output: RecordBatch,
    base: usize,
    scratch_reservation: &mut HostMemoryReservation,
) -> Result<RecordBatch> {
    let output_bytes = output.get_array_memory_size();
    scratch_reservation.resize(output_bytes.max(base))?;
    scratch_reservation.transfer_to_arrow(output_bytes)?;
    scratch_reservation.resize(0)?;
    Ok(output)
}
