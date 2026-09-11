// Copyright 2026 StreamFusion Authors. Licensed under the Apache License, Version 2.0.

use super::*;
use crate::exchange::{encode_binary_row_into, KeyField};
use arrow::array::{ArrayRef, BinaryBuilder};
use arrow::datatypes::{DataType, Field, Schema};

/// Encode the routing column once at the native edge. Routing borrows these same bytes and
/// a native receiving operator can use them without reconstructing a Java BinaryRow key.
pub(super) fn append(
    batch: RecordBatch,
    keys: &[(usize, KeyField)],
    broker: Arc<dyn MemoryReservationBroker>,
) -> Result<(RecordBatch, HostMemoryReservation)> {
    let bound = keys.iter().try_fold(
        batch.num_rows().saturating_mul(16),
        |bytes, &(column, _)| {
            bytes
                .checked_add(workspace::nested_key_bytes(
                    &batch.column(column).to_data(),
                )?)
                .ok_or_else(workspace::overflow)
        },
    )?;
    let mut memory = HostMemoryReservation::new(broker, "native exchange generated keys");
    memory.resize(
        bound
            .checked_mul(4)
            .and_then(|bytes| bytes.checked_add(4096))
            .ok_or_else(workspace::overflow)?,
    )?;
    let mut builder = BinaryBuilder::with_capacity(batch.num_rows(), 0);
    let mut scratch = Vec::new();
    for row in 0..batch.num_rows() {
        encode_binary_row_into(&batch, row, keys, &mut scratch)?;
        if builder
            .values_slice()
            .len()
            .checked_add(scratch.len())
            .is_none_or(|bytes| bytes > i32::MAX as usize)
        {
            return Err(DataFusionError::ResourcesExhausted(
                "native routing keys exceed Arrow Binary offset capacity".into(),
            ));
        }
        builder.append_value(&scratch);
    }
    let mut fields = batch.schema().fields().to_vec();
    fields.push(Arc::new(Field::new(
        "__streamfusion_key",
        DataType::Binary,
        false,
    )));
    let mut columns = batch.columns().to_vec();
    columns.push(Arc::new(builder.finish()) as ArrayRef);
    Ok((
        RecordBatch::try_new(Arc::new(Schema::new(fields)), columns)?,
        memory,
    ))
}
