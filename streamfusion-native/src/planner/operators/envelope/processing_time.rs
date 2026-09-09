// Copyright 2026 StreamFusion Authors
// Licensed under the Apache License, Version 2.0.

//! Per-record clock metadata exists only between the receiving edge and its bound consumer.
use super::*;
use arrow::array::{Array, Int64Array, RecordBatch};
use arrow::datatypes::Field;

pub(crate) const FIELD: &str = "__streamfusion_processing_time_v1";

pub(crate) fn column(batch: &RecordBatch) -> Result<Option<&Int64Array>> {
    if !batch
        .schema()
        .fields()
        .iter()
        .any(|field| field.name().starts_with("__streamfusion_processing_time_"))
    {
        return Ok(None);
    }
    let envelope = Envelope::from_schema(batch.schema().as_ref())?;
    let clock = batch
        .column(envelope.payload_width)
        .as_any()
        .downcast_ref::<Int64Array>()
        .ok_or_else(|| DataFusionError::Plan("processing-time input must be Int64".into()))?;
    if clock.null_count() != 0 {
        return Err(DataFusionError::Plan(
            "processing-time input must not contain nulls".into(),
        ));
    }
    Ok(Some(clock))
}

/// Called after normal C Data/IPC import. Only array references move; all producer-owned
/// payload and clock buffers retain their existing leases and release callbacks.
pub(crate) fn attach(batch: RecordBatch, clock: RecordBatch) -> Result<RecordBatch> {
    let schema = batch.schema();
    let envelope = Envelope::from_schema(schema.as_ref())?;
    if column(&batch)?.is_some() || owned_timestamp_index(schema.as_ref())?.is_none() {
        return Err(DataFusionError::Plan(
            "clock input requires an unclocked owned envelope".into(),
        ));
    }
    let expected = Field::new(FIELD, DataType::Int64, false);
    if clock.num_columns() != 1
        || clock.schema().field(0) != &expected
        || clock.num_rows() != batch.num_rows()
        || clock.column(0).null_count() != 0
    {
        return Err(DataFusionError::Plan(
            "clock vector schema or row count differs from its input batch".into(),
        ));
    }
    let mut fields = schema.fields().to_vec();
    let mut columns = batch.columns().to_vec();
    fields.insert(envelope.payload_width, Arc::new(expected));
    columns.insert(envelope.payload_width, clock.column(0).clone());
    Ok(RecordBatch::try_new(
        Arc::new(Schema::new_with_metadata(fields, schema.metadata().clone())),
        columns,
    )?)
}
