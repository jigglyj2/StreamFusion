// Copyright 2026 StreamFusion Authors
// Licensed under the Apache License, Version 2.0.

//! Shared record metadata selection for synchronous stateful operators. Historical
//! payload rows can differ from the triggering input whose record envelope is emitted.

use super::*;
use arrow::array::{Array, ArrayRef, Int32Array, RecordBatch};
use arrow::datatypes::{Field, FieldRef};

pub(crate) fn owned_timestamp_index(schema: &Schema) -> Result<Option<usize>> {
    if !schema
        .fields()
        .iter()
        .any(|field| field.name().starts_with("__streamfusion_owned_timestamp_"))
    {
        return Ok(None);
    }
    let envelope = Envelope::from_schema(schema)?;
    Ok(Some(envelope.payload_width))
}

/// Validate before state mutation, not only when an output happens to be selected.
pub(crate) fn validate_owned_input(batch: &RecordBatch) -> Result<()> {
    if owned_timestamp_index(&batch.schema())?.is_none() {
        return Ok(());
    }
    let ordinals = batch
        .column(batch.num_columns() - 1)
        .as_any()
        .downcast_ref::<Int32Array>()
        .expect("envelope schema validated");
    if ordinals.null_count() != 0 || ordinals.values().iter().any(|ordinal| *ordinal != -1) {
        return Err(DataFusionError::Execution(
            "owned native input requires ordinal -1".into(),
        ));
    }
    Ok(())
}

pub(crate) fn selected_output_fields(
    input: &Schema,
    payload_width: usize,
) -> Result<Vec<FieldRef>> {
    let mut fields = input.fields()[..payload_width].to_vec();
    if let Some(index) = owned_timestamp_index(input)? {
        fields.push(input.fields()[index].clone());
    }
    fields.push(Arc::new(Field::new(ROW_KIND, DataType::Int8, false)));
    fields.push(Arc::new(Field::new(INPUT_ROW, DataType::Int32, false)));
    Ok(fields)
}

/// The caller admits kernel workspace and retains the completed batch's output lease.
/// Payload computation is separate; this helper selects only the record metadata.
pub(crate) fn select_output(
    input: &RecordBatch,
    arrival_rows: Int32Array,
    kinds: ArrayRef,
    columns: &mut Vec<ArrayRef>,
) -> Result<()> {
    if let Some(index) = owned_timestamp_index(&input.schema())? {
        columns.push(arrow::compute::take(
            input.column(index).as_ref(),
            &arrival_rows,
            None,
        )?);
    }
    columns.push(kinds);
    columns.push(match input.schema().index_of(INPUT_ROW) {
        Ok(index) => arrow::compute::take(input.column(index).as_ref(), &arrival_rows, None)?,
        Err(_) => Arc::new(arrival_rows),
    });
    Ok(())
}
