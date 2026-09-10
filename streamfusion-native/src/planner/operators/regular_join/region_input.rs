// Copyright 2026 StreamFusion Authors
// Licensed under the Apache License, Version 2.0

use super::*;
use arrow::array::BinaryBuilder;

/// Canonical port schemas are known before either input arrives. Only routing metadata is
/// added/reordered; all visible columns remain the producer's reference-counted Arrow arrays.
pub(super) fn schema(visible: &SchemaRef) -> SchemaRef {
    let mut fields = visible.fields().to_vec();
    fields.push(Arc::new(Field::new(
        "__streamfusion_input_row_kind",
        DataType::Int8,
        false,
    )));
    fields.push(Arc::new(Field::new(
        "__streamfusion_key",
        DataType::Binary,
        false,
    )));
    Arc::new(Schema::new(fields))
}

pub(super) fn normalize(
    batch: RecordBatch,
    schema: SchemaRef,
    keys: &[u32],
    memory: &mut HostMemoryReservation,
) -> Result<RecordBatch> {
    let visible = schema.fields().len() - 2;
    let actual = batch.schema();
    if batch.num_columns() < visible
        || actual.fields()[..visible]
            .iter()
            .zip(&schema.fields()[..visible])
            .any(|(a, b)| a.data_type() != b.data_type())
    {
        return Err(DataFusionError::Execution(
            "regular join region input does not match its planned schema".into(),
        ));
    }
    let kind = metadata_index(&actual, "__streamfusion_input_row_kind")
        .or_else(|| metadata_index(&actual, "__streamfusion_row_kind"))
        .filter(|index| *index >= visible)
        .ok_or_else(|| {
            DataFusionError::Execution("regular join region requires RowKind metadata".into())
        })?;
    if batch.column(kind).data_type() != &DataType::Int8 || batch.column(kind).null_count() != 0 {
        return Err(DataFusionError::Execution(
            "regular join RowKind metadata must be non-null Int8".into(),
        ));
    }
    let mut columns = batch.columns()[..visible].to_vec();
    columns.push(batch.column(kind).clone());
    let key: ArrayRef = if let Some(index) = metadata_index(&actual, "__streamfusion_key") {
        if index < visible
            || batch.column(index).data_type() != &DataType::Binary
            || batch.column(index).null_count() != 0
        {
            return Err(DataFusionError::Execution(
                "regular join key metadata must be non-null Binary".into(),
            ));
        }
        batch.column(index).clone()
    } else {
        let fields = keys
            .iter()
            .map(|&index| {
                let field = actual
                    .fields()
                    .get(index as usize)
                    .filter(|_| (index as usize) < visible)
                    .ok_or_else(|| {
                        DataFusionError::Plan("regular join key is outside the visible row".into())
                    })?;
                Ok((
                    index as usize,
                    KeyField::from_arrow_type(field.data_type())?,
                ))
            })
            .collect::<Result<Vec<_>>>()?;
        // Existing columns retain their producer's reservation. Only newly encoded keys
        // need workspace here; flat key slices can share a much larger IPC allocation.
        let key_columns = fields.iter().map(|(index, _)| *index).collect::<Vec<_>>();
        let key_batch = batch.project(&key_columns)?;
        memory.resize(input_memory::workspace(&key_batch, key_columns.len())?)?;
        let mut builder = BinaryBuilder::with_capacity(batch.num_rows(), 0);
        for row in 0..batch.num_rows() {
            if fields.is_empty() {
                builder.append_value([]);
            } else {
                builder.append_value(encode_binary_row(&batch, row, &fields)?);
            }
        }
        Arc::new(builder.finish())
    };
    columns.push(key);
    Ok(RecordBatch::try_new(schema, columns)?)
}
