// Copyright 2026 StreamFusion Authors
// Licensed under the Apache License, Version 2.0.

use std::sync::Arc;

use arrow::array::{Array, ArrayRef, Int64Array, Int8Array};
use arrow::datatypes::{DataType, Field, Schema};
use arrow::error::{ArrowError, Result};
use arrow::record_batch::RecordBatch;

pub const ROW_KIND_COLUMN: &str = "__streamfusion_row_kind";
pub const STREAM_RECORD_TIMESTAMP_COLUMN: &str = "__streamfusion_stream_record_timestamp";

/// Adds Flink's per-record envelope to an Arrow batch without copying its data columns.
pub fn attach_record_metadata(
    batch: &RecordBatch,
    row_kinds: Int8Array,
    timestamps: Option<Int64Array>,
) -> Result<RecordBatch> {
    if row_kinds.len() != batch.num_rows() || row_kinds.null_count() != 0 {
        return Err(ArrowError::InvalidArgumentError(
            "exchange row kinds must contain one non-null value per row".to_string(),
        ));
    }
    if row_kinds
        .iter()
        .flatten()
        .any(|kind| !(0..=3).contains(&kind))
    {
        return Err(ArrowError::InvalidArgumentError(
            "exchange row kind is outside Flink's 0..=3 encoding".to_string(),
        ));
    }
    if timestamps
        .as_ref()
        .is_some_and(|values| values.len() != batch.num_rows())
    {
        return Err(ArrowError::InvalidArgumentError(
            "exchange timestamps must contain one value or null per row".to_string(),
        ));
    }

    let mut fields = batch.schema().fields().iter().cloned().collect::<Vec<_>>();
    let mut columns = batch.columns().to_vec();
    fields.push(Arc::new(Field::new(ROW_KIND_COLUMN, DataType::Int8, false)));
    columns.push(Arc::new(row_kinds) as ArrayRef);
    if let Some(timestamps) = timestamps {
        fields.push(Arc::new(Field::new(
            STREAM_RECORD_TIMESTAMP_COLUMN,
            DataType::Int64,
            true,
        )));
        columns.push(Arc::new(timestamps) as ArrayRef);
    }
    RecordBatch::try_new(Arc::new(Schema::new(fields)), columns)
}

#[cfg(test)]
mod tests {
    use arrow::array::{Int32Array, StringArray};

    use super::*;

    fn input() -> RecordBatch {
        RecordBatch::try_from_iter(vec![
            ("key", Arc::new(Int32Array::from(vec![1, 2])) as ArrayRef),
            (
                "value",
                Arc::new(StringArray::from(vec!["one", "two"])) as ArrayRef,
            ),
        ])
        .unwrap()
    }

    #[test]
    fn appends_flink_envelope_without_copying_data_columns() {
        let input = input();
        let key_buffer = input.column(0).to_data().buffers()[0].as_ptr();
        let output = attach_record_metadata(
            &input,
            Int8Array::from(vec![0, 3]),
            Some(Int64Array::from(vec![Some(100), None])),
        )
        .unwrap();

        assert_eq!(output.num_columns(), 4);
        assert_eq!(output.schema().field(2).name(), ROW_KIND_COLUMN);
        assert_eq!(
            output.schema().field(3).name(),
            STREAM_RECORD_TIMESTAMP_COLUMN
        );
        assert_eq!(output.column(0).to_data().buffers()[0].as_ptr(), key_buffer);
        assert_eq!(
            output
                .column(2)
                .as_any()
                .downcast_ref::<Int8Array>()
                .unwrap(),
            &Int8Array::from(vec![0, 3])
        );
        assert_eq!(
            output
                .column(3)
                .as_any()
                .downcast_ref::<Int64Array>()
                .unwrap(),
            &Int64Array::from(vec![Some(100), None])
        );
    }

    #[test]
    fn rejects_unknown_flink_row_kind() {
        let error =
            attach_record_metadata(&input(), Int8Array::from(vec![0, 4]), None).unwrap_err();
        assert!(error.to_string().contains("0..=3"));
    }

    #[test]
    fn rejects_metadata_length_mismatch() {
        let error = attach_record_metadata(&input(), Int8Array::from(vec![0]), None).unwrap_err();
        assert!(error.to_string().contains("one non-null value per row"));
    }
}
