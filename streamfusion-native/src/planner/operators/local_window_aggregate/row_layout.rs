// Copyright 2026 StreamFusion Authors
// Licensed under the Apache License, Version 2.0.

use super::*;
use arrow::array::StringArray;

/// Flink BinaryRow byte geometry read directly from Arrow, without constructing RowData.
pub(super) struct RowLayout {
    pub(super) fixed: usize,
    strings: Vec<usize>,
}

impl RowLayout {
    pub(super) fn new(schema: &SchemaRef, columns: &[usize]) -> Result<Self> {
        let arity = columns.len();
        let fixed = arity
            .checked_add(71)
            .map(|n| n / 64)
            .and_then(|words| words.checked_add(arity))
            .and_then(|words| words.checked_mul(8))
            .ok_or_else(overflow)?;
        let strings = columns
            .iter()
            .copied()
            .filter(|&index| schema.field(index).data_type() == &DataType::Utf8)
            .collect();
        Ok(Self { fixed, strings })
    }

    pub(super) fn bytes(&self, batch: &RecordBatch, row: usize) -> Result<usize> {
        self.strings.iter().try_fold(self.fixed, |bytes, &index| {
            let column = batch
                .column(index)
                .as_any()
                .downcast_ref::<StringArray>()
                .ok_or_else(|| {
                    DataFusionError::Execution(
                        "local window string layout does not match Arrow input".into(),
                    )
                })?;
            let length = if column.is_null(row) {
                0
            } else {
                column.value(row).len()
            };
            // AbstractBinaryWriter embeds at most seven UTF-8 bytes in the fixed word.
            // Longer values occupy an eight-byte-aligned variable region, including padding.
            let extra = if length <= 7 {
                0
            } else {
                length.checked_add(7).ok_or_else(overflow)? / 8 * 8
            };
            bytes.checked_add(extra).ok_or_else(overflow)
        })
    }

    pub(super) fn encoding_payload(&self, batch: &RecordBatch) -> Result<usize> {
        self.strings.iter().try_fold(0usize, |bytes, &index| {
            let column = batch
                .column(index)
                .as_any()
                .downcast_ref::<StringArray>()
                .ok_or_else(|| {
                    DataFusionError::Execution(
                        "local window string layout does not match Arrow input".into(),
                    )
                })?;
            let offsets = column.value_offsets();
            bytes
                .checked_add((offsets[column.len()] - offsets[0]) as usize)
                .ok_or_else(overflow)
        })
    }
}

fn overflow() -> DataFusionError {
    DataFusionError::ResourcesExhausted("local window row geometry overflow".into())
}
