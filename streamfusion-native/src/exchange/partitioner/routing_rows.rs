// Copyright 2026 StreamFusion Authors
// Licensed under the Apache License, Version 2.0.

use crate::exchange::{assign_key_group, encode_binary_row_into, KeyField};
use arrow::array::{Array, BinaryArray};
use arrow::error::{ArrowError, Result};
use arrow::record_batch::RecordBatch;

/// One routing loop for both stable key-group frames and aligned destination frames.
/// Callers validate the parallelism geometry before entering this function.
pub(super) fn routing_rows(
    batch: &RecordBatch,
    fields: &[(usize, KeyField)],
    max_parallelism: u32,
    destinations: u32,
) -> Result<Vec<Vec<u32>>> {
    if batch.num_rows().saturating_sub(1) > u32::MAX as usize {
        return Err(ArrowError::InvalidArgumentError(
            "exchange batch exceeds UInt32 indexing".into(),
        ));
    }
    for &(column, _) in fields {
        if column >= batch.num_columns() {
            return Err(ArrowError::InvalidArgumentError(
                "exchange key column is outside the input schema".into(),
            ));
        }
    }
    let preencoded = if fields
        .iter()
        .any(|(_, kind)| *kind == KeyField::PreencodedBinaryRow)
    {
        if fields.len() != 1 {
            return Err(ArrowError::InvalidArgumentError(
                "a preencoded BinaryRow key must be the only exchange key field".into(),
            ));
        }
        Some(
            batch
                .column(fields[0].0)
                .as_any()
                .downcast_ref::<BinaryArray>()
                .ok_or_else(|| {
                    ArrowError::CastError(
                        "a preencoded BinaryRow exchange key requires Arrow Binary".into(),
                    )
                })?,
        )
    } else {
        None
    };
    let mut rows = vec![Vec::new(); destinations as usize];
    let mut scratch = Vec::new();
    for row in 0..batch.num_rows() {
        let key = if let Some(keys) = preencoded {
            if keys.is_null(row) {
                return Err(ArrowError::InvalidArgumentError(
                    "a preencoded BinaryRow exchange key cannot be null".into(),
                ));
            }
            let key = keys.value(row);
            if !key.len().is_multiple_of(4) {
                return Err(ArrowError::InvalidArgumentError(
                    "a preencoded BinaryRow exchange key must be word aligned".into(),
                ));
            }
            // The sidecar already contains Flink's exact key bytes; hashing borrows its Arrow buffer.
            key
        } else {
            encode_binary_row_into(batch, row, fields, &mut scratch)?;
            scratch.as_slice()
        };
        let key_group = assign_key_group(key, max_parallelism);
        let destination = key_group * destinations / max_parallelism;
        rows[destination as usize].push(row as u32);
    }
    Ok(rows)
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::allocation_test_support::measure;
    use arrow::array::{ArrayRef, Int32Array, StringArray};
    use std::sync::Arc;

    #[test]
    fn repeated_keys_reuse_scratch_in_both_routing_modes() {
        let batch = RecordBatch::try_from_iter(vec![
            (
                "id",
                Arc::new(Int32Array::from(vec![7; 10_000])) as ArrayRef,
            ),
            (
                "key",
                Arc::new(StringArray::from(vec![
                    "repeated-variable-width-key";
                    10_000
                ])) as ArrayRef,
            ),
        ])
        .unwrap();
        let fields = [(0, KeyField::Integer), (1, KeyField::String)];
        let key = crate::exchange::encode_binary_row(&batch, 0, &fields).unwrap();
        let key_group = assign_key_group(&key, 128);
        for destinations in [4, 128] {
            let (rows, observed) =
                measure(|| routing_rows(&batch, &fields, 128, destinations).unwrap());
            assert_eq!(
                rows[(key_group * destinations / 128) as usize],
                (0..10_000).collect::<Vec<_>>()
            );
            assert!(
                observed.allocations < 64,
                "keys allocated per row: {observed:?}"
            );
        }
    }

    #[test]
    fn preencoded_keys_are_borrowed_and_malformed_keys_are_recoverable_errors() {
        let large = vec![7u8; 2 << 20];
        let batch = RecordBatch::try_from_iter(vec![(
            "key",
            Arc::new(BinaryArray::from(vec![large.as_slice(); 2])) as ArrayRef,
        )])
        .unwrap();
        let fields = [(0, KeyField::PreencodedBinaryRow)];
        let (rows, observed) = measure(|| routing_rows(&batch, &fields, 128, 4).unwrap());
        assert_eq!(rows.iter().map(Vec::len).sum::<usize>(), 2);
        assert!(
            observed.peak < 64 << 10,
            "preencoded payload was copied: {observed:?}"
        );
        for value in [None, Some(&[1u8, 2, 3][..])] {
            let batch = RecordBatch::try_from_iter(vec![(
                "key",
                Arc::new(BinaryArray::from(vec![value])) as ArrayRef,
            )])
            .unwrap();
            assert!(routing_rows(&batch, &fields, 128, 4).is_err());
        }
        assert!(routing_rows(&batch, &[(1, KeyField::PreencodedBinaryRow)], 128, 4).is_err());
        assert!(routing_rows(
            &batch,
            &[(0, KeyField::PreencodedBinaryRow), (0, KeyField::Binary)],
            128,
            4
        )
        .is_err());
    }
}
