// Copyright 2026 StreamFusion Authors
// Licensed under the Apache License, Version 2.0.

use std::sync::Arc;

use arrow::array::UInt32Array;
use arrow::error::{ArrowError, Result};
use arrow::record_batch::RecordBatch;

use super::{assign_key_group, encode_binary_row, KeyField};

/// One destination's lightweight selection over a shared Arrow batch.
#[derive(Debug, Clone)]
pub struct RoutedBatch {
    destination: u32,
    batch: Arc<RecordBatch>,
    rows: UInt32Array,
}

impl RoutedBatch {
    pub fn destination(&self) -> u32 {
        self.destination
    }

    pub fn batch(&self) -> &Arc<RecordBatch> {
        &self.batch
    }

    pub fn rows(&self) -> &UInt32Array {
        &self.rows
    }
}

/// Routes rows by Flink key group without serializing or eagerly gathering Arrow columns.
pub fn route_batch(
    batch: RecordBatch,
    key_fields: &[(usize, KeyField)],
    max_parallelism: u32,
    parallelism: u32,
) -> Result<Vec<RoutedBatch>> {
    if parallelism == 0 || parallelism > max_parallelism || max_parallelism > 32_768 {
        return Err(ArrowError::InvalidArgumentError(format!(
            "Flink exchange requires 0 < parallelism ({parallelism}) <= max parallelism ({max_parallelism}) <= 32768"
        )));
    }
    let mut destination_rows = vec![Vec::new(); parallelism as usize];
    for row in 0..batch.num_rows() {
        let key = encode_binary_row(&batch, row, key_fields)?;
        let key_group = assign_key_group(&key, max_parallelism);
        let destination = key_group * parallelism / max_parallelism;
        destination_rows[destination as usize].push(u32::try_from(row).map_err(|_| {
            ArrowError::InvalidArgumentError("exchange batch exceeds UInt32 indexing".to_string())
        })?);
    }
    let batch = Arc::new(batch);
    Ok(destination_rows
        .into_iter()
        .enumerate()
        .filter(|(_, rows)| !rows.is_empty())
        .map(|(destination, rows)| RoutedBatch {
            destination: destination as u32,
            batch: Arc::clone(&batch),
            rows: UInt32Array::from(rows),
        })
        .collect())
}

#[cfg(test)]
mod tests {
    use arrow::array::{ArrayRef, Int32Array};
    use arrow::datatypes::{DataType, Field, Schema};

    use super::*;

    #[test]
    fn routes_by_flink_key_group_and_shares_the_arrow_batch() {
        let schema = Arc::new(Schema::new(vec![Field::new("key", DataType::Int32, false)]));
        let values = Arc::new(Int32Array::from(vec![-1, 0, 1, 42]));
        let buffer = values.values().as_ptr();
        let batch = RecordBatch::try_new(schema, vec![values as ArrayRef]).unwrap();

        let routed = route_batch(batch, &[(0, KeyField::Integer)], 128, 4).unwrap();

        assert_eq!(
            routed
                .iter()
                .map(|partition| (partition.destination(), partition.rows().values().to_vec()))
                .collect::<Vec<_>>(),
            vec![(2, vec![2, 3]), (3, vec![0, 1])]
        );
        assert!(routed
            .windows(2)
            .all(|pair| Arc::ptr_eq(pair[0].batch(), pair[1].batch())));
        assert_eq!(
            routed[0]
                .batch()
                .column(0)
                .as_any()
                .downcast_ref::<Int32Array>()
                .unwrap()
                .values()
                .as_ptr(),
            buffer
        );
    }
}
