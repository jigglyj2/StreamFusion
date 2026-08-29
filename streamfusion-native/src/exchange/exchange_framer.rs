// Copyright 2026 StreamFusion Authors
// Licensed under the Apache License, Version 2.0.

use arrow::error::Result;
use arrow::record_batch::RecordBatch;

use super::{route_batch, IpcBatchFrame, KeyField};

/// A schema-free Arrow frame tagged for one downstream Flink subtask.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct RoutedFrame {
    destination: u32,
    frame: IpcBatchFrame,
}

impl RoutedFrame {
    pub fn destination(&self) -> u32 {
        self.destination
    }

    pub fn frame(&self) -> &IpcBatchFrame {
        &self.frame
    }
}

/// Computes Flink destinations and encodes independently decodable network records.
pub fn frame_hash_exchange_batch(
    batch: RecordBatch,
    key_fields: &[(usize, KeyField)],
    max_parallelism: u32,
    parallelism: u32,
) -> Result<Vec<RoutedFrame>> {
    route_batch(batch, key_fields, max_parallelism, parallelism)?
        .into_iter()
        .map(|routed| {
            Ok(RoutedFrame {
                destination: routed.destination(),
                frame: IpcBatchFrame::encode(&routed.materialize()?)?,
            })
        })
        .collect()
}

#[cfg(test)]
mod tests {
    use std::sync::Arc;

    use arrow::array::{ArrayRef, Int32Array, Int8Array};
    use arrow::datatypes::{DataType, Field, Schema};

    use super::*;

    #[test]
    fn creates_independent_flink_network_records() {
        let schema = Arc::new(Schema::new(vec![
            Field::new("key", DataType::Int32, false),
            Field::new("__streamfusion_row_kind", DataType::Int8, false),
        ]));
        let batch = RecordBatch::try_new(
            Arc::clone(&schema),
            vec![
                Arc::new(Int32Array::from(vec![-1, 0, 1, 42])) as ArrayRef,
                Arc::new(Int8Array::from(vec![1, 2, 0, 3])) as ArrayRef,
            ],
        )
        .unwrap();

        let frames = frame_hash_exchange_batch(batch, &[(0, KeyField::Integer)], 128, 4).unwrap();

        assert_eq!(
            frames
                .iter()
                .map(RoutedFrame::destination)
                .collect::<Vec<_>>(),
            vec![2, 3]
        );
        assert_eq!(
            frames[0].frame().decode(Arc::clone(&schema)).unwrap(),
            RecordBatch::try_new(
                Arc::clone(&schema),
                vec![
                    Arc::new(Int32Array::from(vec![1, 42])) as ArrayRef,
                    Arc::new(Int8Array::from(vec![0, 3])) as ArrayRef,
                ],
            )
            .unwrap()
        );
        assert_eq!(
            frames[1].frame().decode(schema.clone()).unwrap(),
            RecordBatch::try_new(
                schema,
                vec![
                    Arc::new(Int32Array::from(vec![-1, 0])) as ArrayRef,
                    Arc::new(Int8Array::from(vec![1, 2])) as ArrayRef,
                ],
            )
            .unwrap()
        );
    }
}
