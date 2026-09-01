// Copyright 2026 StreamFusion Authors
// Licensed under the Apache License, Version 2.0.

use arrow::datatypes::Schema;
use arrow::error::Result;
use arrow::record_batch::RecordBatch;
use std::sync::Arc;

use super::{route_batch, route_batch_by_key_group, IpcBatchFrame, KeyField};

/// A schema-free Arrow frame tagged with a Flink key group that selects its destination.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct RoutedFrame {
    key_group: u32,
    frame: IpcBatchFrame,
}

impl RoutedFrame {
    pub fn key_group(&self) -> u32 {
        self.key_group
    }

    pub fn frame(&self) -> &IpcBatchFrame {
        &self.frame
    }
}

/// Encodes independently decodable network records. Unaligned checkpoints retain one key group
/// per frame; aligned checkpoints can gather all key groups for one destination.
pub fn frame_hash_exchange_batch(
    batch: RecordBatch,
    key_fields: &[(usize, KeyField)],
    max_parallelism: u32,
    parallelism: u32,
    preserve_key_groups: bool,
) -> Result<Vec<RoutedFrame>> {
    let transport_column_count = batch.num_columns();
    frame_hash_exchange_batch_projected(
        batch,
        key_fields,
        max_parallelism,
        parallelism,
        preserve_key_groups,
        transport_column_count,
    )
}

/// Routes with an optional input-only key sidecar while encoding only transport columns.
pub fn frame_hash_exchange_batch_projected(
    batch: RecordBatch,
    key_fields: &[(usize, KeyField)],
    max_parallelism: u32,
    parallelism: u32,
    preserve_key_groups: bool,
    transport_column_count: usize,
) -> Result<Vec<RoutedFrame>> {
    if preserve_key_groups {
        route_batch_by_key_group(batch, key_fields, max_parallelism)?
            .into_iter()
            .map(|routed| {
                Ok(RoutedFrame {
                    key_group: routed.key_group(),
                    frame: IpcBatchFrame::encode(&transport_batch(
                        routed.materialize()?,
                        transport_column_count,
                    )?)?,
                })
            })
            .collect()
    } else {
        route_batch(batch, key_fields, max_parallelism, parallelism)?
            .into_iter()
            .map(|routed| {
                let destination = routed.destination();
                let key_group = destination
                    .saturating_mul(max_parallelism)
                    .saturating_add(parallelism - 1)
                    / parallelism;
                Ok(RoutedFrame {
                    key_group,
                    frame: IpcBatchFrame::encode(&transport_batch(
                        routed.materialize()?,
                        transport_column_count,
                    )?)?,
                })
            })
            .collect()
    }
}

fn transport_batch(batch: RecordBatch, column_count: usize) -> Result<RecordBatch> {
    if column_count == batch.num_columns() {
        return Ok(batch);
    }
    let fields = batch.schema().fields()[..column_count]
        .iter()
        .map(|field| field.as_ref().clone())
        .collect::<Vec<_>>();
    RecordBatch::try_new(
        Arc::new(Schema::new(fields)),
        batch.columns()[..column_count].to_vec(),
    )
}

#[cfg(test)]
mod tests {
    use std::sync::Arc;

    use arrow::array::{ArrayRef, BinaryArray, Int32Array, Int8Array};
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

        let frames =
            frame_hash_exchange_batch(batch, &[(0, KeyField::Integer)], 128, 4, true).unwrap();

        let mut decoded_rows = Vec::new();
        for routed in frames {
            let decoded = routed.frame().decode(Arc::clone(&schema)).unwrap();
            let keys = decoded
                .column(0)
                .as_any()
                .downcast_ref::<Int32Array>()
                .unwrap();
            let row_kinds = decoded
                .column(1)
                .as_any()
                .downcast_ref::<Int8Array>()
                .unwrap();
            for row in 0..decoded.num_rows() {
                let one = RecordBatch::try_new(
                    Arc::new(Schema::new(vec![Field::new("key", DataType::Int32, false)])),
                    vec![Arc::new(Int32Array::from(vec![keys.value(row)])) as ArrayRef],
                )
                .unwrap();
                let encoded =
                    crate::exchange::encode_binary_row(&one, 0, &[(0, KeyField::Integer)]).unwrap();
                assert_eq!(
                    crate::exchange::assign_key_group(&encoded, 128),
                    routed.key_group()
                );
                decoded_rows.push((keys.value(row), row_kinds.value(row)));
            }
        }
        decoded_rows.sort_unstable();
        assert_eq!(decoded_rows, vec![(-1, 1), (0, 2), (1, 0), (42, 3)]);
    }

    #[test]
    fn aligned_frames_gather_once_per_nonempty_destination() {
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

        let frames =
            frame_hash_exchange_batch(batch, &[(0, KeyField::Integer)], 128, 4, false).unwrap();

        assert_eq!(frames.len(), 2);
        let mut decoded_rows = Vec::new();
        for routed in frames {
            let tagged_destination = routed.key_group() * 4 / 128;
            let decoded = routed.frame().decode(Arc::clone(&schema)).unwrap();
            let keys = decoded
                .column(0)
                .as_any()
                .downcast_ref::<Int32Array>()
                .unwrap();
            for row in 0..decoded.num_rows() {
                let one = RecordBatch::try_new(
                    Arc::new(Schema::new(vec![Field::new("key", DataType::Int32, false)])),
                    vec![Arc::new(Int32Array::from(vec![keys.value(row)])) as ArrayRef],
                )
                .unwrap();
                let encoded =
                    crate::exchange::encode_binary_row(&one, 0, &[(0, KeyField::Integer)]).unwrap();
                let destination = crate::exchange::assign_key_group(&encoded, 128) * 4 / 128;
                assert_eq!(destination, tagged_destination);
                decoded_rows.push(keys.value(row));
            }
        }
        decoded_rows.sort_unstable();
        assert_eq!(decoded_rows, vec![-1, 0, 1, 42]);
    }

    #[test]
    fn routes_preencoded_binary_rows_without_reencoding_and_strips_the_sidecar() {
        let transport_schema = Arc::new(Schema::new(vec![
            Field::new("value", DataType::Int32, false),
            Field::new("__streamfusion_row_kind", DataType::Int8, false),
        ]));
        let input_schema = Arc::new(Schema::new(vec![
            Field::new("value", DataType::Int32, false),
            Field::new("__streamfusion_row_kind", DataType::Int8, false),
            Field::new("__streamfusion_routing_key", DataType::Binary, false),
        ]));
        let canonical_keys = vec![
            vec![0_u8, 0, 0, 0, 1, 0, 0, 0],
            vec![0_u8, 0, 0, 0, 2, 0, 0, 0],
        ];
        let batch = RecordBatch::try_new(
            input_schema,
            vec![
                Arc::new(Int32Array::from(vec![7, 9])) as ArrayRef,
                Arc::new(Int8Array::from(vec![0, 3])) as ArrayRef,
                Arc::new(BinaryArray::from_iter_values(
                    canonical_keys.iter().map(Vec::as_slice),
                )) as ArrayRef,
            ],
        )
        .unwrap();

        let frames = frame_hash_exchange_batch_projected(
            batch,
            &[(2, KeyField::PreencodedBinaryRow)],
            128,
            4,
            true,
            2,
        )
        .unwrap();

        assert_eq!(frames.len(), 2);
        let mut decoded_values = Vec::new();
        for routed in frames {
            let decoded = routed
                .frame()
                .decode(Arc::clone(&transport_schema))
                .unwrap();
            assert_eq!(decoded.num_columns(), 2);
            let values = decoded
                .column(0)
                .as_any()
                .downcast_ref::<Int32Array>()
                .unwrap();
            for row in 0..decoded.num_rows() {
                let value = values.value(row);
                let original = if value == 7 { 0 } else { 1 };
                assert_eq!(
                    crate::exchange::assign_key_group(&canonical_keys[original], 128),
                    routed.key_group()
                );
                decoded_values.push(value);
            }
        }
        decoded_values.sort_unstable();
        assert_eq!(decoded_values, vec![7, 9]);
    }
}
