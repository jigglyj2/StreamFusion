// Copyright 2026 StreamFusion Authors
// Licensed under the Apache License, Version 2.0

use arrow::record_batch::RecordBatch;
use datafusion::error::Result;

use super::group_aggregate::GroupAggregateProcessor;
use crate::memory_pool::HostMemoryReservation;

/// Persistent global half of Flink's local/global mini-batch aggregation pair.
///
/// The shared aggregate state machine owns canonical accumulator encoding and changelog emission;
/// this distinct processor narrows the contract to merging opaque local accumulator batches.
pub(crate) struct GlobalGroupAggregateProcessor {
    inner: GroupAggregateProcessor,
}

impl GlobalGroupAggregateProcessor {
    pub(crate) fn new(
        serialized_plan: &[u8],
        max_parallelism: u32,
        first_key_group: u32,
        last_key_group: u32,
        state_reservation: HostMemoryReservation,
    ) -> Result<Self> {
        Ok(Self {
            inner: GroupAggregateProcessor::new(
                serialized_plan,
                max_parallelism,
                first_key_group,
                last_key_group,
                state_reservation,
            )?,
        })
    }

    #[allow(clippy::too_many_arguments)]
    pub(crate) fn new_rocksdb(
        serialized_plan: &[u8],
        max_parallelism: u32,
        first_key_group: u32,
        last_key_group: u32,
        plugin_path: &std::path::Path,
        database_path: &std::path::Path,
        memory_limit: usize,
        scratch_reservation: HostMemoryReservation,
    ) -> Result<Self> {
        Ok(Self {
            inner: GroupAggregateProcessor::new_rocksdb(
                serialized_plan,
                max_parallelism,
                first_key_group,
                last_key_group,
                plugin_path,
                database_path,
                memory_limit,
                scratch_reservation,
            )?,
        })
    }

    pub(crate) fn process_arrow(&mut self, batch: RecordBatch) -> Result<RecordBatch> {
        self.inner.process_arrow(batch)
    }

    pub(crate) fn finish_bundle(&mut self) -> Result<RecordBatch> {
        self.inner.finish_bundle()
    }

    pub(crate) fn pending_element_count(&self) -> usize {
        self.inner.pending_element_count()
    }

    pub(crate) fn pending_key_count(&self) -> usize {
        self.inner.pending_key_count()
    }

    pub(crate) fn statistics(&self) -> [u64; 2] {
        self.inner.statistics()
    }

    pub(crate) fn snapshot_key_group(&self, key_group: u32) -> Result<Vec<u8>> {
        self.inner.snapshot_key_group(key_group)
    }

    pub(crate) fn restore_key_group(&mut self, key_group: u32, bytes: &[u8]) -> Result<()> {
        self.inner.restore_key_group(key_group, bytes)
    }

    pub(crate) fn checkpoint(&self, directory: &std::path::Path) -> Result<()> {
        self.inner.checkpoint(directory)
    }
}

#[cfg(test)]
mod tests {
    use std::sync::Arc;

    use arrow::array::{ArrayRef, BinaryArray, Int64Array, Int8Array};
    use arrow::datatypes::{DataType, Field, Schema};
    use prost::Message;

    use super::*;
    use crate::memory_pool::tests_support::TestBroker;
    use crate::planner::operators::group_aggregate::{
        encode_state, lower_call, AccumulatorState, AggregateValue,
    };
    use crate::proto;

    fn bigint(nullable: bool) -> proto::LogicalType {
        proto::LogicalType {
            nullable,
            r#type: Some(proto::logical_type::Type::Bigint(proto::EmptyType {})),
        }
    }

    fn binary(nullable: bool) -> proto::LogicalType {
        proto::LogicalType {
            nullable,
            r#type: Some(proto::logical_type::Type::Binary(proto::EmptyType {})),
        }
    }

    fn schema(fields: &[(&str, proto::LogicalType)]) -> proto::Schema {
        proto::Schema {
            fields: fields
                .iter()
                .map(|(name, logical)| proto::Field {
                    name: (*name).to_string(),
                    r#type: Some(logical.clone()),
                })
                .collect(),
        }
    }

    fn proto_calls() -> Vec<proto::AggregateCall> {
        vec![
            proto::AggregateCall {
                function: proto::AggregateFunction::CountStar as i32,
                input_index: None,
                input_type: None,
                output_type: Some(bigint(false)),
                retractable: true,
                filter_index: None,
                distinct: false,
                accumulator_type: None,
            },
            proto::AggregateCall {
                function: proto::AggregateFunction::Sum as i32,
                input_index: Some(1),
                input_type: Some(bigint(true)),
                output_type: Some(bigint(true)),
                retractable: true,
                filter_index: None,
                distinct: false,
                accumulator_type: None,
            },
        ]
    }

    fn plan(size: u64) -> Vec<u8> {
        proto::NativePlan {
            protocol_version: crate::PLAN_PROTOCOL_VERSION,
            root: Some(proto::Operator {
                operator: Some(proto::operator::Operator::GlobalGroupAggregate(Box::new(
                    proto::GlobalGroupAggregate {
                        input: None,
                        grouping_indices: vec![0],
                        aggregate_calls: proto_calls(),
                        generate_update_before: true,
                        mini_batch_size: size,
                        input_schema: Some(schema(&[
                            ("key", bigint(false)),
                            ("accumulator", binary(false)),
                        ])),
                        output_schema: Some(schema(&[
                            ("key", bigint(false)),
                            ("count", bigint(false)),
                            ("sum", bigint(true)),
                        ])),
                    },
                ))),
            }),
        }
        .encode_to_vec()
    }

    fn partial(values: &[i64], accumulate: bool) -> Vec<u8> {
        let calls = proto_calls()
            .iter()
            .map(lower_call)
            .collect::<Result<Vec<_>>>()
            .unwrap();
        let mut state = AccumulatorState::new(&calls);
        for value in values {
            state
                .apply_values(
                    &calls,
                    &[None, Some(AggregateValue::Int(*value as i128))],
                    accumulate,
                )
                .unwrap();
        }
        encode_state(&state)
    }

    fn batch(rows: Vec<(i64, Vec<u8>)>) -> RecordBatch {
        let keys = rows.iter().map(|(key, _)| *key).collect::<Vec<_>>();
        let accumulators = rows
            .iter()
            .map(|(_, accumulator)| accumulator.as_slice())
            .collect::<Vec<_>>();
        let encoded_keys = rows
            .iter()
            .map(|(key, _)| key.to_le_bytes().to_vec())
            .collect::<Vec<_>>();
        RecordBatch::try_new(
            Arc::new(Schema::new(vec![
                Field::new("key", DataType::Int64, false),
                Field::new("accumulator", DataType::Binary, false),
                Field::new("__streamfusion_key", DataType::Binary, false),
            ])),
            vec![
                Arc::new(Int64Array::from(keys)) as ArrayRef,
                Arc::new(BinaryArray::from_iter_values(accumulators)) as ArrayRef,
                Arc::new(BinaryArray::from_iter_values(
                    encoded_keys.iter().map(Vec::as_slice),
                )) as ArrayRef,
            ],
        )
        .unwrap()
    }

    fn processor() -> GlobalGroupAggregateProcessor {
        GlobalGroupAggregateProcessor::new(
            &plan(2),
            128,
            0,
            127,
            HostMemoryReservation::new(Arc::new(TestBroker::new(1 << 20)), "global aggregate test"),
        )
        .unwrap()
    }

    #[test]
    fn merges_local_deltas_at_receiving_bundle_boundaries_and_retracts() {
        let mut processor = processor();
        let first = processor
            .process_arrow(batch(vec![(1, partial(&[10, 20], true))]))
            .unwrap();
        assert_eq!(first.num_rows(), 0);

        let second = processor
            .process_arrow(batch(vec![
                (1, partial(&[5], true)),
                (2, partial(&[7], true)),
            ]))
            .unwrap();
        assert_eq!(second.num_rows(), 1);
        assert_eq!(
            second
                .column(1)
                .as_any()
                .downcast_ref::<Int64Array>()
                .unwrap()
                .value(0),
            3
        );
        assert_eq!(
            second
                .column(2)
                .as_any()
                .downcast_ref::<Int64Array>()
                .unwrap()
                .value(0),
            35
        );
        assert_eq!(processor.finish_bundle().unwrap().num_rows(), 1);

        let deleted = processor
            .process_arrow(batch(vec![
                (1, partial(&[10, 20], false)),
                (1, partial(&[5], false)),
            ]))
            .unwrap();
        assert_eq!(deleted.num_rows(), 1);
        assert_eq!(
            deleted
                .column(3)
                .as_any()
                .downcast_ref::<Int8Array>()
                .unwrap()
                .value(0),
            3
        );
        assert_eq!(processor.statistics(), [3, 3]);
    }

    #[test]
    fn canonical_global_state_moves_between_memory_and_rocksdb() {
        let Ok(plugin_path) = std::env::var("STREAMFUSION_TEST_ROCKSDB_PLUGIN") else {
            return;
        };
        let mut memory = processor();
        memory
            .process_arrow(batch(vec![
                (1, partial(&[10, 20], true)),
                (2, partial(&[7], true)),
            ]))
            .unwrap();
        let snapshots = (0..128)
            .map(|key_group| memory.snapshot_key_group(key_group).unwrap())
            .collect::<Vec<_>>();

        let directory = tempfile::tempdir().unwrap();
        let mut rocks = GlobalGroupAggregateProcessor::new_rocksdb(
            &plan(2),
            128,
            0,
            127,
            std::path::Path::new(&plugin_path),
            directory.path(),
            64 << 20,
            HostMemoryReservation::new(Arc::new(TestBroker::new(256 << 20)), "global RocksDB test"),
        )
        .unwrap();
        for (key_group, snapshot) in snapshots.iter().enumerate() {
            rocks.restore_key_group(key_group as u32, snapshot).unwrap();
            assert_eq!(
                rocks.snapshot_key_group(key_group as u32).unwrap(),
                *snapshot
            );
        }

        let update = batch(vec![(1, partial(&[5], true))]);
        memory.process_arrow(update.clone()).unwrap();
        rocks.process_arrow(update).unwrap();
        let memory_output = memory.finish_bundle().unwrap();
        let rocks_output = rocks.finish_bundle().unwrap();
        assert_eq!(memory_output, rocks_output);
    }
}
