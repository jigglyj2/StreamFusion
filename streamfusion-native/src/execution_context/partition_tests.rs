// Copyright 2026 StreamFusion Authors
// Licensed under the Apache License, Version 2.0

use super::*;
use crate::memory_pool::tests_support::TestBroker;
use crate::memory_pool::HostMemoryReservation;
use crate::planner::operators::deduplicate::{
    execution_plan::DeduplicateFactory, DeduplicateProcessor,
};
use arrow::array::{Int32Array, RecordBatch};
use arrow::datatypes::{DataType, Field, Schema};
use futures::StreamExt;

fn input(index: u32) -> proto::Operator {
    proto::Operator {
        plan_node_id: u64::from(index) + 1,
        metric_name: String::new(),
        clear_record_timestamps: false,
        metric_uid: None,
        operator: Some(proto::operator::Operator::Input(proto::Input {
            input_index: index,
            schema: None,
        })),
    }
}

#[test]
fn shared_stream_consumes_every_union_partition_and_keeps_stage_counts() {
    let schema = Arc::new(Schema::new(vec![Field::new(
        "value",
        DataType::Int32,
        false,
    )]));
    let batches = (0..3)
        .map(|input| {
            RecordBatch::try_new(
                schema.clone(),
                vec![Arc::new(Int32Array::from(vec![input, input]))],
            )
            .unwrap()
        })
        .collect::<Vec<_>>();
    let plan = proto::NativePlan {
        protocol_version: crate::PLAN_PROTOCOL_VERSION,
        root: Some(proto::Operator {
            plan_node_id: 4,
            metric_name: String::new(),
            clear_record_timestamps: false,
            metric_uid: None,
            operator: Some(proto::operator::Operator::Union(proto::Union {
                inputs: (0..3).map(input).collect(),
            })),
        }),
    }
    .encode_to_vec();
    let broker = Arc::new(TestBroker::new(16 << 20));
    let context = Arc::new(
        NativeExecutionContext::new(
            &plan,
            Arc::new(FlinkMemoryPool::new(broker.clone(), 16 << 20)),
        )
        .unwrap(),
    );
    for _ in 0..2 {
        let mut stream = context.start(batches.clone()).unwrap();
        let mut output = Vec::new();
        while let Some(next) = context.runtime().block_on(stream.next()) {
            output.push(next.unwrap());
        }
        assert_eq!(
            output.len(),
            3,
            "one task must consume all local Union partitions"
        );
        for expected in &batches {
            assert!(output
                .iter()
                .any(|actual| Arc::ptr_eq(actual.column(0), expected.column(0))));
        }
    }
    assert_eq!(
        context.metric_snapshot().unwrap(),
        vec![4, 12, 12, 1, 0, 4, 2, 0, 4, 3, 0, 4]
    );
    drop(context);
    assert_eq!(broker.reserved(), 0);
}

fn dedup_union_plan() -> Vec<u8> {
    proto::NativePlan {
        protocol_version: crate::PLAN_PROTOCOL_VERSION,
        root: Some(proto::Operator {
            plan_node_id: 5,
            metric_name: String::new(),
            clear_record_timestamps: false,
            metric_uid: None,
            operator: Some(proto::operator::Operator::Deduplicate(Box::new(
                proto::Deduplicate {
                    input: Some(Box::new(proto::Operator {
                        plan_node_id: 4,
                        metric_name: String::new(),
                        clear_record_timestamps: false,
                        metric_uid: None,
                        operator: Some(proto::operator::Operator::Union(proto::Union {
                            inputs: (0..3).map(input).collect(),
                        })),
                    })),
                    key_indices: vec![0],
                    processing_time: true,
                    keep_last: false,
                    ..Default::default()
                },
            ))),
        }),
    }
    .encode_to_vec()
}

fn dedup_inputs() -> Vec<RecordBatch> {
    let schema = Arc::new(Schema::new(vec![
        Field::new("key", DataType::Int32, false),
        Field::new("__streamfusion_input_row", DataType::Int32, false),
    ]));
    (0..3)
        .map(|index| {
            RecordBatch::try_new(
                schema.clone(),
                vec![
                    Arc::new(Int32Array::from(vec![index, index])),
                    Arc::new(Int32Array::from(vec![2 * index, 2 * index + 1])),
                ],
            )
            .unwrap()
        })
        .collect()
}

#[test]
fn partition_normalization_below_persistent_stage_preserves_state_and_cross_backend_restore() {
    let plugin = std::env::var("STREAMFUSION_TEST_ROCKSDB_PLUGIN").ok();
    let directory = tempfile::tempdir().unwrap();
    let broker = Arc::new(TestBroker::new(128 << 20));
    let bytes = dedup_union_plan();
    let mut snapshot = Vec::new();
    for rocks in [false, true] {
        if rocks && plugin.is_none() {
            continue;
        }
        let memory = HostMemoryReservation::new(broker.clone(), "partitioned state");
        let mut processor = if rocks {
            DeduplicateProcessor::new_rocksdb(
                &bytes,
                1,
                0,
                0,
                std::path::Path::new(plugin.as_ref().unwrap()),
                directory.path(),
                32 << 20,
                memory,
            )
            .unwrap()
        } else {
            DeduplicateProcessor::new(&bytes, 1, 0, 0, memory).unwrap()
        };
        if rocks {
            processor.restore_key_group(0, &snapshot).unwrap();
        }
        let processor = Arc::new(Mutex::new(processor));
        let mut context = NativeExecutionContext::new(
            &bytes,
            Arc::new(FlinkMemoryPool::new(broker.clone(), 128 << 20)),
        )
        .unwrap();
        context
            .bind_persistent(vec![(5, Arc::new(DeduplicateFactory(processor.clone())))])
            .unwrap();
        let context = Arc::new(context);
        for invocation in 0..2 {
            let mut stream = context.start(dedup_inputs()).unwrap();
            let mut keys = Vec::new();
            while let Some(batch) = context.runtime().block_on(stream.next()) {
                let batch = batch.unwrap();
                keys.extend(
                    batch
                        .column(0)
                        .as_any()
                        .downcast_ref::<Int32Array>()
                        .unwrap()
                        .values()
                        .iter()
                        .copied(),
                );
            }
            if rocks || invocation != 0 {
                assert!(keys.is_empty());
            } else {
                assert_eq!(keys, vec![0, 1, 2]);
            }
        }
        let state = processor.lock().unwrap().snapshot_key_group(0).unwrap();
        if rocks {
            assert_eq!(&*state, snapshot.as_slice());
        } else {
            snapshot.extend_from_slice(&state);
        }
        let outputs = if rocks { 0 } else { 3 };
        assert_eq!(
            context.metric_snapshot().unwrap(),
            vec![5, 12, outputs, 4, 12, 12, 1, 0, 4, 2, 0, 4, 3, 0, 4]
        );
        drop(state);
        drop(context);
        drop(processor);
        assert_eq!(broker.reserved(), 0);
    }
}
