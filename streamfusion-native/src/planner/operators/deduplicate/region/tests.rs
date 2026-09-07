// Copyright 2026 StreamFusion Authors
// Licensed under the Apache License, Version 2.0

use super::*;
use crate::memory_pool::tests_support::TestBroker;
use arrow::array::{Array, Int32Array, Int64Array, StringArray, TimestampMillisecondArray};
use arrow::datatypes::{DataType, Field, Schema, TimeUnit};
use futures::StreamExt;

fn reference(index: u32) -> proto::Expression {
    proto::Expression {
        expression: Some(proto::expression::Expression::InputReference(
            proto::InputReference {
                index,
                r#type: None,
            },
        )),
    }
}
fn bytes(tail: bool) -> Vec<u8> {
    let input = proto::Operator {
        plan_node_id: 4,
        metric_name: String::new(),
        clear_record_timestamps: false,
        metric_uid: None,
        operator: Some(proto::operator::Operator::Input(proto::Input {
            schema: None,
            input_index: 0,
        })),
    };
    let upstream = proto::Operator {
        plan_node_id: 3,
        metric_name: String::new(),
        clear_record_timestamps: false,
        metric_uid: None,
        operator: Some(proto::operator::Operator::Calc(Box::new(proto::Calc {
            preserve_input_envelope: false,
            input: Some(Box::new(input)),
            projections: (0..5).map(reference).collect(),
            condition: None,
        }))),
    };
    let mut node = proto::Operator {
        plan_node_id: 2,
        metric_name: String::new(),
        clear_record_timestamps: false,
        metric_uid: None,
        operator: Some(proto::operator::Operator::Deduplicate(Box::new(
            proto::Deduplicate {
                input: Some(Box::new(upstream)),
                key_indices: vec![0],
                order_index: 2,
                keep_last: true,
                generate_insert: true,
                input_changelog: false,
                generate_update_before: true,
                processing_time: false,
            },
        ))),
    };
    if tail {
        node = proto::Operator {
            plan_node_id: 1,
            metric_name: String::new(),
            clear_record_timestamps: false,
            metric_uid: None,
            operator: Some(proto::operator::Operator::Calc(Box::new(proto::Calc {
                preserve_input_envelope: false,
                input: Some(Box::new(node)),
                projections: (0..6).map(reference).collect(),
                condition: None,
            }))),
        };
    }
    proto::NativePlan {
        protocol_version: crate::PLAN_PROTOCOL_VERSION,
        root: Some(node),
    }
    .encode_to_vec()
}
fn batch(time: i64) -> RecordBatch {
    RecordBatch::try_new(
        Arc::new(Schema::new(vec![
            Field::new("key", DataType::Int64, false),
            Field::new("value", DataType::Utf8, true),
            Field::new(
                "ts",
                DataType::Timestamp(TimeUnit::Millisecond, None),
                false,
            ),
            Field::new("nullable", DataType::Int64, true),
            Field::new("__streamfusion_input_row", DataType::Int32, false),
        ])),
        vec![
            Arc::new(Int64Array::from(vec![1, 1, 2])),
            Arc::new(StringArray::from(vec![Some("é"), None, Some("wide value")])),
            Arc::new(TimestampMillisecondArray::from(vec![time, time + 1, time])),
            Arc::new(Int64Array::from(vec![None, Some(8), None])),
            Arc::new(Int32Array::from(vec![9, 4, 17])),
        ],
    )
    .unwrap()
}
fn handle(broker: &Arc<TestBroker>) -> DeduplicateHandle {
    DeduplicateHandle::new(
        &bytes(true),
        HostMemoryReservation::new(broker.clone(), "test region"),
        |bare| {
            DeduplicateProcessor::new(
                bare,
                128,
                0,
                127,
                HostMemoryReservation::new(broker.clone(), "test state"),
            )
        },
    )
    .unwrap()
}

#[test]
fn calc_dedup_calc_reuses_tree_preserves_history_envelopes_and_stage_counts() {
    let broker = Arc::new(TestBroker::new(32 << 20));
    let mut handle = handle(&broker);
    let mut produced = 0;
    let mut physical = None;
    for time in [100, 200, 50] {
        let output = handle.process_arrow(batch(time)).unwrap();
        let expected = if time == 100 {
            4
        } else if time == 200 {
            6
        } else {
            0
        };
        assert_eq!(output.num_rows(), expected);
        let ordinals = output
            .column(5)
            .as_any()
            .downcast_ref::<Int32Array>()
            .unwrap();
        assert_eq!(
            ordinals.values().as_ref(),
            match time {
                100 => &[9, 4, 4, 17][..],
                200 => &[9, 9, 4, 4, 17, 17][..],
                _ => &[][..],
            }
        );
        produced += expected as i64;
        let input = if time == 100 {
            3
        } else if time == 200 {
            6
        } else {
            9
        };
        assert_eq!(
            handle.metrics().unwrap(),
            vec![1, produced, produced, 2, input, produced, 3, input, input, 4, 0, input]
        );
        let plan = handle.context.execute_plan(vec![batch(time)], Ok).unwrap();
        if let Some(previous) = physical.as_ref() {
            assert!(Arc::ptr_eq(previous, &plan));
        }
        physical = Some(plan.clone());
        handle
            .processor
            .lock()
            .unwrap()
            .snapshot_key_group(0)
            .unwrap();
    }
    drop(physical);
    drop(handle);
    assert_eq!(broker.reserved(), 0);
}

#[test]
fn region_canonical_state_and_retractions_match_across_memory_and_rocksdb() {
    let plugin = match std::env::var("STREAMFUSION_TEST_ROCKSDB_PLUGIN") {
        Ok(plugin) => plugin,
        Err(_) => return,
    };
    let broker = Arc::new(TestBroker::new(128 << 20));
    let mut memory = handle(&broker);
    memory.process_arrow(batch(100)).unwrap();
    let directory = tempfile::tempdir().unwrap();
    let mut rocks = DeduplicateHandle::new(
        &bytes(true),
        HostMemoryReservation::new(broker.clone(), "test Rocks region"),
        |bare| {
            DeduplicateProcessor::new_rocksdb(
                bare,
                128,
                0,
                127,
                std::path::Path::new(&plugin),
                directory.path(),
                32 << 20,
                HostMemoryReservation::new(broker.clone(), "test Rocks state"),
            )
        },
    )
    .unwrap();
    for group in 0..128 {
        let snapshot = memory
            .processor
            .lock()
            .unwrap()
            .snapshot_key_group(group)
            .unwrap();
        rocks
            .processor
            .lock()
            .unwrap()
            .restore_key_group(group, &snapshot)
            .unwrap();
        assert_eq!(
            snapshot,
            rocks
                .processor
                .lock()
                .unwrap()
                .snapshot_key_group(group)
                .unwrap()
        );
    }
    for time in [150, 75, 250] {
        let expected = memory.process_arrow(batch(time)).unwrap();
        let actual = rocks.process_arrow(batch(time)).unwrap();
        assert_eq!(actual, expected);
        for group in 0..128 {
            assert_eq!(
                memory
                    .processor
                    .lock()
                    .unwrap()
                    .snapshot_key_group(group)
                    .unwrap(),
                rocks
                    .processor
                    .lock()
                    .unwrap()
                    .snapshot_key_group(group)
                    .unwrap()
            );
        }
    }
    drop(rocks);
    drop(memory);
    assert_eq!(broker.reserved(), 0);
}

#[test]
fn cancelled_native_streams_cannot_checkpoint_even_if_output_was_already_produced() {
    for poll in [false, true] {
        let broker = Arc::new(TestBroker::new(32 << 20));
        let mut handle = handle(&broker);
        handle.process_arrow(batch(100)).unwrap();
        let mut stream = handle.context.start(vec![batch(200)]).unwrap();
        assert!(handle
            .processor
            .lock()
            .unwrap()
            .snapshot_key_group(0)
            .is_err());
        if poll {
            handle
                .context
                .runtime()
                .block_on(stream.next())
                .unwrap()
                .unwrap();
        }
        drop(stream);
        assert!(handle
            .processor
            .lock()
            .unwrap()
            .snapshot_key_group(0)
            .is_err());
        assert!(handle.process_arrow(batch(300)).is_err());
        drop(handle);
        assert_eq!(broker.reserved(), 0);
    }
}

#[test]
fn invalid_schema_retries_do_not_retain_prospective_codec_memory() {
    let broker = Arc::new(TestBroker::new(32 << 20));
    let handle = handle(&broker);
    let baseline = broker.reserved();
    for _ in 0..3 {
        let schema = Arc::new(Schema::new(vec![Field::new("bad", DataType::Utf8, false)]));
        assert!(handle
            .processor
            .lock()
            .unwrap()
            .prepare_schema(schema, 1)
            .is_err());
        assert_eq!(broker.reserved(), baseline);
    }
    drop(handle);
    assert_eq!(broker.reserved(), 0);
}
