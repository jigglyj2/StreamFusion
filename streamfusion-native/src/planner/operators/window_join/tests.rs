// Copyright 2026 StreamFusion Authors
// Licensed under the Apache License, Version 2.0

use super::*;
use crate::memory_pool::{tests_support::TestBroker, HostMemoryReservation};
use arrow::array::{ArrayRef, BinaryArray, Int64Array, TimestampMillisecondArray};
use prost::Message;

#[test]
fn canonical_state_preserves_both_sides_and_duplicate_rows() {
    let state = JoinWindowState {
        left: vec![b"left".to_vec(), b"left".to_vec()],
        right: vec![b"right".to_vec()],
    };
    assert_eq!(decode_state(&encode_state(&state)).unwrap(), state);
}

#[test]
fn accounts_join_rows_keys_timers_and_state_in_host_memory() {
    let broker = Arc::new(TestBroker::new(64 << 20));
    let mut processor = WindowJoinProcessor::new(
        &plan(),
        128,
        0,
        127,
        HostMemoryReservation::new(broker.clone(), "window join accounting"),
    )
    .unwrap();
    let empty_state = broker.reserved();
    let output = processor
        .process_arrow(
            0,
            batch(
                &[7, 8],
                &[100, 100],
                &[b"left", b"right"],
                &[INSERT, INSERT],
            ),
        )
        .unwrap();

    assert!(broker.reserved() > empty_state);
    drop(output);
    drop(processor);
    assert_eq!(broker.reserved(), 0);
}

#[test]
fn duplicate_arrivals_restore_and_rescale_without_changing_window_contents() {
    let broker = Arc::new(TestBroker::new(1 << 30));
    let mut source = WindowJoinProcessor::new(
        &plan(),
        128,
        0,
        127,
        HostMemoryReservation::new(broker.clone(), "window join source"),
    )
    .unwrap();
    source
        .process_arrow(
            0,
            batch(&[7, 7], &[100, 100], &[b"left", b"left"], &[INSERT, INSERT]),
        )
        .unwrap();
    source
        .process_arrow(1, batch(&[7], &[100], &[b"right"], &[INSERT]))
        .unwrap();
    assert_eq!(&source.statistics()[..2], &[2, 2]);
    let snapshots = (0..128)
        .map(|key_group| source.snapshot_key_group(key_group).unwrap())
        .collect::<Vec<_>>();

    let mut lower = WindowJoinProcessor::new(
        &plan(),
        128,
        0,
        63,
        HostMemoryReservation::new(broker.clone(), "window join lower"),
    )
    .unwrap();
    let mut upper = WindowJoinProcessor::new(
        &plan(),
        128,
        64,
        127,
        HostMemoryReservation::new(broker, "window join upper"),
    )
    .unwrap();
    for (key_group, snapshot) in snapshots.iter().enumerate() {
        let target = if key_group < 64 {
            &mut lower
        } else {
            &mut upper
        };
        target
            .restore_key_group(key_group as u32, snapshot)
            .unwrap();
    }

    let outputs = [
        lower.advance_event_time(99).unwrap(),
        upper.advance_event_time(99).unwrap(),
    ];
    let rows = outputs
        .iter()
        .flat_map(|output| {
            [2, 5].into_iter().flat_map(|column| {
                output
                    .column(column)
                    .as_any()
                    .downcast_ref::<BinaryArray>()
                    .unwrap()
                    .iter()
                    .flatten()
            })
        })
        .collect::<Vec<_>>();
    assert_eq!(
        rows,
        vec![b"left".as_slice(), b"left".as_slice(), b"right".as_slice()]
    );
    assert_eq!(lower.statistics()[5] + upper.statistics()[5], 0);
}

#[test]
fn canonical_join_state_moves_from_memory_to_rocksdb() {
    let Ok(plugin_path) = std::env::var("STREAMFUSION_TEST_ROCKSDB_PLUGIN") else {
        return;
    };
    let broker = Arc::new(TestBroker::new(1 << 30));
    let mut memory = WindowJoinProcessor::new(
        &plan(),
        128,
        0,
        127,
        HostMemoryReservation::new(broker.clone(), "window join memory source"),
    )
    .unwrap();
    memory
        .process_arrow(0, batch(&[7], &[100], &[b"left"], &[INSERT]))
        .unwrap();
    memory
        .process_arrow(1, batch(&[7], &[100], &[b"right"], &[INSERT]))
        .unwrap();
    let snapshots = (0..128)
        .map(|key_group| memory.snapshot_key_group(key_group).unwrap())
        .collect::<Vec<_>>();

    let directory = tempfile::tempdir().unwrap();
    let mut rocks = WindowJoinProcessor::new_rocksdb(
        &plan(),
        128,
        0,
        127,
        std::path::Path::new(&plugin_path),
        directory.path(),
        64 << 20,
        HostMemoryReservation::new(broker, "window join RocksDB scratch"),
    )
    .unwrap();
    for (key_group, snapshot) in snapshots.iter().enumerate() {
        rocks.restore_key_group(key_group as u32, snapshot).unwrap();
        assert_eq!(
            rocks.snapshot_key_group(key_group as u32).unwrap(),
            *snapshot
        );
    }
    let output = rocks.advance_event_time(99).unwrap();
    assert_eq!(output.num_rows(), 2);
    assert_eq!(rocks.statistics()[5], 0);
}

pub(super) fn plan() -> Vec<u8> {
    proto::NativePlan {
        protocol_version: crate::PLAN_PROTOCOL_VERSION,
        root: Some(proto::Operator {
            plan_node_id: 0,
            metric_name: String::new(),
            clear_record_timestamps: false,
            metric_uid: None,
            operator: Some(proto::operator::Operator::WindowJoin(Box::new(
                proto::WindowJoin {
                    left_key_indices: vec![0],
                    right_key_indices: vec![0],
                    left_window_end_index: 1,
                    right_window_end_index: 1,
                    left_schema: Some(test_schema()),
                    right_schema: Some(test_schema()),
                    shift_time_zone: "UTC".to_string(),
                    ..Default::default()
                },
            ))),
        }),
    }
    .encode_to_vec()
}

fn test_schema() -> proto::Schema {
    proto::Schema {
        fields: vec![
            proto_field(
                "key",
                proto::logical_type::Type::Bigint(proto::EmptyType::default()),
            ),
            proto_field(
                "window_end",
                proto::logical_type::Type::Timestamp(proto::PrecisionType { precision: 3 }),
            ),
            proto_field(
                "payload",
                proto::logical_type::Type::Binary(proto::EmptyType::default()),
            ),
        ],
    }
}

fn proto_field(name: &str, r#type: proto::logical_type::Type) -> proto::Field {
    proto::Field {
        name: name.to_string(),
        r#type: Some(proto::LogicalType {
            nullable: true,
            r#type: Some(r#type),
        }),
    }
}

pub(super) fn batch(keys: &[i64], window_end: &[i64], rows: &[&[u8]], kinds: &[i8]) -> RecordBatch {
    RecordBatch::try_from_iter(vec![
        ("key", Arc::new(Int64Array::from(keys.to_vec())) as ArrayRef),
        (
            "window_end",
            Arc::new(TimestampMillisecondArray::from(window_end.to_vec())) as ArrayRef,
        ),
        (
            "payload",
            Arc::new(BinaryArray::from_vec(rows.to_vec())) as ArrayRef,
        ),
        (
            "__streamfusion_input_row_kind",
            Arc::new(Int8Array::from(kinds.to_vec())) as ArrayRef,
        ),
    ])
    .unwrap()
}
