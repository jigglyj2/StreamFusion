// Copyright 2026 StreamFusion Authors
// Licensed under the Apache License, Version 2.0

use std::sync::Arc;

use arrow::array::{Int64Array, TimestampMillisecondArray};
use prost::Message;

use super::*;
use crate::exchange::encode_binary_row;
use crate::memory_pool::tests_support::TestBroker;
use crate::PLAN_PROTOCOL_VERSION;

pub(super) fn logical_bigint(nullable: bool) -> proto::LogicalType {
    proto::LogicalType {
        nullable,
        r#type: Some(proto::logical_type::Type::Bigint(proto::EmptyType {})),
    }
}

pub(super) fn logical_timestamp(nullable: bool) -> proto::LogicalType {
    proto::LogicalType {
        nullable,
        r#type: Some(proto::logical_type::Type::Timestamp(proto::PrecisionType {
            precision: 3,
        })),
    }
}

pub(super) fn field(name: &str, r#type: proto::LogicalType) -> proto::Field {
    proto::Field {
        name: name.to_string(),
        r#type: Some(r#type),
    }
}

pub(super) fn plan(
    kind: proto::WindowKind,
    size: i64,
    slide: i64,
    input_changelog: bool,
) -> Vec<u8> {
    let count_window = matches!(
        kind,
        proto::WindowKind::CountTumble | proto::WindowKind::CountHop
    );
    proto::NativePlan {
        protocol_version: PLAN_PROTOCOL_VERSION,
        root: Some(proto::Operator {
            plan_node_id: 0,
            metric_name: String::new(),
            clear_record_timestamps: false,
            metric_uid: None,
            operator: Some(proto::operator::Operator::WindowAggregate(Box::new(
                proto::WindowAggregate {
                    shift_time_zone: "UTC".to_string(),
                    input: None,
                    grouping_indices: vec![0],
                    aggregate_calls: vec![proto::AggregateCall {
                        function: proto::AggregateFunction::CountStar as i32,
                        input_index: None,
                        input_type: None,
                        output_type: Some(logical_bigint(false)),
                        retractable: true,
                        filter_index: None,
                        distinct: false,
                        accumulator_type: None,
                    }],
                    input_changelog,
                    time_attribute_index: 1,
                    kind: kind as i32,
                    size_millis: size,
                    slide_or_step_millis: slide,
                    offset_millis: 0,
                    processing_time: count_window,
                    window_properties: if count_window {
                        Vec::new()
                    } else {
                        vec![
                            proto::WindowProperty::Start as i32,
                            proto::WindowProperty::End as i32,
                        ]
                    },
                    input_schema: Some(proto::Schema {
                        fields: vec![
                            field("key", logical_bigint(false)),
                            field("ts", logical_timestamp(false)),
                        ],
                    }),
                    output_schema: Some(proto::Schema {
                        fields: if count_window {
                            vec![
                                field("key", logical_bigint(false)),
                                field("count", logical_bigint(false)),
                            ]
                        } else {
                            vec![
                                field("key", logical_bigint(false)),
                                field("count", logical_bigint(false)),
                                field("window_start", logical_timestamp(false)),
                                field("window_end", logical_timestamp(false)),
                            ]
                        },
                    }),
                    attached_window_start_index: None,
                    attached_window_end_index: None,
                    partial_accumulator_index: None,
                    partial_slice_end_index: None,
                    partial_window_start_index: None,
                    partial_windows_are_slices: false,
                },
            ))),
        }),
    }
    .encode_to_vec()
}

pub(super) fn processor(plan: &[u8], broker: Arc<TestBroker>) -> WindowAggregateProcessor {
    processor_for_range(plan, broker, 0, 127)
}

fn processor_for_range(
    plan: &[u8],
    broker: Arc<TestBroker>,
    first_key_group: u32,
    last_key_group: u32,
) -> WindowAggregateProcessor {
    WindowAggregateProcessor::new(
        plan,
        128,
        first_key_group,
        last_key_group,
        HostMemoryReservation::new(broker, "window state test"),
    )
    .unwrap()
}

fn batch(keys: Vec<i64>, timestamps: Vec<i64>, kinds: Option<Vec<i8>>) -> RecordBatch {
    let mut fields = vec![
        Field::new("key", DataType::Int64, false),
        Field::new(
            "ts",
            DataType::Timestamp(arrow::datatypes::TimeUnit::Millisecond, None),
            false,
        ),
    ];
    let mut columns: Vec<ArrayRef> = vec![
        Arc::new(Int64Array::from(keys)),
        Arc::new(TimestampMillisecondArray::from(timestamps)),
    ];
    if let Some(kinds) = kinds {
        fields.push(Field::new(
            "__streamfusion_input_row_kind",
            DataType::Int8,
            false,
        ));
        columns.push(Arc::new(Int8Array::from(kinds)));
    }
    RecordBatch::try_new(Arc::new(Schema::new(fields)), columns).unwrap()
}

fn attached_plan(input_changelog: bool) -> Vec<u8> {
    let mut native = proto::NativePlan::decode(
        plan(proto::WindowKind::Hop, 10_000, 2_000, input_changelog).as_slice(),
    )
    .unwrap();
    let aggregate = match native.root.as_mut().unwrap().operator.as_mut().unwrap() {
        proto::operator::Operator::WindowAggregate(aggregate) => aggregate,
        _ => unreachable!(),
    };
    aggregate.time_attribute_index = 0;
    aggregate.attached_window_start_index = Some(1);
    aggregate.attached_window_end_index = Some(2);
    aggregate.input_schema = Some(proto::Schema {
        fields: vec![
            field("key", logical_bigint(false)),
            field("window_start", logical_timestamp(false)),
            field("window_end", logical_timestamp(false)),
        ],
    });
    native.encode_to_vec()
}

fn attached_batch(
    keys: Vec<i64>,
    starts: Vec<i64>,
    ends: Vec<i64>,
    kinds: Option<Vec<i8>>,
) -> RecordBatch {
    let timestamp = DataType::Timestamp(arrow::datatypes::TimeUnit::Millisecond, None);
    let mut fields = vec![
        Field::new("key", DataType::Int64, false),
        Field::new("window_start", timestamp.clone(), false),
        Field::new("window_end", timestamp, false),
    ];
    let mut columns: Vec<ArrayRef> = vec![
        Arc::new(Int64Array::from(keys)),
        Arc::new(TimestampMillisecondArray::from(starts)),
        Arc::new(TimestampMillisecondArray::from(ends)),
    ];
    if let Some(kinds) = kinds {
        fields.push(Field::new(
            "__streamfusion_input_row_kind",
            DataType::Int8,
            false,
        ));
        columns.push(Arc::new(Int8Array::from(kinds)));
    }
    RecordBatch::try_new(Arc::new(Schema::new(fields)), columns).unwrap()
}

#[test]
fn tumble_emits_only_when_the_watermark_closes_the_window() {
    let broker = Arc::new(TestBroker::new(16 << 20));
    let bytes = plan(proto::WindowKind::Tumble, 10_000, 0, false);
    let mut processor = processor(&bytes, broker.clone());
    let pending = processor
        .process_arrow(batch(vec![1, 1, 2], vec![1_000, 2_000, 3_000], None), 0)
        .unwrap();
    assert_eq!(pending.num_rows(), 0);
    assert_eq!(processor.advance_event_time(9_998).unwrap().num_rows(), 0);
    let output = processor.advance_event_time(9_999).unwrap();
    assert_eq!(output.num_rows(), 2);
    let mut counts = output
        .column(1)
        .as_any()
        .downcast_ref::<Int64Array>()
        .unwrap()
        .values()
        .to_vec();
    counts.sort_unstable();
    assert_eq!(counts, [1, 2]);
    drop(output);
    drop(processor);
    assert_eq!(broker.reserved(), 0);
}

#[test]
fn terminal_windows_drain_in_managed_output_batches() {
    let broker = Arc::new(TestBroker::new(16 << 20));
    let bytes = plan(proto::WindowKind::Tumble, 10_000, 0, false);
    let mut processor = processor(&bytes, broker.clone());
    let rows = MAX_TIMERS_PER_OUTPUT + 137;
    let keys = (0..rows).map(|key| key as i64).collect::<Vec<_>>();
    assert_eq!(
        processor
            .process_arrow(batch(keys, vec![1_000; rows], None), 0)
            .unwrap()
            .num_rows(),
        0
    );

    let first = processor.advance_event_time(i64::MAX).unwrap();
    assert_eq!(first.num_rows(), MAX_TIMERS_PER_OUTPUT);
    drop(first);
    let second = processor.advance_event_time(i64::MAX).unwrap();
    assert_eq!(second.num_rows(), 137);
    drop(second);
    assert_eq!(
        processor.advance_event_time(i64::MAX).unwrap().num_rows(),
        0
    );

    drop(processor);
    assert_eq!(broker.reserved(), 0);
}

#[test]
fn attached_windows_retract_and_restore_canonically_without_reassignment() {
    let broker = Arc::new(TestBroker::new(1 << 30));
    let bytes = attached_plan(true);
    let mut source = processor(&bytes, broker.clone());
    source
        .process_arrow(
            attached_batch(
                vec![7, 7, 7],
                vec![0, 0, 0],
                vec![10_000, 10_000, 10_000],
                Some(vec![INSERT, INSERT, DELETE]),
            ),
            0,
        )
        .unwrap();
    let snapshots = (0..128)
        .map(|group| source.snapshot_key_group(group).unwrap())
        .collect::<Vec<_>>();

    let mut restored = processor(&bytes, broker.clone());
    for (group, snapshot) in snapshots.iter().enumerate() {
        restored.restore_key_group(group as u32, snapshot).unwrap();
    }
    let output = restored.advance_event_time(9_999).unwrap();
    assert_eq!(output.num_rows(), 1);
    assert_eq!(
        output
            .column(1)
            .as_any()
            .downcast_ref::<Int64Array>()
            .unwrap()
            .value(0),
        1
    );
    assert_eq!(
        output
            .column(2)
            .as_any()
            .downcast_ref::<TimestampMillisecondArray>()
            .unwrap()
            .value(0),
        0
    );
    assert_eq!(
        output
            .column(3)
            .as_any()
            .downcast_ref::<TimestampMillisecondArray>()
            .unwrap()
            .value(0),
        10_000
    );
    // If the attached row had been assigned as an ordinary HOP input, four more
    // windows would remain and fire at later watermarks.
    assert_eq!(restored.advance_event_time(i64::MAX).unwrap().num_rows(), 0);

    let Ok(plugin_path) = std::env::var("STREAMFUSION_TEST_ROCKSDB_PLUGIN") else {
        return;
    };
    let directory = tempfile::tempdir().unwrap();
    let mut rocks = WindowAggregateProcessor::new_rocksdb(
        &bytes,
        128,
        0,
        127,
        std::path::Path::new(&plugin_path),
        directory.path(),
        64 << 20,
        HostMemoryReservation::new(broker, "attached window RocksDB scratch"),
    )
    .unwrap();
    for (group, snapshot) in snapshots.iter().enumerate() {
        rocks.restore_key_group(group as u32, snapshot).unwrap();
        assert_eq!(rocks.snapshot_key_group(group as u32).unwrap(), *snapshot);
    }
    let output = rocks.advance_event_time(9_999).unwrap();
    assert_eq!(output.num_rows(), 1);
    assert_eq!(
        output
            .column(1)
            .as_any()
            .downcast_ref::<Int64Array>()
            .unwrap()
            .value(0),
        1
    );
}

#[test]
fn count_tumble_and_hop_emit_on_flink_element_boundaries_and_restore() {
    let broker = Arc::new(TestBroker::new(32 << 20));
    let tumble = plan(proto::WindowKind::CountTumble, 3, 0, false);
    let mut before = processor(&tumble, broker.clone());
    let first = before
        .process_arrow(batch(vec![1, 1, 1, 1, 2, 2, 2], vec![0; 7], None), 0)
        .unwrap();
    assert_eq!(first.num_rows(), 2);
    assert!(first
        .column(1)
        .as_any()
        .downcast_ref::<Int64Array>()
        .unwrap()
        .iter()
        .all(|value| value == Some(3)));
    let key_group = assign_key_group(
        &encode_binary_row(&batch(vec![1], vec![0], None), 0, &[(0, KeyField::BigInt)]).unwrap(),
        128,
    );
    let snapshot = before.snapshot_key_group(key_group).unwrap();
    drop(before);
    let mut after = processor(&tumble, broker.clone());
    after.restore_key_group(key_group, &snapshot).unwrap();
    let restored = after
        .process_arrow(batch(vec![1, 1], vec![0; 2], None), 0)
        .unwrap();
    assert_eq!(restored.num_rows(), 1);
    assert_eq!(
        restored
            .column(1)
            .as_any()
            .downcast_ref::<Int64Array>()
            .unwrap()
            .value(0),
        3
    );

    let hop = plan(proto::WindowKind::CountHop, 3, 2, false);
    let mut hopping = processor(&hop, broker);
    let output = hopping
        .process_arrow(batch(vec![9, 9, 9, 9, 9], vec![0; 5], None), 0)
        .unwrap();
    assert_eq!(output.num_rows(), 2);
    assert!(output
        .column(1)
        .as_any()
        .downcast_ref::<Int64Array>()
        .unwrap()
        .iter()
        .all(|value| value == Some(3)));
}

#[test]
fn count_window_state_rescales_and_moves_from_memory_to_rocksdb() {
    let broker = Arc::new(TestBroker::new(1 << 30));
    let bytes = plan(proto::WindowKind::CountTumble, 3, 0, false);
    let mut source = processor(&bytes, broker.clone());
    assert_eq!(
        source
            .process_arrow(batch(vec![1, 1, 2, 2], vec![0; 4], None), 0)
            .unwrap()
            .num_rows(),
        0
    );
    let snapshots = (0..128)
        .map(|group| source.snapshot_key_group(group).unwrap())
        .collect::<Vec<_>>();

    let mut lower = processor_for_range(&bytes, broker.clone(), 0, 63);
    let mut upper = processor_for_range(&bytes, broker.clone(), 64, 127);
    for (group, snapshot) in snapshots.iter().enumerate() {
        if group < 64 {
            lower.restore_key_group(group as u32, snapshot).unwrap();
        } else {
            upper.restore_key_group(group as u32, snapshot).unwrap();
        }
    }
    let mut rescaled_rows = 0;
    for key in [1, 2] {
        let input = batch(vec![key], vec![0], None);
        let encoded = encode_binary_row(&input, 0, &[(0, KeyField::BigInt)]).unwrap();
        let target = if assign_key_group(&encoded, 128) < 64 {
            &mut lower
        } else {
            &mut upper
        };
        rescaled_rows += target.process_arrow(input, 0).unwrap().num_rows();
    }
    assert_eq!(rescaled_rows, 2);

    let Ok(plugin_path) = std::env::var("STREAMFUSION_TEST_ROCKSDB_PLUGIN") else {
        return;
    };
    let directory = tempfile::tempdir().unwrap();
    let mut rocks = WindowAggregateProcessor::new_rocksdb(
        &bytes,
        128,
        0,
        127,
        std::path::Path::new(&plugin_path),
        directory.path(),
        64 << 20,
        HostMemoryReservation::new(broker, "window aggregate RocksDB scratch"),
    )
    .unwrap();
    for (group, snapshot) in snapshots.iter().enumerate() {
        rocks.restore_key_group(group as u32, snapshot).unwrap();
        assert_eq!(rocks.snapshot_key_group(group as u32).unwrap(), *snapshot);
    }
    let before_io = rocks.statistics();
    let output = rocks
        .process_arrow(batch(vec![1, 2], vec![0; 2], None), 0)
        .unwrap();
    assert_eq!(output.num_rows(), 2);
    let after_io = rocks.statistics();
    assert_eq!(after_io[0] - before_io[0], 2);
    assert_eq!(after_io[1] - before_io[1], 1);
}

#[test]
fn hopping_windows_retract_and_restore_canonical_timer_state() {
    let broker = Arc::new(TestBroker::new(32 << 20));
    let bytes = plan(proto::WindowKind::Hop, 10_000, 5_000, true);
    let mut before = processor(&bytes, broker.clone());
    before
        .process_arrow(
            batch(
                vec![7, 7, 7],
                vec![6_000, 6_000, 6_000],
                Some(vec![INSERT, INSERT, DELETE]),
            ),
            0,
        )
        .unwrap();
    let key_group = assign_key_group(
        &encode_binary_row(
            &batch(vec![7], vec![6_000], None),
            0,
            &[(0, KeyField::BigInt)],
        )
        .unwrap(),
        128,
    );
    let snapshot = before.snapshot_key_group(key_group).unwrap();
    let mut after = processor(&bytes, broker.clone());
    after.restore_key_group(key_group, &snapshot).unwrap();
    let first = after.advance_event_time(9_999).unwrap();
    assert_eq!(first.num_rows(), 1);
    assert_eq!(
        first
            .column(1)
            .as_any()
            .downcast_ref::<Int64Array>()
            .unwrap()
            .value(0),
        1
    );
    let second = after.advance_event_time(14_999).unwrap();
    assert_eq!(second.num_rows(), 1);
}

#[test]
fn drops_null_and_late_event_time_rows() {
    let broker = Arc::new(TestBroker::new(16 << 20));
    let bytes = plan(proto::WindowKind::Tumble, 10_000, 0, false);
    let mut processor = processor(&bytes, broker);
    processor
        .process_arrow(batch(vec![1], vec![1_000], None), 0)
        .unwrap();
    processor.advance_event_time(9_999).unwrap();
    processor
        .process_arrow(batch(vec![1], vec![2_000], None), 0)
        .unwrap();
    assert_eq!(processor.late_records_dropped(), 1);
}

#[test]
fn session_retractions_preserve_flinks_merged_namespace() {
    let broker = Arc::new(TestBroker::new(32 << 20));
    let bytes = plan(proto::WindowKind::Session, 10_000, 0, true);
    let mut before = processor(&bytes, broker.clone());
    before
        .process_arrow(
            batch(vec![7, 7], vec![0, 8_000], Some(vec![INSERT, INSERT])),
            0,
        )
        .unwrap();
    before
        .process_arrow(batch(vec![7], vec![8_000], Some(vec![DELETE])), 0)
        .unwrap();
    let key_group = assign_key_group(
        &encode_binary_row(&batch(vec![7], vec![0], None), 0, &[(0, KeyField::BigInt)]).unwrap(),
        128,
    );
    let snapshot = before.snapshot_key_group(key_group).unwrap();
    drop(before);
    let mut after = processor(&bytes, broker);
    after.restore_key_group(key_group, &snapshot).unwrap();
    assert_eq!(after.advance_event_time(9_999).unwrap().num_rows(), 0);
    let output = after.advance_event_time(17_999).unwrap();
    assert_eq!(output.num_rows(), 1);
    assert_eq!(
        output
            .column(1)
            .as_any()
            .downcast_ref::<Int64Array>()
            .unwrap()
            .value(0),
        1
    );
    assert_eq!(
        output
            .column(3)
            .as_any()
            .downcast_ref::<TimestampMillisecondArray>()
            .unwrap()
            .value(0),
        18_000
    );
}

#[test]
fn dst_timer_resolution_matches_flinks_gap_and_overlap_rules() {
    let zone = "America/Los_Angeles".parse::<Tz>().unwrap();
    let spring_gap =
        NaiveDateTime::parse_from_str("2021-03-14 02:59:59.999", "%Y-%m-%d %H:%M:%S%.3f").unwrap();
    assert_eq!(
        local_to_timer_epoch(spring_gap, zone).unwrap(),
        1_615_716_000_000
    );
    let fall_overlap =
        NaiveDateTime::parse_from_str("2021-11-07 01:59:00.000", "%Y-%m-%d %H:%M:%S%.3f").unwrap();
    assert_eq!(
        local_to_timer_epoch(fall_overlap, zone).unwrap(),
        1_636_279_140_000
    );
}
