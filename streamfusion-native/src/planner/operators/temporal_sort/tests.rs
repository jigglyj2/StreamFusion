// Copyright 2026 StreamFusion Authors
// Licensed under the Apache License, Version 2.0.

use super::legacy_state::{decode_rows, encode_rows};
use super::*;
use crate::memory_pool::{tests_support::TestBroker, HostMemoryReservation};
use arrow::array::{ArrayRef, Int32Array, StringArray, TimestampMillisecondArray};
use prost::Message;

#[test]
fn row_state_round_trips_every_changelog_kind() {
    let rows = vec![
        BufferedRow {
            kind: INSERT,
            sort_key: b"sort-insert".to_vec(),
            row: b"insert".to_vec(),
        },
        BufferedRow {
            kind: UPDATE_BEFORE,
            sort_key: b"sort-before".to_vec(),
            row: b"before".to_vec(),
        },
        BufferedRow {
            kind: UPDATE_AFTER,
            sort_key: b"sort-after".to_vec(),
            row: b"after".to_vec(),
        },
        BufferedRow {
            kind: DELETE,
            sort_key: b"sort-delete".to_vec(),
            row: b"delete".to_vec(),
        },
    ];
    assert_eq!(decode_rows(&encode_rows(&rows).unwrap()).unwrap(), rows);
}

#[test]
fn event_time_sorts_secondary_fields_stably_and_drops_already_fired_rows() {
    let broker = Arc::new(TestBroker::new(64 << 20));
    let mut processor = processor(false, broker.clone());
    processor
        .process_arrow(batch(
            &[1_000, 1_000, 2_000],
            &[2, 1, 9],
            &["second", "first", "later"],
            &[UPDATE_AFTER, DELETE, INSERT],
            None,
        ))
        .unwrap();
    let output = processor.advance_event_time(1_000).unwrap();

    assert_eq!(
        output
            .column(1)
            .as_any()
            .downcast_ref::<Int32Array>()
            .unwrap()
            .values(),
        &[1, 2]
    );
    assert_eq!(
        output
            .column(3)
            .as_any()
            .downcast_ref::<Int8Array>()
            .unwrap()
            .values(),
        &[DELETE, UPDATE_AFTER]
    );
    processor
        .process_arrow(batch(&[1_000], &[0], &["late"], &[INSERT], None))
        .unwrap();
    assert_eq!(processor.statistics()[7], 1);
    assert_eq!(processor.next_event_time_timer(), 2_000);
    drop(output);
    drop(processor);
    assert_eq!(broker.reserved(), 0);
}

#[test]
fn event_time_coalesces_all_due_timestamp_groups_in_timer_order() {
    let broker = Arc::new(TestBroker::new(64 << 20));
    let mut processor = processor(false, broker.clone());
    processor
        .process_arrow(batch(
            &[3_000, 1_000, 2_000, 1_000],
            &[1, 2, 4, 1],
            &["last", "second", "middle", "first"],
            &[INSERT, INSERT, INSERT, INSERT],
            None,
        ))
        .unwrap();

    let output = processor.advance_event_time(3_000).unwrap();
    assert_eq!(output.num_rows(), 4);
    assert_eq!(
        output
            .column(1)
            .as_any()
            .downcast_ref::<Int32Array>()
            .unwrap()
            .values(),
        &[1, 2, 4, 1]
    );
    assert_eq!(processor.statistics()[4], 3);
    assert_eq!(processor.statistics()[5], 0);
    drop(output);
    drop(processor);
    assert_eq!(broker.reserved(), 0);
}

#[test]
fn processing_time_and_timer_state_restore_from_canonical_key_group_bytes() {
    let source_broker = Arc::new(TestBroker::new(64 << 20));
    let mut source = processor(true, source_broker);
    source
        .process_arrow(batch(
            &[0, 0, 0],
            &[3, 1, 2],
            &["third", "first", "second"],
            &[INSERT, UPDATE_BEFORE, UPDATE_AFTER],
            Some(&[40, 40, 40]),
        ))
        .unwrap();
    let snapshot = source.snapshot_key_group(0).unwrap();

    let target_broker = Arc::new(TestBroker::new(64 << 20));
    let mut target = processor(true, target_broker.clone());
    target.restore_key_group(0, &snapshot).unwrap();
    assert_eq!(target.next_processing_time_timer(), 41);
    let output = target.advance_processing_time(41).unwrap();
    assert_eq!(
        output
            .column(1)
            .as_any()
            .downcast_ref::<Int32Array>()
            .unwrap()
            .values(),
        &[1, 2, 3]
    );
    assert_eq!(target.statistics()[6], 0);
    drop(output);
    drop(target);
    assert_eq!(target_broker.reserved(), 0);
}

#[test]
fn first_processing_time_callback_sorts_and_clears_the_complete_flink_list_state() {
    let broker = Arc::new(TestBroker::new(64 << 20));
    let mut processor = processor(true, broker.clone());
    processor
        .process_arrow(batch(
            &[0, 0, 0],
            &[3, 1, 2],
            &["third", "first", "second"],
            &[INSERT, INSERT, INSERT],
            Some(&[42, 40, 42]),
        ))
        .unwrap();

    let output = processor.advance_processing_time(41).unwrap();
    assert_eq!(
        output
            .column(1)
            .as_any()
            .downcast_ref::<Int32Array>()
            .unwrap()
            .values(),
        &[1, 2, 3]
    );
    assert_eq!(processor.next_processing_time_timer(), 43);
    assert_eq!(processor.advance_processing_time(43).unwrap().num_rows(), 0);
    drop(output);
    drop(processor);
    assert_eq!(broker.reserved(), 0);
}

fn processor(processing_time: bool, broker: Arc<TestBroker>) -> TemporalSortProcessor {
    TemporalSortProcessor::new(
        &plan(processing_time),
        1,
        0,
        0,
        HostMemoryReservation::new(broker, "temporal sort test"),
    )
    .unwrap()
}

pub(super) fn plan(processing_time: bool) -> Vec<u8> {
    proto::NativePlan {
        protocol_version: crate::PLAN_PROTOCOL_VERSION,
        root: Some(proto::Operator {
            plan_node_id: 0,
            metric_name: String::new(),
            clear_record_timestamps: false,
            metric_uid: None,
            operator: Some(proto::operator::Operator::TemporalSort(Box::new(
                proto::TemporalSort {
                    input: None,
                    input_schema: Some(proto::Schema {
                        fields: vec![
                            proto_field(
                                "rowtime",
                                proto::logical_type::Type::Timestamp(proto::PrecisionType {
                                    precision: 3,
                                }),
                            ),
                            proto_field(
                                "number",
                                proto::logical_type::Type::Integer(proto::EmptyType::default()),
                            ),
                            proto_field(
                                "payload",
                                proto::logical_type::Type::Varchar(proto::EmptyType::default()),
                            ),
                        ],
                    }),
                    time_index: 0,
                    processing_time,
                    secondary_key_indices: vec![1],
                    secondary_ascending: vec![true],
                    secondary_nulls_last: vec![true],
                },
            ))),
        }),
    }
    .encode_to_vec()
}

fn proto_field(name: &str, r#type: proto::logical_type::Type) -> proto::Field {
    proto::Field {
        name: name.to_string(),
        r#type: Some(proto::LogicalType {
            nullable: false,
            r#type: Some(r#type),
        }),
    }
}

fn batch(
    timestamps: &[i64],
    values: &[i32],
    payloads: &[&str],
    kinds: &[i8],
    processing_times: Option<&[i64]>,
) -> RecordBatch {
    let mut columns = vec![
        (
            "rowtime",
            Arc::new(TimestampMillisecondArray::from(timestamps.to_vec())) as ArrayRef,
        ),
        (
            "number",
            Arc::new(Int32Array::from(values.to_vec())) as ArrayRef,
        ),
        (
            "payload",
            Arc::new(StringArray::from(payloads.to_vec())) as ArrayRef,
        ),
        (
            INPUT_KIND_COLUMN,
            Arc::new(Int8Array::from(kinds.to_vec())) as ArrayRef,
        ),
    ];
    if let Some(processing_times) = processing_times {
        columns.push((
            PROCESSING_TIME_COLUMN,
            Arc::new(Int64Array::from(processing_times.to_vec())) as ArrayRef,
        ));
    }
    RecordBatch::try_from_iter(columns).unwrap()
}

#[test]
fn processing_callbacks_bound_timer_work_but_clear_all_pending_rows_on_the_first_callback() {
    let broker = Arc::new(TestBroker::new(64 << 20));
    let mut processor = processor(true, broker.clone());
    let count = MAX_TIMERS_PER_OUTPUT + 113;
    let numbers = (0..count)
        .map(|index| (index % 7) as i32)
        .collect::<Vec<_>>();
    let timestamps = (0..count)
        .map(|index| index as i64 + 100)
        .collect::<Vec<_>>();
    processor
        .process_arrow(batch(
            &vec![0; count],
            &numbers,
            &vec!["payload"; count],
            &vec![INSERT; count],
            Some(&timestamps),
        ))
        .unwrap();
    let output = processor.advance_processing_time(i64::MAX).unwrap();
    assert_eq!(
        output.num_rows(),
        count,
        "Flink's first callback clears the complete logical list"
    );
    assert_eq!(processor.statistics()[4], MAX_TIMERS_PER_OUTPUT as u64);
    assert_eq!(processor.statistics()[6], 113);
    assert_eq!(
        processor
            .advance_processing_time(i64::MAX)
            .unwrap()
            .num_rows(),
        0
    );
    assert_eq!(processor.statistics()[4], count as u64);
    assert_eq!(processor.statistics()[6], 0);
    let snapshot = processor.snapshot_key_group(0).unwrap();
    assert!(crate::state::decode_key_group_snapshot(0, &snapshot)
        .unwrap()
        .is_empty());
    drop(snapshot);
    drop(output);
    drop(processor);
    assert_eq!(broker.reserved(), 0);
}
