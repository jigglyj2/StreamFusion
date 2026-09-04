// Copyright 2026 StreamFusion Authors
// Licensed under the Apache License, Version 2.0

use super::*;
use crate::memory_pool::{tests_support::TestBroker, HostMemoryReservation};
use arrow::array::{ArrayRef, Int64Array, StringArray, TimestampMillisecondArray};
use prost::Message;

#[test]
fn rows_frame_recomputes_ordered_suffix_for_insert_and_retraction() {
    let broker = Arc::new(TestBroker::new(64 << 20));
    let mut processor = processor(broker.clone(), true);

    let first = processor
        .process_arrow(batch(&["a"], &[2], &[20], &[INSERT]))
        .unwrap();
    assert_eq!(kinds(&first), vec![INSERT]);
    assert_eq!(sums(&first), vec![20]);

    let inserted_before = processor
        .process_arrow(batch(&["a"], &[1], &[10], &[INSERT]))
        .unwrap();
    assert_eq!(
        kinds(&inserted_before),
        vec![INSERT, UPDATE_BEFORE, UPDATE_AFTER]
    );
    assert_eq!(sums(&inserted_before), vec![10, 20, 30]);

    let removed = processor
        .process_arrow(batch(&["a"], &[1], &[10], &[DELETE]))
        .unwrap();
    assert_eq!(kinds(&removed), vec![DELETE, UPDATE_BEFORE, UPDATE_AFTER]);
    assert_eq!(sums(&removed), vec![10, 30, 20]);
    assert_eq!(processor.statistics(), [3, 3, 0, 0, 0, 0, 0, 0, 0]);
    drop(processor);
    assert_eq!(broker.reserved(), 0);
}

#[test]
fn update_before_is_canonicalized_to_delete_like_flink() {
    let broker = Arc::new(TestBroker::new(64 << 20));
    let mut processor = processor(broker, true);
    processor
        .process_arrow(batch(&["a"], &[1], &[10], &[INSERT]))
        .unwrap();

    let removed = processor
        .process_arrow(batch(&["a"], &[1], &[10], &[UPDATE_BEFORE]))
        .unwrap();
    assert_eq!(kinds(&removed), vec![DELETE]);
    assert_eq!(sums(&removed), vec![10]);
}

#[test]
fn range_peers_share_one_aggregate_and_restore_canonical_state() {
    let broker = Arc::new(TestBroker::new(128 << 20));
    let mut source = processor(broker.clone(), false);
    let output = source
        .process_arrow(batch(&["a", "a"], &[1, 1], &[10, 20], &[INSERT; 2]))
        .unwrap();
    assert_eq!(
        kinds(&output),
        vec![INSERT, UPDATE_BEFORE, UPDATE_AFTER, INSERT]
    );
    assert_eq!(sums(&output), vec![10, 10, 30, 30]);

    let snapshots = (0..128)
        .map(|group| source.snapshot_key_group(group).unwrap())
        .collect::<Vec<_>>();
    let mut restored = processor(broker, false);
    for (group, snapshot) in snapshots.iter().enumerate() {
        restored.restore_key_group(group as u32, snapshot).unwrap();
    }
    let output = restored
        .process_arrow(batch(&["a"], &[2], &[5], &[INSERT]))
        .unwrap();
    assert_eq!(kinds(&output), vec![INSERT]);
    assert_eq!(sums(&output), vec![35]);
}

#[test]
fn processing_time_rows_and_range_are_incremental_in_arrival_order() {
    for rows_frame in [true, false] {
        let broker = Arc::new(TestBroker::new(64 << 20));
        let mut processor = processor_with_time(
            broker.clone(),
            rows_frame,
            proto::OverTimeAttribute::ProcessingTime,
        );
        let output = processor
            .process_arrow(batch(&["a", "a"], &[7, 7], &[10, 20], &[INSERT; 2]))
            .unwrap();
        assert_eq!(kinds(&output), vec![INSERT, INSERT]);
        assert_eq!(sums(&output), vec![10, 30]);

        let removed = processor
            .process_arrow(batch(&["a"], &[7], &[10], &[DELETE]))
            .unwrap();
        assert_eq!(kinds(&removed), vec![DELETE, UPDATE_BEFORE, UPDATE_AFTER]);
        assert_eq!(sums(&removed), vec![10, 30, 20]);
        drop(processor);
        assert_eq!(broker.reserved(), 0);
    }
}

#[test]
fn append_only_processing_time_uses_bounded_accumulator_state_and_restores_legacy_rows() {
    let legacy_broker = Arc::new(TestBroker::new(64 << 20));
    let mut legacy = processor_with_time(
        legacy_broker.clone(),
        true,
        proto::OverTimeAttribute::ProcessingTime,
    );
    legacy
        .process_arrow(batch(&["a", "a"], &[7, 7], &[10, 20], &[INSERT; 2]))
        .unwrap();
    let legacy_snapshots = (0..128)
        .map(|group| legacy.snapshot_key_group(group).unwrap())
        .collect::<Vec<_>>();
    drop(legacy);
    assert_eq!(legacy_broker.reserved(), 0);

    let broker = Arc::new(TestBroker::new(64 << 20));
    let mut compact = OverAggregateProcessor::new(
        &plan(true, proto::OverTimeAttribute::ProcessingTime, false),
        128,
        0,
        127,
        HostMemoryReservation::new(broker.clone(), "compact OVER test"),
    )
    .unwrap();
    for (group, snapshot) in legacy_snapshots.iter().enumerate() {
        compact.restore_key_group(group as u32, snapshot).unwrap();
    }
    let output = compact
        .process_arrow(batch(&["a"], &[7], &[5], &[INSERT]))
        .unwrap();
    assert_eq!(sums(&output), vec![35]);
    drop(output);

    let first_size = (0..128)
        .map(|group| compact.snapshot_key_group(group).unwrap().len())
        .sum::<usize>();
    let keys = vec!["a"; 2_000];
    let order = vec![7; 2_000];
    let values = vec![1; 2_000];
    let kinds = vec![INSERT; 2_000];
    let output = compact
        .process_arrow(batch(&keys, &order, &values, &kinds))
        .unwrap();
    assert_eq!(sums(&output).last(), Some(&2_035));
    drop(output);
    let second_size = (0..128)
        .map(|group| compact.snapshot_key_group(group).unwrap().len())
        .sum::<usize>();
    assert_eq!(second_size, first_size);
    drop(compact);
    assert_eq!(broker.reserved(), 0);
}

#[test]
fn event_time_waits_for_watermarks_orders_rows_and_drops_late_input() {
    let broker = Arc::new(TestBroker::new(64 << 20));
    let mut processor =
        processor_with_time(broker.clone(), true, proto::OverTimeAttribute::EventTime);
    let buffered = processor
        .process_arrow(event_batch(
            &["a", "a"],
            &[2_000, 1_000],
            &[20, 10],
            &[INSERT; 2],
        ))
        .unwrap();
    assert_eq!(buffered.num_rows(), 0);
    assert_eq!(processor.statistics()[7], 2);

    let first = processor.advance_event_time(1_500).unwrap();
    assert_eq!(kinds(&first), vec![INSERT]);
    assert_eq!(sums(&first), vec![10]);
    let second = processor.advance_event_time(2_000).unwrap();
    assert_eq!(kinds(&second), vec![INSERT]);
    assert_eq!(sums(&second), vec![30]);

    processor
        .process_arrow(event_batch(&["a"], &[1_500], &[5], &[INSERT]))
        .unwrap();
    assert_eq!(processor.late_records_dropped(), 1);
    drop(processor);
    assert_eq!(broker.reserved(), 0);
}

#[test]
fn event_time_range_peers_share_the_same_watermark_result() {
    let broker = Arc::new(TestBroker::new(64 << 20));
    let mut processor =
        processor_with_time(broker.clone(), false, proto::OverTimeAttribute::EventTime);
    assert_eq!(
        processor
            .process_arrow(event_batch(
                &["a", "a"],
                &[1_000, 1_000],
                &[10, 20],
                &[INSERT; 2],
            ))
            .unwrap()
            .num_rows(),
        0
    );

    let output = processor.advance_event_time(1_000).unwrap();
    assert_eq!(kinds(&output), vec![INSERT, INSERT]);
    assert_eq!(sums(&output), vec![30, 30]);
    drop(processor);
    assert_eq!(broker.reserved(), 0);
}

fn processor(broker: Arc<TestBroker>, rows_frame: bool) -> OverAggregateProcessor {
    processor_with_time(broker, rows_frame, proto::OverTimeAttribute::NonTime)
}

fn processor_with_time(
    broker: Arc<TestBroker>,
    rows_frame: bool,
    time_attribute: proto::OverTimeAttribute,
) -> OverAggregateProcessor {
    OverAggregateProcessor::new(
        &plan(rows_frame, time_attribute, true),
        128,
        0,
        127,
        HostMemoryReservation::new(broker, "OVER test"),
    )
    .unwrap()
}

fn plan(
    rows_frame: bool,
    time_attribute: proto::OverTimeAttribute,
    input_changelog: bool,
) -> Vec<u8> {
    let order_type = if time_attribute == proto::OverTimeAttribute::EventTime {
        timestamp()
    } else {
        bigint()
    };
    let input = schema(&[
        ("key", string()),
        ("order", order_type.clone()),
        ("value", bigint()),
    ]);
    let output = schema(&[
        ("key", string()),
        ("order", order_type),
        ("value", bigint()),
        ("running_sum", bigint()),
        ("running_count", bigint()),
    ]);
    proto::NativePlan {
        protocol_version: crate::PLAN_PROTOCOL_VERSION,
        root: Some(proto::Operator {
            operator: Some(proto::operator::Operator::OverAggregate(Box::new(
                proto::OverAggregate {
                    input: Some(Box::new(proto::Operator {
                        operator: Some(proto::operator::Operator::Input(proto::Input {
                            schema: Some(input.clone()),
                            input_index: 0,
                        })),
                    })),
                    partition_key_indices: vec![0],
                    order_key_index: 1,
                    rows_frame,
                    preceding_offset: None,
                    time_attribute: time_attribute as i32,
                    aggregate_calls: vec![
                        call(proto::AggregateFunction::Sum, Some(2)),
                        call(proto::AggregateFunction::CountStar, None),
                    ],
                    input_schema: Some(input),
                    output_schema: Some(output),
                    input_changelog,
                    state_ttl_millis: 0,
                    sort_ascending: true,
                    sort_nulls_last: false,
                },
            ))),
        }),
    }
    .encode_to_vec()
}

fn call(function: proto::AggregateFunction, input_index: Option<u32>) -> proto::AggregateCall {
    proto::AggregateCall {
        function: function as i32,
        input_index,
        input_type: input_index.map(|_| bigint()),
        output_type: Some(bigint()),
        retractable: true,
    }
}

fn schema(fields: &[(&str, proto::LogicalType)]) -> proto::Schema {
    proto::Schema {
        fields: fields
            .iter()
            .map(|(name, r#type)| proto::Field {
                name: (*name).to_string(),
                r#type: Some(r#type.clone()),
            })
            .collect(),
    }
}

fn string() -> proto::LogicalType {
    proto::LogicalType {
        nullable: true,
        r#type: Some(proto::logical_type::Type::Varchar(
            proto::EmptyType::default(),
        )),
    }
}

fn bigint() -> proto::LogicalType {
    proto::LogicalType {
        nullable: true,
        r#type: Some(proto::logical_type::Type::Bigint(
            proto::EmptyType::default(),
        )),
    }
}

fn timestamp() -> proto::LogicalType {
    proto::LogicalType {
        nullable: true,
        r#type: Some(proto::logical_type::Type::Timestamp(proto::PrecisionType {
            precision: 3,
        })),
    }
}

fn batch(keys: &[&str], order: &[i64], values: &[i64], row_kinds: &[i8]) -> RecordBatch {
    RecordBatch::try_from_iter(vec![
        (
            "key",
            Arc::new(StringArray::from(keys.to_vec())) as ArrayRef,
        ),
        (
            "order",
            Arc::new(Int64Array::from(order.to_vec())) as ArrayRef,
        ),
        (
            "value",
            Arc::new(Int64Array::from(values.to_vec())) as ArrayRef,
        ),
        (
            "__streamfusion_input_row_kind",
            Arc::new(Int8Array::from(row_kinds.to_vec())) as ArrayRef,
        ),
    ])
    .unwrap()
}

fn event_batch(keys: &[&str], order: &[i64], values: &[i64], row_kinds: &[i8]) -> RecordBatch {
    RecordBatch::try_from_iter(vec![
        (
            "key",
            Arc::new(StringArray::from(keys.to_vec())) as ArrayRef,
        ),
        (
            "order",
            Arc::new(TimestampMillisecondArray::from(order.to_vec())) as ArrayRef,
        ),
        (
            "value",
            Arc::new(Int64Array::from(values.to_vec())) as ArrayRef,
        ),
        (
            "__streamfusion_input_row_kind",
            Arc::new(Int8Array::from(row_kinds.to_vec())) as ArrayRef,
        ),
    ])
    .unwrap()
}

fn kinds(batch: &RecordBatch) -> Vec<i8> {
    batch
        .column(batch.num_columns() - 2)
        .as_any()
        .downcast_ref::<Int8Array>()
        .unwrap()
        .values()
        .to_vec()
}

fn sums(batch: &RecordBatch) -> Vec<i64> {
    batch
        .column(3)
        .as_any()
        .downcast_ref::<Int64Array>()
        .unwrap()
        .values()
        .to_vec()
}
