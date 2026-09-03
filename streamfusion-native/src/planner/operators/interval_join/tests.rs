// Copyright 2026 StreamFusion Authors
// Licensed under the Apache License, Version 2.0

use super::*;
use crate::memory_pool::{tests_support::TestBroker, HostMemoryReservation};
use arrow::array::{ArrayRef, Int64Array, StringArray, TimestampMillisecondArray};
use prost::Message;

#[test]
fn event_time_inner_join_respects_bounds_and_retractions() {
    let broker = Arc::new(TestBroker::new(64 << 20));
    let mut processor = processor(broker.clone(), proto::RegularJoinType::Inner, true, 0, 127);
    assert_eq!(
        kinds(
            &processor
                .process_arrow(
                    0,
                    batch(&[1, 1], &[1_000, 2_000], &["a", "b"], &[INSERT; 2]),
                    0,
                )
                .unwrap(),
        ),
        Vec::<i8>::new(),
    );
    assert_eq!(
        kinds(
            &processor
                .process_arrow(
                    1,
                    batch(&[1, 1], &[1_500, 4_001], &["x", "y"], &[INSERT; 2]),
                    0,
                )
                .unwrap(),
        ),
        vec![INSERT, INSERT],
    );
    assert_eq!(
        kinds(
            &processor
                .process_arrow(1, batch(&[1], &[1_500], &["x"], &[DELETE]), 0)
                .unwrap(),
        ),
        vec![DELETE, DELETE],
    );
    drop(processor);
    assert_eq!(broker.reserved(), 0);
}

#[test]
fn full_join_delays_unmatched_rows_until_the_cleanup_timer() {
    let broker = Arc::new(TestBroker::new(64 << 20));
    let mut processor = processor(broker, proto::RegularJoinType::Full, true, 0, 127);
    assert_eq!(
        kinds(
            &processor
                .process_arrow(0, batch(&[1], &[1_000], &["left"], &[INSERT]), 0)
                .unwrap(),
        ),
        Vec::<i8>::new(),
    );
    assert_eq!(
        kinds(&processor.advance_event_time(2_000).unwrap()),
        Vec::<i8>::new(),
    );
    assert_eq!(
        kinds(&processor.advance_event_time(2_001).unwrap()),
        vec![INSERT],
    );
    assert_eq!(processor.statistics()[5], 0);
}

#[test]
fn processing_time_uses_the_batch_clock_and_exposes_the_next_timer() {
    let broker = Arc::new(TestBroker::new(64 << 20));
    let mut processor = processor(broker, proto::RegularJoinType::Left, false, 0, 127);
    processor
        .process_arrow(0, batch(&[1], &[99], &["left"], &[INSERT]), 5_000)
        .unwrap();
    assert_eq!(processor.next_processing_timer(), Some(6_001));
    assert_eq!(
        kinds(&processor.advance_processing_time(6_001).unwrap()),
        vec![INSERT],
    );
}

#[test]
fn canonical_timer_and_rows_restore_after_rescaling() {
    let broker = Arc::new(TestBroker::new(1 << 30));
    let mut source = processor(broker.clone(), proto::RegularJoinType::Full, true, 0, 127);
    source
        .process_arrow(
            0,
            batch(&[1, 2], &[1_000, 2_000], &["a", "b"], &[INSERT; 2]),
            0,
        )
        .unwrap();
    let snapshots = (0..128)
        .map(|group| source.snapshot_key_group(group).unwrap())
        .collect::<Vec<_>>();
    let mut low = processor(broker.clone(), proto::RegularJoinType::Full, true, 0, 63);
    let mut high = processor(broker, proto::RegularJoinType::Full, true, 64, 127);
    for (group, snapshot) in snapshots.iter().enumerate() {
        if group < 64 {
            low.restore_key_group(group as u32, snapshot).unwrap();
        } else {
            high.restore_key_group(group as u32, snapshot).unwrap();
        }
    }
    let mut emitted = 0;
    for processor in [&mut low, &mut high] {
        emitted += processor.advance_event_time(10_000).unwrap().num_rows();
    }
    assert_eq!(emitted, 2);
}

fn processor(
    broker: Arc<TestBroker>,
    join_type: proto::RegularJoinType,
    event_time: bool,
    first: u32,
    last: u32,
) -> IntervalJoinProcessor {
    IntervalJoinProcessor::new(
        &plan(join_type, event_time),
        128,
        first,
        last,
        HostMemoryReservation::new(broker, "interval join test"),
    )
    .unwrap()
}

fn plan(join_type: proto::RegularJoinType, event_time: bool) -> Vec<u8> {
    proto::NativePlan {
        protocol_version: crate::PLAN_PROTOCOL_VERSION,
        root: Some(proto::Operator {
            operator: Some(proto::operator::Operator::IntervalJoin(
                proto::IntervalJoin {
                    left_key_indices: vec![0],
                    right_key_indices: vec![0],
                    filter_nulls: vec![true],
                    left_schema: Some(schema()),
                    right_schema: Some(schema()),
                    join_type: join_type as i32,
                    event_time,
                    left_lower_bound_millis: -1_000,
                    left_upper_bound_millis: 2_000,
                    left_time_index: 1,
                    right_time_index: 1,
                    allowed_lateness_millis: 0,
                    min_cleanup_interval_millis: 0,
                },
            )),
        }),
    }
    .encode_to_vec()
}

fn schema() -> proto::Schema {
    proto::Schema {
        fields: vec![
            field(
                "key",
                proto::logical_type::Type::Bigint(proto::EmptyType::default()),
            ),
            field(
                "time",
                proto::logical_type::Type::Timestamp(proto::PrecisionType { precision: 3 }),
            ),
            field(
                "value",
                proto::logical_type::Type::Varchar(proto::EmptyType::default()),
            ),
        ],
    }
}

fn field(name: &str, r#type: proto::logical_type::Type) -> proto::Field {
    proto::Field {
        name: name.to_string(),
        r#type: Some(proto::LogicalType {
            nullable: true,
            r#type: Some(r#type),
        }),
    }
}

fn batch(keys: &[i64], times: &[i64], values: &[&str], row_kinds: &[i8]) -> RecordBatch {
    RecordBatch::try_from_iter(vec![
        ("key", Arc::new(Int64Array::from(keys.to_vec())) as ArrayRef),
        (
            "time",
            Arc::new(TimestampMillisecondArray::from(times.to_vec())) as ArrayRef,
        ),
        (
            "value",
            Arc::new(StringArray::from(values.to_vec())) as ArrayRef,
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
