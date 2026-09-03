// Copyright 2026 StreamFusion Authors
// Licensed under the Apache License, Version 2.0

use super::*;
use crate::memory_pool::{tests_support::TestBroker, HostMemoryReservation};
use arrow::array::{ArrayRef, Int64Array, StringArray};
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
    assert_eq!(processor.statistics(), [3, 3, 0, 0]);
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

fn processor(broker: Arc<TestBroker>, rows_frame: bool) -> OverAggregateProcessor {
    OverAggregateProcessor::new(
        &plan(rows_frame),
        128,
        0,
        127,
        HostMemoryReservation::new(broker, "OVER test"),
    )
    .unwrap()
}

fn plan(rows_frame: bool) -> Vec<u8> {
    let input = schema(&[("key", string()), ("order", bigint()), ("value", bigint())]);
    let output = schema(&[
        ("key", string()),
        ("order", bigint()),
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
                    time_attribute: proto::OverTimeAttribute::NonTime as i32,
                    aggregate_calls: vec![
                        call(proto::AggregateFunction::Sum, Some(2)),
                        call(proto::AggregateFunction::CountStar, None),
                    ],
                    input_schema: Some(input),
                    output_schema: Some(output),
                    input_changelog: true,
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
