// Copyright 2026 StreamFusion Authors
// Licensed under the Apache License, Version 2.0.

use super::*;
use crate::memory_pool::tests_support::TestBroker;
use crate::planner::operators::group_aggregate::decode_state;
use arrow::array::{Int64Array, Int8Array};
use arrow::datatypes::{DataType, Field, Schema};

mod execution_plan;
mod memory;

fn logical_bigint(nullable: bool) -> proto::LogicalType {
    proto::LogicalType {
        nullable,
        r#type: Some(proto::logical_type::Type::Bigint(proto::EmptyType {})),
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

fn call(function: proto::AggregateFunction, input: Option<u32>) -> proto::AggregateCall {
    proto::AggregateCall {
        function: function as i32,
        input_index: input,
        input_type: input.map(|_| logical_bigint(true)),
        output_type: Some(logical_bigint(
            function != proto::AggregateFunction::CountStar,
        )),
        retractable: true,
        filter_index: None,
        distinct: false,
        accumulator_type: None,
    }
}

fn plan(size: u64, changelog: bool) -> Vec<u8> {
    proto::NativePlan {
        protocol_version: crate::PLAN_PROTOCOL_VERSION,
        root: Some(proto::Operator {
            plan_node_id: 0,
            metric_name: String::new(),
            clear_record_timestamps: false,
            metric_uid: None,
            operator: Some(proto::operator::Operator::LocalGroupAggregate(Box::new(
                proto::LocalGroupAggregate {
                    input: None,
                    grouping_indices: vec![0],
                    aggregate_calls: vec![
                        call(proto::AggregateFunction::CountStar, None),
                        call(proto::AggregateFunction::Sum, Some(1)),
                    ],
                    input_changelog: changelog,
                    mini_batch_size: size,
                    input_schema: Some(schema(&[
                        ("key", logical_bigint(false)),
                        ("value", logical_bigint(true)),
                    ])),
                    output_schema: Some(proto::Schema {
                        fields: vec![
                            proto::Field {
                                name: "key".to_string(),
                                r#type: Some(logical_bigint(false)),
                            },
                            proto::Field {
                                name: "accumulator".to_string(),
                                r#type: Some(proto::LogicalType {
                                    nullable: false,
                                    r#type: Some(proto::logical_type::Type::Binary(
                                        proto::EmptyType {},
                                    )),
                                }),
                            },
                        ],
                    }),
                    bounded_batch: false,
                },
            ))),
        }),
    }
    .encode_to_vec()
}

fn processor(size: u64, changelog: bool) -> LocalGroupAggregateProcessor {
    LocalGroupAggregateProcessor::new(
        &plan(size, changelog),
        HostMemoryReservation::new(Arc::new(TestBroker::new(1 << 20)), "local aggregate test"),
    )
    .unwrap()
}

fn bounded_processor() -> LocalGroupAggregateProcessor {
    let native = proto::NativePlan::decode(plan(1, false).as_slice()).unwrap();
    let mut aggregate = match native.root.unwrap().operator.unwrap() {
        proto::operator::Operator::LocalGroupAggregate(aggregate) => *aggregate,
        _ => unreachable!(),
    };
    aggregate.mini_batch_size = 0;
    aggregate.bounded_batch = true;
    let plan = proto::NativePlan {
        protocol_version: crate::PLAN_PROTOCOL_VERSION,
        root: Some(proto::Operator {
            plan_node_id: 0,
            metric_name: String::new(),
            clear_record_timestamps: false,
            metric_uid: None,
            operator: Some(proto::operator::Operator::LocalGroupAggregate(Box::new(
                aggregate,
            ))),
        }),
    }
    .encode_to_vec();
    LocalGroupAggregateProcessor::new(
        &plan,
        HostMemoryReservation::new(
            Arc::new(TestBroker::new(1 << 20)),
            "bounded local aggregate test",
        ),
    )
    .unwrap()
}

fn batch(keys: Vec<i64>, values: Vec<i64>, kinds: Option<Vec<i8>>) -> RecordBatch {
    let mut fields = vec![
        Field::new("key", DataType::Int64, false),
        Field::new("value", DataType::Int64, true),
    ];
    let mut columns = vec![
        Arc::new(Int64Array::from(keys)) as ArrayRef,
        Arc::new(Int64Array::from(values)) as ArrayRef,
    ];
    if let Some(kinds) = kinds {
        fields.push(Field::new(
            "__streamfusion_input_row_kind",
            DataType::Int8,
            false,
        ));
        columns.push(Arc::new(Int8Array::from(kinds)) as ArrayRef);
    }
    RecordBatch::try_new(Arc::new(Schema::new(fields)), columns).unwrap()
}

#[test]
fn emits_one_opaque_delta_per_key_at_exact_bundle_boundaries() {
    let mut processor = processor(3, false);
    assert_eq!(
        processor
            .process_arrow(batch(vec![1, 1], vec![10, 20], None))
            .unwrap()
            .num_rows(),
        0
    );
    let output = processor
        .process_arrow(batch(vec![2, 1], vec![5, 7], None))
        .unwrap();
    assert_eq!(output.num_rows(), 2);
    let keys = output
        .column(0)
        .as_any()
        .downcast_ref::<Int64Array>()
        .unwrap();
    assert_eq!(keys.values(), &[2, 1]);
    let encoded = output
        .column(1)
        .as_any()
        .downcast_ref::<BinaryArray>()
        .unwrap();
    let calls = processor.calls.clone();
    let first = decode_state(encoded.value(1), &calls).unwrap();
    assert_eq!(first.row_count, 2);
    assert_eq!(
        first.values(&calls)[1],
        Some(super::super::group_aggregate::AggregateValue::Int(30))
    );
    assert_eq!(processor.pending_element_count(), 1);
    assert_eq!(processor.finish_bundle().unwrap().num_rows(), 1);
}

#[test]
fn bounded_mode_emits_one_opaque_partial_per_key_for_each_arrow_batch() {
    let mut processor = bounded_processor();
    let first = processor
        .process_arrow(batch(vec![1, 1, 2], vec![10, 20, 5], None))
        .unwrap();
    assert_eq!(first.num_rows(), 2);
    assert_eq!(processor.pending_element_count(), 0);
    assert_eq!(processor.pending_key_count(), 0);

    let second = processor
        .process_arrow(batch(vec![1, 3], vec![7, 9], None))
        .unwrap();
    assert_eq!(second.num_rows(), 2);
    assert_eq!(processor.finish_bundle().unwrap().num_rows(), 0);
}

#[test]
fn preserves_negative_retraction_deltas_for_the_global_stage() {
    let mut processor = processor(10, true);
    processor
        .process_arrow(batch(vec![1], vec![7], Some(vec![3])))
        .unwrap();
    let output = processor.finish_bundle().unwrap();
    let encoded = output
        .column(1)
        .as_any()
        .downcast_ref::<BinaryArray>()
        .unwrap();
    let state = decode_state(encoded.value(0), &processor.calls).unwrap();
    assert_eq!(state.row_count, -1);
    assert_eq!(
        state.values(&processor.calls)[1],
        Some(super::super::group_aggregate::AggregateValue::Int(-7))
    );
}

#[test]
fn accounts_pending_and_output_memory_through_the_host_broker() {
    let broker = Arc::new(TestBroker::new(1 << 20));
    let reservation = HostMemoryReservation::new(broker.clone(), "local aggregate accounting test");
    let mut processor = LocalGroupAggregateProcessor::new(&plan(10, false), reservation).unwrap();

    let empty = processor
        .process_arrow(batch(vec![1, 1, 2], vec![10, 20, 30], None))
        .unwrap();
    assert_eq!(empty.num_rows(), 0);
    assert!(broker.reserved() > 0, "pending hash state must be reserved");

    let output = processor.finish_bundle().unwrap();
    assert_eq!(output.num_rows(), 2);
    assert!(
        processor.pending_reservation.size() > 0,
        "empty hash map retains admitted capacity"
    );
    assert_eq!(
        broker.reserved(),
        processor.pending_reservation.size()
            + processor._plan_reservation.size()
            + processor._schema_reservation.size(),
        "only output credit transfers to Arrow; the live processor retains its plan and map"
    );
    drop(processor);
    assert_eq!(broker.reserved(), 0);
}
