// Copyright 2026 StreamFusion Authors
// Licensed under the Apache License, Version 2.0.

use super::super::tests::{field, logical_bigint, logical_timestamp, plan, processor};
use super::*;
use crate::memory_pool::tests_support::TestBroker;
use prost::Message;

fn partial_plan() -> Vec<u8> {
    let mut native =
        proto::NativePlan::decode(plan(proto::WindowKind::Hop, 6_000, 2_000, false).as_slice())
            .unwrap();
    let aggregate = match native.root.as_mut().unwrap().operator.as_mut().unwrap() {
        proto::operator::Operator::WindowAggregate(aggregate) => aggregate,
        _ => unreachable!(),
    };
    aggregate.time_attribute_index = 0;
    aggregate.partial_accumulator_index = Some(1);
    aggregate.partial_window_start_index = Some(2);
    aggregate.partial_slice_end_index = Some(3);
    aggregate.partial_windows_are_slices = true;
    aggregate.input_schema = Some(proto::Schema {
        fields: vec![
            field("key", logical_bigint(false)),
            field(
                "accumulator",
                proto::LogicalType {
                    nullable: false,
                    r#type: Some(proto::logical_type::Type::Binary(proto::EmptyType {})),
                },
            ),
            field("window_start", logical_bigint(false)),
            field("slice_end", logical_bigint(false)),
        ],
    });
    native.encode_to_vec()
}

fn partial_batch() -> RecordBatch {
    use crate::planner::operators::group_aggregate::Accumulator;
    let state = |count| {
        encode_state(&AccumulatorState {
            row_count: count,
            accumulators: vec![Accumulator::Count(count)],
        })
    };
    RecordBatch::try_new(
        Arc::new(Schema::new(vec![
            Field::new("key", DataType::Int64, false),
            Field::new("accumulator", DataType::Binary, false),
            Field::new("window_start", DataType::Int64, false),
            Field::new("slice_end", DataType::Int64, false),
        ])),
        vec![
            Arc::new(Int64Array::from(vec![1, 1])) as ArrayRef,
            Arc::new(BinaryArray::from_iter_values([state(1), state(2)])) as ArrayRef,
            Arc::new(Int64Array::from(vec![0, 2_000])) as ArrayRef,
            Arc::new(Int64Array::from(vec![2_000, 4_000])) as ArrayRef,
        ],
    )
    .unwrap()
}

fn partial_sum_plan() -> Vec<u8> {
    let mut native = proto::NativePlan::decode(partial_plan().as_slice()).unwrap();
    let aggregate = match native.root.as_mut().unwrap().operator.as_mut().unwrap() {
        proto::operator::Operator::WindowAggregate(aggregate) => aggregate,
        _ => unreachable!(),
    };
    aggregate.aggregate_calls.push(proto::AggregateCall {
        function: proto::AggregateFunction::Sum as i32,
        input_index: Some(0),
        input_type: Some(logical_bigint(false)),
        output_type: Some(logical_bigint(false)),
        retractable: true,
        filter_index: None,
        distinct: false,
        accumulator_type: None,
    });
    aggregate.output_schema = Some(proto::Schema {
        fields: vec![
            field("key", logical_bigint(false)),
            field("count", logical_bigint(false)),
            field("sum", logical_bigint(false)),
            field("window_start", logical_timestamp(false)),
            field("window_end", logical_timestamp(false)),
        ],
    });
    native.encode_to_vec()
}

fn single_partial_sum_batch(row_count: i64, sum: i128) -> RecordBatch {
    use crate::planner::operators::group_aggregate::{Accumulator, AggregateValue};
    let state = encode_state(&AccumulatorState {
        row_count,
        accumulators: vec![
            Accumulator::Count(row_count),
            Accumulator::Sum {
                value: Some(AggregateValue::Int(sum)),
                count: row_count,
            },
        ],
    });
    RecordBatch::try_new(
        Arc::new(Schema::new(vec![
            Field::new("key", DataType::Int64, false),
            Field::new("accumulator", DataType::Binary, false),
            Field::new("window_start", DataType::Int64, false),
            Field::new("slice_end", DataType::Int64, false),
        ])),
        vec![
            Arc::new(Int64Array::from(vec![1])) as ArrayRef,
            Arc::new(BinaryArray::from_iter_values([state])) as ArrayRef,
            Arc::new(Int64Array::from(vec![2_000])) as ArrayRef,
            Arc::new(Int64Array::from(vec![4_000])) as ArrayRef,
        ],
    )
    .unwrap()
}

#[test]
fn global_partial_input_merges_slices_with_one_state_batch() {
    let broker = Arc::new(TestBroker::new(16 << 20));
    let mut processor = processor(&partial_plan(), broker);
    assert_eq!(
        processor
            .process_arrow(partial_batch(), 0)
            .unwrap()
            .num_rows(),
        0
    );
    assert_eq!(processor.state_read_batches, 1);
    assert_eq!(processor.state_write_batches, 1);

    let first = processor.advance_event_time(1_999).unwrap();
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
    let second = processor.advance_event_time(3_999).unwrap();
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
}

#[test]
fn global_partial_input_merges_negative_accumulators_across_batches() {
    let broker = Arc::new(TestBroker::new(16 << 20));
    let mut processor = processor(&partial_sum_plan(), broker);
    processor
        .process_arrow(single_partial_sum_batch(1, 20), 0)
        .unwrap();
    processor
        .process_arrow(single_partial_sum_batch(-1, -20), 0)
        .unwrap();
    processor
        .process_arrow(single_partial_sum_batch(1, 5), 0)
        .unwrap();

    let output = processor.advance_event_time(7_999).unwrap();
    assert_eq!(output.num_rows(), 3);
    let sums = output
        .column(2)
        .as_any()
        .downcast_ref::<Int64Array>()
        .unwrap();
    assert_eq!(sums.values(), &[5, 5, 5]);
}

#[test]
fn global_partial_state_moves_from_memory_to_rocksdb_canonically() {
    let Ok(plugin_path) = std::env::var("STREAMFUSION_TEST_ROCKSDB_PLUGIN") else {
        return;
    };
    let broker = Arc::new(TestBroker::new(1 << 30));
    let bytes = partial_plan();
    let mut memory = processor(&bytes, broker.clone());
    memory.process_arrow(partial_batch(), 0).unwrap();
    let snapshots = (0..128)
        .map(|group| memory.snapshot_key_group(group).unwrap())
        .collect::<Vec<_>>();
    drop(memory);

    let directory = tempfile::tempdir().unwrap();
    let mut rocks = WindowAggregateProcessor::new_rocksdb(
        &bytes,
        128,
        0,
        127,
        std::path::Path::new(&plugin_path),
        directory.path(),
        64 << 20,
        HostMemoryReservation::new(broker, "global partial RocksDB scratch"),
    )
    .unwrap();
    for (group, snapshot) in snapshots.iter().enumerate() {
        rocks.restore_key_group(group as u32, snapshot).unwrap();
        assert_eq!(rocks.snapshot_key_group(group as u32).unwrap(), *snapshot);
    }
    let output = rocks.advance_event_time(3_999).unwrap();
    assert_eq!(output.num_rows(), 2);
    let mut counts = output
        .column(1)
        .as_any()
        .downcast_ref::<Int64Array>()
        .unwrap()
        .values()
        .to_vec();
    counts.sort_unstable();
    assert_eq!(counts, [1, 3]);
}

#[test]
fn partial_lateness_counts_inputs_after_the_last_hop_window_on_both_backends() {
    use crate::planner::operators::group_aggregate::Accumulator;
    for rocks in [false, true] {
        let plugin = std::env::var("STREAMFUSION_TEST_ROCKSDB_PLUGIN").ok();
        if rocks && plugin.is_none() {
            continue;
        }
        let directory = tempfile::tempdir().unwrap();
        let broker = Arc::new(TestBroker::new(1 << 30));
        let bytes = partial_plan();
        let mut native = if rocks {
            WindowAggregateProcessor::new_rocksdb(
                &bytes,
                128,
                0,
                127,
                std::path::Path::new(plugin.as_ref().unwrap()),
                directory.path(),
                64 << 20,
                HostMemoryReservation::new(broker.clone(), "partial lateness RocksDB"),
            )
            .unwrap()
        } else {
            processor(&bytes, broker.clone())
        };
        let input = |key, count| {
            let encoded = encode_state(&AccumulatorState {
                row_count: count,
                accumulators: vec![Accumulator::Count(count)],
            });
            RecordBatch::try_new(
                partial_batch().schema(),
                vec![
                    Arc::new(Int64Array::from(vec![key])) as ArrayRef,
                    Arc::new(BinaryArray::from_iter_values([encoded])) as ArrayRef,
                    Arc::new(Int64Array::from(vec![0])) as ArrayRef,
                    Arc::new(Int64Array::from(vec![2000])) as ArrayRef,
                ],
            )
            .unwrap()
        };
        native.process_arrow(input(1, 2), 0).unwrap();
        assert_eq!(native.advance_event_time(1999).unwrap().num_rows(), 1);
        native.process_arrow(input(2, 3), 0).unwrap();
        // Same sequence is checked against the SQL-generated Flink global slicer by
        // GlobalWindowFlinkControlContractTest: the first window has fired, but two remain.
        assert_eq!(native.late_records_dropped(), 0);
        for watermark in [3999, 5999] {
            let batch = native.advance_event_time(watermark).unwrap();
            let keys = batch
                .column(0)
                .as_any()
                .downcast_ref::<Int64Array>()
                .unwrap();
            let counts = batch
                .column(1)
                .as_any()
                .downcast_ref::<Int64Array>()
                .unwrap();
            let mut actual = (0..batch.num_rows())
                .map(|row| (keys.value(row), counts.value(row)))
                .collect::<Vec<_>>();
            actual.sort_unstable();
            assert_eq!(actual, vec![(1, 2), (2, 3)]);
        }
        assert_eq!(native.advance_event_time(7999).unwrap().num_rows(), 0);
        native.process_arrow(input(3, 11), 0).unwrap();
        assert_eq!(native.late_records_dropped(), 1);
        assert_eq!(native.advance_event_time(i64::MAX).unwrap().num_rows(), 0);
        drop(native);
        assert_eq!(broker.reserved(), 0);
    }
}
