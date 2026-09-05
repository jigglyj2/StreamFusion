use super::*;
use crate::memory_pool::{tests_support::TestBroker, HostMemoryReservation};
use crate::state::KeyedState;
use arrow::array::{BooleanArray, Float32Array, Int64Array, StringArray};
use prost::Message;
use std::borrow::Cow;
use std::sync::atomic::{AtomicUsize, Ordering};

fn logical_bigint(nullable: bool) -> proto::LogicalType {
    proto::LogicalType {
        nullable,
        r#type: Some(proto::logical_type::Type::Bigint(proto::EmptyType {})),
    }
}

fn logical_varchar(nullable: bool) -> proto::LogicalType {
    proto::LogicalType {
        nullable,
        r#type: Some(proto::logical_type::Type::Varchar(proto::EmptyType {})),
    }
}

fn group_schema() -> proto::Schema {
    proto::Schema {
        fields: vec![
            proto::Field {
                name: "bidder".to_string(),
                r#type: Some(logical_bigint(false)),
            },
            proto::Field {
                name: "price".to_string(),
                r#type: Some(logical_bigint(true)),
            },
        ],
    }
}

fn group_output_schema() -> proto::Schema {
    proto::Schema {
        fields: vec![
            proto::Field {
                name: "bidder".to_string(),
                r#type: Some(logical_bigint(false)),
            },
            proto::Field {
                name: "count".to_string(),
                r#type: Some(logical_bigint(false)),
            },
            proto::Field {
                name: "sum".to_string(),
                r#type: Some(logical_bigint(true)),
            },
            proto::Field {
                name: "min".to_string(),
                r#type: Some(logical_bigint(true)),
            },
            proto::Field {
                name: "max".to_string(),
                r#type: Some(logical_bigint(true)),
            },
        ],
    }
}

fn plan(input_changelog: bool, update_before: bool) -> Vec<u8> {
    plan_with_grouping(input_changelog, update_before, vec![0])
}

fn plan_with_grouping(
    input_changelog: bool,
    update_before: bool,
    grouping_indices: Vec<u32>,
) -> Vec<u8> {
    let call =
        |function: proto::AggregateFunction, input_index: Option<u32>| proto::AggregateCall {
            function: function as i32,
            input_index,
            input_type: input_index.map(|_| logical_bigint(true)),
            output_type: Some(logical_bigint(
                function != proto::AggregateFunction::CountStar,
            )),
            retractable: input_changelog,
            filter_index: None,
            distinct: false,
            accumulator_type: None,
        };
    proto::NativePlan {
        protocol_version: crate::PLAN_PROTOCOL_VERSION,
        root: Some(proto::Operator {
            plan_node_id: 0,
            operator: Some(proto::operator::Operator::GroupAggregate(Box::new(
                proto::GroupAggregate {
                    input: Some(Box::new(proto::Operator {
                        plan_node_id: 0,
                        operator: Some(proto::operator::Operator::Input(proto::Input {
                            schema: None,
                            input_index: 0,
                        })),
                    })),
                    grouping_indices,
                    aggregate_calls: vec![
                        call(proto::AggregateFunction::CountStar, None),
                        call(proto::AggregateFunction::Sum, Some(1)),
                        call(proto::AggregateFunction::Min, Some(1)),
                        call(proto::AggregateFunction::Max, Some(1)),
                    ],
                    generate_update_before: update_before,
                    input_changelog,
                    ..Default::default()
                },
            ))),
        }),
    }
    .encode_to_vec()
}

fn distinct_plan() -> Vec<u8> {
    proto::NativePlan {
        protocol_version: crate::PLAN_PROTOCOL_VERSION,
        root: Some(proto::Operator {
            plan_node_id: 0,
            operator: Some(proto::operator::Operator::GroupAggregate(Box::new(
                proto::GroupAggregate {
                    input: Some(Box::new(proto::Operator {
                        plan_node_id: 0,
                        operator: Some(proto::operator::Operator::Input(proto::Input {
                            schema: None,
                            input_index: 0,
                        })),
                    })),
                    grouping_indices: vec![0],
                    aggregate_calls: Vec::new(),
                    generate_update_before: false,
                    input_changelog: true,
                    ..Default::default()
                },
            ))),
        }),
    }
    .encode_to_vec()
}

fn aggregate_distinct_plan() -> Vec<u8> {
    let call = |function: proto::AggregateFunction| proto::AggregateCall {
        function: function as i32,
        input_index: Some(1),
        input_type: Some(logical_bigint(true)),
        output_type: Some(logical_bigint(function != proto::AggregateFunction::Count)),
        retractable: true,
        filter_index: None,
        distinct: true,
        accumulator_type: None,
    };
    proto::NativePlan {
        protocol_version: crate::PLAN_PROTOCOL_VERSION,
        root: Some(proto::Operator {
            plan_node_id: 0,
            operator: Some(proto::operator::Operator::GroupAggregate(Box::new(
                proto::GroupAggregate {
                    input: None,
                    grouping_indices: vec![0],
                    aggregate_calls: vec![
                        call(proto::AggregateFunction::Count),
                        call(proto::AggregateFunction::Sum),
                    ],
                    generate_update_before: true,
                    input_changelog: true,
                    ..Default::default()
                },
            ))),
        }),
    }
    .encode_to_vec()
}

fn distinct_processor() -> GroupAggregateProcessor {
    GroupAggregateProcessor::new(
        &distinct_plan(),
        128,
        0,
        127,
        HostMemoryReservation::new(
            Arc::new(TestBroker::new(1 << 30)),
            "test select distinct state",
        ),
    )
    .unwrap()
}

fn processor(input_changelog: bool, update_before: bool) -> GroupAggregateProcessor {
    processor_range(input_changelog, update_before, 0, 127)
}

fn mini_processor(size: u64, input_changelog: bool) -> GroupAggregateProcessor {
    let native = proto::NativePlan::decode(plan(input_changelog, true).as_slice()).unwrap();
    let mut aggregate = match native.root.unwrap().operator.unwrap() {
        proto::operator::Operator::GroupAggregate(aggregate) => *aggregate,
        _ => unreachable!(),
    };
    aggregate.mini_batch_size = size;
    aggregate.input_schema = Some(group_schema());
    aggregate.output_schema = Some(group_output_schema());
    let plan = proto::NativePlan {
        protocol_version: crate::PLAN_PROTOCOL_VERSION,
        root: Some(proto::Operator {
            plan_node_id: 0,
            operator: Some(proto::operator::Operator::GroupAggregate(Box::new(
                aggregate,
            ))),
        }),
    }
    .encode_to_vec();
    GroupAggregateProcessor::new(
        &plan,
        128,
        0,
        127,
        HostMemoryReservation::new(
            Arc::new(TestBroker::new(1 << 30)),
            "test mini-batch group aggregate state",
        ),
    )
    .unwrap()
}

fn bounded_plan() -> Vec<u8> {
    let native = proto::NativePlan::decode(plan(false, false).as_slice()).unwrap();
    let mut aggregate = match native.root.unwrap().operator.unwrap() {
        proto::operator::Operator::GroupAggregate(aggregate) => *aggregate,
        _ => unreachable!(),
    };
    aggregate.bounded_final_output = true;
    aggregate.input_schema = Some(group_schema());
    aggregate.output_schema = Some(group_output_schema());
    proto::NativePlan {
        protocol_version: crate::PLAN_PROTOCOL_VERSION,
        root: Some(proto::Operator {
            plan_node_id: 0,
            operator: Some(proto::operator::Operator::GroupAggregate(Box::new(
                aggregate,
            ))),
        }),
    }
    .encode_to_vec()
}

fn bounded_processor() -> GroupAggregateProcessor {
    GroupAggregateProcessor::new(
        &bounded_plan(),
        128,
        0,
        127,
        HostMemoryReservation::new(
            Arc::new(TestBroker::new(1 << 30)),
            "test bounded group aggregate state",
        ),
    )
    .unwrap()
}

fn processor_range(
    input_changelog: bool,
    update_before: bool,
    first_key_group: u32,
    last_key_group: u32,
) -> GroupAggregateProcessor {
    GroupAggregateProcessor::new(
        &plan(input_changelog, update_before),
        128,
        first_key_group,
        last_key_group,
        HostMemoryReservation::new(
            Arc::new(TestBroker::new(1 << 30)),
            "test group aggregate state",
        ),
    )
    .unwrap()
}

fn batch(keys: Vec<i64>, values: Vec<Option<i64>>, kinds: Option<Vec<i8>>) -> RecordBatch {
    let mut fields = vec![
        Field::new("bidder", DataType::Int64, false),
        Field::new("price", DataType::Int64, true),
    ];
    let mut columns: Vec<ArrayRef> = vec![
        Arc::new(Int64Array::from(keys)),
        Arc::new(Int64Array::from(values)),
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
fn mini_batch_uses_exact_count_boundaries_across_arrow_batches() {
    let mut processor = mini_processor(3, false);
    let first = processor
        .process_arrow(batch(vec![7, 7], vec![Some(10), Some(20)], None))
        .unwrap();
    assert_eq!(first.num_rows(), 0);
    assert_eq!(processor.pending_element_count(), 2);
    assert_eq!(processor.pending_key_count(), 1);

    let second = processor
        .process_arrow(batch(
            vec![8, 7, 8, 9],
            vec![Some(5), Some(30), Some(7), Some(1)],
            None,
        ))
        .unwrap();
    assert_eq!(second.num_rows(), 7);
    assert_eq!(processor.pending_element_count(), 0);
    assert_eq!(
        second
            .column(5)
            .as_any()
            .downcast_ref::<Int8Array>()
            .unwrap()
            .values(),
        &[
            INSERT,
            INSERT,
            UPDATE_BEFORE,
            UPDATE_AFTER,
            INSERT,
            UPDATE_BEFORE,
            UPDATE_AFTER
        ]
    );
}

#[test]
fn bounded_aggregate_updates_state_per_batch_and_emits_only_terminal_inserts() {
    let mut processor = bounded_processor();
    assert_eq!(
        processor
            .process_arrow(batch(vec![7, 7, 8], vec![Some(10), Some(20), None], None))
            .unwrap()
            .num_rows(),
        0
    );
    assert_eq!(
        processor
            .process_arrow(batch(vec![7, 8], vec![Some(5), Some(3)], None))
            .unwrap()
            .num_rows(),
        0
    );
    assert_eq!(processor.statistics(), [2, 2]);

    assert_eq!(
        output_rows(processor.finish_bundle().unwrap()),
        vec![
            (7, 3, Some(35), Some(5), Some(20), INSERT),
            (8, 2, Some(3), Some(3), Some(3), INSERT),
        ]
    );
    assert_eq!(processor.finish_bundle().unwrap().num_rows(), 0);
}

#[test]
fn bounded_aggregate_restores_canonical_state_before_terminal_output() {
    let mut source = bounded_processor();
    source
        .process_arrow(batch(vec![7, 8], vec![Some(10), Some(3)], None))
        .unwrap();
    let snapshots = (0..128)
        .map(|group| source.snapshot_key_group(group).unwrap())
        .collect::<Vec<_>>();
    let mut restored = bounded_processor();
    for (group, snapshot) in snapshots.iter().enumerate() {
        restored.restore_key_group(group as u32, snapshot).unwrap();
    }
    restored
        .process_arrow(batch(vec![7], vec![Some(5)], None))
        .unwrap();

    assert_eq!(
        output_rows(restored.finish_bundle().unwrap()),
        vec![
            (7, 2, Some(15), Some(5), Some(10), INSERT),
            (8, 1, Some(3), Some(3), Some(3), INSERT),
        ]
    );
}

#[test]
fn bounded_aggregate_drains_terminal_rows_in_managed_batches() {
    let mut processor = bounded_processor();
    let keys = (0..20_000).map(i64::from).collect::<Vec<_>>();
    let values = (0..20_000).map(|value| Some(i64::from(value))).collect();
    processor.process_arrow(batch(keys, values, None)).unwrap();

    assert_eq!(processor.finish_bundle().unwrap().num_rows(), 16_384);
    assert_eq!(processor.finish_bundle().unwrap().num_rows(), 3_616);
    assert_eq!(processor.finish_bundle().unwrap().num_rows(), 0);
}

#[test]
fn mini_batch_flushes_partial_bundle_and_suppresses_equal_result() {
    let mut processor = mini_processor(10, true);
    let initial = processor
        .process_arrow(batch(vec![7], vec![Some(10)], Some(vec![INSERT])))
        .unwrap();
    assert_eq!(initial.num_rows(), 0);
    assert_eq!(processor.finish_bundle().unwrap().num_rows(), 1);

    let unchanged = processor
        .process_arrow(batch(
            vec![7, 7],
            vec![Some(10), Some(10)],
            Some(vec![DELETE, INSERT]),
        ))
        .unwrap();
    assert_eq!(unchanged.num_rows(), 0);
    assert_eq!(processor.finish_bundle().unwrap().num_rows(), 0);
}

#[test]
fn emits_flink_per_record_changelog_and_suppresses_equal_results() {
    let output = processor(false, true)
        .process_arrow(batch(
            vec![7, 7, 7, 8],
            vec![Some(10), Some(20), None, Some(5)],
            None,
        ))
        .unwrap();
    assert_eq!(output.num_rows(), 6);
    assert_eq!(
        output
            .column(1)
            .as_any()
            .downcast_ref::<Int64Array>()
            .unwrap()
            .values(),
        &[1, 1, 2, 2, 3, 1]
    );
    assert_eq!(
        output
            .column(output.num_columns() - 1)
            .as_any()
            .downcast_ref::<Int8Array>()
            .unwrap()
            .values(),
        &[
            INSERT,
            UPDATE_BEFORE,
            UPDATE_AFTER,
            UPDATE_BEFORE,
            UPDATE_AFTER,
            INSERT
        ]
    );
}

#[test]
fn global_aggregate_uses_one_empty_binary_row_key() {
    let mut processor = GroupAggregateProcessor::new(
        &plan_with_grouping(false, true, Vec::new()),
        128,
        0,
        127,
        HostMemoryReservation::new(
            Arc::new(TestBroker::new(1 << 30)),
            "test global aggregate state",
        ),
    )
    .unwrap();
    let output = processor
        .process_arrow(batch(vec![7, 8, 9], vec![Some(10), None, Some(20)], None))
        .unwrap();

    assert_eq!(output.num_columns(), 5);
    assert_eq!(
        output
            .column(0)
            .as_any()
            .downcast_ref::<Int64Array>()
            .unwrap()
            .values(),
        &[1, 1, 2, 2, 3]
    );
    assert_eq!(
        output
            .column(output.num_columns() - 1)
            .as_any()
            .downcast_ref::<Int8Array>()
            .unwrap()
            .values(),
        &[
            INSERT,
            UPDATE_BEFORE,
            UPDATE_AFTER,
            UPDATE_BEFORE,
            UPDATE_AFTER
        ]
    );
}

#[test]
fn retracts_sum_and_extrema_and_deletes_the_last_record() {
    let mut processor = processor(true, true);
    processor
        .process_arrow(batch(
            vec![7, 7],
            vec![Some(10), Some(20)],
            Some(vec![INSERT, INSERT]),
        ))
        .unwrap();
    let output = processor
        .process_arrow(batch(
            vec![7, 7],
            vec![Some(20), Some(10)],
            Some(vec![DELETE, DELETE]),
        ))
        .unwrap();
    assert_eq!(output.num_rows(), 3);
    assert_eq!(
        output
            .column(output.num_columns() - 1)
            .as_any()
            .downcast_ref::<Int8Array>()
            .unwrap()
            .values(),
        &[UPDATE_BEFORE, UPDATE_AFTER, DELETE]
    );
    assert_eq!(
        output
            .column(2)
            .as_any()
            .downcast_ref::<Int64Array>()
            .unwrap()
            .iter()
            .collect::<Vec<_>>(),
        vec![Some(30), Some(10), Some(10)]
    );
}

#[test]
fn select_distinct_count_restores_and_emits_the_final_delete() {
    let mut source = distinct_processor();
    let inserted = source
        .process_arrow(batch(
            vec![7, 7],
            vec![Some(10), Some(20)],
            Some(vec![INSERT, UPDATE_AFTER]),
        ))
        .unwrap();
    assert_eq!(inserted.num_rows(), 1);
    let key = encode_binary_row(
        &batch(vec![7], vec![Some(10)], None),
        0,
        &[(0, KeyField::BigInt)],
    )
    .unwrap();
    let key_group = assign_key_group(&key, 128);
    let snapshot = source.snapshot_key_group(key_group).unwrap();

    let mut restored = distinct_processor();
    restored.restore_key_group(key_group, &snapshot).unwrap();
    let output = restored
        .process_arrow(batch(
            vec![7, 7],
            vec![Some(20), Some(10)],
            Some(vec![UPDATE_BEFORE, DELETE]),
        ))
        .unwrap();
    assert_eq!(output.num_rows(), 1);
    assert_eq!(
        output
            .column(output.num_columns() - 1)
            .as_any()
            .downcast_ref::<Int8Array>()
            .unwrap()
            .value(0),
        DELETE
    );
}

#[test]
fn aggregate_distinct_restores_counted_values_and_retracts_membership_boundaries() {
    let plan = aggregate_distinct_plan();
    let reservation = || {
        HostMemoryReservation::new(
            Arc::new(TestBroker::new(1 << 30)),
            "test aggregate distinct state",
        )
    };
    let mut source = GroupAggregateProcessor::new(&plan, 128, 0, 127, reservation()).unwrap();
    let inserted = source
        .process_arrow(batch(
            vec![7, 7, 7],
            vec![Some(10), Some(10), Some(20)],
            Some(vec![INSERT, INSERT, INSERT]),
        ))
        .unwrap();
    assert_eq!(inserted.num_rows(), 3);
    let key = encode_binary_row(
        &batch(vec![7], vec![Some(10)], None),
        0,
        &[(0, KeyField::BigInt)],
    )
    .unwrap();
    let key_group = assign_key_group(&key, 128);
    let snapshot = source.snapshot_key_group(key_group).unwrap();

    let mut restored = GroupAggregateProcessor::new(&plan, 128, 0, 127, reservation()).unwrap();
    restored.restore_key_group(key_group, &snapshot).unwrap();
    let retracted = restored
        .process_arrow(batch(
            vec![7, 7],
            vec![Some(10), Some(10)],
            Some(vec![DELETE, DELETE]),
        ))
        .unwrap();
    assert_eq!(retracted.num_rows(), 2);
    assert_eq!(
        retracted
            .column(1)
            .as_any()
            .downcast_ref::<Int64Array>()
            .unwrap()
            .values(),
        &[2, 1]
    );
    assert_eq!(
        retracted
            .column(2)
            .as_any()
            .downcast_ref::<Int64Array>()
            .unwrap()
            .values(),
        &[30, 20]
    );
}

#[test]
fn delete_then_reinsert_in_one_batch_starts_a_new_group() {
    let mut processor = processor(true, true);
    processor
        .process_arrow(batch(vec![7], vec![Some(10)], Some(vec![INSERT])))
        .unwrap();
    let output = processor
        .process_arrow(batch(
            vec![7, 7],
            vec![Some(10), Some(20)],
            Some(vec![DELETE, INSERT]),
        ))
        .unwrap();
    assert_eq!(output.num_rows(), 2);
    assert_eq!(
        output
            .column(output.num_columns() - 1)
            .as_any()
            .downcast_ref::<Int8Array>()
            .unwrap()
            .values(),
        &[DELETE, INSERT]
    );
}

#[test]
fn count_accepts_nonnumeric_inputs_and_ignores_nulls() {
    let aggregate = proto::GroupAggregate {
        input: Some(Box::new(proto::Operator {
            plan_node_id: 0,
            operator: Some(proto::operator::Operator::Input(proto::Input {
                schema: None,
                input_index: 0,
            })),
        })),
        grouping_indices: vec![0],
        aggregate_calls: vec![proto::AggregateCall {
            function: proto::AggregateFunction::Count as i32,
            input_index: Some(1),
            input_type: Some(logical_varchar(true)),
            output_type: Some(logical_bigint(false)),
            retractable: false,
            filter_index: None,
            distinct: false,
            accumulator_type: None,
        }],
        generate_update_before: false,
        input_changelog: false,
        ..Default::default()
    };
    let bytes = proto::NativePlan {
        protocol_version: crate::PLAN_PROTOCOL_VERSION,
        root: Some(proto::Operator {
            plan_node_id: 0,
            operator: Some(proto::operator::Operator::GroupAggregate(Box::new(
                aggregate,
            ))),
        }),
    }
    .encode_to_vec();
    let mut processor = GroupAggregateProcessor::new(
        &bytes,
        128,
        0,
        127,
        HostMemoryReservation::new(
            Arc::new(TestBroker::new(1 << 30)),
            "test count string state",
        ),
    )
    .unwrap();
    let input = RecordBatch::try_new(
        Arc::new(Schema::new(vec![
            Field::new("key", DataType::Int64, false),
            Field::new("value", DataType::Utf8, true),
        ])),
        vec![
            Arc::new(Int64Array::from(vec![1, 1])),
            Arc::new(StringArray::from(vec![Some("x"), None])),
        ],
    )
    .unwrap();
    let output = processor.process_arrow(input).unwrap();
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
fn performs_one_distinct_multi_get_and_one_write_batch() {
    let reads = Arc::new(AtomicUsize::new(0));
    let read_keys = Arc::new(AtomicUsize::new(0));
    let writes = Arc::new(AtomicUsize::new(0));
    let written_keys = Arc::new(AtomicUsize::new(0));
    let state = CountingState {
        reads: reads.clone(),
        read_keys: read_keys.clone(),
        writes: writes.clone(),
        written_keys: written_keys.clone(),
    };
    let broker = Arc::new(TestBroker::new(64 << 20));
    let mut processor = GroupAggregateProcessor::with_state(
        &plan(false, false),
        128,
        0,
        127,
        Box::new(state),
        HostMemoryReservation::new(broker, "test group aggregate scratch"),
    )
    .unwrap();

    processor
        .process_arrow(batch(
            vec![7, 7, 8, 7],
            vec![Some(1), Some(2), Some(3), Some(4)],
            None,
        ))
        .unwrap();

    assert_eq!(reads.load(Ordering::Relaxed), 1);
    assert_eq!(read_keys.load(Ordering::Relaxed), 2);
    assert_eq!(writes.load(Ordering::Relaxed), 1);
    assert_eq!(written_keys.load(Ordering::Relaxed), 2);
    assert_eq!(processor.statistics(), [1, 1]);
}

#[test]
fn accounts_state_batch_scratch_and_exported_output_in_host_memory() {
    let broker = Arc::new(TestBroker::new(64 << 20));
    let mut processor = GroupAggregateProcessor::new(
        &plan(false, false),
        128,
        0,
        127,
        HostMemoryReservation::new(broker.clone(), "test group aggregate state"),
    )
    .unwrap();
    let state_only = broker.reserved();

    let output = processor
        .process_arrow(batch(vec![7, 8], vec![Some(10), Some(20)], None))
        .unwrap();

    assert!(broker.reserved() > state_only);
    drop(output);
    drop(processor);
    assert_eq!(broker.reserved(), 0);
}

#[test]
fn canonical_key_group_snapshots_rescale_without_changing_results() {
    let keys = (0..64).collect::<Vec<_>>();
    let values = keys.iter().map(|key| Some(key * 10)).collect::<Vec<_>>();
    let mut source = processor(false, false);
    source
        .process_arrow(batch(keys.clone(), values.clone(), None))
        .unwrap();
    let snapshots = (0..128)
        .map(|key_group| source.snapshot_key_group(key_group).unwrap())
        .collect::<Vec<_>>();

    let mut baseline = processor(false, false);
    for (key_group, snapshot) in snapshots.iter().enumerate() {
        baseline
            .restore_key_group(key_group as u32, snapshot)
            .unwrap();
    }
    let update = batch(keys.clone(), vec![Some(1); keys.len()], None);
    let expected = output_rows(baseline.process_arrow(update.clone()).unwrap());

    let mut low_keys = Vec::new();
    let mut low_values = Vec::new();
    let mut high_keys = Vec::new();
    let mut high_values = Vec::new();
    for row in 0..update.num_rows() {
        let key_group = source.state_key(&update, row).unwrap().key_group;
        if key_group <= 63 {
            low_keys.push(keys[row]);
            low_values.push(Some(1));
        } else {
            high_keys.push(keys[row]);
            high_values.push(Some(1));
        }
    }
    let mut low = processor_range(false, false, 0, 63);
    let mut high = processor_range(false, false, 64, 127);
    for key_group in 0..64 {
        low.restore_key_group(key_group, &snapshots[key_group as usize])
            .unwrap();
    }
    for key_group in 64..128 {
        high.restore_key_group(key_group, &snapshots[key_group as usize])
            .unwrap();
    }
    let mut actual = output_rows(
        low.process_arrow(batch(low_keys, low_values, None))
            .unwrap(),
    );
    actual.extend(output_rows(
        high.process_arrow(batch(high_keys, high_values, None))
            .unwrap(),
    ));
    actual.sort_unstable();

    assert_eq!(actual, expected);
}

#[test]
fn canonical_group_state_moves_from_memory_to_rocksdb() {
    let Ok(plugin_path) = std::env::var("STREAMFUSION_TEST_ROCKSDB_PLUGIN") else {
        return;
    };
    let mut memory = processor(false, false);
    memory
        .process_arrow(batch(
            vec![7, 7, 8],
            vec![Some(10), Some(20), Some(5)],
            None,
        ))
        .unwrap();
    let snapshots = (0..128)
        .map(|key_group| memory.snapshot_key_group(key_group).unwrap())
        .collect::<Vec<_>>();
    let directory = tempfile::tempdir().unwrap();
    let broker = Arc::new(TestBroker::new(256 << 20));
    let mut rocks = GroupAggregateProcessor::new_rocksdb(
        &plan(false, false),
        128,
        0,
        127,
        std::path::Path::new(&plugin_path),
        directory.path(),
        64 << 20,
        HostMemoryReservation::new(broker, "test RocksDB group aggregate scratch"),
    )
    .unwrap();
    for (key_group, snapshot) in snapshots.iter().enumerate() {
        rocks.restore_key_group(key_group as u32, snapshot).unwrap();
        assert_eq!(
            rocks.snapshot_key_group(key_group as u32).unwrap(),
            *snapshot
        );
    }

    let update = batch(vec![7, 8], vec![Some(1), Some(2)], None);
    assert_eq!(
        output_rows(rocks.process_arrow(update.clone()).unwrap()),
        output_rows(memory.process_arrow(update).unwrap())
    );
}

#[test]
fn canonical_distinct_state_moves_from_memory_to_rocksdb() {
    let Ok(plugin_path) = std::env::var("STREAMFUSION_TEST_ROCKSDB_PLUGIN") else {
        return;
    };
    let plan = aggregate_distinct_plan();
    let mut memory = GroupAggregateProcessor::new(
        &plan,
        128,
        0,
        127,
        HostMemoryReservation::new(
            Arc::new(TestBroker::new(1 << 30)),
            "test distinct memory state",
        ),
    )
    .unwrap();
    memory
        .process_arrow(batch(
            vec![7, 7, 7],
            vec![Some(10), Some(10), Some(20)],
            Some(vec![INSERT, INSERT, INSERT]),
        ))
        .unwrap();
    let snapshots = (0..128)
        .map(|key_group| memory.snapshot_key_group(key_group).unwrap())
        .collect::<Vec<_>>();
    let directory = tempfile::tempdir().unwrap();
    let mut rocks = GroupAggregateProcessor::new_rocksdb(
        &plan,
        128,
        0,
        127,
        std::path::Path::new(&plugin_path),
        directory.path(),
        64 << 20,
        HostMemoryReservation::new(
            Arc::new(TestBroker::new(256 << 20)),
            "test distinct RocksDB scratch",
        ),
    )
    .unwrap();
    for (key_group, snapshot) in snapshots.iter().enumerate() {
        rocks.restore_key_group(key_group as u32, snapshot).unwrap();
    }

    let update = batch(
        vec![7, 7],
        vec![Some(10), Some(10)],
        Some(vec![DELETE, DELETE]),
    );
    assert_eq!(
        rocks.process_arrow(update.clone()).unwrap(),
        memory.process_arrow(update).unwrap()
    );
}

#[test]
fn state_codec_round_trips_every_accumulator_value_family() {
    let state = AccumulatorState {
        row_count: 3,
        accumulators: vec![
            Accumulator::Count(3),
            Accumulator::Sum {
                value: Some(float32_value(12.5)),
                count: 2,
            },
            Accumulator::Extremum(BTreeMap::from([
                (AggregateValue::Boolean(false), 2),
                (AggregateValue::Boolean(true), 1),
            ])),
            Accumulator::AppendExtremum(Some(AggregateValue::Bytes(b"flink".to_vec()))),
            Accumulator::Extremum(BTreeMap::from([
                (AggregateValue::Int(100), 2),
                (AggregateValue::Int(200), -1),
            ])),
            Accumulator::DistinctCount {
                count: 2,
                values: BTreeMap::from([
                    (AggregateValue::Int(10), 2),
                    (AggregateValue::Int(20), 1),
                ]),
            },
            Accumulator::DistinctSum {
                value: Some(AggregateValue::Int(30)),
                count: 2,
                values: BTreeMap::from([
                    (AggregateValue::Int(10), 2),
                    (AggregateValue::Int(20), 1),
                ]),
            },
            Accumulator::Average {
                value: Some(float64_value(17.5)),
                count: 3,
            },
            Accumulator::DistinctAverage {
                value: Some(AggregateValue::Int(3000)),
                count: 2,
                values: BTreeMap::from([
                    (AggregateValue::Int(1000), 2),
                    (AggregateValue::Int(2000), 1),
                ]),
            },
        ],
    };
    let calls = vec![
        Call {
            function: proto::AggregateFunction::CountStar,
            input_index: None,
            filter_index: None,
            distinct: false,
            input_type: None,
            output_type: DataType::Int64,
            retractable: true,
        },
        Call {
            function: proto::AggregateFunction::Sum,
            input_index: Some(1),
            filter_index: None,
            distinct: false,
            input_type: Some(DataType::Float32),
            output_type: DataType::Float32,
            retractable: true,
        },
        Call {
            function: proto::AggregateFunction::Min,
            input_index: Some(1),
            filter_index: None,
            distinct: false,
            input_type: Some(DataType::Boolean),
            output_type: DataType::Boolean,
            retractable: true,
        },
        Call {
            function: proto::AggregateFunction::Max,
            input_index: Some(1),
            filter_index: None,
            distinct: false,
            input_type: Some(DataType::Utf8),
            output_type: DataType::Utf8,
            retractable: false,
        },
        Call {
            function: proto::AggregateFunction::Min,
            input_index: Some(1),
            filter_index: None,
            distinct: false,
            input_type: Some(DataType::Date32),
            output_type: DataType::Date32,
            retractable: true,
        },
        Call {
            function: proto::AggregateFunction::Count,
            input_index: Some(1),
            filter_index: None,
            distinct: true,
            input_type: Some(DataType::Int64),
            output_type: DataType::Int64,
            retractable: true,
        },
        Call {
            function: proto::AggregateFunction::Sum,
            input_index: Some(1),
            filter_index: None,
            distinct: true,
            input_type: Some(DataType::Int64),
            output_type: DataType::Int64,
            retractable: true,
        },
        Call {
            function: proto::AggregateFunction::Avg,
            input_index: Some(1),
            filter_index: None,
            distinct: false,
            input_type: Some(DataType::Float32),
            output_type: DataType::Float32,
            retractable: true,
        },
        Call {
            function: proto::AggregateFunction::Avg,
            input_index: Some(1),
            filter_index: None,
            distinct: true,
            input_type: Some(DataType::Decimal128(10, 2)),
            output_type: DataType::Decimal128(38, 6),
            retractable: true,
        },
    ];
    assert_eq!(decode_state(&encode_state(&state), &calls).unwrap(), state);

    let neutral = AccumulatorState::new(&calls);
    let encoded_neutral = encode_state(&neutral);
    assert_eq!(encoded_neutral.len(), 4 + 1 + 8 + 4 + calls.len());
    assert_eq!(decode_state(&encoded_neutral, &calls).unwrap(), neutral);
}

#[test]
fn average_accumulates_all_numeric_families_and_distinct_retractions() {
    let calls = vec![
        Call {
            function: proto::AggregateFunction::Avg,
            input_index: Some(0),
            filter_index: None,
            distinct: false,
            input_type: Some(DataType::Int8),
            output_type: DataType::Int8,
            retractable: true,
        },
        Call {
            function: proto::AggregateFunction::Avg,
            input_index: Some(1),
            filter_index: None,
            distinct: false,
            input_type: Some(DataType::Float32),
            output_type: DataType::Float32,
            retractable: true,
        },
        Call {
            function: proto::AggregateFunction::Avg,
            input_index: Some(2),
            filter_index: None,
            distinct: false,
            input_type: Some(DataType::Float64),
            output_type: DataType::Float64,
            retractable: true,
        },
        Call {
            function: proto::AggregateFunction::Avg,
            input_index: Some(3),
            filter_index: None,
            distinct: false,
            input_type: Some(DataType::Decimal128(10, 2)),
            output_type: DataType::Decimal128(38, 6),
            retractable: true,
        },
        Call {
            function: proto::AggregateFunction::Avg,
            input_index: Some(4),
            filter_index: None,
            distinct: true,
            input_type: Some(DataType::Int64),
            output_type: DataType::Int64,
            retractable: true,
        },
    ];
    let first = vec![
        Some(AggregateValue::Int(1)),
        Some(float32_value(1.5)),
        Some(float64_value(2.0)),
        Some(AggregateValue::Int(100)),
        Some(AggregateValue::Int(10)),
    ];
    let second = vec![
        Some(AggregateValue::Int(2)),
        Some(float32_value(2.5)),
        Some(float64_value(4.0)),
        Some(AggregateValue::Int(200)),
        Some(AggregateValue::Int(10)),
    ];
    let mut state = AccumulatorState::new(&calls);
    state.apply_values(&calls, &first, true).unwrap();
    state.apply_values(&calls, &second, true).unwrap();
    assert_eq!(
        state.values(&calls),
        vec![
            Some(AggregateValue::Int(1)),
            Some(float32_value(2.0)),
            Some(float64_value(3.0)),
            Some(AggregateValue::Int(1_500_000)),
            Some(AggregateValue::Int(10)),
        ]
    );

    state.apply_values(&calls, &second, false).unwrap();
    assert_eq!(
        state.values(&calls),
        vec![
            Some(AggregateValue::Int(1)),
            Some(float32_value(1.5)),
            Some(float64_value(2.0)),
            Some(AggregateValue::Int(1_000_000)),
            Some(AggregateValue::Int(10)),
        ]
    );
}

#[test]
fn state_codec_restores_version_one_integer_savepoints() {
    let mut bytes = Vec::new();
    bytes.extend_from_slice(STATE_MAGIC);
    bytes.push(1);
    bytes.extend_from_slice(&2_i64.to_le_bytes());
    bytes.extend_from_slice(&1_u32.to_le_bytes());
    bytes.push(2);
    bytes.extend_from_slice(&123_i128.to_le_bytes());
    bytes.extend_from_slice(&2_i64.to_le_bytes());
    let calls = vec![Call {
        function: proto::AggregateFunction::Sum,
        input_index: Some(1),
        filter_index: None,
        distinct: false,
        input_type: Some(DataType::Int64),
        output_type: DataType::Int64,
        retractable: true,
    }];
    assert_eq!(
        decode_state(&bytes, &calls).unwrap(),
        AccumulatorState {
            row_count: 2,
            accumulators: vec![Accumulator::Sum {
                value: Some(AggregateValue::Int(123)),
                count: 2,
            }],
        }
    );

    let mut version_two = Vec::new();
    version_two.extend_from_slice(STATE_MAGIC);
    version_two.push(2);
    version_two.extend_from_slice(&1_i64.to_le_bytes());
    version_two.extend_from_slice(&1_u32.to_le_bytes());
    version_two.push(2);
    encode_value(&float32_value(1.5), &mut version_two);
    version_two.extend_from_slice(&1_i64.to_le_bytes());
    let float_calls = vec![Call {
        function: proto::AggregateFunction::Sum,
        input_index: Some(1),
        filter_index: None,
        distinct: false,
        input_type: Some(DataType::Float32),
        output_type: DataType::Float32,
        retractable: true,
    }];
    assert_eq!(
        decode_state(&version_two, &float_calls).unwrap(),
        AccumulatorState {
            row_count: 1,
            accumulators: vec![Accumulator::Sum {
                value: Some(float32_value(1.5)),
                count: 1,
            }],
        }
    );
}

#[test]
fn float_extrema_use_flink_nan_and_signed_zero_ordering() {
    let negative_zero = float32_value(-0.0);
    let positive_zero = float32_value(0.0);
    let nan = float32_value(f32::NAN);
    assert!(negative_zero < positive_zero);
    assert!(nan > positive_zero);

    let output = aggregate_array(
        &[Some(negative_zero), Some(positive_zero), Some(nan)],
        &DataType::Float32,
    )
    .unwrap();
    let values = output.as_any().downcast_ref::<Float32Array>().unwrap();
    assert_eq!(values.value(0).to_bits(), (-0.0_f32).to_bits());
    assert_eq!(values.value(1).to_bits(), 0.0_f32.to_bits());
    assert!(values.value(2).is_nan());

    let booleans = aggregate_array(
        &[
            Some(AggregateValue::Boolean(false)),
            None,
            Some(AggregateValue::Boolean(true)),
        ],
        &DataType::Boolean,
    )
    .unwrap();
    assert_eq!(
        booleans
            .as_any()
            .downcast_ref::<BooleanArray>()
            .unwrap()
            .iter()
            .collect::<Vec<_>>(),
        vec![Some(false), None, Some(true)]
    );
}

#[test]
fn sum_uses_flink_integer_wrap_and_decimal_null_on_overflow() {
    assert_eq!(
        aggregate_add(
            &AggregateValue::Int(i64::MAX as i128),
            &AggregateValue::Int(1),
            &DataType::Int64,
        )
        .unwrap(),
        Some(AggregateValue::Int(i64::MIN as i128))
    );
    assert_eq!(
        aggregate_add(
            &AggregateValue::Int(10_i128.pow(38) - 1),
            &AggregateValue::Int(1),
            &DataType::Decimal128(38, 0),
        )
        .unwrap(),
        None
    );
    assert_eq!(
        aggregate_sub(
            &AggregateValue::Int(-(10_i128.pow(38) - 1)),
            &AggregateValue::Int(1),
            &DataType::Decimal128(38, 0),
        )
        .unwrap(),
        None
    );
}

#[test]
fn sum0_returns_a_typed_zero_for_an_empty_or_all_null_accumulator() {
    let calls = vec![Call {
        function: proto::AggregateFunction::Sum0,
        input_index: Some(0),
        filter_index: None,
        distinct: false,
        input_type: Some(DataType::Int64),
        output_type: DataType::Int64,
        retractable: true,
    }];
    let mut state = AccumulatorState::new(&calls);
    assert_eq!(state.values(&calls), vec![Some(AggregateValue::Int(0))]);

    state.apply_values(&calls, &[None], true).unwrap();
    assert_eq!(state.values(&calls), vec![Some(AggregateValue::Int(0))]);
    state
        .apply_values(&calls, &[Some(AggregateValue::Int(5))], true)
        .unwrap();
    assert_eq!(state.values(&calls), vec![Some(AggregateValue::Int(5))]);
    state
        .apply_values(&calls, &[Some(AggregateValue::Int(5))], false)
        .unwrap();
    assert_eq!(state.values(&calls), vec![Some(AggregateValue::Int(0))]);
}

#[test]
fn merged_partial_replacement_preserves_value_delta_with_unchanged_count() {
    let calls = vec![
        Call {
            function: proto::AggregateFunction::Sum,
            input_index: Some(0),
            filter_index: None,
            distinct: false,
            input_type: Some(DataType::Int64),
            output_type: DataType::Int64,
            retractable: true,
        },
        Call {
            function: proto::AggregateFunction::Avg,
            input_index: Some(0),
            filter_index: None,
            distinct: false,
            input_type: Some(DataType::Int64),
            output_type: DataType::Int64,
            retractable: true,
        },
    ];
    let five = Some(AggregateValue::Int(5));
    let ten = Some(AggregateValue::Int(10));
    let mut current = AccumulatorState::new(&calls);
    current
        .apply_values(&calls, &[five.clone(), five.clone()], true)
        .unwrap();

    let mut replacement = AccumulatorState::new(&calls);
    replacement
        .apply_values(&calls, &[five.clone(), five], false)
        .unwrap();
    replacement
        .apply_values(&calls, &[ten.clone(), ten], true)
        .unwrap();
    assert_eq!(replacement.row_count, 0);

    current.merge(&calls, &replacement).unwrap();
    assert_eq!(
        current.values(&calls),
        vec![Some(AggregateValue::Int(10)), Some(AggregateValue::Int(10))]
    );
}

fn output_rows(batch: RecordBatch) -> Vec<(i64, i64, Option<i64>, Option<i64>, Option<i64>, i8)> {
    let column = |index: usize| {
        batch
            .column(index)
            .as_any()
            .downcast_ref::<Int64Array>()
            .unwrap()
    };
    let keys = column(0);
    let counts = column(1);
    let sums = column(2);
    let minimums = column(3);
    let maximums = column(4);
    let kinds = batch
        .column(5)
        .as_any()
        .downcast_ref::<Int8Array>()
        .unwrap();
    let mut rows = (0..batch.num_rows())
        .map(|row| {
            (
                keys.value(row),
                counts.value(row),
                (!sums.is_null(row)).then(|| sums.value(row)),
                (!minimums.is_null(row)).then(|| minimums.value(row)),
                (!maximums.is_null(row)).then(|| maximums.value(row)),
                kinds.value(row),
            )
        })
        .collect::<Vec<_>>();
    rows.sort_unstable();
    rows
}

struct CountingState {
    reads: Arc<AtomicUsize>,
    read_keys: Arc<AtomicUsize>,
    writes: Arc<AtomicUsize>,
    written_keys: Arc<AtomicUsize>,
}

impl KeyedState for CountingState {
    fn get_batch<'a>(&'a self, keys: &[StateKeyRef<'_>]) -> Result<Vec<Option<Cow<'a, [u8]>>>> {
        self.reads.fetch_add(1, Ordering::Relaxed);
        self.read_keys.fetch_add(keys.len(), Ordering::Relaxed);
        Ok(vec![None; keys.len()])
    }

    fn write_batch(&mut self, mutations: Vec<StateMutation>) -> Result<()> {
        self.writes.fetch_add(1, Ordering::Relaxed);
        self.written_keys
            .fetch_add(mutations.len(), Ordering::Relaxed);
        Ok(())
    }

    fn snapshot_key_group(&self, _key_group: u32) -> Result<Vec<u8>> {
        unreachable!("snapshot is not used by the batch-call test")
    }

    fn restore_key_group(&mut self, _key_group: u32, _bytes: &[u8]) -> Result<()> {
        unreachable!("restore is not used by the batch-call test")
    }
}
