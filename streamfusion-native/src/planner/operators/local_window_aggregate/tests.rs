// Copyright 2026 StreamFusion Authors
// Licensed under the Apache License, Version 2.0.

use super::*;
use crate::memory_pool::tests_support::TestBroker;
use crate::planner::operators::group_aggregate::decode_state;
use arrow::array::TimestampMillisecondArray;
use arrow::datatypes::{Field, Schema};
use prost::Message;

fn logical_bigint(nullable: bool) -> proto::LogicalType {
    proto::LogicalType {
        nullable,
        r#type: Some(proto::logical_type::Type::Bigint(proto::EmptyType {})),
    }
}

fn logical_timestamp(nullable: bool) -> proto::LogicalType {
    proto::LogicalType {
        nullable,
        r#type: Some(proto::logical_type::Type::Timestamp(proto::PrecisionType {
            precision: 3,
        })),
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
        input_type: input.map(|_| logical_bigint(false)),
        output_type: Some(logical_bigint(false)),
        retractable: true,
        filter_index: None,
        distinct: false,
        accumulator_type: None,
    }
}

pub(super) fn processor(changelog: bool) -> LocalWindowAggregateProcessor {
    let plan = proto::NativePlan {
        protocol_version: crate::PLAN_PROTOCOL_VERSION,
        root: Some(proto::Operator {
            plan_node_id: 0,
            metric_name: String::new(),
            clear_record_timestamps: false,
            metric_uid: None,
            operator: Some(proto::operator::Operator::LocalWindowAggregate(Box::new(
                proto::LocalWindowAggregate {
                    input: None,
                    grouping_indices: vec![0],
                    aggregate_calls: vec![
                        call(proto::AggregateFunction::CountStar, None),
                        call(proto::AggregateFunction::Sum, Some(1)),
                    ],
                    input_changelog: changelog,
                    time_attribute_index: 2,
                    kind: proto::WindowKind::Hop as i32,
                    size_millis: 6_000,
                    slide_or_step_millis: 2_000,
                    offset_millis: 0,
                    input_schema: Some(schema(&[
                        ("key", logical_bigint(false)),
                        ("value", logical_bigint(false)),
                        ("ts", logical_timestamp(false)),
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
                            proto::Field {
                                name: "window_start".to_string(),
                                r#type: Some(logical_bigint(false)),
                            },
                            proto::Field {
                                name: "slice_end".to_string(),
                                r#type: Some(logical_bigint(false)),
                            },
                        ],
                    }),
                    shift_time_zone: "UTC".to_string(),
                    attached_window_start_index: None,
                    attached_window_end_index: None,
                },
            ))),
        }),
    }
    .encode_to_vec();
    LocalWindowAggregateProcessor::new(
        &plan,
        HostMemoryReservation::new(Arc::new(TestBroker::new(32 << 20)), "local window test"),
    )
    .unwrap()
}

fn batch(kinds: Option<Vec<i8>>) -> RecordBatch {
    let mut fields = vec![
        Field::new("key", DataType::Int64, false),
        Field::new("value", DataType::Int64, false),
        Field::new(
            "ts",
            DataType::Timestamp(arrow::datatypes::TimeUnit::Millisecond, None),
            false,
        ),
    ];
    let mut columns = vec![
        Arc::new(Int64Array::from(vec![1, 1, 1])) as ArrayRef,
        Arc::new(Int64Array::from(vec![10, 30, 30])) as ArrayRef,
        Arc::new(TimestampMillisecondArray::from(vec![1_000, 3_000, 3_500])) as ArrayRef,
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

fn separated_slice_changelog_batch() -> RecordBatch {
    RecordBatch::try_new(
        Arc::new(Schema::new(vec![
            Field::new("key", DataType::Int64, false),
            Field::new("value", DataType::Int64, false),
            Field::new(
                "ts",
                DataType::Timestamp(arrow::datatypes::TimeUnit::Millisecond, None),
                false,
            ),
            Field::new("__streamfusion_input_row_kind", DataType::Int8, false),
        ])),
        vec![
            Arc::new(Int64Array::from(vec![1, 1, 1, 1, 1])) as ArrayRef,
            Arc::new(Int64Array::from(vec![10, 20, 20, 5, 10])) as ArrayRef,
            Arc::new(TimestampMillisecondArray::from(vec![
                1_000, 2_000, 2_000, 2_000, 1_000,
            ])) as ArrayRef,
            Arc::new(Int8Array::from(vec![
                INSERT,
                INSERT,
                UPDATE_BEFORE,
                UPDATE_AFTER,
                DELETE,
            ])) as ArrayRef,
        ],
    )
    .unwrap()
}

fn replacement_only_batch() -> RecordBatch {
    RecordBatch::try_new(
        Arc::new(Schema::new(vec![
            Field::new("key", DataType::Int64, false),
            Field::new("value", DataType::Int64, false),
            Field::new(
                "ts",
                DataType::Timestamp(arrow::datatypes::TimeUnit::Millisecond, None),
                false,
            ),
            Field::new("__streamfusion_input_row_kind", DataType::Int8, false),
        ])),
        vec![
            Arc::new(Int64Array::from(vec![1, 1])) as ArrayRef,
            Arc::new(Int64Array::from(vec![20, 5])) as ArrayRef,
            Arc::new(TimestampMillisecondArray::from(vec![2_000, 2_000])) as ArrayRef,
            Arc::new(Int8Array::from(vec![UPDATE_BEFORE, UPDATE_AFTER])) as ArrayRef,
        ],
    )
    .unwrap()
}

#[test]
fn emits_one_opaque_partial_per_base_slice_and_cancels_retractions() {
    let mut append = processor(false);
    let output = append.process_arrow(batch(None)).unwrap();
    assert_eq!(output.num_rows(), 2);
    assert_eq!(
        output
            .column(3)
            .as_any()
            .downcast_ref::<Int64Array>()
            .unwrap()
            .values(),
        &[2_000, 4_000]
    );
    let partials = output
        .column(1)
        .as_any()
        .downcast_ref::<BinaryArray>()
        .unwrap();
    assert_eq!(
        decode_state(partials.value(0), &append.calls)
            .unwrap()
            .row_count,
        1
    );
    assert_eq!(
        decode_state(partials.value(1), &append.calls)
            .unwrap()
            .row_count,
        2
    );

    let mut retract = processor(true);
    let output = retract
        .process_arrow(batch(Some(vec![INSERT, INSERT, DELETE])))
        .unwrap();
    assert_eq!(output.num_rows(), 1);
    assert_eq!(
        output
            .column(3)
            .as_any()
            .downcast_ref::<Int64Array>()
            .unwrap()
            .value(0),
        2_000
    );
}

#[test]
fn retracting_batches_preserve_the_net_partial_for_each_slice() {
    use crate::planner::operators::group_aggregate::{Accumulator, AggregateValue};

    let mut processor = processor(true);
    let output = processor
        .process_arrow(separated_slice_changelog_batch())
        .unwrap();
    assert_eq!(output.num_rows(), 1);
    assert_eq!(
        output
            .column(3)
            .as_any()
            .downcast_ref::<Int64Array>()
            .unwrap()
            .value(0),
        4_000
    );
    let partial = decode_state(
        output
            .column(1)
            .as_any()
            .downcast_ref::<BinaryArray>()
            .unwrap()
            .value(0),
        &processor.calls,
    )
    .unwrap();
    assert_eq!(partial.row_count, 1);
    assert_eq!(
        partial.accumulators[1],
        Accumulator::Sum {
            value: Some(AggregateValue::Int(5)),
            count: 1,
        }
    );
}

#[test]
fn zero_cardinality_replacement_still_emits_aggregate_deltas() {
    use crate::planner::operators::group_aggregate::{Accumulator, AggregateValue};

    let mut processor = processor(true);
    let output = processor.process_arrow(replacement_only_batch()).unwrap();
    assert_eq!(output.num_rows(), 1);
    let partial = decode_state(
        output
            .column(1)
            .as_any()
            .downcast_ref::<BinaryArray>()
            .unwrap()
            .value(0),
        &processor.calls,
    )
    .unwrap();
    assert_eq!(partial.row_count, 0);
    assert_eq!(
        partial.accumulators[1],
        Accumulator::Sum {
            value: Some(AggregateValue::Int(-15)),
            count: 0,
        }
    );
}

#[test]
fn generated_slice_partials_preserve_ordered_flink_accumulator_bytes() {
    use std::collections::BTreeMap;
    for changelog in [false, true] {
        for seed in 0..4i64 {
            let mut processor = processor(changelog);
            let mut fields = processor.output_schema.fields().to_vec();
            fields[0] = Arc::new(Field::new("key", DataType::Int64, true));
            processor.output_schema = Arc::new(Schema::new(fields));
            processor.calls = [
                proto::AggregateFunction::CountStar,
                proto::AggregateFunction::Count,
                proto::AggregateFunction::Sum,
                proto::AggregateFunction::Min,
                proto::AggregateFunction::Max,
                proto::AggregateFunction::Avg,
            ]
            .into_iter()
            .map(|function| {
                let mut spec = call(
                    function,
                    (function != proto::AggregateFunction::CountStar).then_some(1),
                );
                spec.retractable = changelog;
                lower_call(&spec).unwrap()
            })
            .collect();
            processor.grouped_compute = if changelog {
                None
            } else {
                super::super::group_aggregate::grouped_compute::GroupedCompute::new(
                    &processor.calls,
                )
                .unwrap()
            };
            let length = 257;
            let keys = (0..length)
                .map(|i| (i % 13 != 0).then_some((i + seed) % 7))
                .collect::<Vec<_>>();
            let values = (0..length)
                .map(|i| match i % 11 {
                    0 => None,
                    1 => Some(i64::MAX),
                    2 => Some(i64::MIN),
                    _ => Some((i * 31 + seed) % 97 - 48),
                })
                .collect::<Vec<_>>();
            let times = (0..length)
                .map(|i| (i % 17 != 0).then_some((i * 113 + seed) % 15000 - 7500))
                .collect::<Vec<_>>();
            let kinds = (0..length)
                .map(|i| if changelog { (i % 4) as i8 } else { INSERT })
                .collect::<Vec<_>>();
            let mut fields = vec![
                Field::new("key", DataType::Int64, true),
                Field::new("value", DataType::Int64, true),
                Field::new(
                    "ts",
                    DataType::Timestamp(arrow::datatypes::TimeUnit::Millisecond, None),
                    true,
                ),
            ];
            let mut columns: Vec<ArrayRef> = vec![
                Arc::new(Int64Array::from(keys.clone())),
                Arc::new(Int64Array::from(values)),
                Arc::new(TimestampMillisecondArray::from(times.clone())),
            ];
            if changelog {
                fields.push(Field::new(
                    "__streamfusion_input_row_kind",
                    DataType::Int8,
                    false,
                ));
                columns.push(Arc::new(Int8Array::from(kinds.clone())));
            }
            let input = RecordBatch::try_new(Arc::new(Schema::new(fields)), columns).unwrap();
            let mut expected = BTreeMap::new();
            for row in 0..length as usize {
                if let Some(time) = times[row] {
                    let start = time.div_euclid(2000) * 2000;
                    let state = expected
                        .entry((keys[row], start, start + 2000))
                        .or_insert_with(|| AccumulatorState::new(&processor.calls));
                    state
                        .apply(
                            &processor.calls,
                            &input,
                            row,
                            matches!(kinds[row], INSERT | UPDATE_AFTER),
                        )
                        .unwrap();
                }
            }
            let expected = expected
                .into_iter()
                .filter(|(_, state)| state.has_delta())
                .map(|(key, state)| (key, encode_state(&state)))
                .collect::<BTreeMap<_, _>>();
            let output = processor.process_arrow(input).unwrap();
            let key = output
                .column(0)
                .as_any()
                .downcast_ref::<Int64Array>()
                .unwrap();
            let partial = output
                .column(1)
                .as_any()
                .downcast_ref::<BinaryArray>()
                .unwrap();
            let start = output
                .column(2)
                .as_any()
                .downcast_ref::<Int64Array>()
                .unwrap();
            let end = output
                .column(3)
                .as_any()
                .downcast_ref::<Int64Array>()
                .unwrap();
            let actual = (0..output.num_rows())
                .map(|i| {
                    (
                        (
                            key.is_valid(i).then(|| key.value(i)),
                            start.value(i),
                            end.value(i),
                        ),
                        partial.value(i).to_vec(),
                    )
                })
                .collect::<BTreeMap<_, _>>();
            assert_eq!(actual, expected, "seed={seed}, changelog={changelog}");
        }
    }
}

#[test]
fn workspace_denial_precedes_compute_and_releases_no_unowned_credit() {
    let mut processor = processor(false);
    let broker = Arc::new(TestBroker::new(1024));
    processor.reservation =
        HostMemoryReservation::new(broker.clone(), "denied local window workspace");
    processor.output_reservation = processor.reservation.sibling("denied output");
    assert!(matches!(
        processor.process_arrow(batch(None)),
        Err(DataFusionError::ResourcesExhausted(_))
    ));
    assert_eq!(broker.reserved(), 0);
    assert_eq!(processor.reservation.size(), 0);
}

#[test]
fn null_row_kind_is_rejected_and_workspace_is_released() {
    let mut processor = processor(true);
    let input = batch(Some(vec![INSERT, INSERT, INSERT]));
    let mut fields = input.schema().fields().to_vec();
    fields[3] = Arc::new(Field::new(
        "__streamfusion_input_row_kind",
        DataType::Int8,
        true,
    ));
    let mut columns = input.columns().to_vec();
    columns[3] = Arc::new(Int8Array::from(vec![Some(INSERT), None, Some(INSERT)]));
    let input = RecordBatch::try_new(Arc::new(Schema::new(fields)), columns).unwrap();
    assert!(processor
        .process_arrow(input)
        .unwrap_err()
        .to_string()
        .contains("must not be null"));
    assert_eq!(processor.reservation.size(), 0);
}

#[test]
fn grouped_batch_workspace_covers_observed_allocations_for_hot_and_unique_keys() {
    use crate::allocation_test_support::measure;
    for rows in [1, 1024, 5000] {
        for unique in [false, true] {
            let mut processor = processor(false);
            processor.calls = [
                proto::AggregateFunction::CountStar,
                proto::AggregateFunction::Count,
                proto::AggregateFunction::Sum,
                proto::AggregateFunction::Sum0,
                proto::AggregateFunction::Min,
                proto::AggregateFunction::Max,
                proto::AggregateFunction::Avg,
            ]
            .into_iter()
            .map(|function| {
                let mut spec = call(
                    function,
                    (function != proto::AggregateFunction::CountStar).then_some(1),
                );
                spec.retractable = false;
                lower_call(&spec).unwrap()
            })
            .collect();
            processor.grouped_compute =
                super::super::group_aggregate::grouped_compute::GroupedCompute::new(
                    &processor.calls,
                )
                .unwrap();
            let input = RecordBatch::try_from_iter(vec![
                (
                    "key",
                    Arc::new(Int64Array::from(
                        (0..rows)
                            .map(|row| if unique { row as i64 } else { 1 })
                            .collect::<Vec<_>>(),
                    )) as ArrayRef,
                ),
                (
                    "value",
                    Arc::new(Int64Array::from(vec![i64::MAX; rows])) as ArrayRef,
                ),
                (
                    "ts",
                    Arc::new(TimestampMillisecondArray::from(vec![1000; rows])) as ArrayRef,
                ),
            ])
            .unwrap();
            let allowance = processor.batch_admission(&input).unwrap();
            let (output, observed) = measure(|| processor.process_accounted(&input).unwrap());
            assert!(
                observed.peak <= allowance,
                "rows={rows}, unique={unique}, allowance={allowance}, observed={observed:?}"
            );
            assert_eq!(output.num_rows(), if unique { rows } else { 1 });
        }
    }
}
