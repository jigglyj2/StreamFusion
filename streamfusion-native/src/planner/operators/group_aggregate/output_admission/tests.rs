// Copyright 2026 StreamFusion Authors
// Licensed under the Apache License, Version 2.0

use super::*;
use crate::memory_pool::tests_support::TestBroker;
use arrow::array::{ListArray, StringDictionaryBuilder, StructArray};
use arrow::datatypes::{Int32Type, Int64Type};
use prost::Message;

fn processor(broker: Arc<TestBroker>) -> GroupAggregateProcessor {
    let plan = proto::NativePlan {
        protocol_version: 1,
        root: Some(proto::Operator {
            operator: Some(proto::operator::Operator::GroupAggregate(Box::new(
                proto::GroupAggregate {
                    grouping_indices: vec![0],
                    input_changelog: true,
                    generate_update_before: true,
                    ..Default::default()
                },
            ))),
            ..Default::default()
        }),
    };
    GroupAggregateProcessor::new(
        &plan.encode_to_vec(),
        16,
        0,
        15,
        HostMemoryReservation::new(broker, "output admission test"),
    )
    .unwrap()
}

fn counted_call() -> Call {
    Call {
        function: proto::AggregateFunction::Min,
        input_index: Some(0),
        filter_index: None,
        distinct: false,
        input_type: Some(DataType::Int64),
        output_type: DataType::Int64,
        retractable: true,
    }
}

#[test]
fn control_credit_covers_serialized_mutations_without_recharging_retained_tree_nodes() {
    let mut processor = processor(Arc::new(TestBroker::new(64 << 20)));
    processor.calls.push(counted_call());
    for groups in [1usize, 2048] {
        processor.pending.clear();
        let keys: Vec<_> = (0..groups)
            .map(|index| StateKey {
                key_group: 0,
                key: index.to_le_bytes().to_vec(),
            })
            .collect();
        for key in &keys {
            processor.pending.insert(
                key.clone(),
                PendingGroup {
                    grouping_row: vec![0; 16],
                    original: None,
                    current: Some(AccumulatorState {
                        row_count: 1,
                        accumulators: vec![Accumulator::Extremum(BTreeMap::from([(
                            AggregateValue::Int(3),
                            1,
                        )]))],
                    }),
                },
            );
        }
        let (admission, measured) = crate::allocation_test_support::measure(|| {
            processor.control_output_admission(&keys).unwrap()
        });
        assert_eq!(measured.peak, 0);
        assert_eq!(measured.live, 0);
        let old = keys
            .iter()
            .map(|key| {
                let group = &processor.pending[key];
                8 * (key.key.capacity()
                    + group.grouping_row.capacity()
                    + group.current.as_ref().unwrap().estimated_dynamic_bytes())
            })
            .sum::<usize>()
            + groups * 5 * 256
            + 5 * 4096;
        if groups == 2048 {
            assert!(admission * 4 < old, "admission={admission}, old={old}");
        }
        let (encoded, allocations) = crate::allocation_test_support::measure(|| {
            keys.iter()
                .map(|key| encode_state(processor.pending[key].current.as_ref().unwrap()))
                .collect::<Vec<_>>()
        });
        assert!(allocations.peak < admission);
        assert_eq!(encoded.len(), groups);
    }
}

#[test]
fn visible_payload_follows_positive_count_extrema_without_counting_hidden_history() {
    let state = AccumulatorState {
        row_count: 1,
        accumulators: vec![Accumulator::Extremum(BTreeMap::from([
            (AggregateValue::Bytes(vec![b'a'; 8192]), 0),
            (AggregateValue::Bytes(b"middle".to_vec()), 1),
            (AggregateValue::Bytes(vec![b'z'; 16384]), -1),
        ]))],
    };
    for function in [proto::AggregateFunction::Min, proto::AggregateFunction::Max] {
        let call = Call {
            function,
            input_type: Some(DataType::Utf8),
            output_type: DataType::Utf8,
            ..counted_call()
        };
        assert_eq!(state.visible_payload_bytes(std::slice::from_ref(&call)), 6);
        assert_eq!(
            state.values(&[call]),
            vec![Some(AggregateValue::Bytes(b"middle".to_vec()))]
        );
    }
}

#[test]
fn denied_control_workspace_preserves_pending_groups_and_does_not_write_state() {
    use crate::memory_pool::MemoryReservationBroker;
    let broker = Arc::new(TestBroker::new(1 << 20));
    let mut processor = processor(broker.clone());
    processor.calls.push(counted_call());
    let key = StateKey {
        key_group: 0,
        key: vec![1],
    };
    processor.pending_order.push(key.clone());
    processor.pending.insert(
        key.clone(),
        PendingGroup {
            grouping_row: vec![1],
            original: None,
            current: Some(AccumulatorState {
                row_count: 1,
                accumulators: vec![Accumulator::Extremum(BTreeMap::from([(
                    AggregateValue::Int(1),
                    1,
                )]))],
            }),
        },
    );
    processor
        .bundle_reservation
        .resize(processor.estimated_pending_bytes())
        .unwrap();
    // Model a subsequent bounded pull: ordering is already established and the remaining
    // workspace must be admitted before popping a key or emitting/writing anything.
    processor.control_flushing = true;
    let mut pressure = HostMemoryReservation::new(broker.clone(), "other Flink managed consumer");
    pressure
        .resize(broker.available().unwrap().unwrap() - 1)
        .unwrap();
    assert!(matches!(
        processor.drain_native_bundle(),
        Err(DataFusionError::ResourcesExhausted(_))
    ));
    assert_eq!(processor.pending_order, vec![key.clone()]);
    assert_eq!(
        processor.pending[&key].current.as_ref().unwrap().row_count,
        1
    );
    assert_eq!(processor.state_write_batches, 0);
    drop(pressure);
    drop(processor);
    assert_eq!(broker.reserved(), 0);
}

#[test]
fn sparse_accumulator_denial_precedes_state_reads_and_mutation() {
    use crate::planner::persistent::unary::UnaryBatchProcessor;

    let broker = Arc::new(TestBroker::new(64 << 10));
    let mut processor = processor(broker.clone());
    processor.calls.push(counted_call());
    let batch = RecordBatch::try_new(
        Arc::new(Schema::new(vec![
            Field::new("key", DataType::Int64, false),
            Field::new("__streamfusion_row_kind", DataType::Int8, false),
            Field::new("__streamfusion_input_row", DataType::Int32, false),
        ])),
        vec![
            Arc::new(Int64Array::from_iter_values(0..128)),
            Arc::new(Int8Array::from(vec![INSERT; 128])),
            Arc::new(arrow::array::Int32Array::from_iter_values(0..128)),
        ],
    )
    .unwrap();
    processor.prepare_output_schema(batch.schema()).unwrap();
    let before = broker.reserved();
    let failure = processor.process_batch(batch).unwrap_err();
    assert!(
        matches!(failure, DataFusionError::ResourcesExhausted(_)),
        "{failure}"
    );
    assert_eq!(processor.state_read_batches, 0);
    assert_eq!(processor.state_write_batches, 0);
    assert_eq!(broker.reserved(), before);
    for group in 0..16 {
        assert!(
            decode_key_group_snapshot(group, &processor.snapshot_key_group(group).unwrap())
                .unwrap()
                .is_empty()
        );
    }
    drop(processor);
    assert_eq!(broker.reserved(), 0);
}

#[test]
fn accumulator_credit_scales_by_distinct_groups_and_rejects_overflow() {
    let mut processor = processor(Arc::new(TestBroker::new(8 << 20)));
    processor.calls.push(counted_call());
    let sparse = processor.accumulator_admission(128, 128).unwrap();
    let hot = processor.accumulator_admission(1, 128).unwrap();
    assert!(sparse > hot + 127 * accumulator::COUNTED_MAP_BASE_BYTES);
    let state = AccumulatorState {
        row_count: 1,
        accumulators: vec![Accumulator::Extremum(BTreeMap::from([(
            AggregateValue::Int(1),
            1,
        )]))],
    };
    assert!(processor.accumulator_admission(1, 1).unwrap() >= state.estimated_dynamic_bytes());
    assert!(matches!(
        processor.accumulator_admission(usize::MAX, 1),
        Err(DataFusionError::ResourcesExhausted(_))
    ));
    assert!(matches!(
        processor.accumulator_admission(1, usize::MAX),
        Err(DataFusionError::ResourcesExhausted(_))
    ));
}

#[test]
fn counted_growth_uses_nullable_filter_bitmaps_and_non_null_arguments() {
    for rows in [0, 1, 37, 4096] {
        for nullable in [false, true] {
            let values: ArrayRef = Arc::new(Int64Array::from_iter(
                (0..rows + 5).map(|row| (!nullable || row % 3 != 0).then_some(row as i64)),
            ));
            let filter: ArrayRef = Arc::new(BooleanArray::from_iter((0..rows + 5).map(|row| {
                if row % 7 == 0 {
                    None
                } else {
                    Some(row % 2 == 0)
                }
            })));
            let absent: ArrayRef = Arc::new(BooleanArray::from(vec![Some(false); rows + 5]));
            let batch = RecordBatch::try_from_iter(vec![
                ("value", values),
                ("filter", filter),
                ("absent", absent),
            ])
            .unwrap()
            .slice(3, rows);
            let mut processor = processor(Arc::new(TestBroker::new(16 << 20)));
            processor.calls = [None, Some(1), Some(2)]
                .map(|filter_index| Call {
                    function: proto::AggregateFunction::Count,
                    distinct: true,
                    filter_index,
                    ..counted_call()
                })
                .to_vec();
            processor.calls.push(Call {
                filter_index: Some(1),
                ..counted_call()
            });
            let old_bound = processor.accumulator_admission(1, rows).unwrap();
            let eligible = processor
                .calls
                .iter()
                .map(|call| {
                    (0..rows)
                        .filter(|&row| {
                            aggregate_filter(call, &batch, row).unwrap()
                                && !batch.column(0).is_null(row)
                        })
                        .count()
                })
                .sum::<usize>();
            let bound = processor.accumulator_input_admission(1, &batch).unwrap();
            assert_eq!(
                bound,
                processor.accumulator_admission(1, 0).unwrap()
                    + eligible * accumulator::counted_map_entry_bytes()
            );
            assert!(bound <= old_bound);
            if rows > 1 {
                assert!(bound < old_bound);
            }
        }
    }
}

#[test]
fn gather_capacity_covers_sliced_nullable_nested_decimal_and_dictionary_keys() {
    let wide = "é".repeat(8192);
    let strings: ArrayRef = Arc::new(StringArray::from(vec![
        Some("prefix"),
        None,
        Some(wide.as_str()),
        Some("tail"),
    ]));
    let ints: ArrayRef = Arc::new(Int64Array::from(vec![Some(1), None, Some(3), Some(4)]));
    let list: ArrayRef = Arc::new(ListArray::from_iter_primitive::<Int64Type, _, _>(vec![
        Some(vec![Some(1)]),
        None,
        Some(vec![Some(2), None, Some(3)]),
        Some(vec![]),
    ]));
    let structure: ArrayRef = Arc::new(StructArray::from(vec![
        (
            Arc::new(Field::new("text", DataType::Utf8, true)),
            strings.clone(),
        ),
        (
            Arc::new(Field::new("nested", list.data_type().clone(), true)),
            list.clone(),
        ),
    ]));
    let mut dictionary = StringDictionaryBuilder::<Int32Type>::new();
    dictionary.append("prefix").unwrap();
    dictionary.append_null();
    dictionary.append(&wide).unwrap();
    dictionary.append("tail").unwrap();
    let dictionary: ArrayRef = Arc::new(dictionary.finish());
    let decimals: ArrayRef = Arc::new(
        Decimal128Array::from(vec![Some(10), None, Some(30), Some(40)])
            .with_precision_and_scale(30, 4)
            .unwrap(),
    );
    let binary: ArrayRef = Arc::new(BinaryArray::from(vec![
        Some(b"p".as_slice()),
        None,
        Some(wide.as_bytes()),
        Some(b"t".as_slice()),
    ]));
    for column in [strings, ints, list, structure, dictionary, decimals, binary] {
        for sliced in [false, true] {
            let column = if sliced {
                column.slice(1, 2)
            } else {
                column.clone()
            };
            let rows = column.len();
            let batch = RecordBatch::try_new(
                Arc::new(Schema::new(vec![Field::new(
                    "key",
                    column.data_type().clone(),
                    true,
                )])),
                vec![column],
            )
            .unwrap();
            let broker = Arc::new(TestBroker::new(8 << 20));
            let processor = processor(broker.clone());
            let mut events = OutputEvents::with_capacity(rows * 2, 0);
            for row in (0..rows).rev() {
                events.push(row as u32, UPDATE_BEFORE, vec![]);
                events.push(row as u32, UPDATE_AFTER, vec![]);
            }
            let allowance = processor.output_admission(&batch, &events).unwrap();
            let output = processor.output_batch(&batch, events).unwrap();
            assert!(
                output.get_array_memory_size() <= allowance,
                "{:?}",
                output.schema()
            );
            assert_eq!(output.num_rows(), rows * 2);
            drop(processor);
            assert_eq!(broker.reserved(), 0);
        }
    }
}

#[test]
fn historical_variable_width_values_are_counted_per_emitted_event() {
    let broker = Arc::new(TestBroker::new(8 << 20));
    let mut processor = processor(broker);
    processor.calls.push(Call {
        function: proto::AggregateFunction::Min,
        input_index: Some(0),
        filter_index: None,
        distinct: false,
        input_type: Some(DataType::Utf8),
        output_type: DataType::Utf8,
        retractable: true,
    });
    let batch = RecordBatch::try_new(
        Arc::new(Schema::new(vec![Field::new("key", DataType::Int64, false)])),
        vec![Arc::new(Int64Array::from(vec![1; 32]))],
    )
    .unwrap();
    let mut events = OutputEvents::with_capacity(64, 1);
    for row in 0..32 {
        for kind in [UPDATE_BEFORE, UPDATE_AFTER] {
            events.push(
                row,
                kind,
                vec![Some(AggregateValue::Bytes(vec![b'x'; 65536]))],
            );
        }
    }
    let allowance = processor.output_admission(&batch, &events).unwrap();
    assert!(allowance > 64 * 65536);
    let output = processor.output_batch(&batch, events).unwrap();
    assert!(output.get_array_memory_size() <= allowance);
}

#[test]
fn output_denial_precedes_state_commit_and_releases_batch_scratch() {
    for (limit, reads) in [(128 << 10, 0), (384 << 10, 1)] {
        let broker = Arc::new(TestBroker::new(limit));
        let mut processor = processor(broker.clone());
        let text = "x".repeat(65536);
        let batch = RecordBatch::try_new(
            Arc::new(Schema::new(vec![
                Field::new("key", DataType::Utf8, false),
                Field::new("__streamfusion_input_row_kind", DataType::Int8, false),
            ])),
            vec![
                Arc::new(StringArray::from(vec![text])),
                Arc::new(Int8Array::from(vec![INSERT])),
            ],
        )
        .unwrap();
        processor
            .prepare_schema(batch.schema(), batch.num_columns())
            .unwrap();
        let before = broker.reserved();
        let failure = processor.process_arrow(batch).unwrap_err();
        assert!(
            matches!(failure, DataFusionError::ResourcesExhausted(_)),
            "{failure}"
        );
        assert_eq!(processor.state_read_batches, reads);
        assert_eq!(processor.state_write_batches, 0);
        assert_eq!(broker.reserved(), before);
        // A denied materialization must not create the DISTINCT key in persistent state.
        for group in 0..16 {
            let snapshot = processor.snapshot_key_group(group).unwrap();
            assert!(decode_key_group_snapshot(group, &snapshot)
                .unwrap()
                .is_empty());
        }
        drop(processor);
        assert_eq!(broker.reserved(), 0);
    }
}

#[test]
fn batch_event_credit_includes_hidden_historical_extrema_and_new_key_values() {
    let broker = Arc::new(TestBroker::new(8 << 20));
    let mut processor = processor(broker);
    processor.calls.push(Call {
        function: proto::AggregateFunction::Min,
        input_index: Some(0),
        filter_index: None,
        distinct: false,
        input_type: Some(DataType::Utf8),
        output_type: DataType::Utf8,
        retractable: true,
    });
    let values = BTreeMap::from([
        (AggregateValue::Bytes(b"a".to_vec()), 1),
        (AggregateValue::Bytes(vec![b'z'; 65536]), 1),
    ]);
    let states = vec![
        Some(AccumulatorState {
            row_count: 2,
            accumulators: vec![Accumulator::Extremum(values)],
        }),
        None,
    ];
    let batch = RecordBatch::try_new(
        Arc::new(Schema::new(vec![Field::new("value", DataType::Utf8, true)])),
        vec![Arc::new(StringArray::from(vec![
            Some("a"),
            None,
            Some("longer"),
            Some("x"),
        ]))],
    )
    .unwrap();
    // Removing "a" can expose the much wider "zzz..." value. The fresh second key must
    // include "longer" even in its later row whose incoming value is only one byte.
    assert_eq!(
        processor
            .event_admission(&batch, &states, &[0, 0, 1, 1])
            .unwrap(),
        2 * (2 * 65536 + 2 * 6)
    );
}
