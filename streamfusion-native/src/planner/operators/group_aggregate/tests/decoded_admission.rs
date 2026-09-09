// Copyright 2026 StreamFusion Authors
// Licensed under the Apache License, Version 2.0.

use super::*;

#[test]
fn historical_distinct_state_uses_one_decode_allowance_and_preserves_transitions() {
    let broker = Arc::new(TestBroker::new(5 << 20));
    let mut processor = GroupAggregateProcessor::new(
        &aggregate_distinct_plan(),
        128,
        0,
        127,
        HostMemoryReservation::new(broker.clone(), "historical distinct state"),
    )
    .unwrap();
    let input = batch(vec![7], vec![Some(10_000)], Some(vec![INSERT]));
    processor
        .prepare_schema(input.schema(), input.num_columns())
        .unwrap();
    let key = processor.state_key(&input, 0).unwrap();
    let values = (0..10_000)
        .map(|value| (AggregateValue::Int(value), 1))
        .collect::<BTreeMap<_, _>>();
    let mut original = AccumulatorState {
        row_count: 10_000,
        accumulators: vec![
            Accumulator::DistinctCount {
                count: 10_000,
                values: values.clone(),
            },
            Accumulator::DistinctSum {
                value: Some(AggregateValue::Int(49_995_000)),
                count: 10_000,
                values,
            },
        ],
    };
    processor
        .state
        .write_batch(vec![StateMutation {
            key: key.clone(),
            value: Some(encode_state(&original)),
        }])
        .unwrap();
    // The old second decode allowance needs over 6 MiB for this 500 KiB historical value.
    // Five MiB is enough for the shared allowance, retained backend state, growth and output.
    let output = processor.process_arrow(input.clone()).unwrap();
    assert_eq!(output.num_rows(), 2);
    drop(output);
    original.apply(&processor.calls, &input, 0, true).unwrap();
    let refs = [StateKeyRef {
        key_group: key.key_group,
        key: &key.key,
    }];
    let stored = processor
        .state
        .get_batch(&refs, &processor.scratch_reservation)
        .unwrap();
    assert_eq!(stored[0].as_deref().unwrap(), encode_state(&original));
    drop(stored);
    let retract = batch(vec![7], vec![Some(10_000)], Some(vec![DELETE]));
    let output = processor.process_arrow(retract.clone()).unwrap();
    assert_eq!(output.num_rows(), 2);
    drop(output);
    original
        .apply(&processor.calls, &retract, 0, false)
        .unwrap();
    let stored = processor
        .state
        .get_batch(&refs, &processor.scratch_reservation)
        .unwrap();
    assert_eq!(stored[0].as_deref().unwrap(), encode_state(&original));
    drop(stored);
    drop(processor);
    assert_eq!(broker.reserved(), 0);
}

#[test]
fn shared_decode_credit_covers_counted_maps_and_concurrent_serialized_mutations() {
    for count in [0, 1, 12, 4096] {
        for strings in [false, true] {
            let data_type = if strings {
                DataType::Utf8
            } else {
                DataType::Int64
            };
            let calls = [
                proto::AggregateFunction::Count,
                proto::AggregateFunction::Min,
            ]
            .map(|function| Call {
                function,
                input_index: Some(0),
                filter_index: None,
                distinct: function == proto::AggregateFunction::Count,
                input_type: Some(data_type.clone()),
                output_type: if function == proto::AggregateFunction::Count {
                    DataType::Int64
                } else {
                    data_type.clone()
                },
                retractable: true,
            });
            let values = (0..count)
                .map(|index| {
                    let value = if strings {
                        AggregateValue::Bytes(format!("{index}-{}", "é".repeat(128)).into_bytes())
                    } else {
                        AggregateValue::Int(index)
                    };
                    (value, if index % 7 == 0 { -1 } else { 1 })
                })
                .collect::<BTreeMap<_, _>>();
            let state = AccumulatorState {
                row_count: count as i64,
                accumulators: vec![
                    Accumulator::DistinctCount {
                        count: count as i64,
                        values: values.clone(),
                    },
                    Accumulator::Extremum(values),
                ],
            };
            let encoded = encode_state(&state);
            let broker = Arc::new(TestBroker::new(64 << 20));
            let owner = HostMemoryReservation::new(broker.clone(), "decode test");
            let credit = crate::state::reserve_decoded_values(
                &[Some(crate::state::StateValue::Borrowed(&encoded))],
                &owner,
            )
            .unwrap();
            let ((decoded, mutation), measured) = crate::allocation_test_support::measure(|| {
                let decoded = decode_state(&encoded, &calls).unwrap();
                let mutation = encode_state(&decoded);
                (decoded, mutation)
            });
            // Accumulator headers and sparse initial nodes are part of batch admission,
            // independently of the historical-state allowance.
            let headers = calls.len()
                * (std::mem::size_of::<Accumulator>() + 64 + accumulator::COUNTED_MAP_BASE_BYTES);
            assert!(
                measured.peak <= credit.size() + headers,
                "count={count} strings={strings}: {measured:?}"
            );
            assert_eq!(decoded, state);
            assert_eq!(mutation, encoded);
            drop(credit);
            assert_eq!(broker.reserved(), 0);
        }
    }
}
