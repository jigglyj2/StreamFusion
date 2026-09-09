// Copyright 2026 StreamFusion Authors
// Licensed under the Apache License, Version 2.0.

use super::*;
use crate::allocation_test_support::measure;
use crate::memory_pool::tests_support::TestBroker;
use std::sync::Arc;

fn call(
    function: proto::AggregateFunction,
    kind: DataType,
    distinct: bool,
    retractable: bool,
) -> Call {
    Call {
        function,
        input_index: Some(0),
        filter_index: None,
        distinct,
        input_type: Some(kind.clone()),
        output_type: if function == proto::AggregateFunction::Count {
            DataType::Int64
        } else {
            kind
        },
        retractable,
    }
}

#[test]
fn current_state_workspace_covers_decoded_maps_and_exact_mutations_without_allocating() {
    for count in [0, 1, 2, 11, 12, 64, 256, 4096] {
        for kind in [
            DataType::Boolean,
            DataType::Int64,
            DataType::Float32,
            DataType::Float64,
            DataType::Utf8,
        ] {
            let values = (0..count)
                .map(|index| {
                    let value = match kind {
                        DataType::Boolean => AggregateValue::Boolean(index % 2 == 0),
                        DataType::Int64 => AggregateValue::Int(index),
                        DataType::Float32 => AggregateValue::Float32((index as f32).to_bits()),
                        DataType::Float64 => AggregateValue::Float64((index as f64).to_bits()),
                        _ => AggregateValue::Bytes(
                            format!("{index}-{}", "é".repeat(index as usize % 129)).into_bytes(),
                        ),
                    };
                    (value, if index % 7 == 0 { -1 } else { 1 })
                })
                .collect::<BTreeMap<_, _>>();
            let calls = vec![
                call(proto::AggregateFunction::Count, kind.clone(), true, true),
                call(proto::AggregateFunction::Min, kind.clone(), false, true),
                call(proto::AggregateFunction::Max, kind.clone(), false, false),
            ];
            let state = AccumulatorState {
                row_count: 1,
                accumulators: vec![
                    Accumulator::DistinctCount {
                        count: count as i64,
                        values: values.clone(),
                    },
                    Accumulator::Extremum(values.clone()),
                    Accumulator::AppendExtremum(values.keys().next_back().cloned()),
                ],
            };
            verify(&calls, &state);
        }
    }
    let calls = vec![
        call(proto::AggregateFunction::Sum, DataType::Int64, false, true),
        call(proto::AggregateFunction::Sum, DataType::Int64, true, true),
        call(proto::AggregateFunction::Avg, DataType::Int64, false, true),
        call(proto::AggregateFunction::Avg, DataType::Int64, true, true),
        call(
            proto::AggregateFunction::Count,
            DataType::Int64,
            false,
            true,
        ),
    ];
    let value = Some(AggregateValue::Int(-17));
    let values = BTreeMap::from([(AggregateValue::Int(-17), 1)]);
    verify(
        &calls,
        &AccumulatorState {
            row_count: 1,
            accumulators: vec![
                Accumulator::Sum {
                    value: value.clone(),
                    count: 1,
                },
                Accumulator::DistinctSum {
                    value: value.clone(),
                    count: 1,
                    values: values.clone(),
                },
                Accumulator::Average {
                    value: value.clone(),
                    count: 1,
                },
                Accumulator::DistinctAverage {
                    value,
                    count: 1,
                    values,
                },
                Accumulator::Count(1),
            ],
        },
    );
    let calls = calls.iter().cycle().take(500).cloned().collect::<Vec<_>>();
    verify(&calls, &AccumulatorState::new(&calls));
}

fn verify(calls: &[Call], state: &AccumulatorState) {
    let bytes = encode_state(state);
    let (credit, observed) = measure(|| workspace(&bytes, calls).unwrap());
    assert_eq!(observed.peak, 0);
    assert_eq!(observed.live, 0);
    let ((decoded, mutation), observed) = measure(|| {
        let decoded = decode_state(&bytes, calls).unwrap();
        let mutation = encode_state(&decoded);
        (decoded, mutation)
    });
    // Existing per-key batch admission owns the accumulator vectors and sparse initial nodes.
    let headers = calls.len()
        * (std::mem::size_of::<Accumulator>()
            + 64
            + super::super::super::accumulator::COUNTED_MAP_BASE_BYTES);
    assert!(
        observed.peak <= credit + headers,
        "credit={credit}, headers={headers}, measured={observed:?}"
    );
    assert_eq!(decoded, *state);
    assert_eq!(mutation, bytes);
}

#[test]
fn legacy_states_keep_conservative_admission_and_invalid_current_frames_fail_without_decoding() {
    let calls = [call(
        proto::AggregateFunction::Count,
        DataType::Int64,
        false,
        true,
    )];
    let state = AccumulatorState {
        row_count: 1,
        accumulators: vec![Accumulator::Count(7)],
    };
    let bytes = encode_state(&state);
    for version in 1..STATE_VERSION {
        let mut legacy = bytes.clone();
        legacy[4] = version;
        assert_eq!(workspace(&legacy, &calls).unwrap(), legacy.len() * 8 + 64);
        assert_eq!(decode_state(&legacy, &calls).unwrap(), state);
    }
    for end in 0..bytes.len() {
        assert!(workspace(&bytes[..end], &calls).is_err());
    }
    for (offset, value) in [(0, 0), (4, 0), (4, STATE_VERSION + 1), (13, 2), (17, 255)] {
        let mut invalid = bytes.clone();
        invalid[offset] = value;
        assert!(workspace(&invalid, &calls).is_err());
    }
    let mut trailing = bytes.clone();
    trailing.push(0);
    assert!(workspace(&trailing, &calls).is_err());
    let broker = Arc::new(TestBroker::new(1));
    let owner = HostMemoryReservation::new(broker.clone(), "denied decode");
    let (result, observed) =
        measure(|| reserve_decoded_values(&[Some(StateValue::Borrowed(&bytes))], &calls, &owner));
    assert!(matches!(
        result,
        Err(DataFusionError::ResourcesExhausted(_))
    ));
    assert!(observed.peak < 4096);
    assert_eq!(broker.reserved(), 0);
}
