// Copyright 2026 StreamFusion Authors
// Licensed under the Apache License, Version 2.0.

use super::*;

fn calls(data_type: DataType) -> Vec<Call> {
    [
        proto::AggregateFunction::CountStar,
        proto::AggregateFunction::Count,
        proto::AggregateFunction::Min,
        proto::AggregateFunction::Max,
    ]
    .into_iter()
    .map(|function| Call {
        function,
        input_index: Some(17), // Partial merge never indexes raw input or reapplies FILTER.
        filter_index: Some(23),
        input_type: Some(data_type.clone()),
        output_type: if matches!(
            function,
            proto::AggregateFunction::CountStar | proto::AggregateFunction::Count
        ) {
            DataType::Int64
        } else {
            data_type.clone()
        },
        retractable: false,
        distinct: false,
    })
    .collect()
}

#[test]
fn grouped_partial_merge_matches_canonical_bytes_across_batches_groups_nulls_and_overflow() {
    for data_type in [DataType::Int64, DataType::Utf8] {
        let calls = calls(data_type.clone());
        let mut compute = GroupedMerge::new(&calls).unwrap().unwrap();
        // Reuse the same physical accumulators after emitting all groups.
        for cycle in 0..2 {
            let mut expected = (0..73)
                .map(|_| AccumulatorState::new(&calls))
                .collect::<Vec<_>>();
            let seeds = expected.iter().collect::<Vec<_>>();
            compute
                .merge(&calls, &seeds, &(0..73).collect::<Vec<_>>(), 73)
                .unwrap();
            for pass in 0..7 {
                let groups = (0..1024)
                    .map(|row| (row * 17 + pass * 11) % 73)
                    .collect::<Vec<_>>();
                let partials = (0..groups.len())
                    .map(|row| {
                        let count = match (row + pass) % 5 {
                            0 => i64::MAX,
                            1 => i64::MIN,
                            _ => 7,
                        };
                        let value = ((row + pass) % 11 != 0).then(|| match data_type {
                            DataType::Int64 => {
                                AggregateValue::Int((row as i64 - 512 + cycle) as i128)
                            }
                            _ => AggregateValue::Bytes(
                                format!("v\0{}é", (row + pass) % 101).into_bytes(),
                            ),
                        });
                        AccumulatorState {
                            row_count: count,
                            accumulators: vec![
                                Accumulator::Count(count),
                                Accumulator::Count(if value.is_some() { count } else { 0 }),
                                Accumulator::AppendExtremum(value.clone()),
                                Accumulator::AppendExtremum(value),
                            ],
                        }
                    })
                    .collect::<Vec<_>>();
                for (state, &group) in partials.iter().zip(&groups) {
                    expected[group].merge(&calls, state).unwrap();
                }
                compute
                    .merge(&calls, &partials.iter().collect::<Vec<_>>(), &groups, 73)
                    .unwrap();
            }
            let output = compute.finish().unwrap();
            for (group, expected) in expected.iter().enumerate() {
                assert_eq!(
                    encode_state(&output.state(&calls, group).unwrap()),
                    encode_state(expected)
                );
            }
        }
    }
}

#[test]
fn unsupported_partial_semantics_are_explicit_and_invalid_groups_fail() {
    let mut unsupported = calls(DataType::Float64);
    assert!(GroupedMerge::new(&unsupported).unwrap().is_none());
    unsupported = calls(DataType::Int64);
    unsupported[0].distinct = true;
    assert!(GroupedMerge::new(&unsupported).unwrap().is_none());
    unsupported[0].distinct = false;
    unsupported[2].retractable = true;
    assert!(GroupedMerge::new(&unsupported).unwrap().is_none());
    let calls = calls(DataType::Int64);
    let mut compute = GroupedMerge::new(&calls).unwrap().unwrap();
    let state = AccumulatorState::new(&calls);
    assert!(compute.merge(&calls, &[&state], &[1], 1).is_err());
    assert!(compute.merge(&calls, &[&state], &[], 1).is_err());
}
