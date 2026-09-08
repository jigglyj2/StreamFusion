// Copyright 2026 StreamFusion Authors
// Licensed under the Apache License, Version 2.0.

use super::*;

#[test]
fn compact_group_vectors_preserve_ordered_integer_states_across_batches_and_filters() {
    let calls = [
        proto::AggregateFunction::CountStar,
        proto::AggregateFunction::Count,
        proto::AggregateFunction::Sum,
        proto::AggregateFunction::Sum0,
        proto::AggregateFunction::Min,
        proto::AggregateFunction::Max,
        proto::AggregateFunction::Avg,
    ]
    .into_iter()
    .map(|function| Call {
        function,
        input_index: (function != proto::AggregateFunction::CountStar).then_some(0),
        filter_index: Some(1),
        distinct: false,
        input_type: (function != proto::AggregateFunction::CountStar).then_some(DataType::Int64),
        output_type: DataType::Int64,
        retractable: false,
    })
    .collect::<Vec<_>>();
    let groups = (0..4096).collect::<Vec<_>>();
    let mut compute = GroupedCompute::new(&calls).unwrap().unwrap();
    let mut expected = (0..groups.len())
        .map(|_| AccumulatorState::new(&calls))
        .collect::<Vec<_>>();
    for pass in 0..4 {
        let value = match pass {
            0 => i64::MAX,
            1 => i64::MIN,
            2 => 1,
            _ => -71,
        };
        let values = Int64Array::from(
            (0..groups.len())
                .map(|row| (row % 11 != 0).then_some(value))
                .collect::<Vec<_>>(),
        );
        let filter = BooleanArray::from(
            (0..groups.len())
                .map(|row| match row % 13 {
                    0 => None,
                    1 => Some(false),
                    _ => Some(true),
                })
                .collect::<Vec<_>>(),
        );
        let valid = BooleanArray::from(
            (0..groups.len())
                .map(|row| (row + pass) % 17 != 0)
                .collect::<Vec<_>>(),
        );
        let batch = RecordBatch::try_from_iter(vec![
            ("value", Arc::new(values) as ArrayRef),
            ("filter", Arc::new(filter) as ArrayRef),
        ])
        .unwrap();
        compute
            .update(&calls, &batch, &groups, Some(&valid), groups.len())
            .unwrap();
        for row in 0..groups.len() {
            if valid.value(row) {
                expected[row].apply(&calls, &batch, row, true).unwrap();
            }
        }
    }
    // Seven aggregate vectors plus SUM/AVG counts and row counts stay substantially smaller
    // than a separate seven-accumulator Rust state for each key.
    assert!(
        compute.size() < groups.len() * 160,
        "grouped retained bytes={}",
        compute.size()
    );
    let output = compute.finish().unwrap();
    for (row, expected) in expected.iter().enumerate() {
        assert_eq!(
            encode_state(&output.state(&calls, row).unwrap()),
            encode_state(expected)
        );
    }
    assert!(compute.size() < 4096);
}
