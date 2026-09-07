// Copyright 2026 StreamFusion Authors
// Licensed under the Apache License, Version 2.0
use super::datafusion_compute::Kernels;
use super::*;

#[test]
fn datafusion_batches_preserve_flink_integer_overflow_filters_nulls_and_accumulator_bytes() {
    for data_type in [
        DataType::Int8,
        DataType::Int16,
        DataType::Int32,
        DataType::Int64,
    ] {
        for seed in 0..12i128 {
            let values = (0..67)
                .map(|i| {
                    if i % 9 == 0 {
                        None
                    } else {
                        Some(AggregateValue::Int(match i % 4 {
                            0 => i64::MAX as i128,
                            1 => i64::MIN as i128,
                            _ => seed * 31 + i as i128,
                        }))
                    }
                })
                .collect::<Vec<_>>();
            let input = aggregate_array(&values, &data_type).unwrap();
            let batch = RecordBatch::try_from_iter(vec![
                ("v", input),
                (
                    "active",
                    Arc::new(BooleanArray::from_iter((0..67).map(|i| {
                        if i % 7 == 0 {
                            None
                        } else {
                            Some(i % 3 != 0)
                        }
                    }))) as ArrayRef,
                ),
            ])
            .unwrap();
            let calls = [
                proto::AggregateFunction::CountStar,
                proto::AggregateFunction::Count,
                proto::AggregateFunction::Sum,
                proto::AggregateFunction::Sum0,
                proto::AggregateFunction::Avg,
                proto::AggregateFunction::Min,
                proto::AggregateFunction::Max,
            ]
            .into_iter()
            .map(|function| Call {
                function,
                input_index: (function != proto::AggregateFunction::CountStar).then_some(0),
                filter_index: Some(1),
                distinct: false,
                input_type: (function != proto::AggregateFunction::CountStar)
                    .then_some(data_type.clone()),
                output_type: if matches!(
                    function,
                    proto::AggregateFunction::Count | proto::AggregateFunction::CountStar
                ) {
                    DataType::Int64
                } else {
                    data_type.clone()
                },
                retractable: false,
            })
            .collect::<Vec<_>>();
            let kernels = Kernels::new(&calls).unwrap();
            assert!(kernels.0.iter().all(Option::is_some));
            let mut reference = AccumulatorState::new(&calls);
            let mut actual = reference.clone();
            for start in (0..67).step_by(7) {
                let rows = (start..(start + 7).min(67)).collect::<Vec<_>>();
                for &row in &rows {
                    reference.apply(&calls, &batch, row, true).unwrap();
                }
                actual
                    .apply_append_batch(&calls, &kernels, &batch, &rows)
                    .unwrap();
                assert_eq!(
                    encode_state(&actual),
                    encode_state(&reference),
                    "{data_type:?}, seed {seed}, offset {start}"
                );
            }
        }
    }
}

#[test]
fn mixed_calls_preserve_order_sensitive_flink_arithmetic() {
    let batch = RecordBatch::try_from_iter(vec![(
        "v",
        Arc::new(Float64Array::from(vec![
            1e20,
            1.0,
            -1e20,
            -0.0,
            0.0,
            f64::NAN,
        ])) as ArrayRef,
    )])
    .unwrap();
    let calls = [
        proto::AggregateFunction::Sum,
        proto::AggregateFunction::Avg,
        proto::AggregateFunction::Min,
        proto::AggregateFunction::Max,
    ]
    .into_iter()
    .map(|function| Call {
        function,
        input_index: Some(0),
        filter_index: None,
        distinct: false,
        input_type: Some(DataType::Float64),
        output_type: DataType::Float64,
        retractable: false,
    })
    .collect::<Vec<_>>();
    let kernels = Kernels::new(&calls).unwrap();
    let mut expected = AccumulatorState::new(&calls);
    let mut actual = expected.clone();
    for row in 0..6 {
        expected.apply(&calls, &batch, row, true).unwrap();
    }
    actual
        .apply_append_batch(&calls, &kernels, &batch, &(0..6).collect::<Vec<_>>())
        .unwrap();
    assert_eq!(actual, expected);
    assert!(calls.iter().all(|call| !datafusion_compute::reusable(call)));
}
