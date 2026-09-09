// Copyright 2026 StreamFusion Authors
// Licensed under the Apache License, Version 2.0.

use super::datafusion_compute::Kernels;
use super::datafusion_rows::{RowInputs, RowKernels};
use super::*;

fn fixture(data_type: DataType, seed: usize) -> (Vec<Call>, RecordBatch) {
    let values = (0..203)
        .map(|row| {
            if row % 11 == 0 {
                None
            } else {
                let value = ((row * 17 + seed) % 13) as i128 - 6;
                Some(if data_type == DataType::Utf8 {
                    AggregateValue::Bytes(format!("é\0-{value}").into_bytes())
                } else {
                    AggregateValue::Int(value)
                })
            }
        })
        .collect::<Vec<_>>();
    let batch = RecordBatch::try_from_iter(vec![
        ("v", aggregate_array(&values, &data_type).unwrap()),
        (
            "active",
            Arc::new(BooleanArray::from_iter((0..203).map(|row| {
                if row % 7 == 0 {
                    None
                } else {
                    Some(row % 3 != 0)
                }
            }))) as ArrayRef,
        ),
    ])
    .unwrap()
    .slice(3, 197);
    let calls = [None, Some(1)]
        .into_iter()
        .flat_map(|filter_index| {
            [true, false].into_iter().map({
                let data_type = data_type.clone();
                move |distinct| Call {
                    function: proto::AggregateFunction::Count,
                    input_index: Some(0),
                    input_type: Some(data_type.clone()),
                    output_type: DataType::Int64,
                    retractable: true,
                    filter_index,
                    distinct,
                }
            })
        })
        .collect();
    (calls, batch)
}

#[test]
fn datafusion_distinct_count_rows_preserve_signed_membership_and_checkpoint_bytes() {
    for data_type in [DataType::Int64, DataType::Utf8] {
        for seed in 0..8 {
            let (calls, batch) = fixture(data_type.clone(), seed);
            let kernels = Kernels::new(&calls).unwrap();
            assert!(kernels.0.iter().all(Option::is_some));
            let inputs = RowInputs::new(&calls, &batch).unwrap();
            let mut expected = AccumulatorState::new(&calls);
            let mut actual = expected.clone();
            let mut rows = RowKernels::new(&calls, &kernels, &actual).unwrap();
            // Begin with unmatched retractions, then cancel and revisit the same values.
            for pass in 0..4 {
                for row in 0..batch.num_rows() {
                    let accumulate = (row + seed) % 5 != 0 && pass != 0;
                    expected.apply(&calls, &batch, row, accumulate).unwrap();
                    rows.apply(&mut actual, &calls, &inputs, &batch, row, accumulate)
                        .unwrap();
                    assert_eq!(
                        encode_state(&actual),
                        encode_state(&expected),
                        "{data_type:?}/{seed}/{pass}/{row}"
                    );
                    if row % 31 == 0 {
                        actual = decode_state(&encode_state(&actual), &calls).unwrap();
                        rows = RowKernels::new(&calls, &kernels, &actual).unwrap();
                    }
                }
            }
        }
    }
}

#[test]
fn datafusion_distinct_count_append_and_mixed_runs_preserve_selected_state() {
    for data_type in [DataType::Int64, DataType::Utf8] {
        for seed in 0..8 {
            let (calls, batch) = fixture(data_type.clone(), seed);
            let kernels = Kernels::new(&calls).unwrap();
            let inputs = RowInputs::new(&calls, &batch).unwrap();
            let mut expected = AccumulatorState::new(&calls);
            let mut actual = expected.clone();
            for pass in 0..4 {
                let rows = (0..batch.num_rows())
                    .filter(|row| (row + seed) % 3 != 0)
                    .collect::<Vec<_>>();
                let accumulate = (0..batch.num_rows())
                    .map(|row| pass % 2 == 0 || row % 5 >= 2)
                    .collect::<Vec<_>>();
                for chunk in rows.chunks(23) {
                    for &row in chunk {
                        expected
                            .apply(&calls, &batch, row, accumulate[row])
                            .unwrap();
                    }
                    actual
                        .apply_input_rows(
                            &calls,
                            &kernels,
                            &batch,
                            chunk,
                            &accumulate,
                            Some(&inputs),
                            false,
                        )
                        .unwrap();
                    assert_eq!(
                        encode_state(&actual),
                        encode_state(&expected),
                        "{data_type:?}/{seed}/{pass}"
                    );
                    actual = decode_state(&encode_state(&actual), &calls).unwrap();
                }
            }
            let bytes = encode_state(&actual);
            actual
                .apply_append_batch(&calls, &kernels, &batch, &[])
                .unwrap();
            assert_eq!(bytes, encode_state(&actual));
        }
    }
}

#[test]
fn datafusion_distinct_count_marker_kernel_cannot_accept_raw_grouped_inputs() {
    let (calls, _) = fixture(DataType::Int64, 0);
    assert!(grouped_compute::GroupedCompute::new(&calls)
        .unwrap()
        .is_none());
    assert!(grouped_compute::GroupedMerge::new(&calls)
        .unwrap()
        .is_none());
    let ordinary = calls
        .iter()
        .filter(|call| !call.distinct)
        .cloned()
        .collect::<Vec<_>>();
    assert!(grouped_compute::GroupedCompute::new(&ordinary)
        .unwrap()
        .is_some());
}
