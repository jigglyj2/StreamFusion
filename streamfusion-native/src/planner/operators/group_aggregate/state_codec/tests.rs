// Copyright 2026 StreamFusion Authors
// Licensed under the Apache License, Version 2.0

use super::*;
use crate::allocation_test_support::measure;

#[test]
fn sizing_is_allocation_free_and_matches_every_accumulator_wire_variant() {
    for entries in [0, 1, 12, 1024] {
        for value in [
            AggregateValue::Boolean(true),
            AggregateValue::Int(i128::MIN),
            AggregateValue::Float32(f32::NAN.to_bits()),
            AggregateValue::Float64((-0.0f64).to_bits()),
            AggregateValue::Bytes("é".repeat(4096).into_bytes()),
        ] {
            let counted: BTreeMap<_, _> = (0..entries)
                .map(|index| (AggregateValue::Int(index), 1))
                .collect();
            let state = AccumulatorState {
                row_count: 1,
                accumulators: vec![
                    Accumulator::Count(0),
                    Accumulator::Count(7),
                    Accumulator::DistinctCount {
                        count: entries as i64,
                        values: counted.clone(),
                    },
                    Accumulator::Sum {
                        count: 1,
                        value: Some(value.clone()),
                    },
                    Accumulator::DistinctSum {
                        count: 1,
                        value: Some(value.clone()),
                        values: counted.clone(),
                    },
                    Accumulator::Average {
                        count: 1,
                        value: Some(value.clone()),
                    },
                    Accumulator::DistinctAverage {
                        count: 1,
                        value: Some(value.clone()),
                        values: counted.clone(),
                    },
                    Accumulator::AppendExtremum(None),
                    Accumulator::AppendExtremum(Some(value.clone())),
                    Accumulator::Extremum(counted),
                    Accumulator::Extremum(BTreeMap::from([(value, 1)])),
                ],
            };
            let (size, observed) = measure(|| encoded_state_size(&state));
            assert_eq!(observed.live, 0);
            assert_eq!(observed.peak, 0);
            let (bytes, observed) = measure(|| encode_state(&state));
            assert_eq!(bytes.len(), size);
            assert!(bytes.capacity() <= size * 2);
            assert_eq!(observed.live as usize, bytes.capacity());
            assert!(observed.peak <= size * 3);
            assert_eq!(&bytes[..4], STATE_MAGIC);
            assert_eq!(bytes[4], STATE_VERSION);
        }
    }
}
