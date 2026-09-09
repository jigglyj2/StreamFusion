// Copyright 2026 StreamFusion Authors
// Licensed under the Apache License, Version 2.0.

use super::*;

fn wire(values: &[(AggregateValue, i64)], legacy: bool) -> Vec<u8> {
    let mut bytes = Vec::new();
    bytes.extend_from_slice(&(values.len() as u32).to_le_bytes());
    for (value, count) in values {
        if legacy {
            let AggregateValue::Int(value) = value else {
                panic!("legacy integer")
            };
            bytes.extend_from_slice(&value.to_le_bytes());
        } else {
            encode_value(value, &mut bytes);
        }
        bytes.extend_from_slice(&count.to_le_bytes());
    }
    bytes
}

fn verify(values: Vec<(AggregateValue, i64)>, legacy: bool) {
    let mut expected = BTreeMap::new();
    for (value, count) in &values {
        expected.insert(value.clone(), *count);
    }
    let bytes = wire(&values, legacy);
    let mut cursor = Cursor::new(&bytes);
    let actual = decode_counted_values(&mut cursor, legacy).unwrap();
    assert!(cursor.is_empty());
    assert_eq!(actual, expected);
    let mut expected_bytes = Vec::new();
    let mut actual_bytes = Vec::new();
    encode_counted_values(&expected, &mut expected_bytes);
    encode_counted_values(&actual, &mut actual_bytes);
    assert_eq!(actual_bytes, expected_bytes);
}

#[test]
fn bulk_decode_preserves_sorted_unsorted_duplicate_and_legacy_maps() {
    for entries in [0, 1, 12, 4096] {
        for legacy in [false, true] {
            let mut values = (0..entries)
                .map(|value| {
                    (
                        AggregateValue::Int(value),
                        if value % 7 == 0 { -1 } else { 1 },
                    )
                })
                .collect::<Vec<_>>();
            verify(values.clone(), legacy);
            values.reverse();
            verify(values.clone(), legacy);
            values.extend(values.clone());
            verify(values, legacy);
        }
    }
    verify(
        vec![
            (AggregateValue::Float64(0x7ff8_0000_0000_0001), 1),
            (AggregateValue::Float64(0x7ff8_0000_0000_0002), -3),
            (AggregateValue::Float64((-0.0f64).to_bits()), 5),
            (AggregateValue::Float64(0.0f64.to_bits()), -7),
        ],
        false,
    );
    verify(
        vec![
            (AggregateValue::Float32(0x7fc0_0001), 1),
            (AggregateValue::Float32(0x7fc0_0002), -3),
        ],
        false,
    );
    verify(
        vec![
            (AggregateValue::Bytes("é\0".as_bytes().to_vec()), 2),
            (AggregateValue::Bytes(vec![]), -1),
            (AggregateValue::Bytes("é\0".as_bytes().to_vec()), 7),
        ],
        false,
    );
}

#[test]
fn truncated_map_lengths_fail_before_allocating_the_staging_vector() {
    for legacy in [false, true] {
        let bytes = u32::MAX.to_le_bytes();
        let (result, observed) = crate::allocation_test_support::measure(|| {
            decode_counted_values(&mut Cursor::new(&bytes), legacy)
        });
        assert!(result.is_err());
        assert!(observed.peak < 4096);
    }
}
