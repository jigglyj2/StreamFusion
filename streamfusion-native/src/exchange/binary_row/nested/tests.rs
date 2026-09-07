// Copyright 2026 StreamFusion Authors
// Licensed under the Apache License, Version 2.0.

use super::*;
use arrow::array::{
    ArrayRef, Float32Builder, Int32Builder, ListBuilder, MapBuilder, StringBuilder,
};
use arrow::datatypes::{Field, Schema};
use std::sync::Arc;

fn assert_bytes(array: ArrayRef, expected: &str) {
    let batch = RecordBatch::try_new(
        Arc::new(Schema::new(vec![Field::new(
            "key",
            array.data_type().clone(),
            true,
        )])),
        vec![array],
    )
    .unwrap();
    let bytes = encode_binary_row(&batch, 0, &[(0, KeyField::Nested)]).unwrap();
    let hex: String = bytes.iter().map(|byte| format!("{byte:02x}")).collect();
    assert_eq!(hex, expected);
    let mut scratch = Vec::with_capacity(bytes.len());
    let (result, observed) = crate::allocation_test_support::measure(|| {
        encode_binary_row_into(&batch, 0, &[(0, KeyField::Nested)], &mut scratch)
    });
    result.unwrap();
    assert_eq!(scratch, bytes);
    assert_eq!(
        observed.peak, 0,
        "nested containers must not allocate temporary rows/arrays"
    );
}

// These exact bytes are independently checked with Flink's serializers in
// FlinkNestedKeyFixtureTest, including non-word-aligned map child containers.
#[test]
fn flink_nested_binary_layouts() {
    let mut list = ListBuilder::new(Int32Builder::new());
    list.values().append_value(1);
    list.values().append_null();
    list.values().append_value(-2);
    list.append(true);
    assert_bytes(
        Arc::new(list.finish()),
        concat!(
            "00000000000000001800000010000000",
            "03000000020000000100000000000000feffffff00000000"
        ),
    );

    let mut map = MapBuilder::new(None, StringBuilder::new(), Int32Builder::new());
    map.keys().append_value("a");
    map.values().append_value(7);
    map.keys().append_value("b");
    map.values().append_null();
    map.append(true).unwrap();
    assert_bytes(
        Arc::new(map.finish()),
        concat!(
            "00000000000000002c00000010000000",
            "18000000",
            "020000000000000061000000000000816200000000000081",
            "02000000020000000700000000000000",
            "00000000"
        ),
    );

    let structure = StructArray::from(vec![
        (
            Arc::new(Field::new("i", DataType::Int32, true)),
            Arc::new(Int32Array::from(vec![1])) as ArrayRef,
        ),
        (
            Arc::new(Field::new("s", DataType::Utf8, true)),
            Arc::new(StringArray::from(vec!["abc"])) as ArrayRef,
        ),
    ]);
    assert_bytes(
        Arc::new(structure),
        concat!(
            "00000000000000001800000010000000",
            "000000000000000001000000000000006162630000000083"
        ),
    );
}

#[test]
fn sliced_nested_keys_keep_offsets_and_reuse_scratch() {
    let mut list = ListBuilder::new(Int32Builder::new());
    for values in [vec![9], vec![1, 2], vec![]] {
        for value in values {
            list.values().append_value(value);
        }
        list.append(true);
    }
    let array: ArrayRef = Arc::new(list.finish());
    let schema = Arc::new(Schema::new(vec![Field::new(
        "key",
        array.data_type().clone(),
        true,
    )]));
    let full = RecordBatch::try_new(schema.clone(), vec![array.clone()]).unwrap();
    let slice = RecordBatch::try_new(schema, vec![array.slice(1, 2)]).unwrap();
    let fields = [(0, KeyField::Nested)];
    let mut scratch = Vec::with_capacity(256);
    let address = scratch.as_ptr();
    for row in 0..2 {
        encode_binary_row_into(&slice, row, &fields, &mut scratch).unwrap();
        assert_eq!(scratch, encode_binary_row(&full, row + 1, &fields).unwrap());
        assert_eq!(address, scratch.as_ptr());
    }
}

#[test]
fn recursive_offsets_and_array_nan_normalization_match_flink() {
    let mut list = ListBuilder::new(Int32Builder::new());
    list.values().append_value(1);
    list.values().append_null();
    list.values().append_value(-2);
    list.append(true);
    let list: ArrayRef = Arc::new(list.finish());
    let structure = StructArray::from(vec![(
        Arc::new(Field::new("a", list.data_type().clone(), true)),
        list,
    )]);
    assert_bytes(
        Arc::new(structure),
        concat!(
            "00000000000000002800000010000000",
            "00000000000000001800000010000000",
            "03000000020000000100000000000000feffffff00000000"
        ),
    );

    let mut floats = ListBuilder::new(Float32Builder::new());
    for bits in [0x7f80_0001, 0x8000_0000, 0xffc0_0012] {
        floats.values().append_value(f32::from_bits(bits));
    }
    floats.append(true);
    assert_bytes(
        Arc::new(floats.finish()),
        "0000000000000000180000001000000003000000000000000000c07f000000800000c07f00000000",
    );
}
