// Copyright 2026 StreamFusion Authors
// Licensed under the Apache License, Version 2.0.

use super::*;
use arrow::array::{
    Array, ArrayRef, BinaryArray, Decimal128Array, Int32Array, Int8Array, ListArray, StringArray,
};
use arrow::datatypes::Int32Type;

fn nested_batch() -> RecordBatch {
    RecordBatch::try_from_iter(vec![
        (
            "key",
            Arc::new(Int32Array::from(vec![0, 1, 2, 3, 4, 5])) as ArrayRef,
        ),
        (
            "label",
            Arc::new(StringArray::from(vec![
                Some("prefix"),
                None,
                Some("é"),
                Some("long-value"),
                None,
                Some("suffix"),
            ])) as ArrayRef,
        ),
        (
            "nested",
            Arc::new(ListArray::from_iter_primitive::<Int32Type, _, _>(vec![
                Some(vec![Some(9)]),
                None,
                Some(vec![Some(2), None]),
                Some(vec![]),
                Some(vec![Some(4)]),
                Some(vec![Some(5)]),
            ])) as ArrayRef,
        ),
        (
            "decimal",
            Arc::new(
                Decimal128Array::from(vec![Some(10), None, Some(-20), Some(30), None, Some(40)])
                    .with_precision_and_scale(20, 3)
                    .unwrap(),
            ) as ArrayRef,
        ),
        (
            "__streamfusion_row_kind",
            Arc::new(Int8Array::from(vec![0, 1, 2, 3, 0, 1])) as ArrayRef,
        ),
    ])
    .unwrap()
}

#[test]
fn contiguous_destinations_share_sliced_payload_buffers_and_ipc_preserves_offsets() {
    let batch = nested_batch().slice(1, 4);
    let rows = UInt32Array::from(vec![1, 2]);
    let selected = materialize(&batch, &rows, batch.num_columns()).unwrap();
    let original_keys = batch
        .column(0)
        .as_any()
        .downcast_ref::<Int32Array>()
        .unwrap();
    let selected_keys = selected
        .column(0)
        .as_any()
        .downcast_ref::<Int32Array>()
        .unwrap();
    assert_eq!(selected_keys.values().as_ptr(), unsafe {
        original_keys.values().as_ptr().add(1)
    });
    let original_strings = batch
        .column(1)
        .as_any()
        .downcast_ref::<StringArray>()
        .unwrap();
    let selected_strings = selected
        .column(1)
        .as_any()
        .downcast_ref::<StringArray>()
        .unwrap();
    assert_eq!(
        selected_strings.value_data().as_ptr(),
        original_strings.value_data().as_ptr()
    );
    let original_list = batch
        .column(2)
        .as_any()
        .downcast_ref::<ListArray>()
        .unwrap();
    let selected_list = selected
        .column(2)
        .as_any()
        .downcast_ref::<ListArray>()
        .unwrap();
    assert!(Arc::ptr_eq(original_list.values(), selected_list.values()));
    let expected = RecordBatch::try_new(
        batch.schema(),
        batch
            .columns()
            .iter()
            .map(|column| take(column.as_ref(), &rows, None).unwrap())
            .collect(),
    )
    .unwrap();
    let decoded = crate::exchange::IpcBatchFrame::encode(&selected)
        .unwrap()
        .decode(batch.schema())
        .unwrap();
    assert_eq!(decoded, expected);
    assert_eq!(
        materialize(
            &batch,
            &UInt32Array::from(vec![0, 1, 2, 3]),
            batch.num_columns()
        )
        .unwrap(),
        batch
    );
}

#[test]
fn scattered_destinations_never_gather_input_only_key_sidecars() {
    let large = vec![7u8; 2 << 20];
    let batch = RecordBatch::try_from_iter(vec![
        ("key", Arc::new(Int32Array::from(vec![0, 1, 2])) as ArrayRef),
        (
            "preencoded",
            Arc::new(BinaryArray::from(vec![large.as_slice(); 3])) as ArrayRef,
        ),
    ])
    .unwrap();
    let rows = UInt32Array::from(vec![0, 2]);
    let (selected, observed) =
        crate::allocation_test_support::measure(|| materialize(&batch, &rows, 1).unwrap());
    assert!(
        observed.peak < 64 << 10,
        "input-only key payload was gathered: {observed:?}"
    );
    assert_eq!(selected.num_columns(), 1);
    assert_eq!(
        selected
            .column(0)
            .as_any()
            .downcast_ref::<Int32Array>()
            .unwrap()
            .values()
            .as_ref(),
        &[0, 2]
    );
    assert!(materialize(&batch, &rows, 3)
        .unwrap_err()
        .to_string()
        .contains("transport column count"));
    assert_eq!(
        materialize(&batch, &UInt32Array::from(Vec::<u32>::new()), 1)
            .unwrap()
            .num_rows(),
        0
    );
}
