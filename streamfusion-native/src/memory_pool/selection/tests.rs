// Copyright 2026 StreamFusion Authors
// Licensed under the Apache License, Version 2.0

use super::*;
use arrow::array::{
    BooleanArray, Decimal128Array, DictionaryArray, Int32Array, ListArray, MapArray, StringArray,
    StringViewArray, StructArray, UInt32Array,
};
use arrow::buffer::{NullBuffer, OffsetBuffer};
use arrow::compute::take;
use arrow::datatypes::{Field, Int32Type};
use std::sync::Arc;

#[test]
fn skewed_sliced_nested_dictionary_decimal_and_view_selections_fit_the_allowance() {
    let wide = "é".repeat(64 * 1024);
    let text: ArrayRef = Arc::new(StringArray::from(vec![
        Some("discard"),
        Some(&wide),
        None,
        Some("tail"),
    ]));
    let list: ArrayRef = Arc::new(ListArray::from_iter_primitive::<Int32Type, _, _>([
        Some(vec![Some(0)]),
        Some(vec![Some(7); 8192]),
        None,
        Some(vec![None]),
    ]));
    let nested: ArrayRef = Arc::new(StructArray::new(
        vec![
            Arc::new(Field::new("text", text.data_type().clone(), true)),
            Arc::new(Field::new("list", list.data_type().clone(), true)),
        ]
        .into(),
        vec![text.clone(), list.clone()],
        Some(NullBuffer::from(vec![true, true, false, true])),
    ));
    let dictionary: ArrayRef = Arc::new(
        DictionaryArray::<Int32Type>::try_new(Int32Array::from(vec![0, 1, 2, 3]), text.clone())
            .unwrap(),
    );
    let views: ArrayRef = Arc::new(StringViewArray::from(vec![
        Some("discard"),
        Some(&wide),
        None,
        Some("tail"),
    ]));
    let keys: ArrayRef = Arc::new(StringArray::from(vec!["k", "one", "two", "last"]));
    let entries = StructArray::new(
        vec![
            Arc::new(Field::new("key", keys.data_type().clone(), false)),
            Arc::new(Field::new("value", text.data_type().clone(), true)),
        ]
        .into(),
        vec![keys, text.clone()],
        None,
    );
    let map: ArrayRef = Arc::new(MapArray::new(
        Arc::new(Field::new("entries", entries.data_type().clone(), false)),
        OffsetBuffer::new(vec![0, 1, 3, 3, 4].into()),
        entries,
        None,
        false,
    ));
    let sources = vec![
        text,
        list,
        nested,
        dictionary,
        views,
        map,
        Arc::new(
            Decimal128Array::from(vec![Some(0), Some(-999), None, Some(123)])
                .with_precision_and_scale(20, 3)
                .unwrap(),
        ),
        Arc::new(BooleanArray::from(vec![
            Some(false),
            Some(true),
            None,
            Some(false),
        ])),
    ]
    .into_iter()
    .map(|array| array.slice(1, 3))
    .collect::<Vec<_>>();
    let selected = [0, 0, 0, 1, 2, 0, 2];
    let indices = UInt32Array::from(selected.to_vec());
    let allowance = selected
        .iter()
        .try_fold(fixed_allowance(&sources).unwrap(), |bytes, row| {
            add(bytes, row_allowance(&sources, *row as usize)?)
        })
        .unwrap();
    let outputs = sources
        .iter()
        .map(|source| take(source.as_ref(), &indices, None).unwrap())
        .collect::<Vec<_>>();
    let actual = outputs
        .iter()
        .map(|array| array.get_array_memory_size())
        .sum::<usize>();
    assert!(allowance >= actual, "{allowance} must cover {actual}");
    assert!(row_allowance(&sources, 0).unwrap() > row_allowance(&sources, 2).unwrap());
}

#[test]
fn repeated_wide_row_is_not_estimated_from_average_width() {
    let wide = "x".repeat(64 * 1024);
    let mut rows = vec![""; 4096];
    rows[17] = &wide;
    let sources: Vec<ArrayRef> = vec![Arc::new(StringArray::from(rows))];
    let average = sources[0].get_array_memory_size() / 4096;
    assert!(row_allowance(&sources, 17).unwrap() >= wide.len());
    assert!(row_allowance(&sources, 17).unwrap() > average * 100);
    assert!(row_allowance(&sources, 4096).is_err());
    assert!(multiply(usize::MAX, 2).is_err());
    assert!(add(usize::MAX, 1).is_err());
}
