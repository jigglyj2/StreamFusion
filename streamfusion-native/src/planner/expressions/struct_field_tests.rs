// Copyright 2026 StreamFusion Authors
// Licensed under the Apache License, Version 2.0

use std::sync::Arc;

use arrow::array::{Array, ArrayRef, Int32Array, NullArray, StringArray, StructArray};
use arrow::buffer::NullBuffer;
use arrow::datatypes::{DataType, Field, Schema};
use arrow::record_batch::RecordBatch;
use datafusion::physical_expr::expressions::Column;

#[test]
fn nullable_parent_masks_non_null_child_storage_after_slicing() {
    let fields = vec![
        Field::new("number", DataType::Int32, true),
        Field::new("text", DataType::Utf8, true),
    ];
    let parent = StructArray::new(
        fields.into(),
        vec![
            Arc::new(Int32Array::from(vec![Some(99), Some(7), Some(42), None])),
            Arc::new(StringArray::from(vec![
                Some("prefix"),
                Some("x"),
                Some("hidden"),
                None,
            ])),
        ],
        Some(NullBuffer::from(vec![true, true, false, true])),
    );
    let parent: ArrayRef = Arc::new(parent.slice(1, 3));
    let schema = Arc::new(Schema::new(vec![Field::new(
        "parent",
        parent.data_type().clone(),
        true,
    )]));
    let batch = RecordBatch::try_new(Arc::clone(&schema), vec![parent]).unwrap();
    for name in ["number", "text"] {
        let expr = super::create(Arc::new(Column::new("parent", 0)), name, &schema).unwrap();
        let result = expr.evaluate(&batch).unwrap().into_array(3).unwrap();
        let parent = batch
            .column(0)
            .as_any()
            .downcast_ref::<StructArray>()
            .unwrap();
        let original = parent.column_by_name(name).unwrap().to_data();
        for (actual, expected) in result.to_data().buffers().iter().zip(original.buffers()) {
            assert_eq!(
                actual.as_ptr(),
                expected.as_ptr(),
                "field values must remain shared"
            );
        }
        assert!(result.is_valid(0));
        assert!(result.is_null(1), "null parent must mask {name}");
        assert!(
            result.is_null(2),
            "child's own null must survive for {name}"
        );
        if name == "number" {
            assert_eq!(
                result
                    .as_any()
                    .downcast_ref::<Int32Array>()
                    .unwrap()
                    .value(0),
                7
            );
        } else {
            assert_eq!(
                result
                    .as_any()
                    .downcast_ref::<StringArray>()
                    .unwrap()
                    .value(0),
                "x"
            );
        }
    }
}

#[test]
fn null_typed_field_needs_no_physical_validity_bitmap() {
    let parent = Arc::new(StructArray::new(
        vec![Field::new("nothing", DataType::Null, true)].into(),
        vec![Arc::new(NullArray::new(2))],
        Some(NullBuffer::from(vec![true, false])),
    ));
    let schema = Arc::new(Schema::new(vec![Field::new(
        "parent",
        parent.data_type().clone(),
        true,
    )]));
    let batch = RecordBatch::try_new(Arc::clone(&schema), vec![parent]).unwrap();
    let result = super::create(Arc::new(Column::new("parent", 0)), "nothing", &schema)
        .unwrap()
        .evaluate(&batch)
        .unwrap()
        .into_array(2)
        .unwrap();
    assert_eq!(result.data_type(), &DataType::Null);
    assert_eq!(result.logical_null_count(), 2);
}
