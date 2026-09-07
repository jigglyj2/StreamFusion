// Copyright 2026 StreamFusion Authors
// Licensed under the Apache License, Version 2.0.

use super::*;
use crate::memory_pool::{tests_support::TestBroker, FlinkMemoryPool};
use arrow::array::{ArrayRef, Int32Array, NullArray, StringArray};
use arrow::datatypes::{DataType, Field};
use datafusion::execution::memory_pool::{MemoryConsumer, MemoryPool};

fn memory(broker: &Arc<TestBroker>) -> MemoryReservation {
    let pool: Arc<dyn MemoryPool> = Arc::new(FlinkMemoryPool::new(broker.clone(), 64 << 20));
    MemoryConsumer::new("C Data test").register(&pool)
}

#[test]
fn bufferless_child_moves_keep_ownership_without_payload_reservations() {
    for input in [
        Arc::new(NullArray::new(7)) as ArrayRef,
        Arc::new(StructArray::new_empty_fields(7, None)) as ArrayRef,
    ] {
        let broker = Arc::new(TestBroker::new(64 << 20));
        let batch = RecordBatch::try_from_iter(vec![("bufferless", input)]).unwrap();
        let output = array(batch, memory(&broker)).unwrap();
        let child = unsafe { FFI_ArrowArray::from_raw(*output.children) };
        assert_eq!(
            child.n_buffers,
            if child.n_children == 0 && child.null_count == 7 {
                0
            } else {
                1
            }
        );
        let retained = broker.reserved();
        assert_eq!(retained, 0);
        drop(output);
        assert_eq!(broker.reserved(), retained);
        std::thread::spawn(move || drop(child)).join().unwrap();
        assert_eq!(broker.reserved(), 0);
    }
}

#[test]
fn wide_nested_schema_and_bufferless_array_exports_need_no_payload_reservations() {
    for width in [1, 64, 512] {
        let columns = (0..width)
            .map(|i| {
                (
                    format!("field_{i}"),
                    Arc::new(NullArray::new(3)) as ArrayRef,
                )
            })
            .collect::<Vec<_>>();
        let flat = RecordBatch::try_from_iter(columns).unwrap();
        let batch = RecordBatch::try_from_iter(vec![(
            "nested",
            Arc::new(StructArray::from(flat)) as ArrayRef,
        )])
        .unwrap();
        let broker = Arc::new(TestBroker::new(64 << 20));
        let m = memory(&broker);
        let schema_ref = batch.schema();
        let schema = schema(&schema_ref, m).unwrap();
        assert_eq!(broker.reserved(), 0);
        drop(schema);
        assert_eq!(broker.reserved(), 0);
        let m = memory(&broker);
        // Borrow source descriptors so dropping the moved batch does not subtract earlier
        // unobserved allocations from the allocation measurement.
        let source = batch.clone();
        let output = array(batch, m).unwrap();
        assert_eq!(broker.reserved(), 0);
        drop(output);
        drop(source);
        assert_eq!(broker.reserved(), 0);
    }
}

#[test]
fn moved_schema_children_release_exactly_once() {
    let broker = Arc::new(TestBroker::new(64 << 20));
    let input = Schema::new(vec![Field::new("null_child", DataType::Null, true)]);
    let output = schema(&input, memory(&broker)).unwrap();
    let mut child = unsafe { FFI_ArrowSchema::from_raw(*output.children) };
    let before = broker.reserved();
    drop(output);
    assert_eq!(broker.reserved(), before);
    assert_eq!(Field::try_from(&child).unwrap(), input.field(0).clone());
    let release = child.release.unwrap();
    unsafe {
        release(&mut child);
    }
    assert!(child.release.is_none());
    drop(child);
    assert_eq!(broker.reserved(), 0);
}

#[test]
fn fixed_width_and_string_slices_export_without_payload_copy() {
    let broker = Arc::new(TestBroker::new(64 << 20));
    let source = RecordBatch::try_from_iter(vec![
        (
            "number",
            Arc::new(Int32Array::from(vec![Some(1), None, Some(3), Some(4)])) as ArrayRef,
        ),
        (
            "text",
            Arc::new(StringArray::from(vec![
                Some("one"),
                None,
                Some("三"),
                Some("four"),
            ])) as ArrayRef,
        ),
    ])
    .unwrap()
    .slice(1, 2);
    let expected = source.clone();
    let output = array(source, memory(&broker)).unwrap();
    for (index, array) in expected.columns().iter().enumerate() {
        let child = unsafe { &**output.children.add(index) };
        for (i, buffer) in array.to_data().buffers().iter().enumerate() {
            assert_eq!(unsafe { *child.buffers.add(i + 1) }, buffer.as_ptr().cast());
        }
    }
    let ffi_schema = schema(expected.schema().as_ref(), memory(&broker)).unwrap();
    let restored = RecordBatch::from(StructArray::from(unsafe {
        arrow::ffi::from_ffi(output, &ffi_schema).unwrap()
    }));
    assert_eq!(restored, expected);
    drop(ffi_schema);
    assert!(broker.reserved() > 0);
    drop(restored);
    assert_eq!(broker.reserved(), 0);
}

#[test]
fn nested_nullable_dictionary_decimal_and_view_slices_round_trip() {
    let broker = Arc::new(TestBroker::new(64 << 20));
    let expected = crate::memory_pool::arrow_lease::tests::fixture();
    let output = array(expected.clone(), memory(&broker)).unwrap();
    let ffi_schema = schema(expected.schema().as_ref(), memory(&broker)).unwrap();
    let restored = RecordBatch::from(StructArray::from(unsafe {
        arrow::ffi::from_ffi(output, &ffi_schema).unwrap()
    }));
    assert_eq!(restored, expected);
    drop(ffi_schema);
    assert!(broker.reserved() > 0);
    drop(restored);
    assert_eq!(broker.reserved(), 0);
}

#[test]
fn moved_dictionary_values_and_schema_outlive_parent_arrays_and_schemas() {
    let broker = Arc::new(TestBroker::new(64 << 20));
    let batch = crate::memory_pool::arrow_lease::tests::fixture();
    let ffi_schema = schema(batch.schema().as_ref(), memory(&broker)).unwrap();
    let output = array(batch, memory(&broker)).unwrap();
    let values = unsafe { FFI_ArrowArray::from_raw((**output.children.add(5)).dictionary) };
    let value_schema =
        unsafe { FFI_ArrowSchema::from_raw((**ffi_schema.children.add(5)).dictionary) };
    let before = broker.reserved();
    drop(output);
    drop(ffi_schema);
    assert_eq!(broker.reserved(), before);
    let values =
        arrow::array::make_array(unsafe { arrow::ffi::from_ffi(values, &value_schema).unwrap() });
    assert_eq!(
        values
            .as_any()
            .downcast_ref::<StringArray>()
            .unwrap()
            .value(1),
        "long é payload"
    );
    drop(value_schema);
    assert!(broker.reserved() > 0);
    drop(values);
    assert_eq!(broker.reserved(), 0);
}
