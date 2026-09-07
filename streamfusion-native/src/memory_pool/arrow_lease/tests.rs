// Copyright 2026 StreamFusion Authors
// Licensed under the Apache License, Version 2.0

use super::*;
use crate::memory_pool::{tests_support::TestBroker, FlinkMemoryPool};
use arrow::array::{
    Array, ArrayRef, BooleanArray, Decimal128Array, DictionaryArray, Int32Array, ListArray,
    StringArray, StringViewArray, StructArray,
};
use arrow::datatypes::{DataType, Field, Int32Type, Schema};
use arrow::ffi::{from_ffi, FFI_ArrowArray, FFI_ArrowSchema};
use datafusion::execution::memory_pool::{MemoryConsumer, MemoryPool};

pub(in crate::memory_pool) fn fixture() -> RecordBatch {
    let text: ArrayRef = Arc::new(StringArray::from(vec![
        Some("discard"),
        Some("long é payload"),
        None,
        Some("尾"),
    ]));
    let lists: ArrayRef = Arc::new(ListArray::from_iter_primitive::<Int32Type, _, _>([
        Some(vec![Some(9)]),
        Some(vec![Some(1), None]),
        None,
        Some(vec![Some(4)]),
    ]));
    let fields = vec![
        Arc::new(Field::new("text", DataType::Utf8, true)),
        Arc::new(Field::new("items", lists.data_type().clone(), true)),
    ];
    let nested: ArrayRef = Arc::new(StructArray::new(
        fields.into(),
        vec![text.clone(), lists.clone()],
        Some(NullBuffer::from(vec![true, true, false, true])),
    ));
    let dictionary: ArrayRef = Arc::new(
        DictionaryArray::<Int32Type>::try_new(
            Int32Array::from(vec![Some(0), Some(2), None, Some(1)]),
            text.clone(),
        )
        .unwrap(),
    );
    let columns: Vec<ArrayRef> = vec![
        Arc::new(Int32Array::from(vec![Some(0), Some(1), None, Some(3)])),
        Arc::new(BooleanArray::from(vec![
            Some(false),
            Some(true),
            None,
            Some(false),
        ])),
        text,
        lists,
        nested,
        dictionary,
        Arc::new(
            Decimal128Array::from(vec![Some(0), Some(123), None, Some(-999)])
                .with_precision_and_scale(20, 3)
                .unwrap(),
        ),
        Arc::new(StringViewArray::from(vec![
            Some("short"),
            Some("a long string stored outside the view"),
            None,
            Some("other long string beyond twelve bytes"),
        ])),
    ];
    let schema = Arc::new(Schema::new(
        columns
            .iter()
            .enumerate()
            .map(|(index, array)| {
                Field::new(format!("field_{index}"), array.data_type().clone(), true)
            })
            .collect::<Vec<_>>(),
    ));
    RecordBatch::try_new(schema, columns).unwrap().slice(1, 3)
}

pub(super) fn assert_shared_data(expected: &ArrayData, actual: &ArrayData) {
    assert_eq!(expected, actual);
    assert_eq!(expected.offset(), actual.offset());
    for (a, b) in expected.buffers().iter().zip(actual.buffers()) {
        assert_eq!(a.as_ptr(), b.as_ptr());
    }
    if let (Some(a), Some(b)) = (expected.nulls(), actual.nulls()) {
        assert_eq!(a.buffer().as_ptr(), b.buffer().as_ptr());
        assert_eq!(a.offset(), b.offset());
    }
    for (a, b) in expected.child_data().iter().zip(actual.child_data()) {
        assert_shared_data(a, b);
    }
}

#[test]
fn sliced_nested_dictionary_decimal_and_view_buffers_keep_admission_until_last_release() {
    let broker = Arc::new(TestBroker::new(16 << 20));
    let input = fixture();
    let bytes = input.get_array_memory_size();
    let mut memory = HostMemoryReservation::new(broker.clone(), "output");
    memory.resize(bytes).unwrap();
    let output = host_batch(input.clone(), memory).unwrap();
    let admitted = broker.reserved();
    assert_eq!(admitted, bytes);
    for (a, b) in input.columns().iter().zip(output.columns()) {
        assert_shared_data(&a.to_data(), &b.to_data());
    }
    // A retaining downstream operator can project one field and slice it after producer EOF.
    let projected = output.project(&[4]).unwrap().slice(1, 2);
    drop(output);
    drop(input);
    assert_eq!(broker.reserved(), admitted);
    let child = projected
        .column(0)
        .as_any()
        .downcast_ref::<StructArray>()
        .unwrap()
        .column(0)
        .clone();
    let buffer = child.to_data().buffers()[1].clone();
    drop(projected);
    drop(child);
    assert_eq!(broker.reserved(), admitted);
    std::thread::spawn(move || drop(buffer)).join().unwrap();
    assert_eq!(broker.reserved(), 0);
}

#[test]
fn arrow_c_data_release_owns_native_allowance_after_producer_and_pool_drop() {
    let broker = Arc::new(TestBroker::new(16 << 20));
    let pool: Arc<dyn MemoryPool> = Arc::new(FlinkMemoryPool::new(broker.clone(), 16 << 20));
    let batch = fixture();
    let memory = MemoryConsumer::new("output").register(&pool);
    memory.try_resize(batch.get_array_memory_size()).unwrap();
    let batch = datafusion_batch(batch, memory).unwrap();
    let data = StructArray::from(batch).to_data();
    let array = FFI_ArrowArray::new(&data);
    let schema = FFI_ArrowSchema::try_from(data.data_type()).unwrap();
    drop(data);
    drop(pool);
    let admitted = broker.reserved();
    assert!(admitted > 0);
    // The import takes the producer callback. Its final buffer, not a stream handle, releases it.
    let imported = unsafe { from_ffi(array, &schema) }.unwrap();
    let clone = imported.clone();
    drop(imported);
    assert_eq!(broker.reserved(), admitted);
    drop(clone);
    assert_eq!(broker.reserved(), 0);
}

#[test]
fn attaching_a_buffer_owner_needs_no_additional_reservation() {
    let batch = fixture();
    let bytes = batch.get_array_memory_size();
    let broker = Arc::new(TestBroker::new(bytes));
    let mut memory = HostMemoryReservation::new(broker.clone(), "output");
    memory.resize(bytes).unwrap();
    let output = host_batch(batch, memory).unwrap();
    assert_eq!(broker.reserved(), bytes);
    drop(output);
    assert_eq!(broker.reserved(), 0);
}

#[test]
fn descriptor_credit_cannot_hide_an_unadmitted_output() {
    let broker = Arc::new(TestBroker::new(16 << 20));
    let memory = HostMemoryReservation::new(broker.clone(), "unadmitted output");
    assert!(host_batch(fixture(), memory)
        .unwrap_err()
        .to_string()
        .contains("smaller than its buffers"));
    assert_eq!(broker.reserved(), 0);
}
