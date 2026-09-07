// Copyright 2026 StreamFusion Authors
// Licensed under the Apache License, Version 2.0.
use super::*;
use crate::memory_pool::{tests_support::TestBroker, FlinkMemoryPool};
use arrow::array::{Array, ArrayRef, Decimal128Array, Int32Array, ListArray, StringArray};
use arrow::datatypes::Int32Type;
use arrow::record_batch::RecordBatch;
use datafusion::execution::memory_pool::MemoryConsumer;
use std::sync::Arc;

#[test]
fn wide_ipc_decodes_without_alignment_copies_and_keeps_one_payload_owner() {
    let broker = Arc::new(TestBroker::new(8 << 20));
    let pool: Arc<dyn datafusion::execution::memory_pool::MemoryPool> =
        Arc::new(FlinkMemoryPool::new(broker.clone(), 8 << 20));
    for width in 1..33 {
        let mut columns = Vec::new();
        for index in 0..width {
            let column: ArrayRef = match index % 4 {
                0 => Arc::new(
                    Decimal128Array::from(vec![Some(1234i128), None, Some(-5)])
                        .with_precision_and_scale(20, 3)
                        .unwrap(),
                ),
                1 => Arc::new(StringArray::from(vec![Some("abc"), None, Some("é")])),
                2 => Arc::new(ListArray::from_iter_primitive::<Int32Type, _, _>(vec![
                    Some(vec![Some(1), None]),
                    None,
                    Some(vec![Some(-5)]),
                ])),
                _ => Arc::new(Int32Array::from(vec![Some(1), None, Some(-5)])),
            };
            columns.push((format!("c{index}"), column));
        }
        let original = RecordBatch::try_from_iter(columns).unwrap();
        let frame = crate::exchange::IpcBatchFrame::encode(&original).unwrap();
        let memory = MemoryConsumer::new("IPC fixture").register(&pool);
        let length = frame.metadata.len() + frame.body.len();
        let (mut storage, padding) = input_storage(length, frame.metadata.len(), &memory).unwrap();
        storage.as_slice_mut()[padding..padding + frame.metadata.len()]
            .copy_from_slice(&frame.metadata);
        storage.as_slice_mut()[padding + frame.metadata.len()..].copy_from_slice(&frame.body);
        let payload = arrow::buffer::Buffer::from(storage).slice(padding);
        let base = payload.data_ptr();
        assert_eq!(
            payload.slice(frame.metadata.len()).as_ptr() as usize % 64,
            0
        );
        let decoded = crate::exchange::IpcBatchFrame::decode_contiguous(
            payload,
            frame.metadata.len(),
            original.schema(),
        )
        .unwrap();
        fn shared(data: &arrow::array::ArrayData, base: std::ptr::NonNull<u8>) {
            for buffer in data.buffers() {
                if !buffer.is_empty() {
                    assert_eq!(buffer.data_ptr(), base);
                }
            }
            if let Some(nulls) = data.nulls() {
                assert_eq!(nulls.buffer().data_ptr(), base);
            }
            for child in data.child_data() {
                shared(child, base);
            }
        }
        for array in decoded.columns() {
            shared(&array.to_data(), base);
        }
        assert_eq!(decoded, original);
        let retained = crate::memory_pool::arrow_lease::datafusion_batch_registered(
            decoded,
            memory,
            crate::memory_pool::buffer_registry(&pool),
        )
        .unwrap();
        assert!(broker.reserved() > 0);
        drop(retained);
        assert_eq!(broker.reserved(), 0);
    }
}
