// Copyright 2026 StreamFusion Authors
// Licensed under the Apache License, Version 2.0.

use super::*;
use crate::memory_pool::{tests_support::TestBroker, FlinkMemoryPool, HostMemoryReservation};
use arrow::array::{ArrayRef, NullArray, RecordBatch};
use arrow::error::ArrowError;
use datafusion::execution::memory_pool::{MemoryConsumer, MemoryPool};
use std::sync::{
    atomic::{AtomicUsize, Ordering},
    Arc,
};

struct Reader {
    batch: RecordBatch,
    remaining: usize,
    polls: Arc<AtomicUsize>,
    drops: Arc<AtomicUsize>,
}
impl Iterator for Reader {
    type Item = std::result::Result<RecordBatch, ArrowError>;
    fn next(&mut self) -> Option<Self::Item> {
        self.polls.fetch_add(1, Ordering::Relaxed);
        if self.remaining == 0 {
            None
        } else {
            self.remaining -= 1;
            Some(Ok(self.batch.clone()))
        }
    }
}
impl RecordBatchReader for Reader {
    fn schema(&self) -> SchemaRef {
        self.batch.schema()
    }
}
impl Drop for Reader {
    fn drop(&mut self) {
        self.drops.fetch_add(1, Ordering::Relaxed);
    }
}
fn fixture() -> (
    Reader,
    Arc<AtomicUsize>,
    Arc<AtomicUsize>,
    Arc<TestBroker>,
    MemoryReservation,
) {
    let polls = Arc::new(AtomicUsize::new(0));
    let drops = Arc::new(AtomicUsize::new(0));
    let broker = Arc::new(TestBroker::new(1 << 20));
    let pool: Arc<dyn MemoryPool> = Arc::new(FlinkMemoryPool::new(broker.clone(), 1 << 20));
    let memory = MemoryConsumer::new("C Stream test").register(&pool);
    let reader = Reader {
        batch: RecordBatch::try_from_iter(vec![("null", Arc::new(NullArray::new(7)) as ArrayRef)])
            .unwrap(),
        remaining: 1,
        polls: polls.clone(),
        drops: drops.clone(),
    };
    (reader, polls, drops, broker, memory)
}

#[test]
fn eof_keeps_reader_until_release_and_bufferless_output_can_outlive_both() {
    let (reader, polls, drops, broker, memory) = fixture();
    let mut stream = export(reader, memory).unwrap();
    let mut schema = FFI_ArrowSchema::empty();
    let mut array = FFI_ArrowArray::empty();
    unsafe {
        assert_eq!(stream.get_schema.unwrap()(&mut stream, &mut schema), 0);
        assert_eq!(stream.get_next.unwrap()(&mut stream, &mut array), 0);
        let mut eof = FFI_ArrowArray::empty();
        assert_eq!(stream.get_next.unwrap()(&mut stream, &mut eof), 0);
        assert!(eof.is_released());
        assert_eq!(stream.get_next.unwrap()(&mut stream, &mut eof), 0);
    }
    assert_eq!(polls.load(Ordering::Relaxed), 2);
    assert_eq!(drops.load(Ordering::Relaxed), 0);
    let child = unsafe { FFI_ArrowArray::from_raw(*array.children) };
    drop(array);
    drop(schema);
    drop(stream);
    assert_eq!(drops.load(Ordering::Relaxed), 1);
    assert_eq!(broker.reserved(), 0);
    drop(child);
    assert_eq!(broker.reserved(), 0);
}

#[test]
fn bufferless_exports_succeed_with_an_exhausted_payload_budget() {
    let (reader, polls, drops, broker, memory) = fixture();
    let mut stream = export(reader, memory).unwrap();
    let mut pressure = HostMemoryReservation::new(broker.clone(), "other task work");
    pressure.resize(1 << 20).unwrap();
    let mut array = FFI_ArrowArray::empty();
    let mut schema = FFI_ArrowSchema::empty();
    unsafe {
        assert_eq!(stream.get_schema.unwrap()(&mut stream, &mut schema), 0);
        assert_eq!(stream.get_next.unwrap()(&mut stream, &mut array), 0);
    }
    assert_eq!(polls.load(Ordering::Relaxed), 1);
    assert_eq!(broker.reserved(), 1 << 20);
    drop(array);
    drop(schema);
    drop(stream);
    drop(pressure);
    assert_eq!(drops.load(Ordering::Relaxed), 1);
    assert_eq!(broker.reserved(), 0);
}

#[test]
fn diagnostic_storage_is_bounded_nul_safe_and_utf8_safe() {
    let mut text = ErrorText::default();
    write!(&mut text, "prefix\0{}", "名".repeat(4096)).unwrap();
    assert!(text.length <= 2047);
    let error = std::ffi::CStr::from_bytes_with_nul(&text.bytes[..=text.length])
        .unwrap()
        .to_str()
        .unwrap();
    assert!(error.starts_with("prefix?"));
}

#[test]
fn reader_panic_becomes_a_terminal_c_stream_error_and_release_still_cleans_up() {
    struct PanicReader(Reader);
    impl Iterator for PanicReader {
        type Item = std::result::Result<RecordBatch, ArrowError>;
        fn next(&mut self) -> Option<Self::Item> {
            self.0.polls.fetch_add(1, Ordering::Relaxed);
            panic!("injected reader panic")
        }
    }
    impl RecordBatchReader for PanicReader {
        fn schema(&self) -> SchemaRef {
            self.0.schema()
        }
    }
    let (reader, polls, drops, broker, memory) = fixture();
    let mut stream = export(PanicReader(reader), memory).unwrap();
    let mut output = FFI_ArrowArray::empty();
    unsafe {
        assert_ne!(stream.get_next.unwrap()(&mut stream, &mut output), 0);
        assert_ne!(stream.get_next.unwrap()(&mut stream, &mut output), 0);
        assert!(
            std::ffi::CStr::from_ptr(stream.get_last_error.unwrap()(&mut stream))
                .to_str()
                .unwrap()
                .contains("panic while exporting")
        );
    }
    assert_eq!(polls.load(Ordering::Relaxed), 1);
    assert!(output.is_released());
    drop(stream);
    assert_eq!(drops.load(Ordering::Relaxed), 1);
    assert_eq!(broker.reserved(), 0);
}
