// Copyright 2026 StreamFusion Authors
// Licensed under the Apache License, Version 2.0.

use arrow::array::{RecordBatch, RecordBatchReader};
use arrow::compute::concat_batches;
use arrow::datatypes::SchemaRef;
use arrow::error::ArrowError;
use arrow::ffi_stream::FFI_ArrowArrayStream;
use datafusion::physical_plan::RecordBatchStream;
use futures::StreamExt;
use std::sync::{
    atomic::{AtomicUsize, Ordering},
    Arc,
};

use super::{
    binding_tests::{lookup, serialized},
    test_support::*,
    LookupTable,
};
use crate::execution_context::{lookup_resources::LookupSource, NativeExecutionContext};

struct Source {
    batches: std::vec::IntoIter<std::result::Result<RecordBatch, ArrowError>>,
    reads: Arc<AtomicUsize>,
    closes: Arc<AtomicUsize>,
}
impl Iterator for Source {
    type Item = std::result::Result<RecordBatch, ArrowError>;
    fn next(&mut self) -> Option<Self::Item> {
        self.reads.fetch_add(1, Ordering::SeqCst);
        self.batches.next()
    }
}
impl RecordBatchReader for Source {
    fn schema(&self) -> SchemaRef {
        schema(false)
    }
}
impl Drop for Source {
    fn drop(&mut self) {
        self.closes.fetch_add(1, Ordering::SeqCst);
    }
}
fn source(
    batches: Vec<std::result::Result<RecordBatch, ArrowError>>,
) -> (Source, Arc<AtomicUsize>, Arc<AtomicUsize>) {
    let reads = Arc::new(AtomicUsize::new(0));
    let closes = Arc::new(AtomicUsize::new(0));
    (
        Source {
            batches: batches.into_iter(),
            reads: reads.clone(),
            closes: closes.clone(),
        },
        reads,
        closes,
    )
}

#[test]
fn lookup_c_stream_drains_all_chunks_at_open_and_releases_source_once() {
    for chunk in [1, 7, 128, 1024] {
        let (session, broker) = context(64 << 20, 31);
        let side = rows(7, 217, false);
        let (reader, reads, closes) = source(
            side.chunks(chunk)
                .map(|rows| Ok(batch(rows, false)))
                .collect(),
        );
        let mut ffi = FFI_ArrowArrayStream::new(Box::new(reader));
        let mut native = NativeExecutionContext::new(
            &serialized(lookup(false), 3),
            session.runtime_env().memory_pool.clone(),
        )
        .unwrap();
        unsafe {
            native
                .install_lookup_sources(&[LookupSource {
                    node_id: 2,
                    stream: &mut ffi,
                }])
                .unwrap();
        }
        assert!(ffi.release.is_none());
        assert_eq!(reads.load(Ordering::SeqCst), side.len().div_ceil(chunk) + 1);
        assert_eq!(closes.load(Ordering::SeqCst), 1);
        let native = Arc::new(native);
        let probe = rows(31, 129, false);
        let mut stream = native.start(vec![batch(&probe, true)]).unwrap();
        let schema = stream.schema();
        let outputs = native.runtime().block_on(async {
            let mut outputs = Vec::new();
            while let Some(batch) = stream.next().await {
                outputs.push(batch.unwrap());
            }
            outputs
        });
        let output = concat_batches(&schema, &outputs).unwrap();
        assert_eq!(output, expected(&probe, &side, false, false, schema));
        drop(output);
        drop(outputs);
        drop(stream);
        drop(native);
        drop(ffi);
        assert_eq!(closes.load(Ordering::SeqCst), 1);
        assert_eq!(broker.reserved(), 0);
    }
}

#[test]
fn lookup_source_error_releases_consumed_stream_and_all_construction_credit() {
    let (session, broker) = context(64 << 20, 31);
    let (reader, _, closes) = source(vec![
        Ok(batch(&rows(1, 17, false), false)),
        Err(ArrowError::ParseError("CSV failure".into())),
    ]);
    let mut ffi = FFI_ArrowArrayStream::new(Box::new(reader));
    let mut native = NativeExecutionContext::new(
        &serialized(lookup(false), 3),
        session.runtime_env().memory_pool.clone(),
    )
    .unwrap();
    let baseline = broker.reserved();
    let error = unsafe {
        native.install_lookup_sources(&[LookupSource {
            node_id: 2,
            stream: &mut ffi,
        }])
    }
    .unwrap_err();
    assert!(error.to_string().contains("CSV failure"));
    assert!(ffi.release.is_none());
    assert_eq!(closes.load(Ordering::SeqCst), 1);
    assert_eq!(broker.reserved(), baseline);
    drop(native);
    assert_eq!(broker.reserved(), 0);
}

#[test]
fn lookup_duplicate_streams_or_wrong_nodes_are_rejected_before_source_callbacks() {
    for duplicate in [false, true] {
        let (session, broker) = context(64 << 20, 31);
        let (reader, reads, closes) = source(vec![Ok(batch(&rows(1, 1, false), false))]);
        let mut ffi = FFI_ArrowArrayStream::new(Box::new(reader));
        let mut native = NativeExecutionContext::new(
            &serialized(lookup(false), 3),
            session.runtime_env().memory_pool.clone(),
        )
        .unwrap();
        let bindings = if duplicate {
            vec![
                LookupSource {
                    node_id: 2,
                    stream: &mut ffi,
                },
                LookupSource {
                    node_id: 2,
                    stream: &mut ffi,
                },
            ]
        } else {
            vec![LookupSource {
                node_id: 4,
                stream: &mut ffi,
            }]
        };
        assert!(unsafe { native.install_lookup_sources(&bindings) }.is_err());
        assert!(ffi.release.is_some());
        assert_eq!(reads.load(Ordering::SeqCst), 0);
        assert_eq!(closes.load(Ordering::SeqCst), 0);
        drop(ffi);
        drop(native);
        assert_eq!(closes.load(Ordering::SeqCst), 1);
        assert_eq!(broker.reserved(), 0);
    }
}

#[test]
fn single_lookup_source_batch_keeps_its_original_arrow_buffers() {
    let (session, broker) = context(64 << 20, 31);
    let batch = batch(&rows(1, 17, false), false);
    let pointer = batch.column(0).to_data().buffers()[0].as_ptr();
    let (reader, _, closes) = source(vec![Ok(batch)]);
    let table =
        LookupTable::from_source(reader, vec![0], session.runtime_env().memory_pool.clone())
            .unwrap();
    assert_eq!(
        table.batch.column(0).to_data().buffers()[0].as_ptr(),
        pointer
    );
    assert_eq!(closes.load(Ordering::SeqCst), 1);
    drop(table);
    assert_eq!(broker.reserved(), 0);
}
