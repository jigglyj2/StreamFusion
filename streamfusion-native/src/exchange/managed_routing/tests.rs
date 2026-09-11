// Copyright 2026 StreamFusion Authors
// Licensed under the Apache License, Version 2.0.

use super::*;
use crate::allocation_test_support::measure;
use arrow::array::{ArrayRef, Int32Array, StringArray, StructArray};
use arrow::datatypes::{DataType, Field, Fields};
use std::sync::atomic::{AtomicUsize, Ordering};

#[derive(Debug)]
struct Broker {
    live: AtomicUsize,
    peak: AtomicUsize,
    requests: AtomicUsize,
    limit: usize,
    deny_request: usize,
}
impl Broker {
    fn new(limit: usize, deny_request: usize) -> Arc<Self> {
        Arc::new(Self {
            live: AtomicUsize::new(0),
            peak: AtomicUsize::new(0),
            requests: AtomicUsize::new(0),
            limit,
            deny_request,
        })
    }
}
impl MemoryReservationBroker for Broker {
    fn try_reserve(&self, bytes: usize) -> Result<bool> {
        let request = self.requests.fetch_add(1, Ordering::Relaxed) + 1;
        let next = self.live.load(Ordering::Relaxed) + bytes;
        if next > self.limit || request == self.deny_request {
            return Ok(false);
        }
        self.live.store(next, Ordering::Relaxed);
        self.peak.fetch_max(next, Ordering::Relaxed);
        Ok(true)
    }
    fn release(&self, bytes: usize) -> Result<()> {
        assert!(self.live.fetch_sub(bytes, Ordering::Relaxed) >= bytes);
        Ok(())
    }
}
fn plan(columns: usize, preserve: bool) -> crate::proto::NativeExchangePlan {
    crate::proto::NativeExchangePlan {
        schema: Some(crate::proto::Schema {
            fields: vec![crate::proto::Field::default(); columns],
        }),
        max_parallelism: 32768,
        parallelism: 4,
        preserve_key_groups: preserve,
        ..Default::default()
    }
}

#[test]
fn denies_bucket_storage_before_routing_a_tiny_batch() {
    let batch = RecordBatch::try_from_iter(vec![(
        "key",
        Arc::new(Int32Array::from(vec![1])) as ArrayRef,
    )])
    .unwrap();
    let broker = Broker::new(65536, 0);
    let (result, observed) = measure(|| {
        route_record_batch(
            &plan(1, true),
            &[(0, crate::exchange::KeyField::Integer)],
            batch.clone(),
            broker.clone(),
        )
    });
    assert!(result.is_err());
    assert!(
        observed.peak < 32768,
        "allocated destination table before admission: {observed:?}"
    );
    assert_eq!(broker.live.load(Ordering::Relaxed), 0);
}

#[test]
fn denies_large_ipc_workspace_before_copying_payload() {
    let value = "x".repeat(1 << 20);
    let batch = RecordBatch::try_from_iter(vec![
        ("key", Arc::new(Int32Array::from(vec![1])) as ArrayRef),
        (
            "value",
            Arc::new(StringArray::from(vec![value.as_str()])) as ArrayRef,
        ),
    ])
    .unwrap();
    let broker = Broker::new(65536, 0);
    let (result, observed) = measure(|| {
        route_record_batch(
            &plan(2, false),
            &[(0, crate::exchange::KeyField::Integer)],
            batch.clone(),
            broker.clone(),
        )
    });
    assert!(result.is_err());
    assert!(
        observed.peak < 32768,
        "allocated payload before admission: {observed:?}"
    );
    assert_eq!(broker.live.load(Ordering::Relaxed), 0);
}

#[test]
fn accounts_overlapping_frames_and_releases_partial_failures() {
    let values = (0..256)
        .map(|row| format!("{row}:{}", "x".repeat(4096)))
        .collect::<Vec<_>>();
    let nested = StructArray::new(
        Fields::from(vec![Field::new("text", DataType::Utf8, false)]),
        vec![Arc::new(StringArray::from(values))],
        None,
    );
    let batch = RecordBatch::try_from_iter(vec![
        (
            "key",
            Arc::new(Int32Array::from_iter_values(0..256)) as ArrayRef,
        ),
        ("value", Arc::new(nested) as ArrayRef),
    ])
    .unwrap()
    .slice(3, 250);
    for preserve in [false, true] {
        let broker = Broker::new(usize::MAX, 0);
        let (frames, observed) = measure(|| {
            route_record_batch(
                &plan(2, preserve),
                &[(0, crate::exchange::KeyField::Integer)],
                batch.clone(),
                broker.clone(),
            )
            .unwrap()
        });
        assert!(frames.frames.len() > 1);
        assert!(
            broker.requests.load(Ordering::Relaxed) <= 8,
            "{} reservation callbacks for {} frames",
            broker.requests.load(Ordering::Relaxed),
            frames.frames.len()
        );
        assert!(
            observed.peak <= broker.peak.load(Ordering::Relaxed),
            "{observed:?}"
        );
        assert_eq!(
            broker.live.load(Ordering::Relaxed),
            frames._reservation.size()
        );
        let decoded_rows: usize = frames
            .frames
            .iter()
            .map(|frame| frame.frame().decode(batch.schema()).unwrap().num_rows())
            .sum();
        assert_eq!(decoded_rows, 250);
        drop(frames);
        assert_eq!(broker.live.load(Ordering::Relaxed), 0);
        // First request admits routing, second admits the first frame, third would admit
        // the next frame with the first still retained. Failure must drop both owners.
        let broker = Broker::new(usize::MAX, 3);
        assert!(route_record_batch(
            &plan(2, preserve),
            &[(0, crate::exchange::KeyField::Integer)],
            batch.clone(),
            broker.clone()
        )
        .is_err());
        assert_eq!(broker.requests.load(Ordering::Relaxed), 3);
        assert_eq!(broker.live.load(Ordering::Relaxed), 0);
    }
}

#[test]
fn decoded_shared_backing_is_not_counted_once_per_column() {
    let array = Arc::new(Int32Array::from_iter_values(0..4096)) as ArrayRef;
    let batch =
        RecordBatch::try_from_iter((0..32).map(|column| (format!("c{column}"), array.clone())))
            .unwrap();
    let decoded = crate::exchange::IpcBatchFrame::encode(&batch)
        .unwrap()
        .decode(batch.schema())
        .unwrap();
    let plain = workspace::allowance(&batch, &[], 128, 4, false, 32).unwrap();
    let shared = workspace::allowance(&decoded, &[], 128, 4, false, 32).unwrap();
    assert_eq!(plain, shared);
}

#[test]
fn generated_key_buffers_are_admitted_before_encoding_and_released_after_frames() {
    let value = "é".repeat(512 << 10);
    let batch = RecordBatch::try_from_iter(vec![(
        "key",
        Arc::new(StringArray::from(vec![value.as_str()])) as ArrayRef,
    )])
    .unwrap();
    let mut plan = plan(1, false);
    plan.protocol_version = 2;
    plan.transport_routing_key = true;
    plan.metadata_columns = Some(crate::proto::ExchangeMetadataColumns::default());
    let keys = [(0, crate::exchange::KeyField::String)];
    let denied = Broker::new(64 << 10, 0);
    let (result, allocations) =
        measure(|| route_record_batch(&plan, &keys, batch.clone(), denied.clone()));
    assert!(result.is_err());
    assert!(allocations.peak < 65536, "{allocations:?}");
    assert_eq!(denied.live.load(Ordering::Relaxed), 0);
    let broker = Broker::new(32 << 20, 0);
    let frames = route_record_batch(&plan, &keys, batch, broker.clone()).unwrap();
    assert_eq!(frames.frames.len(), 1);
    let retained = frames.frames.iter().fold(
        frames.frames.capacity() * std::mem::size_of::<RoutedFrame>(),
        |bytes, frame| bytes + frame.frame().metadata.capacity() + frame.frame().body.capacity(),
    );
    assert_eq!(
        broker.live.load(Ordering::Relaxed),
        retained,
        "only completed IPC frame buffers remain charged after key encoding"
    );
    drop(frames);
    assert_eq!(broker.live.load(Ordering::Relaxed), 0);
}
