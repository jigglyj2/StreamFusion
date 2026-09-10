// Copyright 2026 StreamFusion Authors
// Licensed under the Apache License, Version 2.0.

use super::*;
use crate::allocation_test_support::measure;
use crate::memory_pool::{tests_support::TestBroker, FlinkMemoryPool, MemoryReservationBroker};
use arrow::array::{Array, ArrayRef, Int32Array, StringArray};
use datafusion::execution::memory_pool::{MemoryConsumer, MemoryPool};
use std::sync::atomic::{AtomicUsize, Ordering};

#[derive(Debug)]
struct Broker {
    inner: TestBroker,
    registry: Arc<Registry>,
    ceiling: AtomicUsize,
    peak: AtomicUsize,
}
impl Broker {
    fn new() -> Arc<Self> {
        Arc::new(Self {
            inner: TestBroker::new(64 << 20),
            registry: Arc::default(),
            ceiling: AtomicUsize::new(64 << 20),
            peak: AtomicUsize::new(0),
        })
    }
    fn pool(self: &Arc<Self>) -> Arc<dyn MemoryPool> {
        Arc::new(FlinkMemoryPool::new(self.clone(), 64 << 20))
    }
}
impl MemoryReservationBroker for Broker {
    fn lease_registry(&self) -> Option<Arc<Registry>> {
        Some(self.registry.clone())
    }
    fn try_reserve(&self, bytes: usize) -> Result<bool> {
        if self.inner.reserved() + bytes > self.ceiling.load(Ordering::Relaxed) {
            return Ok(false);
        }
        let accepted = self.inner.try_reserve(bytes)?;
        if accepted {
            self.peak
                .fetch_max(self.inner.reserved(), Ordering::Relaxed);
        }
        Ok(accepted)
    }
    fn release(&self, bytes: usize) -> Result<()> {
        self.inner.release(bytes)
    }
    fn available(&self) -> Result<Option<usize>> {
        Ok(Some(
            self.ceiling
                .load(Ordering::Relaxed)
                .saturating_sub(self.inner.reserved()),
        ))
    }
}
fn source(rows: usize) -> RecordBatch {
    RecordBatch::try_from_iter(vec![(
        "text",
        Arc::new(StringArray::from_iter((0..rows).map(|row| {
            (row % 11 != 0).then(|| format!("é-{row}-{}", "x".repeat(256)))
        }))) as ArrayRef,
    )])
    .unwrap()
}
fn own(batch: RecordBatch, broker: &Arc<Broker>) -> RecordBatch {
    let mut memory = HostMemoryReservation::new(broker.clone(), "producer");
    memory.try_grow(batch.get_array_memory_size()).unwrap();
    host_batch(batch, memory).unwrap()
}
fn edge(batch: RecordBatch, broker: &Arc<Broker>) -> Result<RecordBatch> {
    let pool = broker.pool();
    let memory = MemoryConsumer::new("edge").register(&pool);
    edge_batch(batch, memory, Some(broker.registry.clone()))
}

#[test]
fn leased_slices_cross_edge_without_new_reservations_and_outlive_producer() {
    let broker = Broker::new();
    let producer = own(source(8192), &broker);
    let slice = producer.slice(13, 4096);
    let before = broker.inner.reserved();
    let extra = 0;
    assert!(slice.get_array_memory_size() > extra);
    broker.ceiling.store(before + extra, Ordering::Relaxed);
    let expected = slice.column(0).to_data();
    let output = edge(slice, &broker).unwrap();
    assert_eq!(broker.inner.reserved(), before + extra);
    for (actual, expected) in output
        .column(0)
        .to_data()
        .buffers()
        .iter()
        .zip(expected.buffers())
    {
        assert_eq!(actual.as_ptr(), expected.as_ptr());
    }
    assert_eq!(output.column(0).to_data(), expected);
    drop(expected);
    drop(producer);
    assert_eq!(broker.inner.reserved(), before + extra);
    drop(output);
    assert_eq!(broker.inner.reserved(), 0);
    assert_eq!(broker.registry.len(), 0);
}

#[test]
fn unwrapped_alias_retains_the_registered_owner_not_just_its_address() {
    let broker = Broker::new();
    let unwrapped = source(257);
    let producer = own(unwrapped.clone(), &broker);
    let before = broker.inner.reserved();
    let alias = unwrapped.slice(3, 127);
    let extra = 0;
    let output = edge(alias, &broker).unwrap();
    assert_eq!(broker.inner.reserved(), before + extra);
    drop(producer);
    drop(unwrapped);
    assert_eq!(broker.inner.reserved(), before + extra);
    assert!(output
        .column(0)
        .as_any()
        .downcast_ref::<StringArray>()
        .unwrap()
        .value(0)
        .starts_with("é-3-"));
    drop(output);
    assert_eq!(broker.inner.reserved(), 0);
    assert_eq!(broker.registry.len(), 0);
}

#[test]
fn mixed_batch_charges_each_uncovered_backing_allocation_once() {
    let broker = Broker::new();
    let producer = own(source(4096), &broker);
    let fresh: ArrayRef = Arc::new(Int32Array::from_iter_values(0..4096));
    let fresh_bytes = fresh.get_buffer_memory_size();
    let batch = RecordBatch::try_from_iter(vec![
        ("owned", producer.column(0).clone()),
        ("new", fresh.clone()),
        ("alias", fresh),
    ])
    .unwrap();
    let before = broker.inner.reserved();
    let extra = 0 + fresh_bytes;
    let output = edge(batch, &broker).unwrap();
    assert_eq!(broker.inner.reserved(), before + extra);
    assert_eq!(
        output.column(1).to_data().buffers()[0].as_ptr(),
        output.column(2).to_data().buffers()[0].as_ptr()
    );
    drop(producer);
    assert_eq!(broker.inner.reserved(), before + extra);
    drop(output);
    assert_eq!(broker.inner.reserved(), 0);
    assert_eq!(broker.registry.len(), 0);
}

#[test]
fn uncovered_payload_denial_rolls_back_without_disturbing_existing_leases() {
    let broker = Broker::new();
    let producer = own(source(4096), &broker);
    let batch = RecordBatch::try_from_iter(vec![
        ("owned", producer.column(0).clone()),
        (
            "fresh",
            Arc::new(Int32Array::from_iter_values(0..4096)) as ArrayRef,
        ),
    ])
    .unwrap();
    let before = broker.inner.reserved();
    broker.ceiling.store(before + 0, Ordering::Relaxed);
    assert!(edge(batch, &broker).is_err());
    assert_eq!(broker.inner.reserved(), before);
    assert!(broker.registry.len() > 0);
    drop(producer);
    assert_eq!(broker.inner.reserved(), 0);
    assert_eq!(broker.registry.len(), 0);
}

#[test]
fn registry_metadata_is_freed_after_repeated_task_local_cycles() {
    let broker = Broker::new();
    let input = source(17);
    // Warm non-registry runtime/TLS paths before the controlled same-thread scope.
    drop(edge(own(input.clone(), &broker), &broker).unwrap());
    let (_, observed) = measure(|| {
        for _ in 0..100 {
            let output = edge(own(input.clone(), &broker), &broker).unwrap();
            drop(output);
            assert_eq!(broker.registry.len(), 0);
        }
    });
    assert_eq!(observed.live, 0, "{observed:?}");
    assert_eq!(broker.inner.reserved(), 0);
}

#[test]
fn registered_nested_dictionary_decimal_views_survive_c_data_and_cross_thread_release() {
    use arrow::array::StructArray;
    use arrow::ffi::{from_ffi, FFI_ArrowArray, FFI_ArrowSchema};
    let broker = Broker::new();
    let input = super::tests::fixture();
    let pool = broker.pool();
    let memory = MemoryConsumer::new("native producer").register(&pool);
    memory.try_grow(input.get_array_memory_size()).unwrap();
    let producer =
        datafusion_batch_registered(input.clone(), memory, Some(broker.registry.clone())).unwrap();
    let before = broker.inner.reserved();
    let extra = 0;
    let output = edge(producer, &broker).unwrap();
    assert_eq!(broker.inner.reserved(), before + extra);
    for (expected, actual) in input.columns().iter().zip(output.columns()) {
        super::tests::assert_shared_data(&expected.to_data(), &actual.to_data());
    }
    let data = StructArray::from(output).to_data();
    let exported = FFI_ArrowArray::new(&data);
    let schema = FFI_ArrowSchema::try_from(data.data_type()).unwrap();
    drop(data);
    drop(input);
    drop(pool);
    // SAFETY: these arrays/schema were exported together immediately above.
    let imported = unsafe { from_ffi(exported, &schema) }.unwrap();
    assert_eq!(broker.inner.reserved(), before + extra);
    std::thread::spawn(move || drop(imported)).join().unwrap();
    assert_eq!(broker.inner.reserved(), 0);
    assert_eq!(broker.registry.len(), 0);
}

#[test]
fn separately_exported_raw_slices_move_one_prepaid_allocation_to_output_owners() {
    let broker = Broker::new();
    let source = RecordBatch::try_from_iter(vec![(
        "id",
        Arc::new(Int32Array::from_iter_values(0..4096)) as ArrayRef,
    )])
    .unwrap();
    let bytes = crate::memory_pool::buffer_size::batch_bytes(&source).unwrap();
    let mut workspace = HostMemoryReservation::new(broker.clone(), "prepaid kernel output");
    workspace.resize(bytes).unwrap();
    let first = host_edge_batch(source.slice(13, 128), &mut workspace, &broker.registry).unwrap();
    assert_eq!(workspace.size(), 0);
    let second = host_edge_batch(source.slice(256, 128), &mut workspace, &broker.registry).unwrap();
    assert_eq!(broker.inner.reserved(), bytes);
    drop(first);
    drop(source);
    assert_eq!(broker.inner.reserved(), bytes);
    assert_eq!(
        second
            .column(0)
            .as_any()
            .downcast_ref::<Int32Array>()
            .unwrap()
            .value(0),
        256
    );
    drop(second);
    assert_eq!(broker.inner.reserved(), 0);
    assert_eq!(broker.registry.len(), 0);
}
