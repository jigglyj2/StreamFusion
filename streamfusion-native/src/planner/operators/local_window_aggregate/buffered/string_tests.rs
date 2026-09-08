// Copyright 2026 StreamFusion Authors
// Licensed under the Apache License, Version 2.0.

use super::*;
use crate::memory_pool::{tests_support::TestBroker, MemoryReservationBroker};
use crate::planner::persistent::control::ControlEvent;
use arrow::array::{StringArray, TimestampMillisecondArray};
use std::sync::atomic::{AtomicUsize, Ordering};

#[derive(Debug)]
struct PeakBroker {
    inner: TestBroker,
    peak: AtomicUsize,
}
impl MemoryReservationBroker for PeakBroker {
    fn try_reserve(&self, bytes: usize) -> Result<bool> {
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
        self.inner.available()
    }
}

fn buffer(broker: Arc<dyn MemoryReservationBroker>, flink_bytes: usize) -> BufferedWindow {
    let source = super::super::tests::processor(false);
    let mut plan = source.plan.clone();
    plan.aggregate_calls.truncate(1);
    plan.input_schema.as_mut().unwrap().fields.remove(1);
    plan.time_attribute_index = 1;
    let logical = proto::LogicalType {
        nullable: true,
        r#type: Some(proto::logical_type::Type::Varchar(proto::EmptyType {})),
    };
    plan.input_schema.as_mut().unwrap().fields[0].r#type = Some(logical.clone());
    plan.output_schema.as_mut().unwrap().fields[0].r#type = Some(logical);
    let kernel = LocalWindowAggregateProcessor::from_plan(
        plan,
        HostMemoryReservation::new(broker.clone(), "string buffer workspace"),
        HostMemoryReservation::new(broker, "string buffer plan"),
    )
    .unwrap();
    BufferedWindow::new(kernel, flink_bytes, 32 << 10).unwrap()
}

fn input(buffer: &BufferedWindow, keys: &StringArray) -> RecordBatch {
    RecordBatch::try_new(
        buffer.kernel.input_schema.clone(),
        vec![
            Arc::new(keys.clone()),
            Arc::new(TimestampMillisecondArray::from(vec![1000; keys.len()])) as ArrayRef,
        ],
    )
    .unwrap()
}

#[test]
fn variable_keys_cover_allocation_peaks_and_survive_sliced_input_and_output_ownership() {
    let broker = Arc::new(PeakBroker {
        inner: TestBroker::new(64 << 20),
        peak: AtomicUsize::new(0),
    });
    let mut buffer = buffer(broker.clone(), 64 << 20);
    let keys = StringArray::from_iter((0..1027).map(|i| {
        if i == 5 {
            None
        } else {
            Some(format!("{i}:{}", "é".repeat([0, 3, 4, 128, 2048][i % 5])))
        }
    }));
    let keys = keys.slice(3, 1024);
    let batch = input(&buffer, &keys);
    let before = broker.inner.reserved();
    broker.peak.store(before, Ordering::Relaxed);
    let (outputs, observed) = crate::allocation_test_support::measure(|| {
        assert!(buffer.push(batch).unwrap().is_none());
        let mut outputs = Vec::new();
        if let Some(batch) = buffer.control(ControlEvent::BeforeCheckpoint(1)).unwrap() {
            outputs.push(batch);
        }
        while let Some(batch) = buffer.poll_pending().unwrap() {
            outputs.push(batch);
        }
        outputs
    });
    assert!(
        observed.peak <= broker.peak.load(Ordering::Relaxed) - before,
        "observed {}, reserved {}",
        observed.peak,
        broker.peak.load(Ordering::Relaxed) - before
    );
    let actual = outputs
        .iter()
        .flat_map(|batch| {
            batch
                .column(0)
                .as_any()
                .downcast_ref::<StringArray>()
                .unwrap()
                .iter()
        })
        .collect::<Vec<_>>();
    assert_eq!(actual, keys.iter().collect::<Vec<_>>());
    drop(buffer);
    assert!(broker.inner.reserved() > 0);
    drop(outputs);
    assert_eq!(broker.inner.reserved(), 0);
}

#[test]
fn wide_key_outputs_use_the_available_share_without_a_fixed_row_width_assumption() {
    let broker = Arc::new(TestBroker::new(32 << 20));
    let mut buffer = buffer(broker.clone(), 64 << 20);
    let keys = StringArray::from_iter_values((0..512).map(|i| format!("{i}:{}", "x".repeat(4097))));
    let batch = input(&buffer, &keys);
    assert!(buffer.push(batch).unwrap().is_none());
    let mut pressure = HostMemoryReservation::new(broker.clone(), "other operators");
    pressure
        .resize((32 << 20) - broker.reserved() - (1 << 20))
        .unwrap();
    let mut next = buffer.control(ControlEvent::BeforeCheckpoint(1)).unwrap();
    let mut offset = 0;
    while let Some(batch) = next {
        assert!(batch.num_rows() < 512);
        let values = batch
            .column(0)
            .as_any()
            .downcast_ref::<StringArray>()
            .unwrap();
        for (row, value) in values.iter().enumerate() {
            assert_eq!(value, Some(keys.value(offset + row)));
        }
        offset += batch.num_rows();
        drop(batch);
        next = buffer.poll_pending().unwrap();
    }
    assert_eq!(offset, keys.len());
    drop(pressure);
    drop(buffer);
    assert_eq!(broker.reserved(), 0);
}
