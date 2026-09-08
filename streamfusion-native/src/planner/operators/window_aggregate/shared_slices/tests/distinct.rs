// Copyright 2026 StreamFusion Authors
// Licensed under the Apache License, Version 2.0.

use super::*;
use crate::memory_pool::MemoryReservationBroker;
use arrow::array::StringArray;
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

fn distinct_plan() -> Vec<u8> {
    let mut native = proto::NativePlan::decode(plan().as_slice()).unwrap();
    let Some(proto::operator::Operator::WindowAggregate(window)) =
        &mut native.root.as_mut().unwrap().operator
    else {
        unreachable!()
    };
    window.kind = proto::WindowKind::Tumble as i32;
    window.size_millis = 2000;
    window.slide_or_step_millis = 0;
    window.aggregate_calls.clear();
    window.grouping_indices = vec![0, 1];
    window.partial_accumulator_index = Some(2);
    window.partial_window_start_index = Some(3);
    window.partial_slice_end_index = Some(4);
    let string = proto::Field {
        name: "text".into(),
        r#type: Some(proto::LogicalType {
            nullable: true,
            r#type: Some(proto::logical_type::Type::Varchar(proto::EmptyType {})),
        }),
    };
    window
        .input_schema
        .as_mut()
        .unwrap()
        .fields
        .insert(1, string.clone());
    window.output_schema.as_mut().unwrap().fields[1] = string;
    native.encode_to_vec()
}

fn input() -> RecordBatch {
    let count = 256;
    let partial = encode_state(&AccumulatorState {
        row_count: 1,
        accumulators: Vec::new(),
    });
    RecordBatch::try_from_iter(vec![
        (
            "key",
            Arc::new(Int64Array::from_iter_values(
                (0..count).map(|i| i as i64 - 128),
            )) as ArrayRef,
        ),
        (
            "text",
            Arc::new(StringArray::from_iter((0..count).map(|i| {
                if i % 13 == 0 {
                    None
                } else {
                    Some(format!("{i}:{}", "é".repeat(2048)))
                }
            }))) as ArrayRef,
        ),
        (
            "accumulator",
            Arc::new(BinaryArray::from_iter_values(
                (0..count).map(|_| partial.as_slice()),
            )) as ArrayRef,
        ),
        (
            "window_start",
            Arc::new(Int64Array::from(vec![0; count])) as ArrayRef,
        ),
        (
            "slice_end",
            Arc::new(Int64Array::from(vec![2000; count])) as ArrayRef,
        ),
    ])
    .unwrap()
}

#[test]
fn distinct_composite_key_workspace_covers_actual_allocations_and_returns_all_credit() {
    let broker = Arc::new(PeakBroker {
        inner: TestBroker::new(64 << 20),
        peak: AtomicUsize::new(0),
    });
    let mut window = processor_with_plan(&distinct_plan(), broker.clone(), None, 0, 127);
    let batch = input();
    let before = broker.inner.reserved();
    broker.peak.store(before, Ordering::Relaxed);
    let ((), observed) =
        crate::allocation_test_support::measure(|| window.process(&batch).unwrap());
    assert!(
        observed.peak <= broker.peak.load(Ordering::Relaxed) - before,
        "observed {} bytes, admitted {}",
        observed.peak,
        broker.peak.load(Ordering::Relaxed) - before
    );
    drop(window);
    assert_eq!(broker.inner.reserved(), 0);
}

#[test]
fn distinct_wide_keys_deduplicate_and_restore_across_both_backends() {
    for rocks in backends() {
        let directory = tempfile::tempdir().unwrap();
        let broker = Arc::new(TestBroker::new(64 << 20));
        let mut window = processor_with_plan(
            &distinct_plan(),
            broker.clone(),
            rocks.then_some(directory.path()),
            0,
            127,
        );
        let batch = input();
        window.process(&batch).unwrap();
        window.process(&batch.slice(7, 128)).unwrap();
        assert_eq!(window.kernel.timer_registrations, 256);
        let snapshots = (0..128)
            .map(|group| window.snapshot(group).unwrap())
            .collect::<Vec<_>>();
        drop(window);
        let restored_directory = tempfile::tempdir().unwrap();
        let mut restored = processor_with_plan(
            &distinct_plan(),
            broker.clone(),
            (!rocks && backends().len() > 1).then_some(restored_directory.path()),
            0,
            127,
        );
        for (group, snapshot) in snapshots.iter().enumerate() {
            restored.restore(group as u32, snapshot, 999).unwrap();
        }
        restored.process(&batch).unwrap();
        let output = restored.advance(1999).unwrap();
        let mut actual = (0..output.num_rows())
            .map(|row| {
                (
                    output
                        .column(0)
                        .as_any()
                        .downcast_ref::<Int64Array>()
                        .unwrap()
                        .value(row),
                    output
                        .column(1)
                        .as_any()
                        .downcast_ref::<StringArray>()
                        .unwrap()
                        .iter()
                        .nth(row)
                        .unwrap()
                        .map(str::to_owned),
                )
            })
            .collect::<Vec<_>>();
        let mut expected = (0..batch.num_rows())
            .map(|row| {
                (
                    batch
                        .column(0)
                        .as_any()
                        .downcast_ref::<Int64Array>()
                        .unwrap()
                        .value(row),
                    batch
                        .column(1)
                        .as_any()
                        .downcast_ref::<StringArray>()
                        .unwrap()
                        .iter()
                        .nth(row)
                        .unwrap()
                        .map(str::to_owned),
                )
            })
            .collect::<Vec<_>>();
        actual.sort();
        expected.sort();
        assert_eq!(actual, expected);
        assert!(output
            .column(2)
            .as_any()
            .downcast_ref::<TimestampMillisecondArray>()
            .unwrap()
            .values()
            .iter()
            .all(|&start| start == 0));
        assert!(output
            .column(3)
            .as_any()
            .downcast_ref::<TimestampMillisecondArray>()
            .unwrap()
            .values()
            .iter()
            .all(|&end| end == 2000));
        assert_eq!(restored.next_timer(), None);
        assert_eq!(slice_count(&restored), 0);
        drop(output);
        drop(restored);
        drop(snapshots);
        assert_eq!(broker.reserved(), 0);
    }
}

#[test]
fn wide_key_credit_is_denied_before_encoding_or_state_mutation() {
    let broker = Arc::new(TestBroker::new(1 << 20));
    let mut window = processor_with_plan(&distinct_plan(), broker.clone(), None, 0, 127);
    assert!(matches!(
        window.process(&input()),
        Err(DataFusionError::ResourcesExhausted(_))
    ));
    assert_eq!(slice_count(&window), 0);
    assert_eq!(window.next_timer(), None);
    drop(window);
    assert_eq!(broker.reserved(), 0);
}
