// Copyright 2026 StreamFusion Authors
// Licensed under the Apache License, Version 2.0.

use super::*;
use crate::planner::persistent::control::ControlEvent;
use arrow::array::TimestampMillisecondArray;

fn buffer() -> BufferedWindow {
    buffer_with_broker(Arc::new(
        crate::memory_pool::tests_support::TestBroker::new(256 << 20),
    ))
}
pub(super) fn buffer_with_broker(
    broker: Arc<dyn crate::memory_pool::MemoryReservationBroker>,
) -> BufferedWindow {
    let source = super::super::tests::processor(false);
    let mut plan = source.plan.clone();
    plan.aggregate_calls.truncate(1);
    plan.input_schema.as_mut().unwrap().fields.remove(1);
    plan.time_attribute_index = 1;
    let kernel = LocalWindowAggregateProcessor::from_plan(
        plan,
        HostMemoryReservation::new(broker, "buffered test workspace"),
        source.reservation.sibling("buffered test plan"),
    )
    .unwrap();
    BufferedWindow::new(kernel, 3 << 20, 32 << 10).unwrap()
}
fn input(buffer: &BufferedWindow, rows: &[(i64, i64)]) -> RecordBatch {
    RecordBatch::try_new(
        buffer.kernel.input_schema.clone(),
        vec![
            Arc::new(Int64Array::from_iter_values(rows.iter().map(|r| r.0))) as ArrayRef,
            Arc::new(TimestampMillisecondArray::from_iter_values(
                rows.iter().map(|r| r.1),
            )) as ArrayRef,
        ],
    )
    .unwrap()
}
fn partials(batch: RecordBatch) -> Vec<(i64, i64, i64)> {
    let keys = batch
        .column(0)
        .as_any()
        .downcast_ref::<Int64Array>()
        .unwrap();
    let values = batch
        .column(1)
        .as_any()
        .downcast_ref::<BinaryArray>()
        .unwrap();
    let ends = batch
        .column(3)
        .as_any()
        .downcast_ref::<Int64Array>()
        .unwrap();
    (0..batch.num_rows())
        .map(|row| {
            let state = super::super::super::group_aggregate::decode_state(
                values.value(row),
                &[Call {
                    function: proto::AggregateFunction::CountStar,
                    input_index: None,
                    input_type: None,
                    output_type: DataType::Int64,
                    retractable: true,
                    filter_index: None,
                    distinct: false,
                }],
            )
            .unwrap();
            (keys.value(row), state.row_count, ends.value(row))
        })
        .collect()
}
pub(super) fn drain(
    buffer: &mut BufferedWindow,
    first: Option<RecordBatch>,
) -> Vec<(i64, i64, i64)> {
    let mut result = first.map(partials).unwrap_or_default();
    while let Some(batch) = buffer.poll_pending().unwrap() {
        result.extend(partials(batch));
    }
    result
}

#[test]
fn matches_generated_flink_watermark_prebarrier_and_first_appearance_oracle() {
    let mut buffer = buffer();
    let batch = input(&buffer, &[(9, 5000), (2, 1000), (9, 5001)]);
    assert!(buffer.push(batch).unwrap().is_none());
    for watermark in [999, 1998] {
        assert!(buffer
            .control(ControlEvent::Watermark(watermark))
            .unwrap()
            .is_none());
    }
    let first = buffer.control(ControlEvent::Watermark(1999)).unwrap();
    assert_eq!(drain(&mut buffer, first), vec![(9, 2, 6000), (2, 1, 2000)]);
    let batch = input(&buffer, &[(2, 1000)]);
    assert!(buffer.push(batch).unwrap().is_none());
    assert!(buffer
        .control(ControlEvent::Watermark(2000))
        .unwrap()
        .is_none());
    assert!(buffer.control(ControlEvent::EndInput).unwrap().is_none());
    let first = buffer.control(ControlEvent::BeforeCheckpoint(1)).unwrap();
    assert_eq!(drain(&mut buffer, first), vec![(2, 1, 2000)]);
    let batch = input(&buffer, &[(3, -1), (4, 9000)]);
    assert!(buffer.push(batch).unwrap().is_none());
    let first = buffer.control(ControlEvent::Watermark(i64::MAX)).unwrap();
    assert_eq!(drain(&mut buffer, first), vec![(3, 1, 0), (4, 1, 10000)]);
    assert!(!buffer.has_pending());
    assert_eq!(buffer.kernel.reservation.size(), 0);
}

#[test]
fn pressure_partial_boundaries_match_flink_independently_of_arrow_batch_boundaries() {
    for batch_size in [127, 4096, 100_000] {
        let mut buffer = buffer();
        let mut flushes = Vec::new();
        for start in (0..180_000).step_by(batch_size) {
            let rows: Vec<_> = (start..(start + batch_size).min(180_000))
                .map(|row| ((row % 17) as i64, 1000))
                .collect();
            let batch = input(&buffer, &rows);
            let first = buffer.push(batch).unwrap();
            let partials = drain(&mut buffer, first);
            for chunk in partials.chunks(17) {
                flushes.push(chunk.to_vec());
            }
        }
        let first = buffer.control(ControlEvent::BeforeCheckpoint(1)).unwrap();
        flushes.push(drain(&mut buffer, first));
        assert_eq!(flushes.len(), 3, "batch size {batch_size}");
        assert_eq!(
            flushes
                .iter()
                .map(|f| f.iter().map(|p| p.1).sum::<i64>())
                .collect::<Vec<_>>(),
            vec![64_529, 64_529, 50_942]
        );
        let mut consumed = 0;
        for flush in flushes {
            let count: i64 = flush.iter().map(|p| p.1).sum();
            for (offset, (key, value, end)) in flush.iter().enumerate() {
                assert_eq!(*key, ((consumed + offset) % 17) as i64);
                assert_eq!(*value, count / 17 + i64::from((offset as i64) < count % 17));
                assert_eq!(*end, 2000);
            }
            consumed += count as usize;
        }
    }
}

#[test]
fn control_output_is_bounded_and_must_drain_before_more_input() {
    let mut buffer = buffer();
    let batch = input(
        &buffer,
        &(0..5000).map(|key| (key, 1000)).collect::<Vec<_>>(),
    );
    assert!(buffer.push(batch).unwrap().is_none());
    let first = buffer
        .control(ControlEvent::BeforeCheckpoint(1))
        .unwrap()
        .unwrap();
    assert_eq!(first.num_rows(), OUTPUT_ROWS);
    let next_input = input(&buffer, &[(9999, 1000)]);
    assert!(buffer
        .push(next_input)
        .unwrap_err()
        .to_string()
        .contains("cursor"));
    assert!(buffer
        .control(ControlEvent::Watermark(1999))
        .unwrap_err()
        .to_string()
        .contains("drain"));
    let second = buffer.poll_pending().unwrap().unwrap();
    let third = buffer.poll_pending().unwrap().unwrap();
    assert_eq!(second.num_rows(), OUTPUT_ROWS);
    assert_eq!(third.num_rows(), 5000 - 2 * OUTPUT_ROWS);
    assert!(buffer.poll_pending().unwrap().is_none());
    let mut all = partials(first);
    all.extend(partials(second));
    all.extend(partials(third));
    assert_eq!(all, (0..5000).map(|key| (key, 1, 2000)).collect::<Vec<_>>());
}

#[test]
fn retained_memory_and_exported_outputs_outlive_workspace_until_last_owner_drops() {
    let broker = Arc::new(crate::memory_pool::tests_support::TestBroker::new(64 << 20));
    let mut buffer = buffer_with_broker(broker.clone());
    let baseline = broker.reserved();
    let batch = input(
        &buffer,
        &(0..5000).map(|key| (key, 1000)).collect::<Vec<_>>(),
    );
    let (result, observed) = crate::allocation_test_support::measure(|| buffer.push(batch.clone()));
    assert!(result.unwrap().is_none());
    assert_eq!(buffer.kernel.reservation.size(), 0);
    assert!(broker.reserved() > baseline);
    assert!(
        buffer.retained.size() as isize >= observed.live,
        "retained={}, observed={observed:?}",
        buffer.retained.size()
    );
    let output = buffer
        .control(ControlEvent::BeforeCheckpoint(1))
        .unwrap()
        .unwrap();
    let retained_output = output.column(1).slice(0, 1);
    drop(output);
    drop(buffer); // Includes a partially drained flush and all remaining grouped state.
    assert!(broker.reserved() > 0);
    drop(retained_output);
    assert_eq!(broker.reserved(), 0);
}

#[test]
fn denied_input_admission_leaves_existing_partials_and_credit_intact() {
    let broker = Arc::new(crate::memory_pool::tests_support::TestBroker::new(
        256 << 10,
    ));
    let mut buffer = buffer_with_broker(broker.clone());
    let batch = input(&buffer, &[(1, 1000), (1, 1001)]);
    assert!(buffer.push(batch).unwrap().is_none());
    let retained = broker.reserved();
    let batch = input(
        &buffer,
        &(0..1000).map(|key| (key, 1000)).collect::<Vec<_>>(),
    );
    assert!(buffer
        .push(batch)
        .unwrap_err()
        .to_string()
        .contains("Flink denied"));
    assert_eq!(broker.reserved(), retained);
    let first = buffer.control(ControlEvent::BeforeCheckpoint(1)).unwrap();
    assert_eq!(drain(&mut buffer, first), vec![(1, 2, 2000)]);
    drop(buffer);
    assert_eq!(broker.reserved(), 0);
}

#[test]
fn nullable_attached_end_plans_are_rejected_before_shared_buffer_mutation() {
    let mut kernel = crate::planner::operators::local_window_aggregate::tests::processor(false);
    let mut fields = kernel.input_schema.fields().to_vec();
    fields[2] = Arc::new(fields[2].as_ref().clone().with_nullable(true));
    kernel.input_schema = Arc::new(arrow::datatypes::Schema::new(fields));
    kernel.plan.attached_window_end_index = Some(2);
    assert!(BufferedWindow::new(kernel, 3 << 20, 32 << 10)
        .err()
        .unwrap()
        .to_string()
        .contains("nullable event-time"));
}

#[test]
fn cancelled_pressure_cursor_frees_allocations_before_returning_workspace_credit() {
    use crate::memory_pool::{tests_support::TestBroker, MemoryReservationBroker};
    use std::sync::atomic::{AtomicBool, Ordering};
    #[derive(Debug)]
    struct ObservingBroker {
        inner: TestBroker,
        active: AtomicBool,
    }
    impl MemoryReservationBroker for ObservingBroker {
        fn try_reserve(&self, bytes: usize) -> Result<bool> {
            self.inner.try_reserve(bytes)
        }
        fn available(&self) -> Result<Option<usize>> {
            self.inner.available()
        }
        fn release(&self, bytes: usize) -> Result<()> {
            self.inner.release(bytes)?;
            if self.active.load(Ordering::Relaxed) {
                let live = crate::allocation_test_support::current().live.max(0) as usize;
                assert!(
                    live <= self.inner.reserved(),
                    "returned live buffer credit: live={live}, reserved={}",
                    self.inner.reserved()
                );
            }
            Ok(())
        }
    }
    let broker = Arc::new(ObservingBroker {
        inner: TestBroker::new(256 << 20),
        active: AtomicBool::new(false),
    });
    let mut buffer = buffer_with_broker(broker.clone());
    broker.active.store(true, Ordering::Relaxed);
    let (_, _) = crate::allocation_test_support::measure(|| {
        let batch = input(
            &buffer,
            &(0..100_000).map(|row| (row % 17, 1000)).collect::<Vec<_>>(),
        );
        let output = buffer.push(batch).unwrap().unwrap();
        assert!(buffer.has_pending());
        drop(buffer);
        drop(output);
    });
    broker.active.store(false, Ordering::Relaxed);
    assert_eq!(broker.inner.reserved(), 0);
}

#[test]
fn end_only_attached_hop_uses_window_size_instead_of_reassigning_a_slice() {
    let source = buffer();
    let mut plan = source.kernel.plan.clone();
    plan.attached_window_end_index = Some(1);
    // Deliberately invalid unused direct-time index: only the attached end is accessed.
    plan.time_attribute_index = u32::MAX;
    let kernel = LocalWindowAggregateProcessor::from_plan(
        plan,
        source.kernel.reservation.sibling("attached workspace"),
        source.kernel.reservation.sibling("attached plan"),
    )
    .unwrap();
    let mut attached = BufferedWindow::new(kernel, 3 << 20, 32 << 10).unwrap();
    let batch = input(
        &attached,
        &[(1, 2000), (1, 2000), (2, -2000), (3, i64::MIN)],
    );
    assert!(attached.push(batch).unwrap().is_none());
    let output = attached
        .control(ControlEvent::BeforeCheckpoint(1))
        .unwrap()
        .unwrap();
    let starts = output
        .column(2)
        .as_any()
        .downcast_ref::<Int64Array>()
        .unwrap();
    assert_eq!(
        starts.values().as_ref(),
        &[-4000, -8000, i64::MIN.wrapping_sub(6000)]
    );
    assert_eq!(
        partials(output),
        vec![(1, 2, 2000), (2, 1, -2000), (3, 1, i64::MIN)]
    );
    assert!(!attached.has_pending());
}

#[test]
fn rejects_ambiguous_or_unverified_attached_bound_contracts() {
    let source = buffer();
    for variant in 0..4 {
        let mut plan = source.kernel.plan.clone();
        plan.attached_window_end_index = Some(1);
        match variant {
            0 => {
                plan.attached_window_start_index = Some(1);
                plan.attached_window_end_index = None;
            }
            1 => plan.kind = proto::WindowKind::Cumulate as i32,
            2 => plan.shift_time_zone = "America/New_York".into(),
            _ => plan.attached_window_end_index = Some(0),
        }
        let result = LocalWindowAggregateProcessor::from_plan(
            plan,
            source
                .kernel
                .reservation
                .sibling("invalid attached workspace"),
            source.kernel.reservation.sibling("invalid attached plan"),
        );
        assert!(result.is_err(), "invalid attached variant {variant}");
    }
}

#[test]
fn compact_buffered_admission_fits_small_flink_shares_without_eager_output_credit() {
    use crate::memory_pool::tests_support::TestBroker;
    for unique in [false, true] {
        let broker = Arc::new(TestBroker::new(1 << 20));
        let mut buffer = buffer_with_broker(broker.clone());
        let rows = (0..1025)
            .map(|row| (if unique { row } else { row % 17 }, 1000))
            .collect::<Vec<_>>();
        let batch = input(&buffer, &rows);
        assert!(buffer.kernel.batch_admission(&batch).unwrap() > 1 << 20);
        assert!(buffer.push(batch).unwrap().is_none());
        let first = buffer.control(ControlEvent::BeforeCheckpoint(1)).unwrap();
        if unique {
            assert!(first.as_ref().unwrap().num_rows() < 1025);
        }
        let actual = drain(&mut buffer, first);
        let groups = if unique { 1025 } else { 17 };
        let expected = (0..groups)
            .map(|key| {
                (
                    key,
                    rows.iter().filter(|row| row.0 == key).count() as i64,
                    2000,
                )
            })
            .collect::<Vec<_>>();
        assert_eq!(actual, expected);
        drop(buffer);
        assert_eq!(broker.reserved(), 0);
    }
}

#[test]
fn large_distinct_buffer_fits_flink_share_and_preserves_order_across_index_growth() {
    use crate::memory_pool::tests_support::TestBroker;
    for batch_size in [1024, 16_384] {
        let broker = Arc::new(TestBroker::new(16 << 20));
        let mut buffer = buffer_with_broker(broker.clone());
        buffer.layout = BufferLayout::new(16 << 20, 32 << 10).unwrap();
        for start in (0..100_000).step_by(batch_size) {
            let rows = (start..(start + batch_size).min(100_000))
                .map(|key| (key as i64, 1000))
                .collect::<Vec<_>>();
            let batch = input(&buffer, &rows);
            assert!(buffer.push(batch).unwrap().is_none());
        }
        // Probe the same encoded keys after several index rehashes; do not duplicate groups.
        let batch = input(&buffer, &[(17, 1000), (0, 1000), (99_999, 1000)]);
        assert!(buffer.push(batch).unwrap().is_none());
        assert_eq!(buffer.groups.len(), 100_000);
        assert!(buffer.retained.size() < 10 << 20);
        let first = buffer.control(ControlEvent::BeforeCheckpoint(1)).unwrap();
        let actual = drain(&mut buffer, first);
        let expected = (0..100_000)
            .map(|key| (key, 1 + i64::from([0, 17, 99_999].contains(&key)), 2000))
            .collect::<Vec<_>>();
        assert_eq!(actual, expected);
        drop(buffer);
        assert_eq!(broker.reserved(), 0);
    }
}

#[test]
fn nullable_rowtime_schema_accepts_values_but_rejects_null_before_state_changes() {
    let template = buffer();
    let mut plan = template.kernel.plan.clone();
    plan.input_schema.as_mut().unwrap().fields[1]
        .r#type
        .as_mut()
        .unwrap()
        .nullable = true;
    let kernel = LocalWindowAggregateProcessor::from_plan(
        plan,
        template
            .kernel
            .reservation
            .sibling("nullable time workspace"),
        template.kernel.reservation.sibling("nullable time plan"),
    )
    .unwrap();
    let mut buffer = BufferedWindow::new(kernel, 3 << 20, 32 << 10).unwrap();
    let invalid = RecordBatch::try_new(
        buffer.kernel.input_schema.clone(),
        vec![
            Arc::new(Int64Array::from(vec![1, 2])) as ArrayRef,
            Arc::new(TimestampMillisecondArray::from(vec![Some(1000), None])) as ArrayRef,
        ],
    )
    .unwrap();
    assert!(buffer
        .push(invalid)
        .unwrap_err()
        .to_string()
        .contains("RowTime field should not be null"));
    assert!(buffer.order.is_empty());
    let valid = input(&buffer, &[(1, 1000), (2, 1000)]);
    assert!(buffer.push(valid).unwrap().is_none());
    let first = buffer.control(ControlEvent::BeforeCheckpoint(1)).unwrap();
    assert_eq!(drain(&mut buffer, first), vec![(1, 1, 2000), (2, 1, 2000)]);
}
