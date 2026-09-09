// Copyright 2026 StreamFusion Authors
// Licensed under the Apache License, Version 2.0.
use super::tests::drain;
use super::*;
use crate::memory_pool::tests_support::TestBroker;
use crate::planner::persistent::control::ControlEvent;

fn buffer(gap: i64, broker: Arc<TestBroker>) -> BufferedWindow {
    let source = super::tests::buffer_with_broker(broker.clone());
    let mut plan = source.kernel.plan.clone();
    plan.kind = proto::WindowKind::Tumble as i32;
    plan.size_millis = gap;
    plan.slide_or_step_millis = gap;
    plan.input_schema.as_mut().unwrap().fields[1]
        .r#type
        .as_mut()
        .unwrap()
        .nullable = true;
    plan.input_schema.as_mut().unwrap().fields[1]
        .r#type
        .as_mut()
        .unwrap()
        .r#type = Some(proto::logical_type::Type::TimestampLtz(
        proto::PrecisionType { precision: 3 },
    ));
    let kernel = LocalWindowAggregateProcessor::from_plan(
        plan,
        HostMemoryReservation::new(broker.clone(), "processing-time buffer workspace"),
        HostMemoryReservation::new(broker, "processing-time buffer plan"),
    )
    .unwrap();
    BufferedWindow::new_processing_time(kernel, 3 << 20, 32 << 10).unwrap()
}
fn input(buffer: &BufferedWindow, keys: &[i64]) -> RecordBatch {
    RecordBatch::try_new(
        buffer.kernel.input_schema.clone(),
        vec![
            Arc::new(Int64Array::from_iter_values(keys.iter().copied())) as ArrayRef,
            arrow::array::new_null_array(
                buffer.kernel.input_schema.field(1).data_type(),
                keys.len(),
            ),
        ],
    )
    .unwrap()
}
fn push(buffer: &mut BufferedWindow, keys: &[i64], clocks: &[i64]) -> Vec<(i64, i64, i64)> {
    let batch = input(buffer, keys);
    let first = buffer
        .push_processing_time(batch, Int64Array::from_iter_values(clocks.iter().copied()))
        .unwrap();
    drain(buffer, first)
}
fn control(buffer: &mut BufferedWindow, event: ControlEvent) -> Vec<(i64, i64, i64)> {
    let first = buffer.control(event).unwrap();
    drain(buffer, first)
}

#[test]
fn processing_time_grouped_compute_uses_each_clock_and_flushes_future_partials_with_due_partials() {
    for gap in [1000, 10000, 37000] {
        let broker = Arc::new(TestBroker::new(256 << 20));
        let mut buffer = buffer(gap, broker.clone());
        assert!(push(&mut buffer, &[7, 7, 9], &[-1, gap + 1, gap - 1]).is_empty());
        assert!(control(&mut buffer, ControlEvent::Watermark(i64::MAX)).is_empty());
        assert_eq!(
            control(&mut buffer, ControlEvent::ProcessingTime(gap - 1)),
            vec![(7, 1, 0), (7, 1, 2 * gap), (9, 1, gap)]
        );
        drop(buffer);
        assert_eq!(broker.reserved(), 0);
    }
}

#[test]
fn same_and_older_timer_deadlines_preserve_buffer_until_checkpoint_like_flink() {
    for gap in [1000, 10000, 37000] {
        let broker = Arc::new(TestBroker::new(256 << 20));
        let mut buffer = buffer(gap, broker.clone());
        assert!(push(&mut buffer, &[7, 7], &[gap + 1; 2]).is_empty());
        assert_eq!(
            control(&mut buffer, ControlEvent::ProcessingTime(2 * gap - 1)),
            vec![(7, 2, 2 * gap)]
        );
        assert!(push(&mut buffer, &[7, 7, 7], &[1; 3]).is_empty());
        for event in [
            ControlEvent::ProcessingTime(gap - 1),
            ControlEvent::ProcessingTime(2 * gap - 1),
            ControlEvent::Watermark(i64::MAX),
            ControlEvent::EndInput,
        ] {
            assert!(control(&mut buffer, event).is_empty());
        }
        assert_eq!(
            control(&mut buffer, ControlEvent::BeforeCheckpoint(9)),
            vec![(7, 3, gap)]
        );
        assert!(push(&mut buffer, &[7; 5], &[1; 5]).is_empty());
        assert!(control(&mut buffer, ControlEvent::ProcessingTime(gap - 1)).is_empty());
        assert_eq!(
            control(&mut buffer, ControlEvent::BeforeCheckpoint(10)),
            vec![(7, 5, gap)]
        );
        drop(buffer);
        assert_eq!(broker.reserved(), 0);
    }
}

#[test]
fn negative_initial_processing_time_can_flush_and_invalid_clock_inputs_do_not_mutate_buffer() {
    let broker = Arc::new(TestBroker::new(256 << 20));
    let mut buffer = buffer(10000, broker.clone());
    let batch = input(&buffer, &[7, 7]);
    let before = broker.reserved();
    assert!(buffer.push(batch.clone()).is_err());
    assert!(buffer
        .push_processing_time(batch.clone(), Int64Array::from(vec![Some(1), None]))
        .is_err());
    assert!(buffer
        .push_processing_time(batch, Int64Array::from(vec![1]))
        .is_err());
    assert!(buffer.order.is_empty());
    assert_eq!(broker.reserved(), before);
    assert!(push(&mut buffer, &[7, 7], &[-9999, -2]).is_empty());
    assert_eq!(
        control(&mut buffer, ControlEvent::ProcessingTime(-1)),
        vec![(7, 2, 0)]
    );
    drop(buffer);
    assert_eq!(broker.reserved(), 0);
}
