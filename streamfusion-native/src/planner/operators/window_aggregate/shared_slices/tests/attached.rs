// Copyright 2026 StreamFusion Authors
// Licensed under the Apache License, Version 2.0.

use super::*;
use crate::planner::operators::group_aggregate::AggregateValue;

fn attached_plan() -> Vec<u8> {
    let mut native = proto::NativePlan::decode(plan().as_slice()).unwrap();
    let Some(proto::operator::Operator::WindowAggregate(window)) =
        &mut native.root.as_mut().unwrap().operator
    else {
        unreachable!()
    };
    window.partial_windows_are_slices = false;
    let call = &mut window.aggregate_calls[0];
    call.function = proto::AggregateFunction::Max as i32;
    call.retractable = false;
    call.input_index = Some(0);
    call.input_type = call.output_type.clone();
    native.encode_to_vec()
}

fn attached_batch(rows: &[(i64, i64, i64)]) -> RecordBatch {
    let original = batch(rows);
    let mut columns = original.columns().to_vec();
    columns[1] = Arc::new(BinaryArray::from_iter_values(rows.iter().map(|row| {
        encode_state(&AccumulatorState {
            row_count: 1,
            accumulators: vec![Accumulator::AppendExtremum(Some(AggregateValue::Int(
                row.1 as i128,
            )))],
        })
    })));
    columns[2] = Arc::new(Int64Array::from_iter_values(
        rows.iter().map(|row| row.2 - 6000),
    ));
    RecordBatch::try_new(original.schema(), columns).unwrap()
}

#[test]
fn attached_max_keeps_one_namespace_and_fires_once_on_each_backend() {
    for rocks in backends() {
        let directory = tempfile::tempdir().unwrap();
        let broker = Arc::new(TestBroker::new(256 << 20));
        let mut window = processor_with_plan(
            &attached_plan(),
            broker.clone(),
            rocks.then_some(directory.path()),
            0,
            127,
        );
        window
            .process(&attached_batch(&[
                (1, -8, 2000),
                (1, -2, 2000),
                (1, 9, 4000),
                (2, i64::MIN, 4000),
            ]))
            .unwrap();
        assert_eq!(slice_count(&window), 3);
        assert_eq!(window.kernel.state_read_batches, 1);
        assert_eq!(window.kernel.state_write_batches, 1);
        assert_eq!(
            output(&window.advance(1999).unwrap()),
            [(1, -2, -4000, 2000)]
        );
        assert_eq!(slice_count(&window), 2);
        window
            .process(&attached_batch(&[
                (1, 100, 2000),
                (2, 100, 2000),
                (1, 11, 4000),
            ]))
            .unwrap();
        assert_eq!(window.kernel.late_records_dropped, 2);
        let mut actual = output(&window.advance(i64::MAX).unwrap());
        actual.sort();
        assert_eq!(actual, [(1, 11, -2000, 4000), (2, i64::MIN, -2000, 4000)]);
        assert_eq!(slice_count(&window), 0);
        assert_eq!(window.next_timer(), None);
        assert_eq!(window.kernel.timer_registrations, 3);
        drop(window);
        assert_eq!(broker.reserved(), 0);
    }
}

#[test]
fn attached_restore_pins_layout_and_expires_late_partials_before_watermark_replay() {
    let broker = Arc::new(TestBroker::new(512 << 20));
    let mut source = processor_with_plan(&attached_plan(), broker.clone(), None, 0, 127);
    source
        .process(&attached_batch(&[(1, 2, 2000), (1, 7, 4000)]))
        .unwrap();
    drop(source.advance(1999).unwrap());
    let snapshots = (0..128)
        .map(|group| source.snapshot(group).unwrap())
        .collect::<Vec<_>>();
    drop(source);
    for rocks in backends() {
        let directory = tempfile::tempdir().unwrap();
        let mut restored = processor_with_plan(
            &attached_plan(),
            broker.clone(),
            rocks.then_some(directory.path()),
            0,
            127,
        );
        for (group, bytes) in snapshots.iter().enumerate() {
            restored.restore(group as u32, bytes, 1999).unwrap();
        }
        restored
            .process(&attached_batch(&[
                (1, 100, 2000),
                (2, 100, 2000),
                (1, 11, 4000),
            ]))
            .unwrap();
        assert_eq!(restored.kernel.late_records_dropped, 2);
        assert_eq!(restored.advance(1999).unwrap().num_rows(), 0);
        assert_eq!(
            output(&restored.advance(i64::MAX).unwrap()),
            [(1, 11, -2000, 4000)]
        );
        assert_eq!(restored.next_timer(), None);
    }
    let mut shared = processor(broker.clone(), None, 0, 127);
    assert!(shared
        .restore(0, &snapshots[0], 1999)
        .unwrap_err()
        .to_string()
        .contains("matching versioned slice state"));
    drop(shared);
    drop(snapshots);
    assert_eq!(broker.reserved(), 0);
}

#[test]
fn attached_bounds_must_match_the_flink_assigner_and_failure_prevents_reuse() {
    let broker = Arc::new(TestBroker::new(128 << 20));
    let mut window = processor_with_plan(&attached_plan(), broker.clone(), None, 0, 127);
    assert!(window
        .process(&batch(&[(1, 1, 2000)]))
        .unwrap_err()
        .to_string()
        .contains("bounds must match"));
    assert!(window
        .process(&attached_batch(&[(1, 1, 2000)]))
        .unwrap_err()
        .to_string()
        .contains("restore a new context"));
    drop(window);
    assert_eq!(broker.reserved(), 0);
}
