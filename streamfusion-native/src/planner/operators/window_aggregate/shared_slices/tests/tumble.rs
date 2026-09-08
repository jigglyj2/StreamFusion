// Copyright 2026 StreamFusion Authors
// Licensed under the Apache License, Version 2.0.

use super::*;
use crate::planner::operators::group_aggregate::AggregateValue;

fn tumble_plan(max: bool, direct: bool) -> Vec<u8> {
    let mut native = proto::NativePlan::decode(plan().as_slice()).unwrap();
    let Some(proto::operator::Operator::WindowAggregate(window)) =
        &mut native.root.as_mut().unwrap().operator
    else {
        unreachable!()
    };
    window.kind = proto::WindowKind::Tumble as i32;
    window.size_millis = 2000;
    window.slide_or_step_millis = 0;
    window.partial_windows_are_slices = direct;
    if max {
        let call = &mut window.aggregate_calls[0];
        call.function = proto::AggregateFunction::Max as i32;
        call.retractable = false;
        call.input_index = Some(0);
        call.input_type = call.output_type.clone();
    }
    native.encode_to_vec()
}

fn partials(rows: &[(i64, i64, i64)], max: bool) -> RecordBatch {
    let original = batch(rows);
    if !max {
        return original;
    }
    let mut columns = original.columns().to_vec();
    columns[1] = Arc::new(BinaryArray::from_iter_values(rows.iter().map(|row| {
        encode_state(&AccumulatorState {
            row_count: 1,
            accumulators: vec![Accumulator::AppendExtremum(Some(AggregateValue::Int(
                row.1 as i128,
            )))],
        })
    })));
    RecordBatch::try_new(original.schema(), columns).unwrap()
}

#[test]
fn tumble_count_and_max_fire_once_and_restore_without_shared_hop_timers() {
    for max in [false, true] {
        for direct in [false, true] {
            for rocks in backends() {
                let directory = tempfile::tempdir().unwrap();
                let broker = Arc::new(TestBroker::new(64 << 20));
                let plan = tumble_plan(max, direct);
                let mut source = processor_with_plan(
                    &plan,
                    broker.clone(),
                    rocks.then_some(directory.path()),
                    0,
                    127,
                );
                source
                    .process(&partials(&[(1, 2, 2000), (1, 7, 2000), (2, 5, 4000)], max))
                    .unwrap();
                assert_eq!(source.kernel.timer_registrations, 2);
                assert_eq!(
                    output(&source.advance(1999).unwrap()),
                    [(1, if max { 7 } else { 9 }, 0, 2000)]
                );
                assert_eq!(source.kernel.timer_registrations, 2);
                assert_eq!(slice_count(&source), 1);
                let snapshots = (0..128)
                    .map(|g| source.snapshot(g).unwrap())
                    .collect::<Vec<_>>();
                drop(source);
                let restored_directory = tempfile::tempdir().unwrap();
                let mut restored = processor_with_plan(
                    &plan,
                    broker.clone(),
                    (!rocks)
                        .then_some(restored_directory.path())
                        .filter(|_| backends().len() > 1),
                    0,
                    127,
                );
                for (group, snapshot) in snapshots.iter().enumerate() {
                    restored.restore(group as u32, snapshot, 1999).unwrap();
                }
                restored
                    .process(&partials(&[(1, 99, 2000), (2, 11, 4000)], max))
                    .unwrap();
                assert_eq!(restored.kernel.late_records_dropped, 1);
                assert_eq!(restored.advance(999).unwrap().num_rows(), 0);
                assert_eq!(
                    output(&restored.advance(i64::MAX).unwrap()),
                    [(2, if max { 11 } else { 16 }, 2000, 4000)]
                );
                assert_eq!(restored.next_timer(), None);
                assert_eq!(slice_count(&restored), 0);
                drop(restored);
                drop(snapshots);
                assert_eq!(broker.reserved(), 0);
            }
        }
    }
}
