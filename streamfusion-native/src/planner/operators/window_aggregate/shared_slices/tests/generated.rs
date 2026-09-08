// Copyright 2026 StreamFusion Authors
// Licensed under the Apache License, Version 2.0.

use super::*;

#[test]
fn generated_shared_windows_match_the_flink_verified_expanded_kernel_at_each_watermark() {
    for rocks in backends() {
        for seed in 0..3u64 {
            for batch_size in [7, 31] {
                let directory = tempfile::tempdir().unwrap();
                let broker = Arc::new(TestBroker::new(512 << 20));
                let mut shared =
                    processor(broker.clone(), rocks.then_some(directory.path()), 0, 127);
                let mut reference = crate::planner::operators::window_aggregate::tests::processor(
                    &plan(),
                    broker.clone(),
                );
                let mut random = seed + 1;
                for phase in 0..12 {
                    let rows = (0..31)
                        .map(|_| {
                            random = random.wrapping_mul(6364136223846793005).wrapping_add(1);
                            let key = ((random >> 32) % 7) as i64;
                            let slice = phase * 2000 + (((random >> 16) % 9) as i64 - 4) * 2000;
                            (key, 1 + (random % 5) as i64, slice)
                        })
                        .collect::<Vec<_>>();
                    for rows in rows.chunks(batch_size) {
                        let input = batch(rows);
                        shared.process(&input).unwrap();
                        assert_eq!(reference.process_arrow(input, 0).unwrap().num_rows(), 0);
                        assert_eq!(
                            shared.kernel.late_records_dropped,
                            reference.late_records_dropped
                        );
                    }
                    let watermark = if phase == 11 {
                        i64::MAX
                    } else {
                        phase * 2000 - 1
                    };
                    let mut actual = Vec::new();
                    loop {
                        actual.extend(output(&shared.advance(watermark).unwrap()));
                        if shared.next_timer().is_none_or(|next| next > watermark) {
                            break;
                        }
                    }
                    let mut expected = Vec::new();
                    loop {
                        expected.extend(output(&reference.advance_event_time(watermark).unwrap()));
                        if reference
                            .next_event_timer()
                            .is_none_or(|next| next > watermark)
                        {
                            break;
                        }
                    }
                    actual.sort();
                    expected.sort();
                    assert_eq!(
                        actual, expected,
                        "seed={seed}, batch={batch_size}, phase={phase}, rocks={rocks}"
                    );
                }
                assert_eq!(slice_count(&shared), 0);
                drop(shared);
                drop(reference);
                assert_eq!(broker.reserved(), 0);
            }
        }
    }
}

#[test]
fn input_writes_are_slice_deltas_and_timer_serialization_waits_for_checkpoint() {
    use crate::state::observed_tests::{Io, Observed};
    use std::sync::atomic::Ordering;
    for rocks in backends() {
        let directory = tempfile::tempdir().unwrap();
        let broker = Arc::new(TestBroker::new(512 << 20));
        let mut shared = processor(broker.clone(), rocks.then_some(directory.path()), 0, 127);
        let replacement = Box::new(
            crate::state::OrderedMemoryKeyedState::new(
                0,
                127,
                HostMemoryReservation::new(broker.clone(), "temporary state swap"),
            )
            .unwrap(),
        );
        let inner = std::mem::replace(&mut shared.kernel.state, replacement);
        let io = Arc::new(Io::default());
        shared.kernel.state = Box::new(Observed {
            inner,
            io: io.clone(),
        });
        let input = batch(
            &(0..1000)
                .map(|index| (1, 1, index * 2000 + 2000))
                .collect::<Vec<_>>(),
        );
        shared.process(&input).unwrap();
        let first = io.written_bytes.load(Ordering::Relaxed);
        assert_eq!(slice_count(&shared), 1000);
        io.reset();
        shared.process(&input).unwrap();
        assert_eq!(io.written_bytes.load(Ordering::Relaxed), first);
        assert_eq!(slice_count(&shared), 1000);
        for group in 0..128 {
            let values = shared
                .kernel
                .state
                .get_batch(
                    &[StateKeyRef {
                        key_group: group,
                        key: TIMER_STATE_KEY,
                    }],
                    &shared.kernel.scratch_reservation,
                )
                .unwrap();
            assert!(
                values[0].is_none(),
                "input must not rewrite the timer index"
            );
        }
        drop(shared);
        assert_eq!(broker.reserved(), 0);
    }
}

#[test]
fn wide_windows_page_state_reads_without_expanding_retained_slice_state() {
    let broker = Arc::new(TestBroker::new(64 << 20));
    let mut native = proto::NativePlan::decode(plan().as_slice()).unwrap();
    let Some(proto::operator::Operator::WindowAggregate(plan)) =
        &mut native.root.as_mut().unwrap().operator
    else {
        unreachable!()
    };
    plan.size_millis = 10_000;
    plan.slide_or_step_millis = 2;
    let mut kernel = crate::planner::operators::window_aggregate::tests::processor(
        &native.encode_to_vec(),
        broker.clone(),
    );
    kernel.state = Box::new(
        crate::state::OrderedMemoryKeyedState::new(
            0,
            127,
            HostMemoryReservation::new(broker.clone(), "wide slice state"),
        )
        .unwrap(),
    );
    let mut window = SharedSlices::new(kernel).unwrap();
    let input = batch(&[(1, 1, 2000)]);
    let mut columns = input.columns().to_vec();
    columns[2] = Arc::new(Int64Array::from(vec![1998]));
    window
        .process(&RecordBatch::try_new(input.schema(), columns).unwrap())
        .unwrap();
    assert_eq!(slice_count(&window), 1);
    let reads = window.kernel.state_read_batches;
    assert_eq!(
        output(&window.advance(1999).unwrap()),
        [(1, 1, -8000, 2000)]
    );
    assert_eq!(window.kernel.state_read_batches - reads, 2);
    assert_eq!(slice_count(&window), 1);
    drop(window);
    assert_eq!(broker.reserved(), 0);
}

#[test]
fn failed_batch_requires_recovery_and_drops_its_admitted_state_on_close() {
    let broker = Arc::new(TestBroker::new(1 << 20));
    let mut window = processor(broker.clone(), None, 0, 127);
    window.process(&batch(&[(1, 1, 2000)])).unwrap();
    let large = batch(&(0..10_000).map(|key| (key, 1, 2000)).collect::<Vec<_>>());
    assert!(window.process(&large).is_err());
    assert!(window
        .process(&batch(&[(1, 1, 2000)]))
        .unwrap_err()
        .to_string()
        .contains("restore a new context"));
    assert!(window.advance(1999).is_err());
    assert_eq!(slice_count(&window), 1);
    drop(window);
    assert_eq!(broker.reserved(), 0);
}
