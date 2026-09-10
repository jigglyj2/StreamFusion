// Copyright 2026 StreamFusion Authors
// Licensed under the Apache License, Version 2.0.

use super::*;
use crate::state::observed_tests::{Io, Observed};
use std::sync::atomic::Ordering;

#[test]
fn firing_frontiers_preserve_group_assignments_across_read_and_output_pages() {
    for rocks in backends() {
        let directory = tempfile::tempdir().unwrap();
        let broker = Arc::new(TestBroker::new(256 << 20));
        let mut native = proto::NativePlan::decode(plan().as_slice()).unwrap();
        let Some(proto::operator::Operator::WindowAggregate(plan)) =
            &mut native.root.as_mut().unwrap().operator
        else {
            unreachable!()
        };
        plan.size_millis = 10_000;
        let mut shared = processor_with_plan(
            &native.encode_to_vec(),
            broker.clone(),
            rocks.then_some(directory.path()),
            0,
            127,
        );
        let replacement = Box::new(
            crate::state::OrderedMemoryKeyedState::new(
                0,
                127,
                HostMemoryReservation::new(broker.clone(), "state observation swap"),
            )
            .unwrap(),
        );
        let inner = std::mem::replace(&mut shared.kernel.state, replacement);
        let io = Arc::new(Io::default());
        shared.kernel.state = Box::new(Observed {
            inner,
            io: io.clone(),
        });
        // More keys than one output page, five slices per window, and duplicate partials.
        // One 4096-key read ends inside a group's slice list, exercising the next page's
        // group offsets. Missing slices must remain neutral, not shift assignments.
        let keys = 1103i64;
        let input = (0..keys)
            .flat_map(|key| [(key, key % 7 + 1, -2000), (key, 2, -2000), (key, 3, 2000)])
            .collect::<Vec<_>>();
        for chunk in input.chunks(257) {
            shared.process(&batch(chunk)).unwrap();
        }
        io.reset();
        let mut actual = Vec::new();
        while shared.next_timer().is_some() {
            actual.extend(output(&shared.advance(20_000).unwrap()));
        }
        actual.sort();
        let mut expected = Vec::new();
        for key in 0..keys {
            for end in [-2000, 0, 2000, 4000, 6000, 8000, 10000] {
                let count =
                    (if end <= 6000 { key % 7 + 3 } else { 0 }) + (if end >= 2000 { 3 } else { 0 });
                expected.push((key, count, end - 10_000, end));
            }
        }
        expected.sort();
        assert_eq!(actual, expected);
        assert_eq!(io.duplicate_read_keys.load(Ordering::Relaxed), 0);
        assert_eq!(io.max_read_rows.load(Ordering::Relaxed), READ_ROWS);
        assert!(io.read_batches.load(Ordering::Relaxed) > 7);
        assert_eq!(slice_count(&shared), 0);
        drop(shared);
        assert_eq!(broker.reserved(), 0);
    }
}
