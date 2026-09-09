// Copyright 2026 StreamFusion Authors
// Licensed under the Apache License, Version 2.0.

use super::*;

#[test]
fn recreating_a_deleted_group_is_independent_of_arrow_batch_boundaries() {
    for plan in [plan(true, true), aggregate_distinct_plan()] {
        let input = batch(
            vec![7; 7],
            vec![
                Some(10),
                Some(20),
                Some(20),
                Some(30),
                Some(10),
                Some(30),
                Some(40),
            ],
            Some(vec![
                INSERT,
                DELETE,
                UPDATE_BEFORE,
                UPDATE_AFTER,
                INSERT,
                DELETE,
                INSERT,
            ]),
        );
        let run = |chunk: usize| {
            let broker = Arc::new(TestBroker::new(16 << 20));
            let mut processor = GroupAggregateProcessor::new(
                &plan,
                128,
                0,
                127,
                HostMemoryReservation::new(broker.clone(), "group lifecycle"),
            )
            .unwrap();
            let mut output = Vec::new();
            for start in (0..input.num_rows()).step_by(chunk) {
                output.push(
                    processor
                        .process_arrow(input.slice(start, chunk.min(input.num_rows() - start)))
                        .unwrap(),
                );
            }
            let output = arrow::compute::concat_batches(&output[0].schema(), &output).unwrap();
            let key = processor.state_key(&input, 0).unwrap();
            let snapshot = processor
                .snapshot_key_group(key.key_group)
                .unwrap()
                .to_vec();
            drop(processor);
            // Output owns its existing Arrow lease until dropped by the assertion below.
            (output, snapshot, broker)
        };
        let (expected, snapshot, _) = run(1);
        for chunk in [2, 3, 7] {
            let (actual, actual_snapshot, broker) = run(chunk);
            assert_eq!(actual, expected, "chunk={chunk}");
            assert_eq!(actual_snapshot, snapshot, "chunk={chunk}");
            drop(actual);
            assert_eq!(broker.reserved(), 0);
        }
    }
}
