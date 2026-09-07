// Copyright 2026 StreamFusion Authors
// Licensed under the Apache License, Version 2.0

use super::*;

#[test]
fn empty_bundles_account_actual_hash_table_storage_after_tombstone_removals() {
    with_backends(10_000, |mut processor| {
        processor
            .process_arrow(batch(
                (0..448).collect(),
                vec![Some(1); 448],
                Some(vec![INSERT; 448]),
            ))
            .unwrap();
        let allocation = processor.pending.allocation_size();
        assert!(allocation > 0);
        processor.finish_bundle().unwrap();
        assert!(processor.pending.is_empty());
        assert_eq!(processor.pending.allocation_size(), allocation);
        assert_eq!(processor.estimated_pending_bytes(), allocation);
        assert_eq!(processor.bundle_reservation.size(), allocation);
    });
}

fn ordered_rows(batch: RecordBatch) -> Vec<(i64, i64, Option<i64>, Option<i64>, Option<i64>, i8)> {
    // The shared helper sorts a whole batch; retain the physical changelog order here.
    (0..batch.num_rows())
        .map(|row| output_rows(batch.slice(row, 1)).pop().unwrap())
        .collect()
}

fn with_backends(size: u64, mut check: impl FnMut(GroupAggregateProcessor)) {
    check(mini_processor(size, true));
    if let Ok(plugin) = std::env::var("STREAMFUSION_TEST_ROCKSDB_PLUGIN") {
        let directory = tempfile::tempdir().unwrap();
        check(
            GroupAggregateProcessor::new_rocksdb(
                &mini_plan(size, true),
                128,
                0,
                127,
                std::path::Path::new(&plugin),
                directory.path(),
                64 << 20,
                HostMemoryReservation::new(
                    Arc::new(TestBroker::new(256 << 20)),
                    "mini-batch RocksDB regression",
                ),
            )
            .unwrap(),
        );
    }
}

#[test]
fn one_backend_read_and_write_even_when_every_row_flushes() {
    with_backends(1, |mut processor| {
        let output = processor
            .process_arrow(batch(
                vec![7; 6],
                vec![Some(10), Some(10), Some(20), Some(20), Some(3), Some(4)],
                Some(vec![INSERT, DELETE, INSERT, DELETE, INSERT, UPDATE_AFTER]),
            ))
            .unwrap();
        assert_eq!(
            ordered_rows(output),
            vec![
                (7, 1, Some(10), Some(10), Some(10), INSERT),
                (7, 1, Some(10), Some(10), Some(10), DELETE),
                (7, 1, Some(20), Some(20), Some(20), INSERT),
                (7, 1, Some(20), Some(20), Some(20), DELETE),
                (7, 1, Some(3), Some(3), Some(3), INSERT),
                (7, 1, Some(3), Some(3), Some(3), UPDATE_BEFORE),
                (7, 2, Some(7), Some(3), Some(4), UPDATE_AFTER),
            ]
        );
        assert_eq!(processor.statistics(), [1, 1]);
        assert_eq!(processor.scratch_reservation.size(), 0);
        assert_eq!(
            processor.bundle_reservation.size(),
            processor.estimated_pending_bytes()
        );
    });
}

#[test]
fn absent_retraction_crossing_a_bundle_can_be_followed_by_insert() {
    with_backends(2, |mut processor| {
        assert_eq!(
            processor
                .process_arrow(batch(vec![7], vec![Some(1)], Some(vec![DELETE]),))
                .unwrap()
                .num_rows(),
            0
        );
        assert_eq!(
            ordered_rows(
                processor
                    .process_arrow(batch(
                        vec![7, 7, 7],
                        vec![Some(1), Some(3), Some(4)],
                        Some(vec![UPDATE_BEFORE, INSERT, UPDATE_AFTER]),
                    ))
                    .unwrap()
            ),
            vec![(7, 2, Some(7), Some(3), Some(4), INSERT)]
        );
        assert_eq!(processor.statistics(), [1, 1]);
    });
}

#[test]
fn bundle_changelog_and_canonical_state_are_independent_of_arrow_chunking() {
    for trigger in [1, 2, 3, 7, 31] {
        let input = batch(
            vec![7, 8, 7, 9, 7, 8, 7, 9, 8, 7, 8, 9, 7, 7, 9, 8, 7],
            vec![
                Some(1),
                None,
                Some(2),
                Some(5),
                Some(1),
                None,
                Some(4),
                Some(5),
                Some(3),
                Some(2),
                Some(3),
                None,
                Some(4),
                Some(8),
                None,
                Some(7),
                Some(8),
            ],
            Some(vec![
                INSERT,
                INSERT,
                INSERT,
                INSERT,
                UPDATE_BEFORE,
                DELETE,
                UPDATE_AFTER,
                DELETE,
                INSERT,
                DELETE,
                DELETE,
                INSERT,
                DELETE,
                INSERT,
                DELETE,
                INSERT,
                DELETE,
            ]),
        );
        let mut reference = mini_processor(trigger, true);
        let mut expected = Vec::new();
        for row in 0..input.num_rows() {
            expected.extend(ordered_rows(
                reference.process_arrow(input.slice(row, 1)).unwrap(),
            ));
        }
        expected.extend(ordered_rows(reference.finish_bundle().unwrap()));
        let snapshots = (0..128)
            .map(|group| reference.snapshot_key_group(group).unwrap())
            .collect::<Vec<_>>();
        with_backends(trigger, |mut processor| {
            let mut actual = Vec::new();
            // Includes pending keys absent from the next Arrow batch and a nonempty tail.
            for (offset, length) in [(0, 1), (1, 1), (2, 11), (13, 4)] {
                let before = processor.statistics();
                actual.extend(ordered_rows(
                    processor
                        .process_arrow(input.slice(offset, length))
                        .unwrap(),
                ));
                let after = processor.statistics();
                assert!(after[0] - before[0] <= 1, "trigger={trigger}");
                assert!(after[1] - before[1] <= 1, "trigger={trigger}");
                assert_eq!(
                    processor.bundle_reservation.size(),
                    processor.estimated_pending_bytes()
                );
            }
            actual.extend(ordered_rows(processor.finish_bundle().unwrap()));
            assert_eq!(actual, expected, "trigger={trigger}");
            assert_eq!(processor.pending_element_count(), 0);
            assert_eq!(
                processor.bundle_reservation.size(),
                processor.estimated_pending_bytes()
            );
            for (group, snapshot) in snapshots.iter().enumerate() {
                assert_eq!(
                    &processor.snapshot_key_group(group as u32).unwrap(),
                    snapshot
                );
            }
        });
    }
}

#[test]
fn backend_commit_does_not_include_the_unflushed_tail() {
    with_backends(2, |mut processor| {
        let input = batch(
            vec![7, 7, 7],
            vec![Some(1), Some(2), Some(4)],
            Some(vec![INSERT; 3]),
        );
        let mut reference = mini_processor(2, true);
        reference.process_arrow(input.slice(0, 2)).unwrap();
        assert_eq!(
            ordered_rows(processor.process_arrow(input).unwrap()),
            vec![(7, 2, Some(3), Some(1), Some(2), INSERT)]
        );
        assert_eq!(processor.pending_element_count(), 1);
        for group in 0..128 {
            assert_eq!(
                processor
                    .state
                    .snapshot_key_group(group, &processor.scratch_reservation)
                    .unwrap(),
                reference
                    .state
                    .snapshot_key_group(group, &reference.scratch_reservation)
                    .unwrap()
            );
        }
        assert_eq!(
            ordered_rows(processor.finish_bundle().unwrap()),
            vec![
                (7, 2, Some(3), Some(1), Some(2), UPDATE_BEFORE),
                (7, 3, Some(7), Some(1), Some(4), UPDATE_AFTER),
            ]
        );
    });
}

#[test]
fn denied_sparse_mini_batch_admission_does_not_read_or_commit_state() {
    let mut processor = GroupAggregateProcessor::new(
        &mini_plan(1, true),
        128,
        0,
        127,
        HostMemoryReservation::new(Arc::new(TestBroker::new(64 << 10)), "denied mini-batch"),
    )
    .unwrap();
    let snapshots = (0..128)
        .map(|group| processor.snapshot_key_group(group).unwrap())
        .collect::<Vec<_>>();
    assert!(processor
        .process_arrow(batch(
            (0..128).collect(),
            vec![Some(1); 128],
            Some(vec![INSERT; 128]),
        ))
        .is_err());
    assert_eq!(processor.statistics(), [0, 0]);
    assert_eq!(processor.pending_element_count(), 0);
    assert_eq!(processor.scratch_reservation.size(), 0);
    for (group, snapshot) in snapshots.iter().enumerate() {
        assert_eq!(
            &processor.snapshot_key_group(group as u32).unwrap(),
            snapshot
        );
    }
}
