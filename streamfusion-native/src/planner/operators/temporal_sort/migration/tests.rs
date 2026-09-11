// Copyright 2026 StreamFusion Authors
// Licensed under the Apache License, Version 2.0.

use super::*;
use crate::memory_pool::tests_support::TestBroker;
use crate::planner::operators::temporal_sort::row_state_tests::backends;
use crate::state::observed_tests::{Io, Observed};
use std::sync::atomic::Ordering;

#[test]
fn legacy_lists_migrate_with_bounded_writes_and_preserve_stable_order() {
    let broker = Arc::new(TestBroker::new(64 << 20));
    let owner = HostMemoryReservation::new(broker.clone(), "legacy temporal source");
    let workspace = Arc::new(TestBroker::new(12 << 20));
    let scratch = HostMemoryReservation::new(workspace.clone(), "bounded legacy migration");
    let directory = tempfile::tempdir().unwrap();
    let rows = (0..8192usize)
        .map(|row| BufferedRow {
            kind: (row % 4) as i8,
            sort_key: ((row * 17 % 23) as u32).to_be_bytes().to_vec(),
            row: vec![(row % 251) as u8; 512],
        })
        .collect::<Vec<_>>();
    let legacy = legacy_state::encode_rows(&rows).unwrap();
    let mut expected = rows;
    expected.sort_by(|a, b| a.sort_key.cmp(&b.sort_key));
    for (case, root) in [rows_state_key(0, -42), processing_time_rows_state_key(0)]
        .into_iter()
        .enumerate()
    {
        for inner in backends(&owner, &directory.path().join(format!("rocks-{case}"))) {
            let io = Arc::new(Io::default());
            let mut state = Observed {
                inner,
                io: io.clone(),
            };
            state
                .write_batch(vec![StateMutation {
                    key: root.clone(),
                    value: Some(legacy.clone()),
                }])
                .unwrap();
            io.reset();
            let mut writes = 0;
            migrate_legacy_groups(&mut state, 0, &scratch, &mut writes).unwrap();
            assert!(writes > 16);
            assert_eq!(writes as usize, io.write_batches.load(Ordering::Relaxed));
            assert_eq!(workspace.reserved(), 0);
            let loaded = row_state::load(&state, &[root.clone()], &owner).unwrap();
            assert_eq!(loaded.groups, vec![expected.clone()]);
            drop(loaded);
            io.reset();
            migrate_legacy_groups(&mut state, 0, &scratch, &mut writes).unwrap();
            assert_eq!(io.write_batches.load(Ordering::Relaxed), 0);
        }
    }
    drop(scratch);
    drop(owner);
    assert_eq!(workspace.reserved(), 0);
    assert_eq!(broker.reserved(), 0);
}

#[test]
fn malformed_legacy_list_fails_validation_before_migration_writes() {
    let broker = Arc::new(TestBroker::new(16 << 20));
    let owner = HostMemoryReservation::new(broker.clone(), "corrupt legacy temporal state");
    let directory = tempfile::tempdir().unwrap();
    let root = rows_state_key(0, 42);
    let mut legacy = legacy_state::encode_rows(&[BufferedRow {
        kind: INSERT,
        sort_key: vec![1],
        row: vec![3; 1024],
    }])
    .unwrap();
    legacy.pop();
    for inner in backends(&owner, directory.path()) {
        let io = Arc::new(Io::default());
        let mut state = Observed {
            inner,
            io: io.clone(),
        };
        state
            .write_batch(vec![StateMutation {
                key: root.clone(),
                value: Some(legacy.clone()),
            }])
            .unwrap();
        io.reset();
        let mut writes = 0;
        assert!(migrate_legacy_groups(&mut state, 0, &owner, &mut writes).is_err());
        assert_eq!(writes, 0);
        assert_eq!(io.write_batches.load(Ordering::Relaxed), 0);
        let values = state
            .get_batch(
                &[StateKeyRef {
                    key_group: 0,
                    key: &root.key,
                }],
                &owner,
            )
            .unwrap();
        assert_eq!(values[0].as_ref().unwrap().as_ref(), legacy);
    }
    drop(owner);
    assert_eq!(broker.reserved(), 0);
}

#[test]
fn processor_restores_legacy_rows_then_drains_pages_on_both_backends() {
    use crate::planner::operators::temporal_sort::tests::{batch, plan};
    use arrow::array::Int32Array;
    let broker = Arc::new(TestBroker::new(64 << 20));
    let owner = HostMemoryReservation::new(broker.clone(), "legacy processor restore");
    let directory = tempfile::tempdir().unwrap();
    for processing in [false, true] {
        let mut source =
            TemporalSortProcessor::new(&plan(processing), 1, 0, 0, owner.sibling("source"))
                .unwrap();
        let numbers = (0..2049).rev().collect::<Vec<i32>>();
        source
            .process_arrow(batch(
                &vec![1000; numbers.len()],
                &numbers,
                &vec!["payload"; numbers.len()],
                &vec![INSERT; numbers.len()],
                processing.then_some(vec![40; numbers.len()]).as_deref(),
            ))
            .unwrap();
        let root = if processing {
            processing_time_rows_state_key(0)
        } else {
            rows_state_key(0, 1000)
        };
        let loaded = row_state::load(source.state.as_ref(), &[root.clone()], &owner).unwrap();
        let legacy = legacy_state::encode_rows(&loaded.groups[0]).unwrap();
        let mut mutations = loaded.mutations;
        mutations.push(StateMutation {
            key: root,
            value: Some(legacy),
        });
        source.state.write_batch(mutations).unwrap();
        let snapshot = source.snapshot_key_group(0).unwrap();
        for inner in backends(
            &owner,
            &directory.path().join(format!("restore-{processing}")),
        ) {
            let mut target = TemporalSortProcessor::with_state(
                &plan(processing),
                1,
                0,
                0,
                inner,
                owner.sibling("timers"),
                owner.sibling("output"),
            )
            .unwrap();
            target.restore_key_group(0, &snapshot).unwrap();
            let mut actual = Vec::new();
            loop {
                let next = if processing {
                    target.next_processing_time_timer()
                } else {
                    target.next_event_time_timer()
                };
                if next == i64::MAX {
                    break;
                }
                let output = if processing {
                    target.advance_processing_time(41)
                } else {
                    target.advance_event_time(1000)
                }
                .unwrap();
                assert!(output.num_rows() <= row_state::PAGE_ROWS);
                actual.extend_from_slice(
                    output
                        .column(1)
                        .as_any()
                        .downcast_ref::<Int32Array>()
                        .unwrap()
                        .values(),
                );
            }
            assert_eq!(actual, (0..2049).collect::<Vec<i32>>());
        }
    }
    let mut failed =
        TemporalSortProcessor::new(&plan(false), 1, 0, 0, owner.sibling("bad restore")).unwrap();
    assert!(failed.restore_key_group(0, b"truncated").is_err());
    assert!(failed
        .snapshot_key_group(0)
        .unwrap_err()
        .to_string()
        .contains("recreate"));
    drop(failed);
    drop(owner);
    assert_eq!(broker.reserved(), 0);
}
