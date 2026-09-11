// Copyright 2026 StreamFusion Authors
// Licensed under the Apache License, Version 2.0.

use super::*;
use crate::memory_pool::tests_support::TestBroker;
use crate::planner::operators::temporal_sort::{
    row_state_tests::backends,
    tests::{batch, plan},
};
use crate::state::observed_tests::{Io, Observed};
use arrow::array::StringArray;
use std::sync::atomic::Ordering;

#[test]
fn wide_fired_groups_drain_with_four_mib_workspace_and_keep_callback_order() {
    const LIMIT: usize = 128 << 20;
    const COUNT: usize = 4097;
    let broker = Arc::new(TestBroker::new(LIMIT));
    let owner = HostMemoryReservation::new(broker.clone(), "bounded temporal drain test");
    let directory = tempfile::tempdir().unwrap();
    for processing in [false, true] {
        for inner in backends(
            &owner,
            &directory.path().join(format!("rocks-{processing}")),
        ) {
            let io = Arc::new(Io::default());
            let mut processor = TemporalSortProcessor::with_state(
                &plan(processing),
                1,
                0,
                0,
                Box::new(Observed {
                    inner,
                    io: io.clone(),
                }),
                owner.sibling("timers"),
                owner.sibling("output"),
            )
            .unwrap();
            for start in (0..COUNT).step_by(32) {
                let end = (start + 32).min(COUNT);
                let timestamps = (start..end)
                    .map(|row| {
                        if processing {
                            0
                        } else {
                            1000 + (row % 3) as i64
                        }
                    })
                    .collect::<Vec<_>>();
                let values = (start..end)
                    .map(|row| ((row * 17) % 23) as i32)
                    .collect::<Vec<_>>();
                let payloads = (start..end)
                    .map(|row| format!("{row}:{}", "é".repeat(2048)))
                    .collect::<Vec<_>>();
                processor
                    .process_arrow(batch(
                        &timestamps,
                        &values,
                        &payloads.iter().map(String::as_str).collect::<Vec<_>>(),
                        &(start..end).map(|row| (row % 4) as i8).collect::<Vec<_>>(),
                        processing.then_some(vec![40; end - start]).as_deref(),
                    ))
                    .unwrap();
            }
            let mut blocker = owner.sibling("leave four MiB for firing sixteen MiB of rows");
            blocker
                .resize(LIMIT - broker.reserved() - (4 << 20))
                .unwrap();
            io.reset();
            let mut actual = Vec::new();
            let mut calls = 0;
            loop {
                let next = if processing {
                    processor.next_processing_time_timer()
                } else {
                    processor.next_event_time_timer()
                };
                if next == i64::MAX {
                    break;
                }
                let output = if processing {
                    processor.advance_processing_time(41)
                } else {
                    processor.advance_event_time(2000)
                }
                .unwrap();
                assert!(output.num_rows() <= row_state::PAGE_ROWS);
                assert!(output.get_array_memory_size() < 1 << 20);
                let payloads = output
                    .column(2)
                    .as_any()
                    .downcast_ref::<StringArray>()
                    .unwrap();
                let kinds = output
                    .column(3)
                    .as_any()
                    .downcast_ref::<Int8Array>()
                    .unwrap();
                for row in 0..output.num_rows() {
                    let id = payloads
                        .value(row)
                        .split(':')
                        .next()
                        .unwrap()
                        .parse::<usize>()
                        .unwrap();
                    assert_eq!(kinds.value(row), (id % 4) as i8);
                    actual.push(id);
                }
                calls += 1;
                if calls == 1 {
                    assert!(processor.pending_drain.is_some());
                    assert!(processor
                        .snapshot_key_group(0)
                        .err()
                        .unwrap()
                        .to_string()
                        .contains("must drain"));
                    assert!(processor
                        .checkpoint(&directory.path().join("incomplete"))
                        .unwrap_err()
                        .to_string()
                        .contains("must drain"));
                    assert!(processor
                        .restore_key_group(0, &[])
                        .unwrap_err()
                        .to_string()
                        .contains("must drain"));
                    let rejected = batch(
                        &[0],
                        &[0],
                        &["interleaved"],
                        &[INSERT],
                        processing.then_some([40]).as_ref().map(|v| &v[..]),
                    );
                    assert!(processor
                        .process_arrow(rejected)
                        .unwrap_err()
                        .to_string()
                        .contains("must drain"));
                    assert!(!processor.failed);
                }
            }
            let mut expected = (0..COUNT).collect::<Vec<_>>();
            expected
                .sort_by_key(|row| (if processing { 0 } else { row % 3 }, (row * 17) % 23, *row));
            assert_eq!(actual, expected);
            assert!(
                calls > 32,
                "must use bounded payload pages, not one complete fired batch"
            );
            assert_eq!(
                io.scanned_rows.load(Ordering::Relaxed),
                COUNT,
                "do not reread emitted rows or fetch an extra probe page"
            );
            drop(blocker);
            let snapshot = processor.snapshot_key_group(0).unwrap();
            let entries = crate::state::decode_key_group_snapshot(0, &snapshot).unwrap();
            assert_eq!(entries.len(), if processing { 0 } else { 1 });
            drop(snapshot);
            drop(processor);
        }
    }
    drop(owner);
    assert_eq!(broker.reserved(), 0);
}

#[test]
fn failed_page_write_prevents_reuse_and_checkpointing_on_both_backends() {
    let broker = Arc::new(TestBroker::new(32 << 20));
    let owner = HostMemoryReservation::new(broker.clone(), "failed temporal drain test");
    let directory = tempfile::tempdir().unwrap();
    for inner in backends(&owner, directory.path()) {
        let io = Arc::new(Io::default());
        let mut processor = TemporalSortProcessor::with_state(
            &plan(false),
            1,
            0,
            0,
            Box::new(Observed {
                inner,
                io: io.clone(),
            }),
            owner.sibling("timers"),
            owner.sibling("output"),
        )
        .unwrap();
        processor
            .process_arrow(batch(
                &vec![1000; 2049],
                &vec![1; 2049],
                &vec!["payload"; 2049],
                &vec![INSERT; 2049],
                None,
            ))
            .unwrap();
        let checkpoint = processor.snapshot_key_group(0).unwrap();
        io.reset();
        io.fail_write_batch.store(2, Ordering::Relaxed);
        let first = processor.advance_event_time(1000).unwrap();
        assert_eq!(first.num_rows(), row_state::PAGE_ROWS);
        drop(first);
        assert!(processor.pending_drain.is_some());
        assert!(processor
            .advance_event_time(1000)
            .unwrap_err()
            .to_string()
            .contains("injected state write failure"));
        assert!(processor
            .advance_event_time(1000)
            .unwrap_err()
            .to_string()
            .contains("recreate"));
        assert!(processor
            .snapshot_key_group(0)
            .err()
            .unwrap()
            .to_string()
            .contains("recreate"));
        assert!(processor
            .checkpoint(directory.path())
            .unwrap_err()
            .to_string()
            .contains("recreate"));
        assert!(processor.pending_drain.is_none());
        let mut recovered =
            TemporalSortProcessor::new(&plan(false), 1, 0, 0, owner.sibling("recovered")).unwrap();
        recovered.restore_key_group(0, &checkpoint).unwrap();
        let mut rows = 0;
        while recovered.next_event_time_timer() != i64::MAX {
            rows += recovered.advance_event_time(1000).unwrap().num_rows();
        }
        assert_eq!(rows, 2049);
    }
    drop(owner);
    assert_eq!(broker.reserved(), 0);
}
