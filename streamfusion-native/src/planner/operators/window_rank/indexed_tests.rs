// Copyright 2026 StreamFusion Authors
// Licensed under the Apache License, Version 2.0
use super::*;
use crate::memory_pool::tests_support::TestBroker;
use crate::state::observed_tests::{Io, Observed};
use std::sync::atomic::Ordering as AtomicOrdering;

#[test]
fn inserting_into_a_large_window_reads_only_metadata_and_retraction_reads_one_identity() {
    let owner =
        HostMemoryReservation::new(Arc::new(TestBroker::new(64 << 20)), "window indexed test");
    let io = Arc::new(Io::default());
    let state = Observed {
        inner: Box::new(OrderedMemoryKeyedState::new(0, 127, owner.sibling("state")).unwrap()),
        io: io.clone(),
    };
    let mut p = WindowRankProcessor::with_state(
        &tests::plan(),
        128,
        0,
        127,
        Box::new(state),
        owner.sibling("timer"),
        owner,
    )
    .unwrap();
    p.process_arrow(tests::batch(
        &vec![1; 1000],
        &vec![200; 1000],
        &vec![b"same".as_slice(); 1000],
        &vec![INSERT; 1000],
    ))
    .unwrap();
    io.reset();
    p.process_arrow(tests::batch(&[1], &[200], &[b"next"], &[INSERT]))
        .unwrap();
    assert_eq!(io.scanned_rows.load(AtomicOrdering::Relaxed), 0);
    assert_eq!(io.read_bytes.load(AtomicOrdering::Relaxed), 22);
    assert!(io.written_bytes.load(AtomicOrdering::Relaxed) < 1024);
    io.reset();
    p.process_arrow(tests::batch(&[1], &[200], &[b"same"], &[DELETE]))
        .unwrap();
    assert_eq!(io.scanned_rows.load(AtomicOrdering::Relaxed), 1);
    assert!(io.read_bytes.load(AtomicOrdering::Relaxed) < 1024);
    assert_eq!(p.advance_event_time(199).unwrap().num_rows(), 2);
}

#[test]
fn legacy_window_candidates_migrate_on_input_and_can_fire_without_migration() {
    for touch in [false, true] {
        let owner =
            HostMemoryReservation::new(Arc::new(TestBroker::new(64 << 20)), "legacy window test");
        let mut p = WindowRankProcessor::new(&tests::plan(), 128, 0, 127, owner).unwrap();
        // Produce a canonical old-format window value and its existing Flink timer.
        p.sort_keys = None;
        p.process_arrow(tests::batch(
            &[1, 1],
            &[200, 200],
            &[b"z", b"a"],
            &[INSERT, INSERT],
        ))
        .unwrap();
        let snapshots = (0..128)
            .map(|g| p.snapshot_key_group(g).unwrap())
            .collect::<Vec<_>>();
        let mut restored =
            WindowRankProcessor::new(&tests::plan(), 128, 0, 127, p.state_memory()).unwrap();
        for (g, s) in snapshots.iter().enumerate() {
            restored.restore_key_group(g as u32, s).unwrap();
        }
        if touch {
            restored
                .process_arrow(tests::batch(&[1], &[200], &[b"z"], &[DELETE]))
                .unwrap();
        }
        let output = restored.advance_event_time(199).unwrap();
        assert_eq!(output.num_rows(), if touch { 1 } else { 2 });
        assert_eq!(
            output
                .column(2)
                .as_any()
                .downcast_ref::<BinaryArray>()
                .unwrap()
                .value(0),
            b"a"
        );
    }
}
