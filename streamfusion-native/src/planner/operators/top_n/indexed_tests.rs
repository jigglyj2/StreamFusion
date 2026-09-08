// Copyright 2026 StreamFusion Authors
// Licensed under the Apache License, Version 2.0
use super::*;
use crate::memory_pool::tests_support::TestBroker;
use crate::state::observed_tests::{Io, Observed};
use std::sync::atomic::Ordering as AtomicOrdering;

#[test]
fn a_discarded_candidate_does_not_rewrite_retained_payloads() {
    let owner = HostMemoryReservation::new(Arc::new(TestBroker::new(64 << 20)), "top-n index test");
    let io = Arc::new(Io::default());
    let state = Observed {
        inner: Box::new(OrderedMemoryKeyedState::new(0, 127, owner.sibling("state")).unwrap()),
        io: io.clone(),
    };
    let mut p =
        TopNProcessor::with_state_with_range(&tests::plan(), 128, 0, 127, Box::new(state), owner)
            .unwrap();
    let wide = "z".repeat(16_000);
    p.process_arrow(tests::batch(vec![1, 1], vec![&wide, "y"]), 0)
        .unwrap();
    io.reset();
    p.process_arrow(tests::batch(vec![1], vec!["a"]), 1)
        .unwrap();
    assert_eq!(io.scanned_rows.load(AtomicOrdering::Relaxed), 2);
    // Only metadata (sequence/last-access time) changes, not the 16 KiB payload.
    assert!(io.written_bytes.load(AtomicOrdering::Relaxed) < 256);
}

#[test]
fn old_top_n_state_migrates_with_the_next_batch() {
    let owner =
        HostMemoryReservation::new(Arc::new(TestBroker::new(64 << 20)), "top-n legacy test");
    let mut p = TopNProcessor::new(&tests::plan(), 128, 0, 127, owner).unwrap();
    p.sort_keys = None;
    p.process_arrow(tests::batch(vec![1, 1], vec!["z", "a"]), 0)
        .unwrap();
    let snapshots = (0..128)
        .map(|g| p.snapshot_key_group(g).unwrap())
        .collect::<Vec<_>>();
    let mut restored = TopNProcessor::new(&tests::plan(), 128, 0, 127, p.state_memory()).unwrap();
    for (g, s) in snapshots.iter().enumerate() {
        restored.restore_key_group(g as u32, s).unwrap();
    }
    let out = restored
        .process_arrow(tests::batch(vec![1], vec!["y"]), 1)
        .unwrap();
    assert_eq!(out.num_rows(), 2);
    let mut indexed = 0;
    for g in 0..128 {
        restored
            .state
            .visit_range(g, &[0xf0], Some(&[0xf1]), 256, 1 << 20, &mut |page| {
                indexed += page.len();
                Ok(true)
            })
            .unwrap();
    }
    assert_eq!(indexed, 2);
}
