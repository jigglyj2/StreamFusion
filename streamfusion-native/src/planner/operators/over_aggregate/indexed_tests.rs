// Copyright 2026 StreamFusion Authors
// Licensed under the Apache License, Version 2.0
use super::*;
use crate::memory_pool::tests_support::TestBroker;
use crate::state::observed_tests::{Io, Observed};
use std::sync::atomic::Ordering;

#[test]
fn bounded_input_reads_and_rewrites_only_the_affected_order_bucket() {
    let owner =
        HostMemoryReservation::new(Arc::new(TestBroker::new(64 << 20)), "OVER indexed test");
    let mut p =
        OverAggregateProcessor::new(&tests::bounded_plan(true, Some(3)), 128, 0, 127, owner)
            .unwrap();
    let io = Arc::new(Io::default());
    let replacement = OrderedMemoryKeyedState::new(0, 127, p.state_memory()).unwrap();
    let inner = std::mem::replace(&mut p.state, Box::new(replacement));
    p.state = Box::new(Observed {
        inner,
        io: io.clone(),
    });
    p.process_arrow(tests::batch(
        &vec!["a"; 1000],
        &(0..1000).collect::<Vec<i64>>(),
        &vec![1; 1000],
        &vec![INSERT; 1000],
    ))
    .unwrap();
    io.reset();
    p.process_arrow(tests::batch(&["a"], &[500], &[1], &[DELETE]))
        .unwrap();
    assert_eq!(io.scanned_rows.load(Ordering::Relaxed), 1);
    assert!(io.read_bytes.load(Ordering::Relaxed) < 512);
    assert!(io.written_bytes.load(Ordering::Relaxed) < 512);
    let mut rows = 0;
    loop {
        let out = p.finish().unwrap();
        if out.num_rows() == 0 {
            break;
        }
        rows += out.num_rows();
    }
    assert_eq!(rows, 999);
}
