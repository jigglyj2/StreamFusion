// Copyright 2026 StreamFusion Authors
// Licensed under the Apache License, Version 2.0.

use super::*;
use crate::state::observed_tests::{Io, Observed};
use std::sync::atomic::Ordering;

#[test]
fn hot_session_update_seeks_past_unrelated_state_and_writes_one_delta_batch() {
    for rocks in [false, true] {
        let directory = tempfile::tempdir().unwrap();
        let broker = Arc::new(TestBroker::new(64 << 20));
        let mut target = processor(broker.clone(), rocks.then_some(directory.path()));
        let replacement = Box::new(
            crate::state::OrderedMemoryKeyedState::new(
                0,
                127,
                HostMemoryReservation::new(broker.clone(), "state swap"),
            )
            .unwrap(),
        );
        let inner = std::mem::replace(&mut target.kernel.state, replacement);
        let io = Arc::new(Io::default());
        target.kernel.state = Box::new(Observed {
            inner,
            io: io.clone(),
        });
        target
            .process(&batch((0..10_000).map(|i| i * 30_000).collect()))
            .unwrap();
        io.reset();
        // Sixty-four events touch one of ten thousand disjoint sessions for the same key.
        target.process(&batch(vec![150_000_001; 64])).unwrap();
        assert_eq!(io.range_reads.load(Ordering::Relaxed), 1);
        assert_eq!(io.read_batches.load(Ordering::Relaxed), 0);
        assert!(io.scanned_rows.load(Ordering::Relaxed) <= sortable_state::PAGE_ROWS);
        assert_eq!(io.write_batches.load(Ordering::Relaxed), 1);
        assert!(io.written_bytes.load(Ordering::Relaxed) < 256);
        assert_eq!(
            target.kernel.timers.timer_count(TimerDomain::EventTime),
            10_000
        );
        io.reset();
        let mut competitor = HostMemoryReservation::new(broker.clone(), "competing operator");
        competitor
            .resize((64 << 20) - broker.reserved() - 32 * 1024)
            .unwrap();
        assert!(matches!(
            target.process(&batch(vec![150_000_002])),
            Err(DataFusionError::ResourcesExhausted(_))
        ));
        assert_eq!(io.range_reads.load(Ordering::Relaxed), 0);
        assert_eq!(io.write_batches.load(Ordering::Relaxed), 0);
        drop(competitor);
        assert!(target
            .process(&batch(vec![150_000_002]))
            .unwrap_err()
            .to_string()
            .contains("restore a new context"));
        drop(target);
        assert_eq!(broker.reserved(), 0);
    }
}
