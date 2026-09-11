// Copyright 2026 StreamFusion Authors
// Licensed under the Apache License, Version 2.0.

use super::*;
use crate::memory_pool::tests_support::TestBroker;
use crate::state::observed_tests::{Io, Observed};
use std::sync::atomic::Ordering;

const LIMIT: usize = 128 << 20;
const MARKER_KEY: &[u8] = b"checkpoint-test-marker";
const MARKER: &[u8] = b"version-and-plan-fingerprint";

fn fixture(
    broker: Arc<TestBroker>,
    rocks: Option<&std::path::Path>,
) -> (WindowAggregateProcessor, Arc<Io>) {
    let plan = super::super::tests::plan(proto::WindowKind::Tumble, 10000, 0, false);
    let mut kernel = super::super::tests::processor(&plan, broker);
    if let Some(path) = rocks {
        kernel.state = Box::new(
            RocksPluginKeyedState::open_for_owner(
                std::path::Path::new(&std::env::var("STREAMFUSION_TEST_ROCKSDB_PLUGIN").unwrap()),
                path,
                0,
                127,
                1 << 20,
                &kernel.scratch_reservation,
            )
            .unwrap(),
        );
    }
    let io = Arc::new(Io::default());
    kernel.state = Box::new(Observed {
        inner: kernel.state,
        io: io.clone(),
    });
    for group in 0u32..128 {
        kernel
            .timers
            .register_batch(
                (0..16)
                    .map(|row| {
                        (
                            group,
                            if row % 2 == 0 {
                                TimerDomain::EventTime
                            } else {
                                TimerDomain::ProcessingTime
                            },
                            TimerKey {
                                timestamp: row * 1000,
                                key: vec![row as u8; 4096],
                                namespace: group.to_le_bytes().to_vec(),
                            },
                        )
                    })
                    .collect(),
            )
            .unwrap();
    }
    (kernel, io)
}

fn verify(kernel: &WindowAggregateProcessor) {
    for group in 0u32..128 {
        let values = kernel
            .state
            .get_batch(
                &[
                    StateKeyRef {
                        key_group: group,
                        key: TIMER_STATE_KEY,
                    },
                    StateKeyRef {
                        key_group: group,
                        key: MARKER_KEY,
                    },
                ],
                &kernel.scratch_reservation,
            )
            .unwrap();
        assert_eq!(
            values[0].as_deref().unwrap(),
            kernel.timers.snapshot_key_group(group).unwrap()
        );
        assert_eq!(values[1].as_deref().unwrap(), MARKER);
    }
}

#[test]
fn checkpoint_timer_pages_do_not_require_all_key_group_snapshots_in_memory() {
    if std::env::var("STREAMFUSION_TEST_ROCKSDB_PLUGIN").is_err() {
        return;
    }
    let directory = tempfile::tempdir().unwrap();
    let broker = Arc::new(TestBroker::new(LIMIT));
    let (mut kernel, io) = fixture(broker.clone(), Some(directory.path()));
    let bytes: usize = (0..128)
        .map(|group| kernel.timers.snapshot_size(group).unwrap())
        .sum();
    assert!(bytes > 8 << 20);
    let mut pressure = kernel.scratch_reservation.sibling("other slot consumers");
    pressure
        .resize(LIMIT - broker.reserved() - (2 << 20))
        .unwrap();
    let before = broker.reserved();
    let ((), allocations) = crate::allocation_test_support::measure(|| {
        flush_timers(&mut kernel, 0..=127, MARKER_KEY, MARKER).unwrap();
    });
    assert!(allocations.peak < 2 << 20, "{allocations:?}");
    assert_eq!(broker.reserved(), before, "release checkpoint workspace");
    assert!(io.write_batches.load(Ordering::Relaxed) > 1);
    assert_eq!(
        io.written_bytes.load(Ordering::Relaxed),
        bytes + 128 * (TIMER_STATE_KEY.len() + MARKER_KEY.len() + MARKER.len())
    );
    assert_eq!(
        kernel.statistics()[1],
        0,
        "control writes are not input batches"
    );
    verify(&kernel);
    drop((kernel, pressure));
    assert_eq!(broker.reserved(), 0);
}

#[test]
fn failed_checkpoint_control_page_can_be_retried_before_taking_a_snapshot() {
    for rocks in [false, true] {
        if rocks && std::env::var("STREAMFUSION_TEST_ROCKSDB_PLUGIN").is_err() {
            continue;
        }
        let directory = tempfile::tempdir().unwrap();
        let broker = Arc::new(TestBroker::new(LIMIT));
        let (mut kernel, io) = fixture(broker.clone(), rocks.then_some(directory.path()));
        io.fail_write_batch.store(2, Ordering::Relaxed);
        assert!(flush_timers(&mut kernel, 0..=127, MARKER_KEY, MARKER)
            .unwrap_err()
            .to_string()
            .contains("injected state write failure"));
        assert_eq!(io.write_batches.load(Ordering::Relaxed), 2);
        // A rejected checkpoint has not changed live timers. A retry rewrites every group's
        // control record, including groups from an earlier successful page.
        kernel
            .timers
            .register(
                0,
                TimerDomain::EventTime,
                TimerKey {
                    timestamp: -1,
                    key: b"new input before checkpoint retry".to_vec(),
                    namespace: vec![],
                },
            )
            .unwrap();
        io.reset();
        flush_timers(&mut kernel, 0..=127, MARKER_KEY, MARKER).unwrap();
        verify(&kernel);
        drop(kernel);
        assert_eq!(broker.reserved(), 0);
    }
}
