// Copyright 2026 StreamFusion Authors
// Licensed under the Apache License, Version 2.0.

use super::*;
use crate::memory_pool::tests_support::TestBroker;
use crate::planner::operators::temporal_sort::row_state_tests::backends;
use crate::state::observed_tests::{Io, Observed};
use std::sync::atomic::Ordering;

#[test]
fn timer_registration_writes_constant_size_markers_and_restores_both_domains() {
    let broker = Arc::new(TestBroker::new(32 << 20));
    let owner = HostMemoryReservation::new(broker.clone(), "temporal marker tests");
    let directory = tempfile::tempdir().unwrap();
    for (mode, domain) in [TimerDomain::EventTime, TimerDomain::ProcessingTime]
        .into_iter()
        .enumerate()
    {
        for (backend, inner) in backends(&owner, &directory.path().join(format!("source-{mode}")))
            .into_iter()
            .enumerate()
        {
            let io = Arc::new(Io::default());
            let mut state = Observed {
                inner,
                io: io.clone(),
            };
            let mut timers = NativeTimerService::new(0, 1, owner.sibling("timer index")).unwrap();
            for batch in 0..64 {
                io.reset();
                let mut mutations = Vec::new();
                let (inserted, _workspace) = register(
                    &mut timers,
                    0,
                    domain,
                    (0..16).map(|index| batch * 16 + index - 512).collect(),
                    &mut mutations,
                    &owner,
                )
                .unwrap();
                assert_eq!(inserted, 16);
                state.write_batch(mutations).unwrap();
                assert_eq!(io.written_bytes.load(Ordering::Relaxed), 16 * 11);
                assert_eq!(io.write_batches.load(Ordering::Relaxed), 1);
                assert_eq!(io.read_batches.load(Ordering::Relaxed), 0);
            }
            let mut duplicate = Vec::new();
            let (inserted, _workspace) = register(
                &mut timers,
                0,
                domain,
                vec![0, 0, 100],
                &mut duplicate,
                &owner,
            )
            .unwrap();
            assert_eq!(inserted, 0);
            state.write_batch(duplicate).unwrap();
            let snapshot = state.snapshot_key_group(0, &owner).unwrap();
            let expected = timers.snapshot_key_group(0).unwrap();
            for mut destination in backends(
                &owner,
                &directory.path().join(format!("target-{mode}-{backend}")),
            ) {
                destination.restore_key_group(0, &snapshot, &owner).unwrap();
                let mut restored =
                    NativeTimerService::new(0, 1, owner.sibling("restored timer index")).unwrap();
                assert_eq!(
                    restore(destination.as_mut(), &mut restored, 0, domain, &owner).unwrap(),
                    i64::MIN
                );
                assert_eq!(restored.next_timestamp(domain), Some(-512));
                assert_eq!(restored.snapshot_key_group(0).unwrap(), expected);
            }
        }
    }
    assert_eq!(broker.reserved(), 0);
}

#[test]
fn legacy_timer_snapshots_migrate_in_pages_without_retaining_the_old_value() {
    let broker = Arc::new(TestBroker::new(32 << 20));
    let owner = HostMemoryReservation::new(broker.clone(), "legacy timer tests");
    let directory = tempfile::tempdir().unwrap();
    for (mode, domain) in [TimerDomain::EventTime, TimerDomain::ProcessingTime]
        .into_iter()
        .enumerate()
    {
        let mut original = NativeTimerService::new(0, 1, owner.sibling("old timers")).unwrap();
        original
            .register_batch(
                (-1024..3073)
                    .map(|timestamp| (0, domain, identity(0, domain, timestamp)))
                    .collect(),
            )
            .unwrap();
        let legacy = original.snapshot_key_group(0).unwrap();
        for inner in backends(&owner, &directory.path().join(format!("legacy-{mode}"))) {
            let io = Arc::new(Io::default());
            let mut state = Observed {
                inner,
                io: io.clone(),
            };
            state
                .write_batch(vec![
                    StateMutation {
                        key: StateKey {
                            key_group: 0,
                            key: TIMER_STATE_KEY.to_vec(),
                        },
                        value: Some(legacy.clone()),
                    },
                    StateMutation {
                        key: StateKey {
                            key_group: 0,
                            key: LAST_TRIGGER_STATE_KEY.to_vec(),
                        },
                        value: Some((-42i64).to_le_bytes().to_vec()),
                    },
                ])
                .unwrap();
            let mut restored =
                NativeTimerService::new(0, 1, owner.sibling("migrated timers")).unwrap();
            io.reset();
            assert_eq!(
                restore(&mut state, &mut restored, 0, domain, &owner).unwrap(),
                -42
            );
            assert_eq!(restored.snapshot_key_group(0).unwrap(), legacy);
            assert_eq!(io.write_batches.load(Ordering::Relaxed), 5);
            assert_eq!(
                io.written_bytes.load(Ordering::Relaxed),
                4097 * 11 + TIMER_STATE_KEY.len()
            );
            let values = state
                .get_batch(
                    &[StateKeyRef {
                        key_group: 0,
                        key: TIMER_STATE_KEY,
                    }],
                    &owner,
                )
                .unwrap();
            assert!(values[0].is_none());
            drop(values);
            let mut second =
                NativeTimerService::new(0, 1, owner.sibling("second restore")).unwrap();
            assert_eq!(
                restore(&mut state, &mut second, 0, domain, &owner).unwrap(),
                -42
            );
            assert_eq!(second.snapshot_key_group(0).unwrap(), legacy);
        }
    }
    assert_eq!(broker.reserved(), 0);
}

#[test]
fn malformed_markers_mixed_versions_and_denied_restore_fail_without_leaking() {
    let broker = Arc::new(TestBroker::new(8 << 20));
    let owner = HostMemoryReservation::new(broker.clone(), "invalid timers");
    let mut empty = NativeTimerService::new(0, 0, owner.sibling("empty")).unwrap();
    let legacy = empty.snapshot_key_group(0).unwrap();
    let mut invalid_count = legacy.clone();
    invalid_count[9..13].copy_from_slice(&u32::MAX.to_le_bytes());
    assert!(empty
        .restore_key_group(0, &invalid_count)
        .unwrap_err()
        .to_string()
        .contains("snapshot count"));
    let event_key = marker_key(0, TimerDomain::EventTime, 1);
    let cases = [
        vec![StateMutation {
            key: marker_key(0, TimerDomain::ProcessingTime, 1),
            value: Some(Vec::new()),
        }],
        vec![StateMutation {
            key: StateKey {
                key_group: 0,
                key: vec![PREFIX, 99],
            },
            value: Some(Vec::new()),
        }],
        vec![StateMutation {
            key: event_key.clone(),
            value: Some(vec![1]),
        }],
        vec![
            StateMutation {
                key: event_key,
                value: Some(Vec::new()),
            },
            StateMutation {
                key: StateKey {
                    key_group: 0,
                    key: TIMER_STATE_KEY.to_vec(),
                },
                value: Some(legacy),
            },
        ],
    ];
    for mutations in cases {
        let mut state = OrderedMemoryKeyedState::new(0, 0, owner.sibling("bad state")).unwrap();
        state.write_batch(mutations).unwrap();
        let mut timers = NativeTimerService::new(0, 0, owner.sibling("bad timer index")).unwrap();
        let baseline = broker.reserved();
        assert!(
            restore(&mut state, &mut timers, 0, TimerDomain::EventTime, &owner)
                .unwrap_err()
                .to_string()
                .contains("invalid temporal sort timer state")
        );
        assert_eq!(broker.reserved(), baseline);
    }
    let mut state = OrderedMemoryKeyedState::new(0, 0, owner.sibling("large index")).unwrap();
    state
        .write_batch(
            (0..1024)
                .map(|timestamp| StateMutation {
                    key: marker_key(0, TimerDomain::EventTime, timestamp),
                    value: Some(Vec::new()),
                })
                .collect(),
        )
        .unwrap();
    let small = Arc::new(TestBroker::new(64 << 10));
    let budget = HostMemoryReservation::new(small.clone(), "denied timer restore");
    let mut timers = NativeTimerService::new(0, 0, budget.sibling("limited index")).unwrap();
    let baseline = small.reserved();
    assert!(
        restore(&mut state, &mut timers, 0, TimerDomain::EventTime, &budget)
            .unwrap_err()
            .to_string()
            .contains("Flink denied")
    );
    assert_eq!(small.reserved(), baseline);
    drop(timers);
    assert_eq!(small.reserved(), 0);
}

#[test]
fn physical_restore_reads_large_pending_state_in_pages_and_rebuilds_timer_markers() {
    let directory = tempfile::tempdir().unwrap();
    let plugin = std::env::var("STREAMFUSION_TEST_ROCKSDB_PLUGIN").unwrap();
    let plugin = std::path::Path::new(&plugin);
    let mut source =
        RocksPluginKeyedState::open(plugin, &directory.path().join("source"), 0, 0, 1 << 20)
            .unwrap();
    let source_owner =
        HostMemoryReservation::new(Arc::new(TestBroker::new(32 << 20)), "checkpoint seed");
    for start in (0..8192).step_by(128) {
        let rows = (start..start + 128)
            .map(|index| BufferedRow {
                kind: INSERT,
                sort_key: (index as u32).to_be_bytes().to_vec(),
                row: vec![index as u8; 1024],
            })
            .collect();
        let pending = row_state::append(
            &source,
            vec![(rows_state_key(0, 1000), rows)],
            &source_owner,
        )
        .unwrap();
        source.write_batch(pending.mutations).unwrap();
    }
    source
        .write_batch(vec![StateMutation {
            key: marker_key(0, TimerDomain::EventTime, 1000),
            value: Some(Vec::new()),
        }])
        .unwrap();
    let checkpoint = directory.path().join("checkpoint");
    source.checkpoint(&checkpoint).unwrap();
    drop(source);
    let broker = Arc::new(TestBroker::new(4 << 20));
    let owner = HostMemoryReservation::new(broker.clone(), "bounded temporal restore");
    let mut caches = owner.sibling("source and target caches");
    caches.resize(2 << 20).unwrap();
    let source = RocksPluginKeyedState::open(plugin, &checkpoint, 0, 0, 1 << 20).unwrap();
    let target =
        RocksPluginKeyedState::open(plugin, &directory.path().join("target"), 0, 0, 1 << 20)
            .unwrap();
    let mut processor = TemporalSortProcessor::with_state(
        &crate::planner::operators::temporal_sort::tests::plan(false),
        1,
        0,
        0,
        Box::new(target),
        owner.sibling("restored timers"),
        owner.sibling("restore scratch"),
    )
    .unwrap();
    assert!(source
        .snapshot_key_group(0, &owner)
        .unwrap_err()
        .to_string()
        .contains("Flink denied"));
    processor.restore_physical_key_group(0, &source).unwrap();
    assert_eq!(processor.next_event_time_timer(), 1000);
    assert_eq!(processor.statistics()[5], 1);
    let mut entries = 0;
    processor
        .state
        .visit_prefix_admitted(0, &[9], 128, 256 << 10, &owner, &mut |page| {
            entries += page.len();
            for (_, value) in page {
                assert_eq!(value.len(), 1030);
            }
            Ok(())
        })
        .unwrap();
    assert_eq!(entries, 8192);
    drop(processor);
    drop(source);
    drop(caches);
    assert_eq!(broker.reserved(), 0);
}
