// Copyright 2026 StreamFusion Authors. Licensed under the Apache License, Version 2.0.

use super::*;
use crate::memory_pool::tests_support::TestBroker;
use crate::state::{MemoryKeyedState, TimerKey};
use std::sync::Arc;

const TIMER_KEY: &[u8] = b"timers-v1";

fn plugin() -> std::path::PathBuf {
    std::env::var("STREAMFUSION_TEST_ROCKSDB_PLUGIN")
        .expect("real RocksDB component is required for checkpoint tests")
        .into()
}

#[test]
fn paged_physical_restore_preserves_both_timer_domains_and_rescaled_groups() {
    let directory = tempfile::tempdir().unwrap();
    let plugin = plugin();
    let seed_broker = Arc::new(TestBroker::new(32 << 20));
    let seed_owner = HostMemoryReservation::new(seed_broker, "seed timers");
    let mut expected = NativeTimerService::new(2, 4, seed_owner).unwrap();
    let mut source =
        RocksPluginKeyedState::open(&plugin, &directory.path().join("source"), 2, 4, 1 << 20)
            .unwrap();
    for group in 2..=3 {
        for row in 0u8..64 {
            expected
                .register(
                    group,
                    if row % 2 == 0 {
                        TimerDomain::EventTime
                    } else {
                        TimerDomain::ProcessingTime
                    },
                    TimerKey {
                        timestamp: i64::from(row) - 32,
                        key: vec![row; 7],
                        namespace: vec![row % 3],
                    },
                )
                .unwrap();
        }
        source
            .write_batch(vec![StateMutation {
                key: StateKey {
                    key_group: group,
                    key: TIMER_KEY.to_vec(),
                },
                value: Some(expected.snapshot_key_group(group).unwrap()),
            }])
            .unwrap();
    }
    for start in (0u32..8192).step_by(256) {
        source
            .write_batch(
                (start..start + 256)
                    .map(|row| StateMutation {
                        key: StateKey {
                            key_group: 2,
                            key: row.to_be_bytes().to_vec(),
                        },
                        value: Some(vec![row as u8; 1024]),
                    })
                    .collect(),
            )
            .unwrap();
    }
    let checkpoint = directory.path().join("checkpoint");
    source.checkpoint(&checkpoint).unwrap();
    drop(source);

    for rocks in [false, true] {
        let broker = Arc::new(TestBroker::new(if rocks { 4 << 20 } else { 32 << 20 }));
        let owner = HostMemoryReservation::new(broker.clone(), "timer restore transfer");
        let mut caches = owner.sibling("source and target cache allowances");
        caches
            .resize(if rocks { 2 << 20 } else { 1 << 20 })
            .unwrap();
        let reader =
            RocksPluginKeyedState::open_checkpoint(&plugin, &checkpoint, 2, 4, 1 << 20).unwrap();
        let mut target: Box<dyn KeyedState> = if rocks {
            Box::new(
                RocksPluginKeyedState::open(
                    &plugin,
                    &directory.path().join("target"),
                    2,
                    4,
                    1 << 20,
                )
                .unwrap(),
            )
        } else {
            Box::new(MemoryKeyedState::new(2, 4, owner.sibling("retained memory state")).unwrap())
        };
        let mut timers = NativeTimerService::new(2, 4, owner.sibling("restored timers")).unwrap();
        if rocks {
            assert!(reader
                .snapshot_key_group(2, &owner)
                .unwrap_err()
                .to_string()
                .contains("Flink denied"));
        }
        let mut reads = 0;
        // Restore key groups in rescaling order, including a group without a timer record.
        for group in [3, 4, 2] {
            restore_timer_checkpoint(
                target.as_mut(),
                &mut timers,
                group,
                &reader,
                TIMER_KEY,
                &mut reads,
                &owner,
            )
            .unwrap();
            assert_eq!(
                timers.snapshot_key_group(group).unwrap(),
                expected.snapshot_key_group(group).unwrap()
            );
        }
        assert_eq!(reads, 3);
        for start in (0u32..8192).step_by(128) {
            let keys = (start..start + 128)
                .map(u32::to_be_bytes)
                .collect::<Vec<_>>();
            let refs = keys
                .iter()
                .map(|key| StateKeyRef { key_group: 2, key })
                .collect::<Vec<_>>();
            let values = target.get_batch(&refs, &owner).unwrap();
            for (offset, value) in values.iter().enumerate() {
                assert_eq!(
                    value.as_ref().unwrap().as_ref(),
                    vec![(start + offset as u32) as u8; 1024]
                );
            }
        }
        let mut oracle = NativeTimerService::new(2, 4, owner.sibling("timer oracle")).unwrap();
        for group in 2..=4 {
            oracle
                .restore_key_group(group, &expected.snapshot_key_group(group).unwrap())
                .unwrap();
        }
        for domain in [TimerDomain::EventTime, TimerDomain::ProcessingTime] {
            assert_eq!(
                timers.advance(domain, 0).unwrap(),
                oracle.advance(domain, 0).unwrap()
            );
            assert_eq!(
                timers.advance(domain, i64::MAX).unwrap(),
                oracle.advance(domain, i64::MAX).unwrap()
            );
        }
        drop((target, reader, timers, oracle, caches));
        assert_eq!(broker.reserved(), 0);
    }
}

#[test]
fn malformed_timer_record_fails_physical_restore_without_retaining_decoder_memory() {
    let directory = tempfile::tempdir().unwrap();
    let mut source =
        RocksPluginKeyedState::open(&plugin(), directory.path(), 2, 2, 1 << 20).unwrap();
    source
        .write_batch(vec![StateMutation {
            key: StateKey {
                key_group: 2,
                key: TIMER_KEY.to_vec(),
            },
            value: Some(vec![1, 2, 3]),
        }])
        .unwrap();
    let broker = Arc::new(TestBroker::new(1 << 20));
    let owner = HostMemoryReservation::new(broker.clone(), "failed timer restore");
    let mut target = MemoryKeyedState::new(2, 2, owner.sibling("target")).unwrap();
    let mut timers = NativeTimerService::new(2, 2, owner.sibling("timers")).unwrap();
    let mut reads = 0;
    assert!(restore_timer_checkpoint(
        &mut target,
        &mut timers,
        2,
        &source,
        TIMER_KEY,
        &mut reads,
        &owner
    )
    .is_err());
    assert_eq!(timers.timer_count(TimerDomain::EventTime), 0);
    assert_eq!(timers.timer_count(TimerDomain::ProcessingTime), 0);
    drop((target, timers));
    assert_eq!(broker.reserved(), 0);
}
