// Copyright 2026 StreamFusion Authors
// Licensed under the Apache License, Version 2.0.

use super::*;
use crate::memory_pool::tests_support::TestBroker;
use crate::state::observed_tests::{Io, Observed};
use std::sync::atomic::Ordering;

pub(super) fn backends(
    owner: &HostMemoryReservation,
    directory: &std::path::Path,
) -> Vec<Box<dyn KeyedState>> {
    let plugin = std::env::var("STREAMFUSION_TEST_ROCKSDB_PLUGIN").unwrap();
    vec![
        Box::new(
            OrderedMemoryKeyedState::new(0, 1, owner.sibling("ordered temporal rows")).unwrap(),
        ),
        Box::new(
            RocksPluginKeyedState::open_for_owner(
                std::path::Path::new(&plugin),
                directory,
                0,
                1,
                1 << 20,
                owner,
            )
            .unwrap(),
        ),
    ]
}

fn row(index: usize) -> BufferedRow {
    BufferedRow {
        kind: (index % 4) as i8,
        // Fixed-width stand-in for an Arrow ordering key; many ties exercise arrival stability.
        sort_key: ((index % 7) as u32).to_be_bytes().to_vec(),
        row: vec![(index % 251) as u8; 512],
    }
}

#[test]
fn appending_temporal_rows_reads_only_metadata_and_writes_only_new_entries() {
    let broker = Arc::new(TestBroker::new(64 << 20));
    let owner = HostMemoryReservation::new(broker.clone(), "temporal row state");
    let small = Arc::new(TestBroker::new(128 << 10));
    let scratch = HostMemoryReservation::new(small.clone(), "small append workspace");
    let directory = tempfile::tempdir().unwrap();
    for inner in backends(&owner, directory.path()) {
        let io = Arc::new(Io::default());
        let mut state = Observed {
            inner,
            io: io.clone(),
        };
        let key = rows_state_key(0, 1000);
        let mut first_write = 0;
        for batch in 0..64 {
            io.reset();
            let pending = row_state::append(
                &state,
                vec![(
                    key.clone(),
                    (0..16).map(|index| row(batch * 16 + index)).collect(),
                )],
                &scratch,
            )
            .unwrap();
            assert_eq!(pending.was_empty, vec![batch == 0]);
            state.write_batch(pending.mutations).unwrap();
            assert_eq!(io.read_batches.load(Ordering::Relaxed), 1);
            assert_eq!(io.write_batches.load(Ordering::Relaxed), 1);
            assert_eq!(io.range_reads.load(Ordering::Relaxed), 0);
            assert_eq!(
                io.read_bytes.load(Ordering::Relaxed),
                if batch == 0 { 0 } else { 13 }
            );
            let written = io.written_bytes.load(Ordering::Relaxed);
            if batch == 0 {
                first_write = written;
            }
            assert_eq!(
                written, first_write,
                "append must not rewrite previous rows"
            );
        }
        assert_eq!(small.reserved(), 0);
        let loaded = row_state::load(&state, &[key], &owner).unwrap();
        let mut expected = (0..1024).map(row).collect::<Vec<_>>();
        expected.sort_by(|left, right| left.sort_key.cmp(&right.sort_key));
        assert_eq!(loaded.groups, vec![expected]);
        assert_eq!(io.range_reads.load(Ordering::Relaxed), 1);
        assert_eq!(io.scanned_rows.load(Ordering::Relaxed), 1024);
        state.write_batch(loaded.mutations).unwrap();
        let snapshot = state.snapshot_key_group(0, &owner).unwrap();
        assert!(crate::state::decode_key_group_snapshot(0, &snapshot)
            .unwrap()
            .is_empty());
    }
    assert_eq!(broker.reserved(), 0);
}

#[test]
fn legacy_lists_migrate_without_changing_ties_or_cross_backend_restore() {
    let broker = Arc::new(TestBroker::new(64 << 20));
    let owner = HostMemoryReservation::new(broker.clone(), "legacy temporal rows");
    let directory = tempfile::tempdir().unwrap();
    for (case, mut source) in backends(&owner, &directory.path().join("source"))
        .into_iter()
        .enumerate()
    {
        let root = rows_state_key(0, 1000);
        let rows = (0..37).map(row).collect::<Vec<_>>();
        source
            .write_batch(vec![StateMutation {
                key: root.clone(),
                value: Some(legacy_state::encode_rows(&rows).unwrap()),
            }])
            .unwrap();
        let legacy = source.snapshot_key_group(0, &owner).unwrap();
        for (target_index, mut target) in
            backends(&owner, &directory.path().join(format!("target-{case}")))
                .into_iter()
                .enumerate()
        {
            target.restore_key_group(0, &legacy, &owner).unwrap();
            let loaded = row_state::load(target.as_ref(), &[root.clone()], &owner).unwrap();
            assert_eq!(loaded.groups, vec![rows.clone()]);
            drop(loaded);
            assert!(
                row_state::append(target.as_ref(), vec![(root.clone(), vec![row(0)])], &owner)
                    .err()
                    .unwrap()
                    .to_string()
                    .contains("must migrate during restore")
            );
            migration::migrate_legacy_groups(target.as_mut(), 0, &owner, &mut 0).unwrap();
            let pending = row_state::append(
                target.as_ref(),
                vec![(root.clone(), (37..71).map(row).collect())],
                &owner,
            )
            .unwrap();
            assert_eq!(pending.was_empty, vec![false]);
            target.write_batch(pending.mutations).unwrap();
            // A neighboring timestamp and a different key group must stay outside the prefix.
            let other = row_state::append(
                target.as_ref(),
                vec![
                    (rows_state_key(0, 1001), vec![row(99)]),
                    (rows_state_key(1, 1000), vec![row(100)]),
                ],
                &owner,
            )
            .unwrap();
            target.write_batch(other.mutations).unwrap();
            let current = target.snapshot_key_group(0, &owner).unwrap();
            for mut restored in backends(
                &owner,
                &directory
                    .path()
                    .join(format!("restored-{case}-{target_index}")),
            ) {
                restored.restore_key_group(0, &current, &owner).unwrap();
                let loaded = row_state::load(restored.as_ref(), &[root.clone()], &owner).unwrap();
                let mut expected = (0..71).map(row).collect::<Vec<_>>();
                expected.sort_by(|left, right| left.sort_key.cmp(&right.sort_key));
                assert_eq!(loaded.groups, vec![expected]);
                restored.write_batch(loaded.mutations).unwrap();
                let other =
                    row_state::load(restored.as_ref(), &[rows_state_key(0, 1001)], &owner).unwrap();
                assert_eq!(other.groups, vec![vec![row(99)]]);
            }
        }
    }
    assert_eq!(broker.reserved(), 0);
}

#[test]
fn failed_row_scans_and_corrupt_metadata_release_workspace_without_mutating_state() {
    let broker = Arc::new(TestBroker::new(32 << 20));
    let owner = HostMemoryReservation::new(broker.clone(), "temporal failure");
    let directory = tempfile::tempdir().unwrap();
    for mut state in backends(&owner, directory.path()) {
        let root = rows_state_key(0, 42);
        let pending = row_state::append(
            state.as_ref(),
            vec![(root.clone(), (0..1024).map(row).collect())],
            &owner,
        )
        .unwrap();
        state.write_batch(pending.mutations).unwrap();
        let expected = state.snapshot_key_group(0, &owner).unwrap();
        let small = Arc::new(TestBroker::new(64 << 10));
        let scratch = HostMemoryReservation::new(small.clone(), "denied fired rows");
        assert!(row_state::load(state.as_ref(), &[root.clone()], &scratch)
            .err()
            .unwrap()
            .to_string()
            .contains("Flink denied"));
        assert_eq!(small.reserved(), 0);
        assert_eq!(&*state.snapshot_key_group(0, &owner).unwrap(), &*expected);
        let mut overflowing = b"SFTS\x03".to_vec();
        overflowing.extend_from_slice(&u64::MAX.to_be_bytes());
        state
            .write_batch(vec![StateMutation {
                key: root.clone(),
                value: Some(overflowing),
            }])
            .unwrap();
        let baseline = broker.reserved();
        assert!(
            row_state::append(state.as_ref(), vec![(root.clone(), vec![row(0)])], &owner)
                .err()
                .unwrap()
                .to_string()
                .contains("arrival overflow")
        );
        assert_eq!(broker.reserved(), baseline);
        assert!(row_state::load(state.as_ref(), &[root], &owner)
            .err()
            .unwrap()
            .to_string()
            .contains("row count"));
        assert_eq!(broker.reserved(), baseline);
    }
    assert_eq!(broker.reserved(), 0);
}
