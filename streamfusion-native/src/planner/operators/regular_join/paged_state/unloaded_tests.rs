// Copyright 2026 StreamFusion Authors
// Licensed under the Apache License, Version 2.0.

use super::*;
use crate::memory_pool::tests_support::TestBroker;

fn stored(id: u64) -> StoredRow {
    StoredRow {
        id,
        row: Arc::from(id.to_le_bytes()),
        associations: 0,
    }
}

#[test]
fn two_sided_unloaded_directories_flush_only_dirty_payloads_and_match_resident_snapshots() {
    for clear in [false, true] {
        for bounded in [false, true] {
            let broker = Arc::new(TestBroker::new(16 << 20));
            let owner = HostMemoryReservation::new(broker.clone(), "unloaded directory parity");
            let key = StateKey {
                key_group: 0,
                key: b"hot-key".to_vec(),
            };
            let original = JoinState {
                next_row_id: [193, 130],
                left: [0, 63, 64, 65, 192].into_iter().map(stored).collect(),
                right: [1, 64, 129].into_iter().map(stored).collect(),
                left_matchable: Some(true),
                right_matchable: Some(true),
            };
            let mut expected = original.clone();
            let ids = decode_manifest(&encode_rows_manifest(&original))
                .unwrap()
                .pages;
            let mut unloaded = UnloadedRows::new(ids);
            for side in 0..2 {
                let removed = if clear {
                    unloaded.ids[side].iter().collect::<Vec<_>>()
                } else if side == 0 {
                    vec![0, 63, 64]
                } else {
                    vec![64]
                };
                for id in &removed {
                    assert!(unloaded.ids[side].remove(*id));
                    assert!(!unloaded.ids[side].remove(*id));
                }
                assert_eq!(
                    unloaded.ids[side]
                        .removed_from(&unloaded.original_ids[side])
                        .collect::<Vec<_>>(),
                    removed
                );
                if side == 0 {
                    expected.left.retain(|row| !removed.contains(&row.id));
                } else {
                    expected.right.retain(|row| !removed.contains(&row.id));
                }
            }
            let metadata = JoinState {
                left: Vec::new(),
                right: Vec::new(),
                ..original.clone()
            };
            let mut staged = StagedState {
                key: key.clone(),
                value: metadata.clone(),
                original: metadata,
                original_layout: Layout::Rows,
                unloaded: Some(unloaded),
                touched: true,
            };
            if !clear {
                staged.value.left.push(stored(193));
                staged.value.next_row_id[0] += 1;
                expected.left.push(stored(193));
                expected.next_row_id[0] += 1;
            }
            let mut actual = MemoryKeyedState::new(0, 0, owner.sibling("spilled state")).unwrap();
            let mut reference =
                MemoryKeyedState::new(0, 0, owner.sibling("resident state")).unwrap();
            let initial = StagedState {
                key: key.clone(),
                value: original.clone(),
                original: JoinState::default(),
                original_layout: Layout::Compact,
                unloaded: None,
                touched: true,
            };
            actual.write_batch(mutations(&initial).unwrap()).unwrap();
            reference.write_batch(mutations(&initial).unwrap()).unwrap();
            let changes = mutations(&staged).unwrap();
            assert_eq!(changes.len(), if clear { 9 } else { 6 });
            assert_eq!(
                changes
                    .iter()
                    .filter(|change| change.value.is_none())
                    .count(),
                if clear { 9 } else { 4 }
            );
            if bounded {
                let mut memory = owner.sibling("bounded dirty flush");
                memory.resize(64 << 10).unwrap();
                let mut writes = 0;
                flush(&mut actual, vec![staged], &mut memory, &mut writes).unwrap();
                assert!(writes > 0);
            } else {
                actual.write_batch(changes).unwrap();
            }
            let expected = StagedState {
                key,
                value: expected,
                original,
                original_layout: Layout::Rows,
                unloaded: None,
                touched: true,
            };
            reference
                .write_batch(mutations(&expected).unwrap())
                .unwrap();
            assert_eq!(
                actual.snapshot_key_group(0, &owner).unwrap().as_ref(),
                reference.snapshot_key_group(0, &owner).unwrap().as_ref()
            );
            drop(actual);
            drop(reference);
            assert_eq!(broker.reserved(), 0);
        }
    }
}
