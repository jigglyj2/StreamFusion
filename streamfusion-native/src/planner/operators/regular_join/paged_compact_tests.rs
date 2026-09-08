// Copyright 2026 StreamFusion Authors
// Licensed under the Apache License, Version 2.0.

use super::*;
use crate::memory_pool::tests_support::TestBroker;

fn value(count: usize, width: usize) -> JoinState {
    JoinState {
        left: (0..count)
            .map(|id| StoredRow {
                // Retractions can leave sparse stable IDs spanning several pages.
                id: 65 + id as u64 * 3,
                row: Arc::from(vec![id as u8; width]),
                associations: id as i32 % 3,
            })
            .collect(),
        next_row_id: [66 + count as u64 * 3, 0],
        left_matchable: Some(true),
        ..Default::default()
    }
}

#[test]
fn compact_pages_validate_lengths_versions_and_sparse_row_identities() {
    for count in [1, 3, 64] {
        let expected = value(count, 12);
        let bytes = paged_codec::encode_compact(&expected).unwrap();
        let ((manifest, original), observed) = crate::allocation_test_support::measure(|| {
            let manifest = paged_codec::decode_manifest(&bytes).unwrap();
            let original = manifest.inline.clone();
            (manifest, original)
        });
        // Backend read buffers retain separate credit. This checks the decoded vectors,
        // payload Arcs and shallow original-state copy admitted from the record bytes.
        assert!(observed.peak <= bytes.len() * 8, "{observed:?}");
        assert_eq!(manifest.next_row_id, expected.next_row_id);
        let [left, right] = manifest.inline.unwrap();
        assert_eq!(left, expected.left);
        for (a, b) in left.iter().zip(&original.unwrap()[0]) {
            assert!(Arc::ptr_eq(&a.row, &b.row));
        }
        assert_eq!(right, expected.right);
        for end in 0..bytes.len() {
            assert!(
                paged_codec::decode_manifest(&bytes[..end]).is_err(),
                "count={count} end={end}"
            );
        }
        let mut corrupt = bytes.clone();
        corrupt[4] = 9;
        assert!(paged_codec::decode_manifest(&corrupt).is_err());
        let mut trailing = bytes;
        trailing.push(0);
        assert!(paged_codec::decode_manifest(&trailing).is_err());
    }
    assert!(!paged_codec::compact_eligible(&value(65, 1)));
    assert!(!paged_codec::compact_eligible(&value(1, 8192)));
}

#[test]
fn old_paged_and_compact_keys_transition_atomically_and_restore_on_both_backends() {
    let Ok(plugin) = std::env::var("STREAMFUSION_TEST_ROCKSDB_PLUGIN") else {
        return;
    };
    for rocks in [false, true] {
        let broker = Arc::new(TestBroker::new(32 << 20));
        let owner = HostMemoryReservation::new(broker.clone(), "compact transition test");
        let directory = tempfile::tempdir().unwrap();
        let mut state: Box<dyn KeyedState> = if rocks {
            Box::new(
                RocksPluginKeyedState::open(
                    std::path::Path::new(&plugin),
                    directory.path(),
                    0,
                    0,
                    16 << 20,
                )
                .unwrap(),
            )
        } else {
            Box::new(
                crate::state::OrderedMemoryKeyedState::new(0, 0, owner.sibling("state")).unwrap(),
            )
        };
        let key = StateKey {
            key_group: 0,
            key: b"compact-key".to_vec(),
        };
        // Emulate the previous release's sparse-key manifest + separate page exactly.
        let old = value(1, 12);
        state
            .write_batch(vec![
                StateMutation {
                    key: paged_codec::manifest_key(&key),
                    value: Some(paged_codec::encode_manifest(&old)),
                },
                StateMutation {
                    key: paged_codec::page_key(&key, 0, 1),
                    value: Some(paged_codec::encode_page(&old.left).unwrap()),
                },
            ])
            .unwrap();
        let mut workspace = owner.sibling("batch");
        let (loaded, reads) =
            paged_state::load(state.as_ref(), vec![key.clone()], &mut workspace).unwrap();
        assert_eq!(reads, 2);
        assert!(!loaded[0].original_compact);
        assert_eq!(loaded[0].value, old);
        drop(loaded);
        workspace.resize(0).unwrap();
        // Both thresholds, sparse IDs, association updates, compaction and complete deletion.
        for (count, width) in [
            (1, 12),
            (64, 12),
            (65, 12),
            (3, 12),
            (1, 8192),
            (1, 12),
            (0, 0),
        ] {
            let expected = value(count, width);
            let (mut loaded, _) =
                paged_state::load(state.as_ref(), vec![key.clone()], &mut workspace).unwrap();
            loaded[0].value = expected.clone();
            loaded[0].touched = true;
            state
                .write_batch(paged_state::batch_mutations(&loaded, &mut workspace).unwrap())
                .unwrap();
            drop(loaded);
            workspace.resize(0).unwrap();
            let snapshot = state.snapshot_key_group(0, &owner).unwrap();
            let entries = decode_key_group_snapshot(0, &snapshot).unwrap();
            let decoded = paged_state::decode_entries(0, &entries).unwrap();
            if count == 0 {
                assert!(decoded.is_empty());
            } else {
                assert_eq!(decoded, vec![(key.key.clone(), expected.clone())]);
                assert_eq!(entries.len() == 1, paged_codec::compact_eligible(&expected));
            }
            let restored_directory = tempfile::tempdir().unwrap();
            let mut restored: Box<dyn KeyedState> = if rocks {
                Box::new(
                    crate::state::OrderedMemoryKeyedState::new(0, 0, owner.sibling("restored"))
                        .unwrap(),
                )
            } else {
                Box::new(
                    RocksPluginKeyedState::open(
                        std::path::Path::new(&plugin),
                        restored_directory.path(),
                        0,
                        0,
                        16 << 20,
                    )
                    .unwrap(),
                )
            };
            paged_state::restore(restored.as_mut(), 0, &snapshot, &owner).unwrap();
            assert_eq!(restored.snapshot_key_group(0, &owner).unwrap(), snapshot);
            let (loaded, reads) =
                paged_state::load(restored.as_ref(), vec![key.clone()], &mut workspace).unwrap();
            if count != 0 {
                assert_eq!(loaded[0].value, expected);
            } else {
                assert_eq!(loaded[0].value, JoinState::default());
            }
            assert_eq!(
                reads,
                if count == 0 || paged_codec::compact_eligible(&expected) {
                    1
                } else {
                    2
                }
            );
            drop(loaded);
            drop(restored);
            workspace.resize(0).unwrap();
        }
        drop(state);
        drop(workspace);
        assert_eq!(broker.reserved(), 0);
    }
}

#[test]
fn mixed_compact_and_external_pages_load_together_without_losing_input_key_order() {
    let broker = Arc::new(TestBroker::new(16 << 20));
    let owner = HostMemoryReservation::new(broker.clone(), "mixed join records");
    let mut state = MemoryKeyedState::new(0, 0, owner.sibling("state")).unwrap();
    let entries = [3, 65, 1]
        .into_iter()
        .enumerate()
        .map(|(key, count)| StagedState {
            key: StateKey {
                key_group: 0,
                key: vec![key as u8],
            },
            value: value(count, 12),
            original: JoinState::default(),
            original_compact: false,
            touched: true,
        })
        .collect::<Vec<_>>();
    let mut workspace = owner.sibling("batch");
    state
        .write_batch(paged_state::batch_mutations(&entries, &mut workspace).unwrap())
        .unwrap();
    workspace.resize(0).unwrap();
    let keys = entries
        .iter()
        .rev()
        .map(|entry| entry.key.clone())
        .collect();
    let (loaded, reads) = paged_state::load(&state, keys, &mut workspace).unwrap();
    assert_eq!(reads, 2);
    for (actual, expected) in loaded.iter().zip(entries.iter().rev()) {
        assert_eq!(actual.key, expected.key);
        assert_eq!(actual.value, expected.value);
        assert_eq!(actual.original, expected.value);
    }
    drop(loaded);
    drop(state);
    drop(workspace);
    assert_eq!(broker.reserved(), 0);
}
