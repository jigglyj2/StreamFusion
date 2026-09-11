// Copyright 2026 StreamFusion Authors
// Licensed under the Apache License, Version 2.0.

use super::*;
use paged_codec::*;

#[test]
fn bitmap_directories_validate_sparse_ids_before_allocation() {
    for ids in [vec![0, 63, 64, 127, 1025], vec![u64::MAX - 2, u64::MAX - 1]] {
        let state = JoinState {
            left: ids
                .iter()
                .map(|&id| StoredRow {
                    id,
                    row: Arc::from(&b"row"[..]),
                    associations: -1,
                })
                .collect(),
            next_row_id: [ids.last().unwrap() + 1, 0],
            left_matchable: Some(false),
            ..Default::default()
        };
        let bytes = encode_rows_manifest(&state);
        let (decoded, allocations) =
            crate::allocation_test_support::measure(|| decode_manifest(&bytes).unwrap());
        assert!(allocations.peak <= manifest_workspace(&bytes).unwrap());
        assert_eq!(decoded.pages[0].iter().collect::<Vec<_>>(), ids);
        assert_eq!(decoded.layout, Layout::Rows);
        for end in 0..bytes.len() {
            assert!(decode_manifest(&bytes[..end]).is_err());
        }
        let mut unknown = bytes.clone();
        unknown[4] = 2;
        assert!(decode_manifest(&unknown).is_err());
        let mut invalid = bytes.clone();
        invalid[7..15].copy_from_slice(&0u64.to_le_bytes());
        assert!(manifest_workspace(&invalid).is_err());
        assert!(decode_manifest(&invalid).is_err());
        let mut invalid = bytes.clone();
        invalid[39..47].fill(0); // first presence bitmap
        assert!(decode_manifest(&invalid).is_err());
        let mut trailing = bytes;
        trailing.push(0);
        assert!(decode_manifest(&trailing).is_err());
    }
}

#[test]
fn row_keys_frame_partitions_and_preserve_stable_identity_order() {
    let key = StateKey {
        key_group: 3,
        key: vec![0, 2, 0, 255],
    };
    let encoded = [0, 63, 64, 255, 256, u64::MAX - 1].map(|id| row_key(&key, 0, id));
    assert!(encoded.windows(2).all(|p| p[0].key < p[1].key));
    assert!(encoded.iter().all(|k| k.key_group == 3));
    assert_ne!(row_key(&key, 0, 0), row_key(&key, 1, 0));
    let mut extended = key.clone();
    extended.key.push(0);
    assert_ne!(row_key(&key, 0, 0), row_key(&extended, 0, 0));
}

#[test]
fn repeated_appends_do_not_rewrite_retained_payloads() {
    // Model a repeated key across 256 separate input batches with wide payloads. Count actual
    // mutation bytes, then verify removal/association updates cannot rewrite neighboring rows.
    let mut entry = StagedState {
        key: StateKey {
            key_group: 0,
            key: b"repeated".to_vec(),
        },
        value: JoinState::default(),
        original: JoinState::default(),
        original_layout: Layout::Compact,
        unloaded: None,
        touched: true,
    };
    let mut written = 0usize;
    for id in 0..256 {
        entry.value.left.push(StoredRow {
            id,
            row: Arc::from(vec![id as u8; 1024]),
            associations: 0,
        });
        entry.value.next_row_id[0] = id + 1;
        let writes = paged_state::mutations(&entry).unwrap();
        written += writes
            .iter()
            .filter_map(|m| m.value.as_ref())
            .map(Vec::len)
            .sum::<usize>();
        if id > 1 {
            assert_eq!(writes.len(), 2);
        }
        entry.original = entry.value.clone();
        entry.original_layout = if id == 0 {
            Layout::Compact
        } else {
            Layout::Rows
        };
    }
    // Bounded metadata and the one-time singleton transition stay below twice the new payload.
    assert!(written < 2 * 256 * 1024, "wrote {written} bytes");
    entry.value.left[10].associations = i32::MIN;
    let writes = paged_state::mutations(&entry).unwrap();
    assert_eq!(writes.len(), 1);
    assert_eq!(writes[0].key, row_key(&entry.key, 0, 10));
    assert_eq!(
        decode_entry(writes[0].value.as_ref().unwrap(), 10, 256, Layout::Rows).unwrap(),
        vec![entry.value.left[10].clone()]
    );
    assert!(decode_entry(writes[0].value.as_ref().unwrap(), 11, 256, Layout::Rows).is_err());
}

#[test]
fn history_load_bounds_bulk_workspace_and_skips_unused_accumulating_payloads() {
    use crate::memory_pool::tests_support::TestBroker;
    let broker = Arc::new(TestBroker::new(12 << 20));
    let owner = HostMemoryReservation::new(broker.clone(), "bounded history loading");
    let mut backend = MemoryKeyedState::new(0, 0, owner.sibling("retained history")).unwrap();
    let mut entry = StagedState {
        key: StateKey {
            key_group: 0,
            key: b"history".to_vec(),
        },
        value: JoinState {
            left: (0..20_003)
                .map(|id| StoredRow {
                    id,
                    row: Arc::from(vec![id as u8; 64]),
                    associations: (id % 5) as i32,
                })
                .collect(),
            next_row_id: [20_003, 0],
            ..Default::default()
        },
        original: JoinState::default(),
        original_layout: Layout::Compact,
        unloaded: None,
        touched: true,
    };
    backend
        .write_batch(paged_state::mutations(&entry).unwrap())
        .unwrap();
    let mut workspace = owner.sibling("loaded history");
    let (loaded, reads) =
        paged_state::load(&backend, vec![entry.key.clone()], &mut workspace).unwrap();
    assert_eq!(reads, 6); // one manifest, five bulk payload reads
    assert_eq!(loaded[0].value, entry.value);
    assert_eq!(loaded[0].original, entry.value);
    assert!(
        workspace.size() < 7 << 20,
        "transport credit survived loading: {}",
        workspace.size()
    );
    drop(loaded);
    drop(workspace);
    let mut pressure = owner.sibling("other operator");
    pressure
        .resize((12 << 20) - broker.reserved() - (1 << 20))
        .unwrap();
    let mut workspace = owner.sibling("accumulating history");
    let (mut loaded, reads) =
        paged_state::load_for_accumulation(&backend, vec![entry.key.clone()], &mut workspace, 0)
            .unwrap();
    assert_eq!(reads, 1);
    assert!(loaded[0].value.left.is_empty());
    assert_eq!(loaded[0].unloaded.as_ref().unwrap().ids.len(), 20_003);
    let new = StoredRow {
        id: 20_003,
        row: Arc::from(&b"new"[..]),
        associations: 7,
    };
    loaded[0].value.left.push(new.clone());
    loaded[0].value.next_row_id[0] += 1;
    loaded[0].touched = true;
    let changes = paged_state::batch_mutations(&loaded, &mut workspace).unwrap();
    assert_eq!(changes.len(), 2);
    backend.write_batch(changes).unwrap();
    drop(loaded);
    drop(workspace);
    // The opposite side needs these payloads for computation and must still enforce its budget.
    let mut workspace = owner.sibling("opposite history");
    assert!(matches!(
        paged_state::load_for_accumulation(&backend, vec![entry.key.clone()], &mut workspace, 1),
        Err(DataFusionError::ResourcesExhausted(_))
    ));
    drop(workspace);
    drop(pressure);
    entry.value.left.push(new);
    entry.value.next_row_id[0] += 1;
    let mut workspace = owner.sibling("full history validation");
    let (loaded, _) = paged_state::load(&backend, vec![entry.key.clone()], &mut workspace).unwrap();
    assert_eq!(loaded[0].value, entry.value);
    drop(loaded);
    drop(workspace);
    drop(backend);
    drop(owner);
    assert_eq!(broker.reserved(), 0);
}

fn dense_directory(count: u64) -> Vec<u8> {
    let mut bytes = b"SFJI\x01\0\0".to_vec();
    bytes.extend_from_slice(&count.to_le_bytes());
    bytes.extend_from_slice(&0u64.to_le_bytes());
    bytes.extend_from_slice(&count.div_ceil(64).to_le_bytes());
    for page in 0..count.div_ceil(64) {
        let present = (count - page * 64).min(64);
        let bits = if present == 64 {
            u64::MAX
        } else {
            (1u64 << present) - 1
        };
        bytes.extend_from_slice(&page.to_le_bytes());
        bytes.extend_from_slice(&bits.to_le_bytes());
    }
    bytes.extend_from_slice(&0u64.to_le_bytes());
    bytes
}

#[test]
fn dense_history_directories_stay_compressed_through_decode_and_reencoding() {
    let count = 1_000_003;
    let bytes = dense_directory(count);
    let (manifest, measured) =
        crate::allocation_test_support::measure(|| decode_manifest(&bytes).unwrap());
    assert_eq!(manifest.pages[0].len(), count as usize);
    assert!(manifest.pages[1].is_empty());
    assert!(measured.peak <= bytes.len() * 3, "{measured:?}");
    assert_eq!(
        manifest.pages[0].iter().sum::<u64>(),
        count * (count - 1) / 2
    );
    let mut entry = StagedState {
        key: StateKey {
            key_group: 0,
            key: b"dense".to_vec(),
        },
        value: JoinState {
            next_row_id: [count, 0],
            ..Default::default()
        },
        original: JoinState {
            next_row_id: [count, 0],
            ..Default::default()
        },
        original_layout: Layout::Rows,
        unloaded: Some(UnloadedRows {
            side: 0,
            ids: manifest.pages.into_iter().next().unwrap(),
        }),
        touched: true,
    };
    assert!(paged_state::mutations(&entry).unwrap().is_empty());
    for id in count..count + 137 {
        entry.value.left.push(StoredRow {
            id,
            row: Arc::from(&b"new"[..]),
            associations: 0,
        });
    }
    entry.value.next_row_id[0] += 137;
    let (changes, measured) =
        crate::allocation_test_support::measure(|| paged_state::mutations(&entry).unwrap());
    assert_eq!(changes.len(), 138);
    assert!(measured.peak <= bytes.len() * 4, "{measured:?}");
    assert_eq!(
        changes.last().unwrap().value.as_ref().unwrap(),
        &dense_directory(count + 137)
    );
    let directory = decode_manifest(changes.last().unwrap().value.as_ref().unwrap()).unwrap();
    assert_eq!(
        directory
            .pages
            .into_iter()
            .next()
            .unwrap()
            .into_iter()
            .sum::<u64>(),
        (count + 137) * (count + 136) / 2
    );
}

#[test]
fn accumulating_a_hot_key_reads_only_its_compressed_directory_with_small_workspace() {
    use crate::memory_pool::tests_support::TestBroker;
    use crate::state::observed_tests::{Io, Observed};
    use std::sync::atomic::Ordering;
    let directory = tempfile::tempdir().unwrap();
    let plugin = std::env::var("STREAMFUSION_TEST_ROCKSDB_PLUGIN").unwrap();
    let mut state = RocksPluginKeyedState::open(
        std::path::Path::new(&plugin),
        directory.path(),
        0,
        0,
        1 << 20,
    )
    .unwrap();
    let key = StateKey {
        key_group: 0,
        key: b"hot-key".to_vec(),
    };
    let count = 64_003;
    let payload: Arc<[u8]> = Arc::from(&b"retained"[..]);
    for start in (0..count).step_by(128) {
        state
            .write_batch(
                (start..(start + 128).min(count))
                    .map(|id| StateMutation {
                        key: row_key(&key, 0, id),
                        value: Some(
                            encode_page(&[StoredRow {
                                id,
                                row: payload.clone(),
                                associations: 0,
                            }])
                            .unwrap(),
                        ),
                    })
                    .collect(),
            )
            .unwrap();
    }
    state
        .write_batch(vec![StateMutation {
            key: manifest_key(&key),
            value: Some(dense_directory(count)),
        }])
        .unwrap();
    let io = Arc::new(Io::default());
    let mut state = Observed {
        inner: Box::new(state),
        io: io.clone(),
    };
    let broker = Arc::new(TestBroker::new(256 << 10));
    let mut workspace = HostMemoryReservation::new(broker.clone(), "compressed directory append");
    let ((mut loaded, reads), measured) = crate::allocation_test_support::measure(|| {
        paged_state::load_for_accumulation(&state, vec![key.clone()], &mut workspace, 0).unwrap()
    });
    assert_eq!(reads, 1);
    assert!(measured.peak < 256 << 10, "{measured:?}");
    assert_eq!(io.read_batches.load(Ordering::Relaxed), 1);
    assert_eq!(
        io.read_bytes.load(Ordering::Relaxed),
        dense_directory(count).len()
    );
    assert!(loaded[0].value.left.is_empty());
    loaded[0].value.left.push(StoredRow {
        id: count,
        row: Arc::from(&b"new"[..]),
        associations: 0,
    });
    loaded[0].value.next_row_id[0] += 1;
    loaded[0].touched = true;
    let changes = paged_state::batch_mutations(&loaded, &mut workspace).unwrap();
    assert_eq!(changes.len(), 2);
    assert_eq!(
        changes.last().unwrap().value.as_ref().unwrap(),
        &dense_directory(count + 1)
    );
    state.write_batch(changes).unwrap();
    assert_eq!(io.write_batches.load(Ordering::Relaxed), 1);
    drop(loaded);
    drop(workspace);
    assert_eq!(broker.reserved(), 0);
}
