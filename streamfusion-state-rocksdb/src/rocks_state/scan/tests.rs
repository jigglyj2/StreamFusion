// Copyright 2026 StreamFusion Authors
// Licensed under the Apache License, Version 2.0.

use super::*;

#[test]
fn completion_distinguishes_exhaustion_from_row_and_byte_limited_pages() {
    let directory = tempfile::tempdir().unwrap();
    let state = RocksStateBackend::open(directory.path(), 0, 1).unwrap();
    state
        .write_batch(
            (0..2)
                .flat_map(|group| {
                    (0..5u8).map(move |key| StateMutation {
                        key: StateKey {
                            key_group: group,
                            key: vec![key],
                        },
                        value: Some(vec![key; 8]),
                    })
                })
                .collect(),
        )
        .unwrap();
    for max_rows in [1, 2, 5, 6] {
        for max_bytes in [109, 218, 4096] {
            let mut after = None;
            let mut keys = Vec::new();
            loop {
                let page = state
                    .scan_range_page(0, &[1], Some(&[4]), after.as_deref(), max_rows, max_bytes)
                    .unwrap();
                assert!(!page.entries.is_empty());
                assert!(page.entries.len() <= max_rows);
                keys.extend(page.entries.iter().map(|(key, _)| key[0]));
                assert_eq!(page.complete, keys.last() == Some(&3));
                if page.complete {
                    break;
                }
                after = Some(page.entries.last().unwrap().0.clone());
            }
            assert_eq!(keys, vec![1, 2, 3]);
        }
    }
    // A full page can also be the final page; a later key group must not imply continuation.
    let all = state.scan_range_page(0, &[], None, None, 5, 4096).unwrap();
    assert_eq!(all.entries.len(), 5);
    assert!(all.complete);
    for (start, end, after) in [(9, None, None), (2, Some(2), None), (0, Some(3), Some(2))] {
        let end_key = end.map(|end| vec![end]);
        let after_key = after.map(|after| vec![after]);
        let empty = state
            .scan_range_page(
                0,
                &[start],
                end_key.as_deref(),
                after_key.as_deref(),
                5,
                4096,
            )
            .unwrap();
        assert!(empty.entries.is_empty());
        assert!(empty.complete);
    }
    assert_eq!(
        state
            .scan_range_page(0, &[], None, None, 1, 108)
            .unwrap_err()
            .kind(),
        ErrorKind::OutOfMemory
    );
}
