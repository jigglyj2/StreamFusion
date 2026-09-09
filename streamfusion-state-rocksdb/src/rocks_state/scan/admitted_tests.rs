// Copyright 2026 StreamFusion Authors
// Licensed under the Apache License, Version 2.0.

use super::*;

#[test]
fn admitted_pages_preserve_ranges_and_large_values_without_growing_with_history() {
    let directory = tempfile::tempdir().unwrap();
    let state = RocksStateBackend::open(directory.path(), 0, 1).unwrap();
    let values = [
        vec![1; 8],
        vec![2; 70_000],
        vec![3; 8],
        vec![4; 80_000],
        vec![5; 8],
    ];
    state
        .write_batch(
            (0..2)
                .flat_map(|group| {
                    values
                        .iter()
                        .enumerate()
                        .map(move |(index, value)| StateMutation {
                            key: StateKey {
                                key_group: group,
                                key: vec![index as u8],
                            },
                            value: Some(value.clone()),
                        })
                })
                .collect(),
        )
        .unwrap();
    for max_rows in [1, 2, 8] {
        for target in [1, 512, 150_000] {
            let mut after = None;
            let mut actual = Vec::new();
            loop {
                let mut charges = Vec::new();
                let page = state
                    .scan_range_page_admitted(
                        0,
                        &[1],
                        Some(&[4]),
                        after.as_deref(),
                        max_rows,
                        target,
                        |bytes| {
                            charges.push(bytes);
                            Ok(())
                        },
                    )
                    .unwrap();
                assert!(!page.entries.is_empty());
                assert!(page.entries.len() <= max_rows);
                assert_eq!(charges.len(), 1, "one admission per populated page");
                let bytes = page
                    .entries
                    .iter()
                    .map(|(k, v)| k.len() + v.len())
                    .sum::<usize>();
                assert!(charges[0] >= bytes);
                assert!(charges[0] <= bytes + page.entries.len() * 512 + 4096);
                if page.entries.len() > 1 {
                    assert!(bytes <= target);
                }
                after = Some(page.entries.last().unwrap().0.clone());
                actual.extend(page.entries);
                if page.complete {
                    break;
                }
            }
            assert_eq!(
                actual,
                (1..4)
                    .map(|i| (vec![i as u8], values[i].clone()))
                    .collect::<Vec<_>>()
            );
        }
    }
    // The same large entry remains an error on the original strict bounded API.
    assert_eq!(
        state
            .scan_range_page(0, &[1], None, None, 1, 512)
            .unwrap_err()
            .kind(),
        ErrorKind::OutOfMemory
    );
}

#[test]
fn denied_page_does_not_advance_cursor_or_mutate_state_and_empty_pages_need_no_admission() {
    let directory = tempfile::tempdir().unwrap();
    let state = RocksStateBackend::open(directory.path(), 2, 3).unwrap();
    state
        .write_batch(vec![StateMutation {
            key: StateKey {
                key_group: 2,
                key: vec![255],
            },
            value: Some(vec![7; 80_000]),
        }])
        .unwrap();
    let mut attempts = 0;
    let error = state
        .scan_range_page_admitted(2, &[], None, None, 32, 512, |bytes| {
            attempts += 1;
            assert!(bytes > 80_000);
            Err(Error::new(ErrorKind::OutOfMemory, "test budget denial"))
        })
        .unwrap_err();
    assert_eq!(error.kind(), ErrorKind::OutOfMemory);
    assert_eq!(attempts, 1);
    let retried = state
        .scan_range_page_admitted(2, &[], None, None, 32, 512, |_| Ok(()))
        .unwrap();
    assert_eq!(retried.entries, vec![(vec![255], vec![7; 80_000])]);
    assert!(retried.complete);
    for (group, start, end, after) in [
        (3, vec![], None, None),
        (2, vec![3], Some(vec![3]), None),
        (2, vec![], None, Some(vec![255])),
    ] {
        let page = state
            .scan_range_page_admitted(
                group,
                &start,
                end.as_deref(),
                after.as_deref(),
                8,
                512,
                |_| panic!("empty page must not acquire payload memory"),
            )
            .unwrap();
        assert!(page.entries.is_empty());
        assert!(page.complete);
    }
    for (group, rows, bytes) in [(1, 8, 512), (2, 0, 512), (2, 8, 0)] {
        assert!(state
            .scan_range_page_admitted(group, &[], None, None, rows, bytes, |_| panic!(
                "invalid scan must fail before admission"
            ))
            .is_err());
    }
}
