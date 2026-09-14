// Copyright 2026 StreamFusion Authors
// Licensed under the Apache License, Version 2.0
use super::*;
use crate::{RocksStateBackend, StateKey, StateMutation};

fn mutations() -> Vec<StateMutation> {
    (0..1103)
        .map(|row| StateMutation {
            key: StateKey {
                key_group: 0,
                key: (0..if row % 11 == 0 { 128 } else { 8 })
                    .map(|index| ((row + index) % 251) as u8)
                    .collect(),
            },
            value: (row % 7 != 0).then(|| {
                (0..if row % 113 == 0 { 4096 } else { row * 37 % 260 })
                    .map(|index| ((row + index) % 251) as u8)
                    .collect()
            }),
        })
        .collect()
}

#[test]
fn generated_flush_boundaries_and_bytes_match_the_actual_flink_bulk_writer() {
    let rows = mutations();
    let mut lines = Vec::new();
    for limit in [0, 1, 50, 4096, 2 << 20, i64::MAX as usize] {
        let mut hash = 0xcbf29ce484222325_u64;
        let mut batches = 0;
        flush_batches(
            rows.iter()
                .map(|m| (m.key.key_group, m.key.key.as_slice(), m.value.as_deref())),
            limit,
            |batch| {
                assert!(batch.len() <= 500);
                for byte in batch.data() {
                    hash = (hash ^ u64::from(*byte)).wrapping_mul(0x100000001b3);
                }
                batches += 1;
                Ok(())
            },
        )
        .unwrap();
        lines.push(format!("{limit} {batches} {hash:x}"));
    }
    assert_eq!(
        lines.join("\n"),
        include_str!("../../../tests/fixtures/flink-write-batch-traces.txt").trim()
    );
}

#[test]
fn invalid_late_key_groups_cannot_commit_earlier_chunks() {
    let directory = tempfile::tempdir().unwrap();
    let mut state = RocksStateBackend::open(directory.path(), 0, 0).unwrap();
    state.write_batch_size = 1;
    let mut rows = mutations();
    rows.last_mut().unwrap().key.key_group = 1;
    assert!(state.write_batch(rows).is_err());
    assert_eq!(state.snapshot_key_group(0).unwrap().len(), 16);
}

#[test]
fn chunked_puts_deletes_and_oversized_values_checkpoint_and_restore_exactly() {
    for limit in [0, 1, 50, 4096, 2 << 20] {
        let directory = tempfile::tempdir().unwrap();
        let config = streamfusion_state_abi::RocksDbDatabaseOptions {
            write_batch_size: limit,
            ..Default::default()
        };
        let state = super::super::filter_configuration_tests::open(
            &directory.path().join("db"),
            &config,
            &[7],
            false,
        );
        let rows = mutations();
        let keys = rows.iter().map(|row| row.key.clone()).collect::<Vec<_>>();
        let mut expected = std::collections::HashMap::new();
        for row in &rows {
            expected.insert(row.key.key.clone(), row.value.clone());
        }
        state.write_batch(rows).unwrap();
        let expected = keys
            .iter()
            .map(|key| expected[&key.key].clone())
            .collect::<Vec<_>>();
        assert_eq!(state.get_batch(&keys).unwrap(), expected);
        let checkpoint = directory.path().join("checkpoint");
        state.checkpoint(&checkpoint).unwrap();
        let restored = super::super::filter_configuration_tests::open(
            &checkpoint,
            &Default::default(),
            &[4],
            true,
        );
        assert_eq!(restored.get_batch(&keys).unwrap(), expected);
    }
}

#[test]
fn empty_flush_does_no_io_and_failed_chunk_stops_before_later_chunks() {
    flush_batches(std::iter::empty(), 0, |_| panic!("empty input wrote state")).unwrap();
    let rows = mutations();
    let mut calls = 0;
    let error = flush_batches(
        rows.iter()
            .map(|m| (0, m.key.key.as_slice(), m.value.as_deref())),
        50,
        |_| {
            calls += 1;
            if calls == 3 {
                Err(std::io::Error::other("injected write failure"))
            } else {
                Ok(())
            }
        },
    )
    .unwrap_err();
    assert_eq!(calls, 3);
    assert!(error.to_string().contains("injected"));
}
