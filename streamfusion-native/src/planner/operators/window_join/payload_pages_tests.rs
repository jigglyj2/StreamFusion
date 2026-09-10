// Copyright 2026 StreamFusion Authors
// Licensed under the Apache License, Version 2.0.

use super::indexed_tests::{backends, observe, other_backend, processor};
use super::tests::batch;
use super::*;
use std::sync::atomic::Ordering;

#[test]
fn hot_window_stores_bounded_pages_with_less_than_one_mib_for_ten_thousand_rows() {
    for rocks in backends() {
        let (mut p, broker, _dir) = processor(rocks, 0, 127);
        let before = broker.reserved();
        for _ in 0..10 {
            p.ingest_arrow(
                0,
                batch(
                    &[7; 1000],
                    &[100; 1000],
                    &[b"payload".as_slice(); 1000],
                    &[INSERT; 1000],
                ),
            )
            .unwrap();
        }
        if !rocks {
            assert!(broker.reserved() - before < 1 << 20);
        }
        let group = *p.dirty_timer_groups.first().unwrap();
        let snapshot = p.snapshot_key_group(group).unwrap();
        let entries = crate::state::decode_key_group_snapshot(group, &snapshot).unwrap();
        let payloads = entries
            .iter()
            .filter(|(key, _)| key[0] == 0x92)
            .collect::<Vec<_>>();
        assert_eq!(payloads.len(), 40);
        assert_eq!(
            payloads
                .iter()
                .map(|(_, v)| payload_pages::Rows::new(v, true).unwrap().len())
                .sum::<usize>(),
            10_000
        );
        assert!(payloads.iter().all(|(_, v)| v.len() <= 16 << 10));
        drop(snapshot);
        drop(p);
        assert_eq!(broker.reserved(), 0);
    }
}

#[test]
fn v3_row_entries_restore_append_and_retire_without_reinterpreting_payloads() {
    for rocks in backends() {
        let (mut source, _, _dir) = processor(false, 0, 127);
        for side in 0..2 {
            source
                .ingest_arrow(
                    side,
                    batch(&[7; 3], &[100; 3], &[b"b", b"a", b"b"], &[INSERT; 3]),
                )
                .unwrap();
        }
        let group = *source.dirty_timer_groups.first().unwrap();
        let snapshot = source.snapshot_key_group(group).unwrap();
        let mut old = Vec::new();
        for (mut key, mut value) in
            crate::state::decode_key_group_snapshot(group, &snapshot).unwrap()
        {
            if key[0] == 0x91 {
                value[4] = 3;
            }
            if key[0] == 0x92 {
                let len = key.len();
                let first = u64::from_be_bytes(key[len - 8..].try_into().unwrap());
                for (i, row) in payload_pages::Rows::new(&value, true)
                    .unwrap()
                    .iter()
                    .enumerate()
                {
                    key[len - 8..].copy_from_slice(&(first + i as u64).to_be_bytes());
                    old.push((key.clone(), row.to_vec()));
                }
            } else {
                old.push((key, value));
            }
        }
        old.sort_by(|a, b| a.0.cmp(&b.0));
        let old = streamfusion_state_abi::encode_key_group_snapshot(
            group,
            old.iter().map(|(k, v)| (k.as_slice(), v.as_slice())),
        )
        .unwrap();
        let (mut target, _, _target_dir) = processor(rocks, 0, 127);
        target.restore_key_group(group, &old).unwrap();
        target
            .ingest_arrow(0, batch(&[7], &[100], &[b"c"], &[UPDATE_AFTER]))
            .unwrap();
        // New windows use pages alongside the old window, and a second backend restore
        // preserves both representations without rewriting growing old partitions.
        target
            .ingest_arrow(0, batch(&[7], &[200], &[b"new"], &[INSERT]))
            .unwrap();
        let mixed = target.snapshot_key_group(group).unwrap();
        let (mut restored, broker, _restore_dir) = processor(other_backend(rocks), 0, 127);
        restored.restore_key_group(group, &mixed).unwrap();
        let io = observe(&mut restored);
        restored.begin_watermark(199).unwrap();
        let mut actual = Vec::new();
        while let Some(closed) = restored.next_closed_window().unwrap() {
            let payload = closed.inputs[0]
                .column(2)
                .as_any()
                .downcast_ref::<BinaryArray>()
                .unwrap();
            actual.extend(payload.iter().map(|v| v.unwrap().to_vec()));
            // The four-row legacy window stops at its first row-entry page; the new
            // singleton paged window already reaches EOF during its initial scan.
            let tail_reads = usize::from(closed.inputs[0].num_rows() == 4);
            drop(closed);
            let reads = io.range_reads.load(Ordering::Relaxed);
            restored.finish_closed_window().unwrap();
            assert_eq!(io.range_reads.load(Ordering::Relaxed), reads + tail_reads);
        }
        assert_eq!(
            actual,
            [
                b"b".to_vec(),
                b"a".to_vec(),
                b"b".to_vec(),
                b"c".to_vec(),
                b"new".to_vec()
            ]
        );
        assert_eq!(restored.timers.timer_count(TimerDomain::EventTime), 0);
        let retired = restored.snapshot_key_group(group).unwrap();
        assert!(crate::state::decode_key_group_snapshot(group, &retired)
            .unwrap()
            .iter()
            .all(|(key, _)| !matches!(key[0], 0x91 | 0x92)));
        drop(retired);
        drop(restored);
        assert_eq!(broker.reserved(), 0);
    }
}

#[test]
fn malformed_page_lengths_counts_versions_and_trailing_bytes_are_rejected() {
    let valid = [
        b"SFWP\x01".as_slice(),
        &1u32.to_le_bytes(),
        &1u32.to_le_bytes(),
        b"x",
    ]
    .concat();
    assert_eq!(
        payload_pages::Rows::new(&valid, true)
            .unwrap()
            .iter()
            .collect::<Vec<_>>(),
        vec![b"x".as_slice()]
    );
    for end in 0..valid.len() {
        assert!(payload_pages::Rows::new(&valid[..end], true).is_err());
    }
    for (offset, bytes) in [
        (4, vec![2]),
        (5, 0u32.to_le_bytes().to_vec()),
        (5, 257u32.to_le_bytes().to_vec()),
        (9, u32::MAX.to_le_bytes().to_vec()),
    ] {
        let mut bad = valid.clone();
        bad[offset..offset + bytes.len()].copy_from_slice(&bytes);
        assert!(payload_pages::Rows::new(&bad, true).is_err());
    }
    let mut trailing = valid;
    trailing.push(0);
    assert!(payload_pages::Rows::new(&trailing, true).is_err());
}
