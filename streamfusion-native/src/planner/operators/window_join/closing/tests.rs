// Copyright 2026 StreamFusion Authors
// Licensed under the Apache License, Version 2.0.

use super::super::indexed_tests::{backends, observe, other_backend, processor};
use super::super::tests::batch;
use super::*;
use std::sync::atomic::Ordering;

fn payloads(batch: &RecordBatch) -> Vec<Vec<u8>> {
    batch
        .column(2)
        .as_any()
        .downcast_ref::<BinaryArray>()
        .unwrap()
        .iter()
        .map(|v| v.unwrap().to_vec())
        .collect()
}

#[test]
fn closes_one_window_at_a_time_and_keeps_timer_until_completion_on_both_backends() {
    for rocks in backends() {
        let (mut p, broker, _dir) = processor(rocks, 0, 127);
        let io = observe(&mut p);
        for end in 1..=48 {
            for side in 0..2 {
                p.ingest_arrow(
                    side,
                    batch(
                        &[9, 9],
                        &[end, end],
                        &[b"first", b"second"],
                        &[INSERT, UPDATE_AFTER],
                    ),
                )
                .unwrap();
            }
        }
        assert_eq!(p.timers.timer_count(TimerDomain::EventTime), 48);
        let group = *p.dirty_timer_groups.first().unwrap();
        p.begin_watermark(47).unwrap();
        assert!(p.snapshot_key_group(group).is_err());
        let mut retained = Vec::new();
        for remaining in (1..=48).rev() {
            let reads = io.range_reads.load(Ordering::Relaxed);
            let closed = p.next_closed_window().unwrap().unwrap();
            // One complete right range and one bounded left page.
            assert_eq!(io.range_reads.load(Ordering::Relaxed), reads + 2);
            assert_eq!(p.timers.timer_count(TimerDomain::EventTime), remaining);
            for input in &closed.inputs {
                assert_eq!(input.num_columns(), 3); // No legacy side/group candidate metadata.
                assert_eq!(payloads(input), vec![b"first".to_vec(), b"second".to_vec()]);
            }
            assert!(p
                .ingest_arrow(0, batch(&[9], &[100], &[b"blocked"], &[INSERT]))
                .is_err());
            p.finish_closed_window().unwrap();
            // Only the required final tail check reads state during acknowledgement.
            // Payload deletion must not rescan either already validated side.
            assert_eq!(io.range_reads.load(Ordering::Relaxed), reads + 3);
            assert_eq!(p.timers.timer_count(TimerDomain::EventTime), remaining - 1);
            retained.extend(closed.inputs);
        }
        assert!(p.next_closed_window().unwrap().is_none());
        let snapshot = p.snapshot_key_group(group).unwrap();
        let (mut restored, _, _restored_dir) = processor(other_backend(rocks), 0, 127);
        restored.restore_key_group(group, &snapshot).unwrap();
        restored.begin_watermark(47).unwrap();
        assert!(restored.next_closed_window().unwrap().is_none());
        drop(snapshot);
        drop(p);
        assert!(
            broker.reserved() > 0,
            "returned payloads must retain admission after processor drop"
        );
        assert_eq!(
            payloads(retained.last().unwrap()),
            vec![b"first".to_vec(), b"second".to_vec()]
        );
        drop(retained);
        assert_eq!(broker.reserved(), 0);
    }
}

#[test]
fn cancelled_window_cannot_checkpoint_and_recovers_original_payload_and_timer() {
    for rocks in backends() {
        let (mut p, broker, _dir) = processor(rocks, 0, 127);
        p.ingest_arrow(
            0,
            batch(
                &[9, 9],
                &[100, 100],
                &[b"duplicate", b"duplicate"],
                &[INSERT, INSERT],
            ),
        )
        .unwrap();
        let group = *p.dirty_timer_groups.first().unwrap();
        let snapshot = p.snapshot_key_group(group).unwrap();
        p.begin_watermark(99).unwrap();
        let closed = p.next_closed_window().unwrap().unwrap();
        assert_eq!(closed.inputs[1].num_rows(), 0);
        drop(closed);
        assert!(p.snapshot_key_group(group).is_err());
        assert!(p.checkpoint(tempfile::tempdir().unwrap().path()).is_err());
        assert!(p.restore_key_group(group, &snapshot).is_err());
        assert!(p.begin_watermark(101).is_err());
        assert!(p.next_closed_window().is_err());
        assert!(p
            .finish_closed_window()
            .unwrap_err()
            .to_string()
            .contains("recover"));
        drop(p);
        let (mut restored, _, _restored_dir) = processor(other_backend(rocks), 0, 127);
        restored.restore_key_group(group, &snapshot).unwrap();
        restored.begin_watermark(99).unwrap();
        let closed = restored.next_closed_window().unwrap().unwrap();
        assert_eq!(payloads(&closed.inputs[0]), vec![b"duplicate".to_vec(); 2]);
        restored.finish_closed_window().unwrap();
        assert!(restored.next_closed_window().unwrap().is_none());
        drop(snapshot);
        assert_eq!(broker.reserved(), 0);
    }
}

#[test]
fn workspace_denial_happens_before_payload_read_and_poisoned_drain_cannot_retry() {
    for rocks in backends() {
        let (mut p, broker, _dir) = processor(rocks, 0, 127);
        let wide = vec![42; 1 << 20];
        p.ingest_arrow(0, batch(&[9], &[100], &[&wide], &[INSERT]))
            .unwrap();
        let io = observe(&mut p);
        let mut pressure = p.state_memory();
        pressure
            .resize(pressure.available_capacity().unwrap().unwrap() - (64 << 10))
            .unwrap();
        p.begin_watermark(99).unwrap();
        assert!(p.next_closed_window().is_err());
        assert_eq!(io.range_reads.load(Ordering::Relaxed), 0);
        assert_eq!(p.timers.timer_count(TimerDomain::EventTime), 1);
        drop(pressure);
        assert!(p
            .next_closed_window()
            .err()
            .unwrap()
            .to_string()
            .contains("recover"));
        drop(p);
        assert_eq!(broker.reserved(), 0);
    }
}

#[test]
fn completed_watermark_controls_lateness_and_repeated_progress_without_reopening_state() {
    for rocks in backends() {
        let (mut p, _, _dir) = processor(rocks, 0, 127);
        p.begin_watermark(99).unwrap();
        assert!(p.next_closed_window().unwrap().is_none());
        p.ingest_arrow(0, batch(&[9], &[100], &[b"late"], &[DELETE]))
            .unwrap();
        p.ingest_arrow(1, batch(&[9], &[101], &[b"on time"], &[INSERT]))
            .unwrap();
        assert_eq!(p.late_records_dropped(), [1, 0]);
        for watermark in [98, 99] {
            p.begin_watermark(watermark).unwrap();
            assert!(p.next_closed_window().unwrap().is_none());
        }
        p.begin_watermark(100).unwrap();
        let closed = p.next_closed_window().unwrap().unwrap();
        assert_eq!(closed.inputs[0].num_rows(), 0);
        assert_eq!(payloads(&closed.inputs[1]), vec![b"on time".to_vec()]);
        p.finish_closed_window().unwrap();
        assert!(p.next_closed_window().unwrap().is_none());
    }
}

#[test]
fn wide_multi_page_acknowledgements_delete_known_entries_without_payload_reads() {
    for rocks in backends() {
        let (mut p, broker, _dir) = processor(rocks, 0, 127);
        let count = 2049;
        let payloads = (0..count)
            .map(|i| vec![(i % 251) as u8; 1000 + i % 9 * 500])
            .collect::<Vec<_>>();
        let values = payloads.iter().map(Vec::as_slice).collect::<Vec<_>>();
        for side in 0..2 {
            p.ingest_arrow(
                side,
                batch(
                    &vec![9; count],
                    &vec![100; count],
                    &values,
                    &vec![INSERT; count],
                ),
            )
            .unwrap();
        }
        let io = observe(&mut p);
        let group = *p.dirty_timer_groups.first().unwrap();
        p.begin_watermark(99).unwrap();
        let mut consumed = 0;
        let mut pages = 0;
        while let Some(closed) = p.next_closed_window().unwrap() {
            let rows = closed.inputs[0].num_rows();
            assert!(rows > 0 && rows <= crate::planner::operators::sortable_state::PAGE_ROWS);
            assert_eq!(closed.inputs[1].num_rows(), count);
            consumed += rows;
            pages += 1;
            let reads = io.range_reads.load(Ordering::Relaxed);
            let bytes = io.read_bytes.load(Ordering::Relaxed);
            p.finish_closed_window().unwrap();
            assert_eq!(
                io.range_reads.load(Ordering::Relaxed),
                reads + usize::from(consumed == count)
            );
            // The final tail check is empty too: no payload bytes may be reread to delete.
            assert_eq!(io.read_bytes.load(Ordering::Relaxed), bytes);
            assert_eq!(closed.inputs[0].column(2).len(), rows);
        }
        assert_eq!(consumed, count);
        assert!(pages > 1);
        let snapshot = p.snapshot_key_group(group).unwrap();
        assert!(crate::state::decode_key_group_snapshot(group, &snapshot)
            .unwrap()
            .iter()
            .all(|(key, _)| !matches!(key[0], 0x91 | 0x92)));
        drop(snapshot);
        drop(p);
        assert_eq!(broker.reserved(), 0);
    }
}
