// Copyright 2026 StreamFusion Authors
// Licensed under the Apache License, Version 2.0.

use super::super::indexed_tests::{backends, observe, processor};
use super::super::tests::batch;
use super::*;
use crate::planner::operators::sortable_state::PAGE_ROWS;
use std::sync::atomic::Ordering;

#[test]
fn range_end_proof_preserves_tail_validation_and_exact_page_boundaries() {
    for rocks in backends() {
        for count in [2, PAGE_ROWS] {
            for corrupt in [false, true] {
                let (mut p, broker, _dir) = processor(rocks, 0, 127);
                p.ingest_arrow(
                    0,
                    batch(
                        &vec![9; count],
                        &vec![100; count],
                        &vec![b"row".as_slice(); count],
                        &vec![INSERT; count],
                    ),
                )
                .unwrap();
                p.ingest_arrow(1, batch(&[9], &[100], &[b"right"], &[INSERT]))
                    .unwrap();
                let group = *p.dirty_timer_groups.first().unwrap();
                if corrupt {
                    // Persist an extra left page after the header's claimed row count.
                    // The two-row window must reject it while initially scanning; the
                    // exactly-full page must reject it in the deferred tail lookahead.
                    let snapshot = p.snapshot_key_group(group).unwrap();
                    let entries =
                        crate::state::decode_key_group_snapshot(group, &snapshot).unwrap();
                    let (mut key, value) = entries
                        .into_iter()
                        .find(|(key, _)| key[0] == 0x92 && key[key.len() - 9] == 0)
                        .unwrap();
                    let len = key.len();
                    key[len - 8..].copy_from_slice(&(count as u64).to_be_bytes());
                    p.state
                        .write_batch(vec![StateMutation {
                            key: StateKey {
                                key_group: group,
                                key,
                            },
                            value: Some(value),
                        }])
                        .unwrap();
                }
                let io = observe(&mut p);
                p.begin_watermark(99).unwrap();
                if corrupt && count < PAGE_ROWS {
                    assert!(p
                        .next_closed_window()
                        .err()
                        .unwrap()
                        .to_string()
                        .contains("index"));
                } else {
                    let closed = p.next_closed_window().unwrap().unwrap();
                    assert_eq!(closed.inputs[0].num_rows(), count);
                    let reads = io.range_reads.load(Ordering::Relaxed);
                    let result = p.finish_closed_window();
                    assert_eq!(
                        io.range_reads.load(Ordering::Relaxed),
                        reads + usize::from(count == PAGE_ROWS)
                    );
                    if corrupt {
                        assert!(result.unwrap_err().to_string().contains("index"));
                    } else {
                        result.unwrap();
                        assert!(p.next_closed_window().unwrap().is_none());
                    }
                }
                if corrupt {
                    assert_eq!(io.write_batches.load(Ordering::Relaxed), 0);
                    assert_eq!(p.timers.timer_count(TimerDomain::EventTime), 1);
                    assert!(p.snapshot_key_group(group).is_err());
                } else {
                    assert_eq!(p.timers.timer_count(TimerDomain::EventTime), 0);
                }
                drop(p);
                assert_eq!(broker.reserved(), 0);
            }
        }
    }
}
