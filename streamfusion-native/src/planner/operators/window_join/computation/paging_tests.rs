// Copyright 2026 StreamFusion Authors
// Licensed under the Apache License, Version 2.0.

use super::super::indexed_tests::{backends, other_backend, processor};
use super::super::tests::batch;
use super::tests::{context, pairs};
use super::*;
use futures::StreamExt;

fn append_left(processor: &mut WindowJoinProcessor, count: usize) -> Vec<Vec<u8>> {
    let values = (0..count)
        .map(|i| format!("left-{:05}-duplicate-{}", i / 2, i % 3).into_bytes())
        .collect::<Vec<_>>();
    for page in values.chunks(256) {
        processor
            .ingest_arrow(
                0,
                batch(
                    &vec![9; page.len()],
                    &vec![100; page.len()],
                    &page.iter().map(Vec::as_slice).collect::<Vec<_>>(),
                    &vec![INSERT; page.len()],
                ),
            )
            .unwrap();
    }
    values
}

#[tokio::test]
async fn large_left_window_closes_in_order_with_four_mib_headroom_on_both_backends() {
    for rocks in backends() {
        let (mut p, broker, _dir) = processor(rocks, 0, 127);
        let left = append_left(&mut p, 20_003);
        let right = [
            b"right-a".as_slice(),
            b"right-a".as_slice(),
            b"right-b".as_slice(),
        ];
        p.ingest_arrow(1, batch(&[9; 3], &[100; 3], &right, &[INSERT; 3]))
            .unwrap();
        let mut pressure = p.state_memory();
        pressure
            .resize(pressure.available_capacity().unwrap().unwrap() - (4 << 20))
            .unwrap();
        let owner = p.state_memory();
        p.begin_watermark(99).unwrap();
        let mut actual = Vec::new();
        let mut pages = 0;
        while let Some(closed) = p.next_closed_window().unwrap() {
            assert!(closed.inputs[0].num_rows() <= 256);
            assert_eq!(closed.inputs[1].num_rows(), right.len());
            let mut stream =
                ClosedWindowStream::try_new(closed, None, context(&owner, 8192), &owner).unwrap();
            while let Some(batch) = stream.next().await {
                actual.extend(pairs(&batch.unwrap()));
            }
            assert!(stream.completed());
            drop(stream);
            p.finish_closed_window().unwrap();
            pages += 1;
        }
        assert!(pages > 1);
        assert_eq!(
            actual,
            left.iter()
                .flat_map(|a| right.iter().map(move |b| (a.clone(), b.to_vec())))
                .collect::<Vec<_>>()
        );
        assert_eq!(p.timers.timer_count(TimerDomain::EventTime), 0);
        drop(pressure);
        drop(p);
        assert_eq!(broker.reserved(), 0);
    }
}

#[test]
fn cancellation_after_an_acknowledged_page_recovers_the_whole_checkpoint_window() {
    for rocks in backends() {
        let (mut p, _, _dir) = processor(rocks, 0, 127);
        let left = append_left(&mut p, 601);
        p.ingest_arrow(1, batch(&[9], &[100], &[b"right"], &[INSERT]))
            .unwrap();
        let group = *p.dirty_timer_groups.first().unwrap();
        let snapshot = p.snapshot_key_group(group).unwrap();
        p.begin_watermark(99).unwrap();
        let first = p.next_closed_window().unwrap().unwrap();
        assert_eq!(first.inputs[0].num_rows(), 256);
        drop(first);
        p.finish_closed_window().unwrap();
        assert_eq!(p.timers.timer_count(TimerDomain::EventTime), 1);
        drop(p.next_closed_window().unwrap().unwrap());
        assert!(p.snapshot_key_group(group).is_err());
        assert!(p.next_closed_window().is_err());
        assert!(p.finish_closed_window().is_err());
        drop(p);
        let (mut restored, broker, _restore_dir) = processor(other_backend(rocks), 0, 127);
        restored.restore_key_group(group, &snapshot).unwrap();
        restored.begin_watermark(99).unwrap();
        let mut actual = Vec::new();
        while let Some(closed) = restored.next_closed_window().unwrap() {
            let column = closed.inputs[0]
                .column(2)
                .as_any()
                .downcast_ref::<BinaryArray>()
                .unwrap();
            actual.extend(column.iter().map(|value| value.unwrap().to_vec()));
            drop(closed);
            restored.finish_closed_window().unwrap();
        }
        assert_eq!(actual, left);
        assert_eq!(restored.timers.timer_count(TimerDomain::EventTime), 0);
        drop(restored);
        assert_eq!(broker.reserved(), 0);
    }
}
