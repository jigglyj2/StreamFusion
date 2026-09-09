// Copyright 2026 StreamFusion Authors
// Licensed under the Apache License, Version 2.0.

use super::*;
use crate::memory_pool::tests_support::TestBroker;
use prost::Message;

mod io;

pub(in crate::planner::operators::window_aggregate) fn plan() -> Vec<u8> {
    let bytes = super::super::tests::plan(proto::WindowKind::Session, 10_000, 0, false);
    let mut native = proto::NativePlan::decode(bytes.as_slice()).unwrap();
    let Some(proto::operator::Operator::WindowAggregate(plan)) =
        &mut native.root.as_mut().unwrap().operator
    else {
        unreachable!()
    };
    plan.aggregate_calls[0].retractable = false;
    native.encode_to_vec()
}

fn processor(broker: Arc<TestBroker>, rocks: Option<&std::path::Path>) -> SharedSessions {
    let memory = HostMemoryReservation::new(broker, "session state");
    let timer = memory.sibling("session timers");
    let scratch = memory.sibling("session workspace");
    let state: Box<dyn KeyedState> = if let Some(directory) = rocks {
        Box::new(
            RocksPluginKeyedState::open_for_owner(
                std::path::Path::new(&std::env::var("STREAMFUSION_TEST_ROCKSDB_PLUGIN").unwrap()),
                directory,
                0,
                127,
                8 << 20,
                &memory,
            )
            .unwrap(),
        )
    } else {
        Box::new(crate::state::OrderedMemoryKeyedState::new(0, 127, memory).unwrap())
    };
    SharedSessions::new(
        WindowAggregateProcessor::with_state(&plan(), 128, 0, 127, state, timer, scratch).unwrap(),
    )
    .unwrap()
}

pub(in crate::planner::operators::window_aggregate) fn batch(timestamps: Vec<i64>) -> RecordBatch {
    RecordBatch::try_from_iter(vec![
        (
            "key",
            Arc::new(Int64Array::from(vec![1; timestamps.len()])) as ArrayRef,
        ),
        (
            "ts",
            Arc::new(TimestampMillisecondArray::from(timestamps)) as ArrayRef,
        ),
    ])
    .unwrap()
}

fn rows(batch: &RecordBatch) -> Vec<(i64, i64, i64)> {
    let counts = batch
        .column(1)
        .as_any()
        .downcast_ref::<Int64Array>()
        .unwrap();
    let starts = batch
        .column(2)
        .as_any()
        .downcast_ref::<TimestampMillisecondArray>()
        .unwrap();
    let ends = batch
        .column(3)
        .as_any()
        .downcast_ref::<TimestampMillisecondArray>()
        .unwrap();
    (0..batch.num_rows())
        .map(|i| (counts.value(i), starts.value(i), ends.value(i)))
        .collect()
}

fn advance(processor: &mut SharedSessions, watermark: i64) -> Vec<(i64, i64, i64)> {
    let mut result = Vec::new();
    loop {
        let batch = processor.advance(watermark).unwrap();
        result.extend(rows(&batch));
        if processor.next_timer().is_none_or(|next| next > watermark) {
            break;
        }
    }
    result
}

#[test]
fn grouped_datafusion_sessions_match_flink_verified_arrivals_on_both_backends() {
    for rocks in [false, true] {
        for seed in [3u64, 19, 71] {
            for batch_size in [1, 7, 31] {
                let directory = tempfile::tempdir().unwrap();
                let broker = Arc::new(TestBroker::new(64 << 20));
                let mut target = processor(broker.clone(), rocks.then_some(directory.path()));
                let mut reference =
                    super::super::tests::processor(&plan(), Arc::new(TestBroker::new(64 << 20)));
                let mut random = seed;
                for phase in 0..8 {
                    let base = phase * 40000i64;
                    let watermark = base + 15000;
                    assert_eq!(
                        advance(&mut target, watermark),
                        rows(&reference.advance_event_time(watermark).unwrap())
                    );
                    let mut timestamps = vec![base, base + 10000, base];
                    for _ in 0..61 {
                        random = random.wrapping_mul(6364136223846793005).wrapping_add(1);
                        timestamps.push(base + (((random >> 32) % 51) as i64 - 10) * 1000);
                    }
                    for times in timestamps.chunks(batch_size) {
                        let input = batch(times.to_vec());
                        target.process(&input).unwrap();
                        reference.process_arrow(input, 0).unwrap();
                        assert_eq!(
                            target.kernel.late_records_dropped,
                            reference.late_records_dropped
                        );
                    }
                }
                assert_eq!(
                    advance(&mut target, i64::MAX),
                    rows(&reference.advance_event_time(i64::MAX).unwrap())
                );
                drop(target);
                assert_eq!(broker.reserved(), 0);
            }
        }
    }
}

#[test]
fn canonical_restore_switches_backend_and_retains_flink_clock() {
    let directory = tempfile::tempdir().unwrap();
    for rocks in [false, true] {
        let broker = Arc::new(TestBroker::new(64 << 20));
        let mut before = processor(broker.clone(), rocks.then_some(directory.path()));
        before.process(&batch(vec![10000])).unwrap();
        advance(&mut before, 15000);
        let snapshots = (0..128)
            .map(|group| before.snapshot(group).unwrap())
            .collect::<Vec<_>>();
        drop(before);
        let target_directory = tempfile::tempdir().unwrap();
        let mut after = processor(broker.clone(), (!rocks).then_some(target_directory.path()));
        for (group, snapshot) in snapshots.iter().enumerate() {
            after.restore(group as u32, snapshot, 15000).unwrap();
        }
        after.process(&batch(vec![0])).unwrap();
        assert_eq!(advance(&mut after, 19999), vec![(2, 0, 20000)]);
        drop(after);
        drop(snapshots);
        assert_eq!(broker.reserved(), 0);
    }
}

#[test]
fn legacy_interval_list_migrates_to_ordered_point_entries_on_both_backends() {
    let mut legacy = super::super::tests::processor(&plan(), Arc::new(TestBroker::new(64 << 20)));
    legacy
        .process_arrow(batch(vec![10_000, 40_000]), 0)
        .unwrap();
    legacy.advance_event_time(15_000).unwrap();
    let snapshots = (0..128)
        .map(|group| legacy.snapshot_key_group(group).unwrap())
        .collect::<Vec<_>>();
    drop(legacy);
    for rocks in [false, true] {
        let directory = tempfile::tempdir().unwrap();
        let broker = Arc::new(TestBroker::new(64 << 20));
        let mut target = processor(broker.clone(), rocks.then_some(directory.path()));
        for (group, snapshot) in snapshots.iter().enumerate() {
            target.restore(group as u32, snapshot, 15_000).unwrap();
        }
        target.process(&batch(vec![0, 20_000, 30_000])).unwrap();
        assert_eq!(advance(&mut target, 49_999), vec![(5, 0, 50_000)]);
        for group in 0..128 {
            let snapshot = target.snapshot(group).unwrap();
            let entries = crate::state::decode_key_group_snapshot(group, &snapshot).unwrap();
            assert!(entries.iter().all(|(key, _)| key[0] == 0));
        }
        drop(target);
        assert_eq!(broker.reserved(), 0);
    }
}
