// Copyright 2026 StreamFusion Authors
// Licensed under the Apache License, Version 2.0.

use super::*;
use crate::state::observed_tests::{Io, Observed};
use arrow::array::StringArray;
use std::sync::atomic::Ordering;

fn wide_plan() -> Vec<u8> {
    let mut native = proto::NativePlan::decode(plan().as_slice()).unwrap();
    let Some(proto::operator::Operator::WindowAggregate(window)) =
        &mut native.root.as_mut().unwrap().operator
    else {
        unreachable!()
    };
    let string = proto::LogicalType {
        nullable: false,
        r#type: Some(proto::logical_type::Type::Varchar(proto::EmptyType {})),
    };
    window
        .input_schema
        .as_mut()
        .unwrap()
        .fields
        .push(proto::Field {
            name: "value".into(),
            r#type: Some(string.clone()),
        });
    window.output_schema.as_mut().unwrap().fields[1].r#type = Some(string.clone());
    window.aggregate_calls[0].function = proto::AggregateFunction::Min as i32;
    window.aggregate_calls[0].input_index = Some(2);
    window.aggregate_calls[0].input_type = Some(string.clone());
    window.aggregate_calls[0].output_type = Some(string);
    native.encode_to_vec()
}

fn wide_input(count: usize, width: usize) -> RecordBatch {
    RecordBatch::try_from_iter(vec![
        (
            "key",
            Arc::new(Int64Array::from(vec![1; count])) as ArrayRef,
        ),
        (
            "ts",
            Arc::new(TimestampMillisecondArray::from_iter_values(
                (0..count).map(|i| 10000 + i as i64 * 20000),
            )) as ArrayRef,
        ),
        (
            "value",
            Arc::new(StringArray::from_iter_values(
                (0..count).map(|i| format!("{i:04}-{}", "é".repeat(width / 2))),
            )) as ArrayRef,
        ),
    ])
    .unwrap()
}

fn target(
    plan: &[u8],
    broker: Arc<TestBroker>,
    rocks: Option<&std::path::Path>,
) -> (SharedSessions, Arc<Io>) {
    let owner = HostMemoryReservation::new(broker, "session migration target");
    let state: Box<dyn KeyedState> = if let Some(path) = rocks {
        Box::new(
            RocksPluginKeyedState::open_for_owner(
                std::path::Path::new(&std::env::var("STREAMFUSION_TEST_ROCKSDB_PLUGIN").unwrap()),
                path,
                0,
                127,
                1 << 20,
                &owner,
            )
            .unwrap(),
        )
    } else {
        Box::new(
            crate::state::OrderedMemoryKeyedState::new(0, 127, owner.sibling("state")).unwrap(),
        )
    };
    let io = Arc::new(Io::default());
    let state = Box::new(Observed {
        inner: state,
        io: io.clone(),
    });
    let kernel = WindowAggregateProcessor::with_state(
        plan,
        128,
        0,
        127,
        state,
        owner.sibling("timers"),
        owner,
    )
    .unwrap();
    (SharedSessions::new(kernel).unwrap(), io)
}

fn source(plan: &[u8], input: &RecordBatch) -> (u32, crate::state::SnapshotBytes) {
    let mut source =
        super::super::super::tests::processor(plan, Arc::new(TestBroker::new(512 << 20)));
    source.process_arrow(input.clone(), 0).unwrap();
    (0..128)
        .map(|group| (group, source.snapshot_key_group(group).unwrap()))
        .max_by_key(|(_, value)| value.len())
        .unwrap()
}

fn physical(bytes: &[u8], group: u32, root: &std::path::Path) -> RocksPluginKeyedState {
    let plugin = std::env::var("STREAMFUSION_TEST_ROCKSDB_PLUGIN").unwrap();
    let mut source = RocksPluginKeyedState::open(
        std::path::Path::new(&plugin),
        &root.join("source"),
        0,
        127,
        1 << 20,
    )
    .unwrap();
    let owner = HostMemoryReservation::new(Arc::new(TestBroker::new(128 << 20)), "legacy fixture");
    source.restore_key_group(group, bytes, &owner).unwrap();
    source.checkpoint(&root.join("checkpoint")).unwrap();
    drop(source);
    RocksPluginKeyedState::open_checkpoint(
        std::path::Path::new(&plugin),
        &root.join("checkpoint"),
        0,
        127,
        1 << 20,
    )
    .unwrap()
}

#[test]
fn wide_legacy_migration_writes_only_current_records_with_bounded_scratch() {
    let Ok(_) = std::env::var("STREAMFUSION_TEST_ROCKSDB_PLUGIN") else {
        return;
    };
    let plan = wide_plan();
    let input = wide_input(1024, 8192);
    let (group, legacy) = source(&plan, &input);
    assert!(legacy.len() > 8 << 20);
    let (mut reference, _) = target(&plan, Arc::new(TestBroker::new(512 << 20)), None);
    reference.process(&input).unwrap();
    let expected = reference.snapshot(group).unwrap();
    let written = streamfusion_state_abi::key_group_snapshot_entries(group, &expected)
        .unwrap()
        .map(|(k, v)| k.len() + v.len())
        .sum::<usize>();
    for physical_input in [false, true] {
        let directory = tempfile::tempdir().unwrap();
        let reader = physical(&legacy, group, directory.path());
        let frame_bytes = if physical_input { 0 } else { legacy.len() };
        let broker = Arc::new(TestBroker::new((4 << 20) + frame_bytes));
        let (mut actual, io) = target(
            &plan,
            broker.clone(),
            Some(&directory.path().join("target")),
        );
        let mut input_memory = actual
            .kernel
            .scratch_reservation
            .sibling("source cache or canonical input");
        input_memory
            .resize(if physical_input { 1 << 20 } else { frame_bytes })
            .unwrap();
        let ((), observed) = crate::allocation_test_support::measure(|| {
            if physical_input {
                actual.restore_physical(group, &reader, i64::MIN).unwrap();
            } else {
                actual.restore(group, &legacy, i64::MIN).unwrap();
            }
        });
        assert!(observed.peak < 3 << 20, "{observed:?}");
        assert_eq!(
            io.written_bytes.load(Ordering::Relaxed),
            written,
            "write each current record once, without importing and deleting legacy records"
        );
        assert!(io.write_batches.load(Ordering::Relaxed) > 1);
        let mut offset = 0;
        actual
            .write_snapshot(group, &mut |bytes| {
                if offset == 0 {
                    assert_eq!(bytes, (expected.len() as i32).to_be_bytes());
                } else {
                    assert_eq!(bytes, &expected[offset - 4..offset - 4 + bytes.len()]);
                }
                offset += bytes.len();
                Ok(())
            })
            .unwrap();
        assert_eq!(offset, expected.len() + 4);
        drop((actual, input_memory, reader));
        assert_eq!(broker.reserved(), 0);
    }
}

#[test]
fn legacy_physical_and_unordered_canonical_restore_preserve_merges_on_both_backends() {
    let Ok(_) = std::env::var("STREAMFUSION_TEST_ROCKSDB_PLUGIN") else {
        return;
    };
    let input = batch(vec![10000, 40000, 80000, 80000]);
    let (group, legacy) = source(&plan(), &input);
    let mut entries = crate::state::decode_key_group_snapshot(group, &legacy).unwrap();
    entries.reverse();
    let unordered = streamfusion_state_abi::encode_key_group_snapshot(
        group,
        entries.iter().map(|(k, v)| (k.as_slice(), v.as_slice())),
    )
    .unwrap();
    let directory = tempfile::tempdir().unwrap();
    let reader = physical(&legacy, group, directory.path());
    let (mut reference, _) = target(&plan(), Arc::new(TestBroker::new(64 << 20)), None);
    reference.process(&input).unwrap();
    let expected = reference.snapshot(group).unwrap();
    for rocks in [false, true] {
        for from_physical in [false, true] {
            let directory = tempfile::tempdir().unwrap();
            let broker = Arc::new(TestBroker::new(64 << 20));
            let (mut actual, _) =
                target(&plan(), broker.clone(), rocks.then_some(directory.path()));
            if from_physical {
                actual.restore_physical(group, &reader, 15000).unwrap();
            } else {
                actual.restore(group, &unordered, 15000).unwrap();
            }
            assert_eq!(actual.snapshot(group).unwrap(), expected);
            actual
                .process(&batch(vec![0, 20000, 30000, 120000]))
                .unwrap();
            assert_eq!(
                advance(&mut actual, i64::MAX),
                [(5, 0, 50000), (2, 80000, 90000), (1, 120000, 130000)]
            );
            drop(actual);
            assert_eq!(broker.reserved(), 0);
        }
    }
}

#[test]
fn invalid_legacy_indexes_timers_and_events_fail_before_writing() {
    let input = batch(vec![10000, 40000]);
    let (group, legacy) = source(&plan(), &input);
    let original = crate::state::decode_key_group_snapshot(group, &legacy).unwrap();
    let window = original
        .iter()
        .position(|(key, _)| key.first() == Some(&WINDOW_KEY_PREFIX))
        .unwrap();
    let timer = original
        .iter()
        .position(|(key, _)| key == TIMER_STATE_KEY)
        .unwrap();
    let index = original
        .iter()
        .position(|(key, _)| key.first() == Some(&SESSION_INDEX_PREFIX))
        .unwrap();
    let mut cases = Vec::new();
    let mut bad = original.clone();
    bad.remove(timer);
    cases.push(bad);
    let mut bad = original.clone();
    bad[timer].1.pop();
    cases.push(bad);
    let mut bad = original.clone();
    bad[index].1 = encode_session_index(&[]);
    cases.push(bad);
    let mut bad = original.clone();
    bad[window].1[13..17].copy_from_slice(&u32::MAX.to_le_bytes());
    cases.push(bad);
    let mut bad = original.clone();
    bad.push(bad[window].clone());
    cases.push(bad);
    for entries in cases {
        let broker = Arc::new(TestBroker::new(16 << 20));
        let (mut actual, io) = target(&plan(), broker.clone(), None);
        let bytes = streamfusion_state_abi::encode_key_group_snapshot(
            group,
            entries.iter().map(|(k, v)| (k.as_slice(), v.as_slice())),
        )
        .unwrap();
        assert!(actual.restore(group, &bytes, 15000).is_err());
        assert_eq!(io.write_batches.load(Ordering::Relaxed), 0);
        assert!(actual.next_timer().is_none());
        actual.restore(group, &legacy, 15000).unwrap();
        assert_eq!(
            advance(&mut actual, i64::MAX),
            [(1, 10000, 20000), (1, 40000, 50000)]
        );
        drop(actual);
        assert_eq!(broker.reserved(), 0);
    }
}

#[test]
fn partial_migration_write_failure_requires_a_new_context() {
    let plan = wide_plan();
    let (group, legacy) = source(&plan, &wide_input(8, 128 << 10));
    for rocks in [false, true] {
        if rocks && std::env::var("STREAMFUSION_TEST_ROCKSDB_PLUGIN").is_err() {
            continue;
        }
        let directory = tempfile::tempdir().unwrap();
        let broker = Arc::new(TestBroker::new(32 << 20));
        let (mut actual, io) = target(&plan, broker.clone(), rocks.then_some(directory.path()));
        io.fail_write_batch.store(2, Ordering::Relaxed);
        assert!(actual
            .restore(group, &legacy, i64::MIN)
            .unwrap_err()
            .to_string()
            .contains("injected state write failure"));
        assert_eq!(io.write_batches.load(Ordering::Relaxed), 2);
        assert!(actual.snapshot(group).is_err());
        assert!(actual.restore(group, &legacy, i64::MIN).is_err());
        assert!(actual.advance(i64::MAX).is_err());
        assert!(actual.process(&wide_input(1, 1)).is_err());
        drop(actual);
        assert_eq!(broker.reserved(), 0);
        let fresh_directory = tempfile::tempdir().unwrap();
        let (mut recovered, _) = target(
            &plan,
            broker.clone(),
            rocks.then_some(fresh_directory.path()),
        );
        recovered.restore(group, &legacy, i64::MIN).unwrap();
        let mut rows = 0;
        while recovered.next_timer().is_some() {
            rows += recovered.advance(i64::MAX).unwrap().num_rows();
        }
        assert_eq!(rows, 8);
        drop(recovered);
        assert_eq!(broker.reserved(), 0);
    }
}
