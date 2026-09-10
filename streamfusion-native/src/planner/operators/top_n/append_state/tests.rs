// Copyright 2026 StreamFusion Authors
// Licensed under the Apache License, Version 2.0.
use super::*;
use crate::memory_pool::tests_support::TestBroker;
use crate::planner::operators::envelope::{INPUT_ROW, ROW_KIND};
use crate::planner::persistent::unary::UnaryBatchProcessor;
use crate::state::observed_tests::{Io, Observed};
use prost::Message;
use std::sync::atomic::Ordering as AtomicOrdering;

fn plan() -> Vec<u8> {
    let mut p = proto::NativePlan::decode(super::super::tests::plan().as_slice()).unwrap();
    let Some(proto::operator::Operator::TopN(top)) =
        p.root.as_mut().and_then(|root| root.operator.as_mut())
    else {
        unreachable!()
    };
    top.rank_end = Some(4);
    p.encode_to_vec()
}
fn input(keys: Vec<i32>, values: Vec<&str>) -> RecordBatch {
    let batch = super::super::tests::batch(keys, values);
    let mut fields = batch.schema().fields().to_vec();
    *fields.last_mut().unwrap() = Arc::new(Field::new(ROW_KIND, DataType::Int8, false));
    fields.push(Arc::new(Field::new(INPUT_ROW, DataType::Int32, false)));
    let mut columns = batch.columns().to_vec();
    columns.push(Arc::new(Int32Array::from_iter_values(
        0..batch.num_rows() as i32,
    )));
    RecordBatch::try_new(Arc::new(Schema::new(fields)), columns).unwrap()
}
fn processor(
    owner: &HostMemoryReservation,
    io: Arc<Io>,
    state: Box<dyn KeyedState>,
) -> TopNProcessor {
    TopNProcessor::with_state_with_range(
        &plan(),
        128,
        0,
        127,
        Box::new(Observed { inner: state, io }),
        owner.sibling("append processor"),
    )
    .unwrap()
}

#[test]
fn shared_ordered_state_writes_only_changed_candidates_and_preserves_losing_snapshots() {
    let plugin = std::env::var("STREAMFUSION_TEST_ROCKSDB_PLUGIN").ok();
    for rocks in [false, true] {
        if rocks && plugin.is_none() {
            continue;
        }
        let directory = tempfile::tempdir().unwrap();
        let broker = Arc::new(TestBroker::new(64 << 20));
        let owner = HostMemoryReservation::new(broker.clone(), "append delta test");
        let io = Arc::new(Io::default());
        let state: Box<dyn KeyedState> = if rocks {
            Box::new(
                RocksPluginKeyedState::open_configured(
                    &proto::NativeRocksDbState {
                        plugin_path: plugin.clone().unwrap(),
                        database_path: directory.path().to_str().unwrap().into(),
                        memory_limit: 8 << 20,
                        log_directory: None,
                    },
                    0,
                    127,
                    Some(&owner),
                )
                .unwrap(),
            )
        } else {
            Box::new(OrderedMemoryKeyedState::new(0, 127, owner.sibling("state")).unwrap())
        };
        let mut p = processor(&owner, io.clone(), state);
        let values = (0..128)
            .map(|row| format!("y{}{}", row % 4, "x".repeat(4096)))
            .collect::<Vec<_>>();
        let first = input(
            (0..128).map(|row| row / 4).collect(),
            values.iter().map(String::as_str).collect(),
        );
        p.prepare_output_schema(first.schema()).unwrap();
        drop(p.process_batch(first).unwrap());
        assert_eq!(io.read_batches.load(AtomicOrdering::Relaxed), 1);
        assert_eq!(io.write_batches.load(AtomicOrdering::Relaxed), 1);
        let snapshots = (0..128)
            .map(|g| p.snapshot_key_group(g).unwrap())
            .collect::<Vec<_>>();
        io.reset();
        assert_eq!(
            p.process_batch(input((0..32).collect(), vec!["a"; 32]))
                .unwrap()
                .num_rows(),
            0
        );
        assert_eq!(io.read_batches.load(AtomicOrdering::Relaxed), 1);
        assert_eq!(io.range_reads.load(AtomicOrdering::Relaxed), 32);
        assert_eq!(io.write_batches.load(AtomicOrdering::Relaxed), 0);
        for (g, snapshot) in snapshots.iter().enumerate() {
            assert_eq!(
                p.snapshot_key_group(g as u32).unwrap().as_ref(),
                snapshot.as_ref()
            );
        }
        io.reset();
        let best = format!("zz{}", "x".repeat(4096));
        let result = p
            .process_batch(input(vec![3, 3, 4], vec![&best, "a", "b"]))
            .unwrap();
        assert_eq!(result.num_rows(), 8);
        assert_eq!(io.read_batches.load(AtomicOrdering::Relaxed), 1);
        assert_eq!(io.write_batches.load(AtomicOrdering::Relaxed), 1);
        assert!(
            io.written_bytes.load(AtomicOrdering::Relaxed) < best.len() * 4,
            "unchanged wide payloads must not be rewritten"
        );
        drop(result);
        drop(snapshots);
        drop(p);
        drop(owner);
        assert_eq!(broker.reserved(), 0);
    }
}

#[test]
fn flat_candidates_migrate_to_ordered_entries_on_a_losing_batch() {
    let owner = HostMemoryReservation::new(Arc::new(TestBroker::new(64 << 20)), "append migration");
    let mut old = TopNProcessor::new(&plan(), 128, 0, 127, owner.sibling("old")).unwrap();
    old.sort_keys = None;
    drop(
        old.process_arrow(
            super::super::tests::batch(vec![1, 1, 1, 1], vec!["z", "y", "x", "w"]),
            0,
        )
        .unwrap(),
    );
    let snapshots = (0..128)
        .map(|g| old.snapshot_key_group(g).unwrap())
        .collect::<Vec<_>>();
    drop(old);
    let io = Arc::new(Io::default());
    let state = Box::new(OrderedMemoryKeyedState::new(0, 127, owner.sibling("new state")).unwrap());
    let mut p = processor(&owner, io.clone(), state);
    for (g, snapshot) in snapshots.iter().enumerate() {
        p.restore_key_group(g as u32, snapshot).unwrap();
    }
    let first = input(vec![1], vec!["a"]);
    p.prepare_output_schema(first.schema()).unwrap();
    assert_eq!(p.process_batch(first).unwrap().num_rows(), 0);
    assert_eq!(io.write_batches.load(AtomicOrdering::Relaxed), 1);
    let mut entries = 0;
    for g in 0..128 {
        p.state
            .visit_key_group(g, 256, 1 << 20, &mut |page| {
                entries += page.len();
                Ok(())
            })
            .unwrap();
    }
    assert_eq!(entries, 5, "four ordered candidates and one metadata value");
    io.reset();
    assert_eq!(
        p.process_batch(input(vec![1], vec!["w"]))
            .unwrap()
            .num_rows(),
        0
    );
    assert_eq!(io.write_batches.load(AtomicOrdering::Relaxed), 0);
    assert_eq!(
        p.process_batch(input(vec![1], vec!["zz"]))
            .unwrap()
            .num_rows(),
        8
    );
}

#[test]
fn small_arrivals_cannot_bypass_retained_history_workspace_admission() {
    let plugin = std::env::var("STREAMFUSION_TEST_ROCKSDB_PLUGIN").ok();
    for rocks in [false, true] {
        if rocks && plugin.is_none() {
            continue;
        }
        let directory = tempfile::tempdir().unwrap();
        let broker = Arc::new(TestBroker::new(64 << 20));
        let owner = HostMemoryReservation::new(broker.clone(), "retained append history");
        let io = Arc::new(Io::default());
        let state: Box<dyn KeyedState> = if rocks {
            Box::new(
                RocksPluginKeyedState::open_configured(
                    &proto::NativeRocksDbState {
                        plugin_path: plugin.clone().unwrap(),
                        database_path: directory.path().to_str().unwrap().into(),
                        memory_limit: 8 << 20,
                        log_directory: None,
                    },
                    0,
                    127,
                    Some(&owner),
                )
                .unwrap(),
            )
        } else {
            Box::new(OrderedMemoryKeyedState::new(0, 127, owner.sibling("state")).unwrap())
        };
        let mut p = processor(&owner, io.clone(), state);
        let values = (0..4)
            .map(|i| format!("z{i}{}", "x".repeat(128 << 10)))
            .collect::<Vec<_>>();
        let first = input(vec![1; 4], values.iter().map(String::as_str).collect());
        p.prepare_output_schema(first.schema()).unwrap();
        drop(p.process_batch(first).unwrap());
        io.reset();
        // Another consumer leaves enough for the tiny input, but not the old wide candidates.
        let mut competing = owner.sibling("other managed consumer");
        competing
            .resize((64 << 20) - broker.reserved() - (4 << 20))
            .unwrap();
        let failure = p.process_batch(input(vec![1], vec!["a"])).unwrap_err();
        assert!(
            matches!(failure, DataFusionError::ResourcesExhausted(_)),
            "{failure}"
        );
        assert_eq!(io.read_batches.load(AtomicOrdering::Relaxed), 1);
        assert_eq!(io.write_batches.load(AtomicOrdering::Relaxed), 0);
        drop(competing);
        drop(p);
        drop(owner);
        assert_eq!(broker.reserved(), 0);
    }
}
