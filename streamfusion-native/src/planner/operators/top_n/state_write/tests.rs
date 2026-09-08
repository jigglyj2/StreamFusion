// Copyright 2026 StreamFusion Authors
// Licensed under the Apache License, Version 2.0.
use super::*;
use crate::memory_pool::tests_support::TestBroker;
use crate::planner::operators::envelope::{INPUT_ROW, ROW_KIND};
use crate::planner::persistent::unary::UnaryBatchProcessor;
use crate::state::observed_tests::{Io, Observed};
use std::sync::atomic::Ordering as AtomicOrdering;

fn input(keys: Vec<i32>, values: Vec<&str>) -> RecordBatch {
    let batch = super::super::tests::batch(keys, values);
    let mut fields = batch.schema().fields().to_vec();
    fields
        .last_mut()
        .unwrap()
        .clone_from(&Arc::new(Field::new(ROW_KIND, DataType::Int8, false)));
    fields.push(Arc::new(Field::new(INPUT_ROW, DataType::Int32, false)));
    let mut columns = batch.columns().to_vec();
    columns.push(Arc::new(Int32Array::from_iter_values(
        0..batch.num_rows() as i32,
    )));
    RecordBatch::try_new(Arc::new(Schema::new(fields)), columns).unwrap()
}

fn processor(
    owner: &HostMemoryReservation,
    io: &Arc<Io>,
    state: Option<Box<dyn KeyedState>>,
) -> TopNProcessor {
    let observed = Observed {
        inner: state.unwrap_or_else(|| {
            Box::new(OrderedMemoryKeyedState::new(0, 127, owner.sibling("state")).unwrap())
        }),
        io: io.clone(),
    };
    TopNProcessor::with_state_with_range(
        &super::super::tests::top_one_plan(true),
        128,
        0,
        127,
        Box::new(observed),
        owner.sibling("Top-1 processor"),
    )
    .unwrap()
}

#[test]
fn point_state_batches_reads_and_writes_only_final_changed_winners() {
    let plugin = std::env::var("STREAMFUSION_TEST_ROCKSDB_PLUGIN").ok();
    for rocks in [false, true] {
        if rocks && plugin.is_none() {
            continue;
        }
        let directory = tempfile::tempdir().unwrap();
        let broker = Arc::new(TestBroker::new(64 << 20));
        let owner = HostMemoryReservation::new(broker.clone(), "point Top-1 test");
        let io = Arc::new(Io::default());
        let state = if rocks {
            Some(Box::new(
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
            ) as Box<dyn KeyedState>)
        } else {
            None
        };
        let mut p = processor(&owner, &io, state);
        let wide = "z".repeat(16_000);
        let first = input((0..128).collect(), vec![&wide; 128]);
        p.prepare_output_schema(first.schema()).unwrap();
        assert_eq!(p.process_batch(first).unwrap().num_rows(), 128);
        assert_eq!(io.read_batches.load(AtomicOrdering::Relaxed), 1);
        assert_eq!(io.write_batches.load(AtomicOrdering::Relaxed), 1);
        assert_eq!(io.range_reads.load(AtomicOrdering::Relaxed), 0);
        io.reset();
        // Losing arrivals neither rewrite the wide payload nor bump a persisted counter.
        assert_eq!(
            p.process_batch(input((0..128).collect(), vec!["a"; 128]))
                .unwrap()
                .num_rows(),
            0
        );
        assert_eq!(io.read_batches.load(AtomicOrdering::Relaxed), 1);
        assert_eq!(io.write_batches.load(AtomicOrdering::Relaxed), 0);
        assert_eq!(io.written_bytes.load(AtomicOrdering::Relaxed), 0);
        assert_eq!(io.range_reads.load(AtomicOrdering::Relaxed), 0);
        io.reset();
        let best = "zz".repeat(16_000);
        let output = p
            .process_batch(input(vec![3, 3, 4], vec![&best, "a", "b"]))
            .unwrap();
        assert_eq!(output.num_rows(), 2);
        assert_eq!(io.read_batches.load(AtomicOrdering::Relaxed), 1);
        assert_eq!(io.write_batches.load(AtomicOrdering::Relaxed), 1);
        assert_eq!(io.range_reads.load(AtomicOrdering::Relaxed), 0);
        // Arrow variable-width row encoding includes block markers and padding.
        assert!(io.written_bytes.load(AtomicOrdering::Relaxed) < best.len() * 2);
        drop(output);
        drop(p);
        drop(owner);
        assert_eq!(broker.reserved(), 0);
    }
}

#[test]
fn old_flat_and_ordered_winners_restore_and_migrate_without_changelog_changes() {
    for indexed in [false, true] {
        let owner =
            HostMemoryReservation::new(Arc::new(TestBroker::new(64 << 20)), "Top-1 migration test");
        let mut old = TopNProcessor::new(
            &super::super::tests::top_one_plan(true),
            128,
            0,
            127,
            owner.sibling("old"),
        )
        .unwrap();
        if !indexed {
            old.sort_keys = None;
        }
        old.process_arrow(super::super::tests::batch(vec![1, 2], vec!["z", "y"]), 0)
            .unwrap();
        let snapshots = (0..128)
            .map(|g| old.snapshot_key_group(g).unwrap())
            .collect::<Vec<_>>();
        drop(old);
        let io = Arc::new(Io::default());
        let mut p = processor(&owner, &io, None);
        for (group, snapshot) in snapshots.iter().enumerate() {
            p.restore_key_group(group as u32, snapshot).unwrap();
        }
        let initial = input(vec![1, 2], vec!["a", "b"]);
        p.prepare_output_schema(initial.schema()).unwrap();
        assert_eq!(p.process_batch(initial).unwrap().num_rows(), 0);
        assert_eq!(io.read_batches.load(AtomicOrdering::Relaxed), 1);
        assert_eq!(
            io.range_reads.load(AtomicOrdering::Relaxed),
            if indexed { 2 } else { 0 }
        );
        assert_eq!(
            io.write_batches.load(AtomicOrdering::Relaxed),
            usize::from(indexed)
        );
        io.reset();
        let next = p.process_batch(input(vec![1, 2], vec!["zz", "a"])).unwrap();
        assert_eq!(next.num_rows(), 2);
        let values = next
            .column(1)
            .as_any()
            .downcast_ref::<arrow::array::StringArray>()
            .unwrap();
        assert_eq!(
            values.iter().collect::<Vec<_>>(),
            vec![Some("z"), Some("zz")]
        );
        assert_eq!(io.range_reads.load(AtomicOrdering::Relaxed), 0);
        let mut entries = 0;
        for group in 0..128 {
            p.state
                .visit_key_group(group, 256, 1 << 20, &mut |page| {
                    entries += page.len();
                    Ok(())
                })
                .unwrap();
        }
        assert_eq!(entries, 2, "one point value per partition after migration");
    }
}
