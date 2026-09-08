// Copyright 2026 StreamFusion Authors
// Licensed under the Apache License, Version 2.0.

//! The same recursive tree wires a task-local producer through Calc into keyed state.
//! No factory in this test recognizes the neighboring operator family.
use super::*;

fn composed_plan() -> proto::NativePlan {
    let mut plan = tree(100_000, true);
    let local = crate::planner::persistent::find_unique(&plan, |node| {
        matches!(
            node.operator,
            Some(proto::operator::Operator::LocalGroupAggregate(_))
        )
    })
    .unwrap();
    let Some(proto::operator::Operator::LocalGroupAggregate(local)) = &local.operator else {
        unreachable!()
    };
    let global = proto::GlobalGroupAggregate {
        input: plan.root.clone().map(Box::new),
        grouping_indices: vec![0],
        aggregate_calls: local.aggregate_calls.clone(),
        generate_update_before: true,
        mini_batch_size: 100_000,
        input_schema: local.output_schema.clone(),
        output_schema: Some(schema(&[
            ("key", logical_bigint(false)),
            ("count", logical_bigint(false)),
            ("sum", logical_bigint(true)),
        ])),
        bounded_final_output: false,
    };
    plan.root = Some(calc(
        6,
        proto::Operator {
            plan_node_id: 5,
            operator: Some(proto::operator::Operator::GlobalGroupAggregate(Box::new(
                global,
            ))),
            ..Default::default()
        },
        3,
    ));
    plan
}

#[test]
fn non_keyed_and_keyed_factories_share_tree_controls_metrics_and_recovery() {
    let plugin = std::env::var("STREAMFUSION_TEST_ROCKSDB_PLUGIN").ok();
    let mut snapshots: Vec<Vec<u8>> = Vec::new();
    for rocks in [false, true] {
        if rocks && plugin.is_none() {
            continue;
        }
        let directory = tempfile::tempdir().unwrap();
        let (broker, memory) = memory();
        let mut context = NativeExecutionContext::new(
            &composed_plan().encode_to_vec(),
            memory.datafusion_pool(256 << 20),
        )
        .unwrap();
        let bindings = proto::NativeStateBindings {
            protocol_version: 1,
            bindings: vec![proto::NativeStateBinding {
                restored_watermark: None,
                plan_node_id: 5,
                max_parallelism: 16,
                first_key_group: 0,
                last_key_group: 15,
                backend: Some(if rocks {
                    proto::native_state_binding::Backend::Rocksdb(proto::NativeRocksDbState {
                        log_directory: None,
                        plugin_path: plugin.as_ref().unwrap().clone(),
                        database_path: directory.path().join("db").to_str().unwrap().into(),
                        memory_limit: 1 << 20,
                    })
                } else {
                    proto::native_state_binding::Backend::Memory(proto::NativeMemoryState {})
                }),
            }],
        };
        context
            .install_state(&bindings.encode_to_vec(), memory.sibling("global state"))
            .unwrap();
        assert!(context
            .install_state(&bindings.encode_to_vec(), memory.sibling("duplicate"))
            .is_err());
        let context = Arc::new(context);
        let (wire, credit) = context.control_capabilities().unwrap();
        let capabilities = proto::NativeControlCapabilities::decode(wire.as_slice()).unwrap();
        assert_eq!(
            capabilities
                .stages
                .iter()
                .map(|stage| stage.plan_node_id)
                .collect::<Vec<_>>(),
            vec![3, 5]
        );
        drop((wire, credit));
        for (group, snapshot) in snapshots.iter().enumerate() {
            context.restore_state(5, group as u32, snapshot).unwrap();
        }
        let kind = if snapshots.is_empty() { 0 } else { 3 };
        let input = input((0..5000).collect(), (0..5000).collect(), vec![kind; 5000]);
        drop(run(&context, input.clone(), &[]));
        assert_eq!(
            context.gauge_snapshot().unwrap().0,
            vec![5000, 1.0f64.to_bits() as i64, 0, 0]
        );
        // Parent listed first on purpose. The physical stream still drains the child first.
        let output = run(
            &context,
            input.slice(0, 0),
            &[
                (5, ControlEvent::BeforeCheckpoint(7)),
                (3, ControlEvent::BeforeCheckpoint(7)),
            ],
        );
        assert_eq!(
            output.iter().map(RecordBatch::num_rows).sum::<usize>(),
            5000
        );
        assert!(output.iter().all(|batch| batch.num_rows() <= 4096));
        for batch in &output {
            let keys = batch
                .column(0)
                .as_any()
                .downcast_ref::<Int64Array>()
                .unwrap();
            let counts = batch
                .column(1)
                .as_any()
                .downcast_ref::<Int64Array>()
                .unwrap();
            let sums = batch
                .column(2)
                .as_any()
                .downcast_ref::<Int64Array>()
                .unwrap();
            let kinds = batch
                .column(4)
                .as_any()
                .downcast_ref::<Int8Array>()
                .unwrap();
            for row in 0..batch.num_rows() {
                assert_eq!(counts.value(row), 1);
                assert_eq!(sums.value(row), keys.value(row));
                assert_eq!(kinds.value(row), kind);
            }
        }
        assert_eq!(
            context.metric_snapshot().unwrap(),
            vec![
                6, 5000, 5000, 5, 5000, 5000, 4, 5000, 5000, 3, 5000, 5000, 2, 5000, 5000, 1, 0,
                5000
            ]
        );
        assert_eq!(context.gauge_snapshot().unwrap().0, vec![0, 0, 0, 0]);
        snapshots = (0..16)
            .map(|group| context.snapshot_state(5, group).unwrap().to_vec())
            .collect();
        assert_eq!(broker.transfers.load(Ordering::Relaxed), 0);
        drop(context);
        assert!(
            broker.inner.reserved() > 0,
            "returned Arrow buffers still own their native credit"
        );
        drop(output);
        assert_eq!(broker.inner.reserved(), 0);
    }
}
