// Copyright 2026 StreamFusion Authors
// Licensed under the Apache License, Version 2.0.

//! Black-box lifecycle checks shared by stateful protobuf fixtures, independent of kernel APIs.
//! Rust heap observations are same-thread only; the independently loaded RocksDB plugin and
//! its C++ background threads still require the separate mixed-runtime allocation profile.

use super::*;
use crate::allocation_test_support::{current, measure};
use crate::planner::operators::envelope::{INPUT_ROW, OWNED_TIMESTAMP_V1, ROW_KIND};
use arrow::array::{
    ArrayRef, Int32Array, Int64Array, Int8Array, RecordBatch, StringArray, StructArray,
};
use arrow::datatypes::{DataType, Field, Schema};
use futures::StreamExt;

fn input(key: ArrayRef, payload_size: usize) -> RecordBatch {
    let count = key.len();
    let text = "p".repeat(payload_size);
    RecordBatch::try_new(
        Arc::new(Schema::new(vec![
            Field::new("key", key.data_type().clone(), true),
            Field::new("payload", DataType::Utf8, false),
            Field::new(OWNED_TIMESTAMP_V1, DataType::Int64, true),
            Field::new(ROW_KIND, DataType::Int8, false),
            Field::new(INPUT_ROW, DataType::Int32, false),
        ])),
        vec![
            key,
            Arc::new(StringArray::from(vec![text.as_str(); count])),
            Arc::new(Int64Array::from(vec![Some(i64::MIN); count])),
            Arc::new(Int8Array::from(vec![0; count])),
            Arc::new(Int32Array::from(vec![-1; count])),
        ],
    )
    .unwrap()
}

fn admitted(broker: &TestBroker, phase: &str) {
    // Diagnostic headroom permits bounded descriptors and control objects, while still
    // detecting substantial unaccounted state growth. Payload leases are checked separately.
    let observed = current();
    assert!(
        observed.live >= 0,
        "{phase}: invalid observation {observed:?}"
    );
    assert!(
        (observed.live as usize).saturating_sub(64 << 10) <= broker.reserved(),
        "{phase}: live Rust heap {observed:?}, Flink reservation {}",
        broker.reserved()
    );
}

fn drain(context: &Arc<NativeExecutionContext>, input: &RecordBatch) -> Vec<RecordBatch> {
    let mut stream = context.start(vec![input.clone()]).unwrap();
    let mut output = Vec::new();
    while let Some(batch) = context.runtime().block_on(stream.next()) {
        output.push(batch.unwrap());
    }
    context.require_idle().unwrap();
    output
}

fn bound(
    plan: &proto::NativePlan,
    bindings: &proto::NativeStateBindings,
    broker: &Arc<TestBroker>,
) -> Arc<NativeExecutionContext> {
    let mut context = context(plan, broker);
    install(&mut context, broker, &bindings.encode_to_vec()).unwrap();
    Arc::new(context)
}

/// Both source and restored contexts share a broker, as competing native owners in a Flink task.
/// Only the fixture varies: no specialized JNI bridge, state processor, or fusion-pair driver.
fn lifecycle(
    plan: &proto::NativePlan,
    source: &proto::NativeStateBindings,
    destination: &proto::NativeStateBindings,
    input: &RecordBatch,
) {
    let broker = Arc::new(TestBroker::new(LIMIT));
    // Initialize test-thread/Tokio/Arrow one-time caches outside task-lifetime observation.
    // A fresh task is measured below, including its constructor. This is not a cold-process
    // allocation profile; repeated task leaks still fail the exact-zero final assertion.
    let warm = bound(plan, &resources(), &broker);
    drop(drain(&warm, input));
    drop(warm);
    assert_eq!(broker.reserved(), 0);
    let (_, observed) = measure(|| {
        let original = bound(plan, source, &broker);
        admitted(&broker, "constructed");
        let first = drain(&original, input);
        assert_eq!(first.iter().map(RecordBatch::num_rows).sum::<usize>(), 3);
        admitted(&broker, "first output retained");
        let restored = bound(plan, destination, &broker);
        for group in 0..16 {
            let snapshot = original.snapshot_state(2, group).unwrap();
            admitted(&broker, "snapshot retained");
            restored.restore_state(2, group, &snapshot).unwrap();
            admitted(&broker, "restored");
        }
        let expected = drain(&original, input);
        let actual = drain(&restored, input);
        assert_eq!(actual, expected);
        assert_eq!(actual.iter().map(RecordBatch::num_rows).sum::<usize>(), 0);
        admitted(&broker, "continued after restore");
        drop(expected);
        drop(actual);
        drop(restored);
        drop(original);
        // Output ownership, not a live execution context, must keep the native lease charged.
        admitted(&broker, "output outlives both contexts");
        assert!(broker.reserved() > 0);
        drop(first);
        assert_eq!(broker.reserved(), 0);
    });
    assert_eq!(
        observed.live, 0,
        "all measured Rust owners must be released"
    );
}

#[test]
fn shared_stateful_lifecycle_covers_memory_ownership_and_cross_backend_restore() {
    let numeric: ArrayRef = Arc::new(Int64Array::from(vec![Some(1), Some(1), None, Some(3)]));
    let text: ArrayRef = Arc::new(StringArray::from(vec![
        Some("é"),
        Some("é"),
        None,
        Some("尾"),
    ]));
    let nested: ArrayRef = Arc::new(StructArray::from(vec![(
        Arc::new(Field::new("nested", DataType::Utf8, true)),
        text.clone(),
    )]));
    let plugin = std::env::var("STREAMFUSION_TEST_ROCKSDB_PLUGIN").ok();
    for plan in plans() {
        for key in [&numeric, &text, &nested] {
            for payload_size in [0, 16384] {
                let batch = input(key.clone(), payload_size);
                lifecycle(&plan, &resources(), &resources(), &batch);
                if let Some(plugin) = &plugin {
                    for rocks_source in [false, true] {
                        let directory = tempfile::tempdir().unwrap();
                        let mut rocks = resources();
                        rocks.bindings[0].backend =
                            Some(proto::native_state_binding::Backend::Rocksdb(
                                proto::NativeRocksDbState {
                                    plugin_path: plugin.clone(),
                                    database_path: directory.path().to_str().unwrap().into(),
                                    memory_limit: 4 << 20,
                                },
                            ));
                        let memory = resources();
                        let (source, destination) = if rocks_source {
                            (&rocks, &memory)
                        } else {
                            (&memory, &rocks)
                        };
                        lifecycle(&plan, source, destination, &batch);
                    }
                }
            }
        }
    }
}

#[test]
fn physical_context_does_not_populate_an_unused_sql_function_registry() {
    let plan = &plans()[0];
    let broker = Arc::new(TestBroker::new(LIMIT));
    drop(context(plan, &broker)); // Initialize the test worker's one-time thread-local state.
    for _ in 0..3 {
        let (_, observed) = measure(|| {
            let context = context(plan, &broker);
            let task = context.task_context();
            assert!(task.scalar_functions().is_empty());
            assert!(task.higher_order_functions().is_empty());
            assert!(task.aggregate_functions().is_empty());
            assert!(task.window_functions().is_empty());
            assert!(Arc::ptr_eq(task.memory_pool(), &context.memory_pool));
        });
        assert_eq!(observed.live, 0, "context retained heap: {observed:?}");
        assert_eq!(broker.reserved(), 0);
    }
}
