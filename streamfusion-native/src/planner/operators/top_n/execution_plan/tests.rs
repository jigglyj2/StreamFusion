// Copyright 2026 StreamFusion Authors
// Licensed under the Apache License, Version 2.0.
use super::*;
use crate::execution_context::NativeExecutionContext;
use crate::memory_pool::{tests_support::TestBroker, MemoryReservationBroker};
use arrow::array::StringArray;
use futures::StreamExt;
use prost::Message;
use std::sync::atomic::{AtomicUsize, Ordering};

fn calc(id: u64, child: proto::Operator) -> proto::Operator {
    proto::Operator {
        plan_node_id: id,
        operator: Some(proto::operator::Operator::Calc(Box::new(proto::Calc {
            input: Some(Box::new(child)),
            preserve_input_envelope: true,
            projections: (0..2)
                .map(|index| proto::Expression {
                    expression: Some(proto::expression::Expression::InputReference(
                        proto::InputReference {
                            index,
                            r#type: None,
                        },
                    )),
                })
                .collect(),
            condition: None,
        }))),
        ..Default::default()
    }
}
fn plan(owned: bool) -> proto::NativePlan {
    let mut plan =
        proto::NativePlan::decode(super::super::tests::top_one_plan(true).as_slice()).unwrap();
    plan.protocol_version = if owned { 3 } else { 2 };
    let mut root = plan.root.take().unwrap();
    root.plan_node_id = 3;
    let Some(proto::operator::Operator::TopN(top)) = root.operator.as_mut() else {
        unreachable!()
    };
    top.input = Some(Box::new(calc(
        2,
        proto::Operator {
            plan_node_id: 1,
            operator: Some(proto::operator::Operator::Input(proto::Input::default())),
            ..Default::default()
        },
    )));
    top.rank_type = proto::TopNRankType::RowNumber as i32;
    plan.root = Some(calc(4, root));
    plan
}
fn resources(rocks: Option<(&str, &std::path::Path)>) -> proto::NativeStateBindings {
    proto::NativeStateBindings {
        protocol_version: 1,
        bindings: vec![proto::NativeStateBinding {
            plan_node_id: 3,
            max_parallelism: 16,
            first_key_group: 0,
            last_key_group: 15,
            restored_watermark: None,
            backend: Some(match rocks {
                None => proto::native_state_binding::Backend::Memory(proto::NativeMemoryState {}),
                Some((plugin, directory)) => {
                    proto::native_state_binding::Backend::Rocksdb(proto::NativeRocksDbState {
                        plugin_path: plugin.into(),
                        database_path: directory.to_str().unwrap().into(),
                        memory_limit: 8 << 20,
                        log_directory: None,
                    })
                }
            }),
        }],
    }
}
fn input(owned: bool, keys: Vec<i32>, values: Vec<&str>, times: Vec<Option<i64>>) -> RecordBatch {
    let count = keys.len();
    let mut fields = vec![
        Field::new("key", DataType::Int32, false),
        Field::new("value", DataType::Utf8, true),
    ];
    let mut columns: Vec<ArrayRef> = vec![
        Arc::new(Int32Array::from(keys)),
        Arc::new(StringArray::from(values)),
    ];
    if owned {
        fields.push(Field::new(OWNED_TIMESTAMP_V1, DataType::Int64, true));
        columns.push(Arc::new(Int64Array::from(times)));
    }
    fields.extend([
        Field::new(ROW_KIND, DataType::Int8, false),
        Field::new(INPUT_ROW, DataType::Int32, false),
    ]);
    columns.push(Arc::new(Int8Array::from(vec![0; count])));
    columns.push(Arc::new(Int32Array::from_iter_values(
        (0..count).map(|row| if owned { -1 } else { row as i32 + 17 }),
    )));
    RecordBatch::try_new(Arc::new(Schema::new(fields)), columns).unwrap()
}
#[derive(Debug)]
struct Broker {
    inner: TestBroker,
    transfers: AtomicUsize,
}
impl MemoryReservationBroker for Broker {
    fn try_reserve(&self, bytes: usize) -> Result<bool> {
        self.inner.try_reserve(bytes)
    }
    fn release(&self, bytes: usize) -> Result<()> {
        self.inner.release(bytes)
    }
    fn available(&self) -> Result<Option<usize>> {
        self.inner.available()
    }
    fn transfer_to_arrow(&self, bytes: usize) -> Result<()> {
        self.transfers.fetch_add(1, Ordering::Relaxed);
        self.inner.release(bytes)
    }
}
fn context(
    plan: &proto::NativePlan,
    resources: &proto::NativeStateBindings,
    owner: &HostMemoryReservation,
) -> Arc<NativeExecutionContext> {
    let mut context =
        NativeExecutionContext::new(&plan.encode_to_vec(), owner.datafusion_pool(64 << 20))
            .unwrap();
    context
        .install_state(&resources.encode_to_vec(), owner.sibling("Top-1 bindings"))
        .unwrap();
    Arc::new(context)
}
fn run(context: &Arc<NativeExecutionContext>, batch: RecordBatch) -> Vec<RecordBatch> {
    let mut stream = context.start(vec![batch]).unwrap();
    assert!(context.snapshot_state(3, 0).is_err());
    let mut output = Vec::new();
    while let Some(batch) = context.runtime().block_on(stream.next()) {
        output.push(batch.unwrap());
    }
    context.require_idle().unwrap();
    output
}
fn check(
    output: &[RecordBatch],
    kinds: &[i8],
    values: &[&str],
    times: Option<Vec<Option<i64>>>,
    ordinals: &[i32],
) {
    assert_eq!(output.len(), 1);
    let batch = &output[0];
    assert_eq!(
        batch
            .column(1)
            .as_any()
            .downcast_ref::<StringArray>()
            .unwrap()
            .iter()
            .collect::<Vec<_>>(),
        values.iter().copied().map(Some).collect::<Vec<_>>()
    );
    let schema = batch.schema();
    assert_eq!(
        batch
            .column(schema.index_of(ROW_KIND).unwrap())
            .as_any()
            .downcast_ref::<Int8Array>()
            .unwrap()
            .values()
            .as_ref(),
        kinds
    );
    assert_eq!(
        batch
            .column(schema.index_of(INPUT_ROW).unwrap())
            .as_any()
            .downcast_ref::<Int32Array>()
            .unwrap()
            .values()
            .as_ref(),
        ordinals
    );
    if let Some(times) = times {
        assert_eq!(
            batch
                .column(schema.index_of(OWNED_TIMESTAMP_V1).unwrap())
                .as_any()
                .downcast_ref::<Int64Array>()
                .unwrap()
                .iter()
                .collect::<Vec<_>>(),
            times
        );
    }
}

#[test]
fn top_one_composes_between_calcs_preserves_triggering_envelopes_and_restores_across_backends() {
    let plugin = std::env::var("STREAMFUSION_TEST_ROCKSDB_PLUGIN").ok();
    for owned in [false, true] {
        for rocks_first in [false, true] {
            if rocks_first && plugin.is_none() {
                continue;
            }
            let directory = tempfile::tempdir().unwrap();
            let broker = Arc::new(Broker {
                inner: TestBroker::new(64 << 20),
                transfers: AtomicUsize::new(0),
            });
            let owner = HostMemoryReservation::new(broker.clone(), "Top-1 context test");
            let first_path = directory.path().join("first");
            let first = context(
                &plan(owned),
                &resources(if rocks_first {
                    Some((plugin.as_ref().unwrap(), &first_path))
                } else {
                    None
                }),
                &owner,
            );
            let initial = input(
                owned,
                vec![1, 2, 1],
                vec!["a", "m", "z"],
                vec![Some(i64::MIN), None, Some(777)],
            );
            let output = run(&first, initial.clone());
            check(
                &output,
                &[INSERT, INSERT, UPDATE_BEFORE, UPDATE_AFTER],
                &["a", "m", "a", "z"],
                owned.then_some(vec![Some(i64::MIN), None, Some(777), Some(777)]),
                if owned {
                    &[-1, -1, -1, -1]
                } else {
                    &[17, 18, 19, 19]
                },
            );
            assert_eq!(
                first.metric_snapshot().unwrap(),
                vec![4, 4, 4, 3, 3, 4, 2, 3, 3, 1, 0, 3]
            );
            let (values, _credit) = first.gauge_snapshot().unwrap();
            assert_eq!(values, vec![0, 1.0f64.to_bits() as i64, 0]);
            drop(_credit);
            let snapshots = (0..16)
                .map(|group| first.snapshot_state(3, group).unwrap())
                .collect::<Vec<_>>();
            drop(first);
            let second_path = directory.path().join("second");
            let second = context(
                &plan(owned),
                &resources(if !rocks_first && plugin.is_some() {
                    Some((plugin.as_ref().unwrap(), &second_path))
                } else {
                    None
                }),
                &owner,
            );
            for (group, snapshot) in snapshots.iter().enumerate() {
                second.restore_state(3, group as u32, snapshot).unwrap();
            }
            let next = input(
                owned,
                vec![1, 2],
                vec!["zz", "a"],
                vec![Some(-12), Some(i64::MAX)],
            );
            let continued = run(&second, next);
            check(
                &continued,
                &[UPDATE_BEFORE, UPDATE_AFTER],
                &["z", "zz"],
                owned.then_some(vec![Some(-12), Some(-12)]),
                if owned { &[-1, -1] } else { &[17, 17] },
            );
            for event in [
                ControlEvent::Watermark(500),
                ControlEvent::BeforeCheckpoint(7),
                ControlEvent::EndInput,
            ] {
                let mut stream = second
                    .start_control(vec![initial.slice(0, 0)], &[(3, event)])
                    .unwrap();
                while let Some(batch) = second.runtime().block_on(stream.next()) {
                    assert_eq!(batch.unwrap().num_rows(), 0);
                }
            }
            drop(continued);
            drop(snapshots);
            drop(second);
            drop(owner);
            assert!(
                broker.inner.reserved() > 0,
                "native output outlives its execution context"
            );
            assert_eq!(
                broker.transfers.load(Ordering::Relaxed),
                0,
                "native stages never transfer output credit to Java"
            );
            drop(output);
            assert_eq!(broker.inner.reserved(), 0);
        }
    }
}

#[test]
fn cancellation_or_invalid_changelog_requires_recovery_and_releases_state() {
    for cancel in [false, true] {
        let broker = Arc::new(TestBroker::new(64 << 20));
        let owner = HostMemoryReservation::new(broker.clone(), "Top-1 failure test");
        let context = context(&plan(true), &resources(None), &owner);
        let initial = input(true, vec![1], vec!["a"], vec![Some(10)]);
        drop(run(&context, initial.clone()));
        let bad = input(true, vec![2, 1], vec!["b", "z"], vec![Some(20), None]);
        let mut columns = bad.columns().to_vec();
        if !cancel {
            let kind = bad.schema().index_of(ROW_KIND).unwrap();
            columns[kind] = Arc::new(Int8Array::from(vec![INSERT, DELETE]));
        }
        let bad = RecordBatch::try_new(bad.schema(), columns).unwrap();
        let mut stream = context.start(vec![bad]).unwrap();
        if !cancel {
            let error = context
                .runtime()
                .block_on(stream.next())
                .unwrap()
                .unwrap_err();
            assert!(error.to_string().contains("INSERT-only"), "{error}");
        }
        drop(stream);
        assert!(context.require_idle().is_err());
        assert!(context.snapshot_state(3, 0).is_err());
        assert!(context.start(vec![initial]).is_err());
        drop(context);
        drop(owner);
        assert_eq!(broker.reserved(), 0);
    }
}

#[test]
fn unsupported_top_one_bindings_fail_before_opening_backend_resources() {
    for variant in 0..8 {
        let mut plan = plan(true);
        let Some(proto::operator::Operator::Calc(root)) = &mut plan.root.as_mut().unwrap().operator
        else {
            unreachable!()
        };
        let Some(proto::operator::Operator::TopN(top)) = &mut root.input.as_mut().unwrap().operator
        else {
            unreachable!()
        };
        match variant {
            0 => top.rank_end = Some(2),
            1 => top.state_ttl_millis = 1,
            2 => top.strategy = proto::TopNStrategy::Retract as i32,
            3 => top.rank_type = 999,
            4 => top.rank_type = proto::TopNRankType::Unspecified as i32,
            5 => top.physical_input_semantics = true,
            6 => top.output_schema.as_mut().unwrap().fields.clear(),
            7 => top.partition_key_indices = vec![999],
            _ => unreachable!(),
        }
        let broker = Arc::new(TestBroker::new(64 << 20));
        let owner = HostMemoryReservation::new(broker.clone(), "Top-1 invalid binding test");
        let mut context =
            NativeExecutionContext::new(&plan.encode_to_vec(), owner.datafusion_pool(64 << 20))
                .unwrap();
        let directory = tempfile::tempdir().unwrap();
        let database = directory.path().join("must-not-open");
        let error = context
            .install_state(
                &resources(Some(("/unavailable-top-one-plugin.so", &database))).encode_to_vec(),
                owner.sibling("invalid binding"),
            )
            .unwrap_err();
        assert!(!database.exists());
        assert!(!error.to_string().contains("load library"), "{error}");
        drop(context);
        drop(owner);
        assert_eq!(broker.reserved(), 0);
    }
}

#[test]
fn large_native_input_is_rejected_before_state_mutation() {
    let broker = Arc::new(TestBroker::new(2 << 20));
    let owner = HostMemoryReservation::new(broker.clone(), "Top-1 bounded input test");
    let context = context(&plan(true), &resources(None), &owner);
    let wide = "x".repeat(1 << 20);
    let batch = input(true, vec![1], vec![&wide], vec![Some(1)]);
    let mut stream = context.start(vec![batch]).unwrap();
    let error = context
        .runtime()
        .block_on(stream.next())
        .unwrap()
        .unwrap_err();
    assert!(
        matches!(error, DataFusionError::ResourcesExhausted(_)),
        "{error}"
    );
    drop(stream);
    assert!(context.require_idle().is_err());
    drop(context);
    drop(owner);
    assert_eq!(broker.reserved(), 0);
}
