// Copyright 2026 StreamFusion Authors
// Licensed under the Apache License, Version 2.0.

use super::super::indexed_tests::backends;
use super::*;

mod checkpoints;
use crate::execution_context::NativeExecutionContext;
use crate::memory_pool::tests_support::TestBroker;
use futures::StreamExt;

fn plan() -> proto::NativePlan {
    let mut plan = proto::NativePlan::decode(super::super::tests::plan().as_slice()).unwrap();
    plan.protocol_version = 3;
    let mut root = plan.root.take().unwrap();
    root.plan_node_id = 3;
    root.clear_record_timestamps = true;
    let Some(proto::operator::Operator::WindowJoin(join)) = &mut root.operator else {
        unreachable!()
    };
    join.join_type = proto::RegularJoinType::Inner as i32;
    join.filter_nulls = vec![true];
    let input = |port: u32| {
        Box::new(proto::Operator {
            plan_node_id: port as u64 + 1,
            operator: Some(proto::operator::Operator::Input(proto::Input {
                input_index: port,
                ..Default::default()
            })),
            ..Default::default()
        })
    };
    join.left_input = Some(input(0));
    join.right_input = Some(input(1));
    plan.root = Some(proto::Operator {
        plan_node_id: 4,
        operator: Some(proto::operator::Operator::Calc(Box::new(proto::Calc {
            input: Some(Box::new(root)),
            preserve_input_envelope: true,
            projections: (0..6)
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
    });
    plan
}
fn resources(
    rocks: Option<&std::path::Path>,
    watermark: Option<i64>,
    first: u32,
    last: u32,
) -> proto::NativeStateBindings {
    proto::NativeStateBindings {
        protocol_version: 3,
        bindings: vec![proto::NativeStateBinding {
            plan_node_id: 3,
            max_parallelism: 128,
            first_key_group: first,
            last_key_group: last,
            restored_watermark: watermark,
            backend: Some(match rocks {
                None => proto::native_state_binding::Backend::Memory(proto::NativeMemoryState {}),
                Some(path) => {
                    proto::native_state_binding::Backend::Rocksdb(proto::NativeRocksDbState {
                        plugin_path: std::env::var("STREAMFUSION_TEST_ROCKSDB_PLUGIN").unwrap(),
                        database_path: path.to_str().unwrap().into(),
                        memory_limit: 8 << 20,
                        log_directory: None,
                    })
                }
            }),
        }],
    }
}
fn context(
    rocks: bool,
    watermark: Option<i64>,
    first: u32,
    last: u32,
) -> (
    Arc<NativeExecutionContext>,
    Arc<TestBroker>,
    tempfile::TempDir,
) {
    context_with_plan(plan(), rocks, watermark, first, last)
}
fn context_with_plan(
    plan: proto::NativePlan,
    rocks: bool,
    watermark: Option<i64>,
    first: u32,
    last: u32,
) -> (
    Arc<NativeExecutionContext>,
    Arc<TestBroker>,
    tempfile::TempDir,
) {
    let dir = tempfile::tempdir().unwrap();
    let broker = Arc::new(TestBroker::new(256 << 20));
    let memory = HostMemoryReservation::new(broker.clone(), "shared window join test");
    let mut context =
        NativeExecutionContext::new(&plan.encode_to_vec(), memory.datafusion_pool(256 << 20))
            .unwrap();
    context
        .install_state(
            &resources(rocks.then_some(dir.path()), watermark, first, last).encode_to_vec(),
            memory,
        )
        .unwrap();
    (Arc::new(context), broker, dir)
}
fn input(keys: &[i64], ends: &[i64], payloads: &[&[u8]], kinds: &[i8]) -> RecordBatch {
    let original = super::super::tests::batch(keys, ends, payloads, kinds);
    let mut columns = original.columns()[..3].to_vec();
    columns.extend([
        Arc::new(Int64Array::from(vec![Some(777); keys.len()])) as ArrayRef,
        original.column(3).clone(),
        Arc::new(Int32Array::from(vec![-1; keys.len()])),
    ]);
    let mut fields = original.schema().fields()[..3].to_vec();
    fields.extend([
        Arc::new(Field::new(OWNED_TIMESTAMP_V1, DataType::Int64, true)),
        Arc::new(Field::new(ROW_KIND, DataType::Int8, false)),
        Arc::new(Field::new(INPUT_ROW, DataType::Int32, false)),
    ]);
    RecordBatch::try_new(Arc::new(Schema::new(fields)), columns).unwrap()
}
fn empty() -> RecordBatch {
    input(&[], &[], &[], &[])
}
fn run(
    context: &Arc<NativeExecutionContext>,
    left: RecordBatch,
    right: RecordBatch,
    event: Option<ControlEvent>,
) -> Result<Vec<RecordBatch>> {
    let mut stream = if let Some(event) = event {
        context.start_control(vec![left, right], &[(3, event)])?
    } else {
        context.start(vec![left, right])?
    };
    let mut out = Vec::new();
    while let Some(batch) = context.runtime().block_on(stream.next()) {
        out.push(batch?);
    }
    Ok(out)
}
fn pairs(output: &[RecordBatch]) -> Vec<(Vec<u8>, Vec<u8>)> {
    output
        .iter()
        .flat_map(|batch| {
            let a = batch
                .column(2)
                .as_any()
                .downcast_ref::<BinaryArray>()
                .unwrap();
            let b = batch
                .column(5)
                .as_any()
                .downcast_ref::<BinaryArray>()
                .unwrap();
            (0..batch.num_rows()).map(move |i| (a.value(i).to_vec(), b.value(i).to_vec()))
        })
        .collect()
}

#[test]
fn shared_binary_window_calc_uses_control_eof_and_flink_metrics_without_java_handoffs() {
    for rocks in backends() {
        let (context, broker, _dir) = context(rocks, None, 0, 127);
        assert!(run(
            &context,
            input(&[9, 9], &[100, 100], &[b"a", b"b"], &[INSERT, UPDATE_AFTER]),
            empty(),
            None
        )
        .unwrap()
        .is_empty());
        assert!(run(
            &context,
            empty(),
            input(&[9, 9], &[100, 100], &[b"x", b"x"], &[INSERT, INSERT]),
            None
        )
        .unwrap()
        .is_empty());
        for event in [ControlEvent::BeforeCheckpoint(1), ControlEvent::EndInput] {
            assert!(run(&context, empty(), empty(), Some(event))
                .unwrap()
                .is_empty());
        }
        let output = run(
            &context,
            empty(),
            empty(),
            Some(ControlEvent::Watermark(99)),
        )
        .unwrap();
        assert_eq!(
            pairs(&output),
            vec![
                (b"a".to_vec(), b"x".to_vec()),
                (b"a".to_vec(), b"x".to_vec()),
                (b"b".to_vec(), b"x".to_vec()),
                (b"b".to_vec(), b"x".to_vec())
            ]
        );
        for batch in &output {
            assert_eq!(batch.num_columns(), 9);
            assert_eq!(batch.column(6).null_count(), batch.num_rows());
            assert_eq!(batch.schema().field(6).name(), OWNED_TIMESTAMP_V1);
            assert!(batch
                .column(7)
                .as_any()
                .downcast_ref::<Int8Array>()
                .unwrap()
                .values()
                .iter()
                .all(|&kind| kind == INSERT));
            assert!(batch
                .column(8)
                .as_any()
                .downcast_ref::<Int32Array>()
                .unwrap()
                .values()
                .iter()
                .all(|&ordinal| ordinal == -1));
        }
        let (schema, credit) = context.gauge_schema().unwrap();
        let schema = proto::NativeGaugeSchema::decode(schema.as_slice()).unwrap();
        assert_eq!(
            schema
                .gauges
                .iter()
                .map(|g| g.name.as_str())
                .collect::<Vec<_>>(),
            [
                "leftNumLateRecordsDropped",
                "rightNumLateRecordsDropped",
                "watermarkLatency"
            ]
        );
        assert_eq!(schema.gauges[0].meter_name, "leftLateRecordsDroppedRate");
        assert_eq!(schema.gauges[1].meter_name, "rightLateRecordsDroppedRate");
        assert!(schema.gauges.iter().all(|g| g.plan_node_id == 3));
        drop(credit);
        run(
            &context,
            input(&[9], &[100], &[b"late"], &[DELETE]),
            input(&[9], &[100], &[b"late"], &[DELETE]),
            None,
        )
        .unwrap();
        assert_eq!(context.gauge_snapshot().unwrap().0, [1, 1, 99]);
        assert_eq!(
            context.metric_snapshot().unwrap(),
            [4, 4, 4, 3, 6, 4, 1, 0, 3, 2, 0, 3]
        );
        assert_eq!(
            schema.gauges[0].metric_kind,
            proto::NativeMetricKind::Counter as i32
        );
        assert_eq!(
            schema.gauges[1].metric_kind,
            proto::NativeMetricKind::Counter as i32
        );
        assert_eq!(
            schema.gauges[2].metric_kind,
            proto::NativeMetricKind::WatermarkLatency as i32
        );
        drop(context);
        assert!(broker.reserved() > 0);
        drop(output);
        assert_eq!(broker.reserved(), 0);
    }
}

#[test]
fn cancelled_shared_output_prevents_checkpoint_and_reuse() {
    for rocks in backends() {
        let (context, _, _dir) = context(rocks, None, 0, 127);
        run(
            &context,
            input(
                &[9; 128],
                &[100; 128],
                &[b"a".as_slice(); 128],
                &[INSERT; 128],
            ),
            empty(),
            None,
        )
        .unwrap();
        run(
            &context,
            empty(),
            input(
                &[9; 128],
                &[100; 128],
                &[b"b".as_slice(); 128],
                &[INSERT; 128],
            ),
            None,
        )
        .unwrap();
        let mut stream = context
            .start_control(vec![empty(), empty()], &[(3, ControlEvent::Watermark(99))])
            .unwrap();
        let output = context.runtime().block_on(stream.next()).unwrap().unwrap();
        assert!(output.num_rows() > 0);
        drop(stream);
        assert!(context.snapshot_state(3, 0).is_err());
        assert!(run(&context, empty(), empty(), None).is_err());
    }
}

#[test]
fn shared_invalid_contract_is_rejected_before_opening_rocksdb() {
    if backends().len() == 1 {
        return;
    }
    let dir = tempfile::tempdir().unwrap();
    let database = dir.path().join("must-not-open");
    let mut plan = plan();
    let Some(proto::operator::Operator::Calc(calc)) = &mut plan.root.as_mut().unwrap().operator
    else {
        unreachable!()
    };
    let Some(proto::operator::Operator::WindowJoin(join)) =
        &mut calc.input.as_mut().unwrap().operator
    else {
        unreachable!()
    };
    join.join_type = proto::RegularJoinType::Left as i32;
    let memory =
        HostMemoryReservation::new(Arc::new(TestBroker::new(256 << 20)), "invalid contract");
    let mut context =
        NativeExecutionContext::new(&plan.encode_to_vec(), memory.datafusion_pool(256 << 20))
            .unwrap();
    let error = context
        .install_state(
            &resources(Some(&database), None, 0, 127).encode_to_vec(),
            memory,
        )
        .unwrap_err();
    assert!(error.to_string().contains("INNER"));
    assert!(!database.exists());
}
