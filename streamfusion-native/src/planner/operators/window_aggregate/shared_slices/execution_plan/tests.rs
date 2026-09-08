// Copyright 2026 StreamFusion Authors
// Licensed under the Apache License, Version 2.0.

use super::*;
use crate::execution_context::NativeExecutionContext;
use crate::memory_pool::tests_support::TestBroker;
use futures::StreamExt;
use prost::Message;

fn calc(id: u64, input: proto::Operator, width: u32) -> proto::Operator {
    proto::Operator {
        plan_node_id: id,
        operator: Some(proto::operator::Operator::Calc(Box::new(proto::Calc {
            input: Some(Box::new(input)),
            preserve_input_envelope: true,
            condition: None,
            projections: (0..width)
                .map(|index| proto::Expression {
                    expression: Some(proto::expression::Expression::InputReference(
                        proto::InputReference {
                            index,
                            r#type: None,
                        },
                    )),
                })
                .collect(),
        }))),
        ..Default::default()
    }
}
fn plan() -> proto::NativePlan {
    let mut plan = proto::NativePlan::decode(super::super::tests::plan().as_slice()).unwrap();
    let mut window = plan.root.take().unwrap();
    window.plan_node_id = 3;
    let Some(proto::operator::Operator::WindowAggregate(aggregate)) = &mut window.operator else {
        unreachable!()
    };
    aggregate.input = Some(Box::new(calc(
        2,
        proto::Operator {
            plan_node_id: 1,
            operator: Some(proto::operator::Operator::Input(proto::Input::default())),
            ..Default::default()
        },
        4,
    )));
    proto::NativePlan {
        protocol_version: 2,
        root: Some(calc(4, window, 4)),
    }
}
fn resources(
    watermark: Option<i64>,
    rocks: Option<&std::path::Path>,
) -> proto::NativeStateBindings {
    proto::NativeStateBindings {
        protocol_version: if watermark.is_some() { 3 } else { 1 },
        bindings: vec![proto::NativeStateBinding {
            plan_node_id: 3,
            max_parallelism: 128,
            first_key_group: 0,
            last_key_group: 127,
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
    broker: Arc<TestBroker>,
    watermark: Option<i64>,
    rocks: Option<&std::path::Path>,
) -> Arc<NativeExecutionContext> {
    let memory = HostMemoryReservation::new(broker, "shared window context");
    let mut context =
        NativeExecutionContext::new(&plan().encode_to_vec(), memory.datafusion_pool(256 << 20))
            .unwrap();
    context
        .install_state(&resources(watermark, rocks).encode_to_vec(), memory)
        .unwrap();
    Arc::new(context)
}
fn input(rows: &[(i64, i64, i64)], kind: i8) -> RecordBatch {
    let payload = super::super::tests::batch(rows);
    let mut columns = payload.columns().to_vec();
    columns.extend([
        Arc::new(Int64Array::from(vec![Some(123); rows.len()])) as ArrayRef,
        Arc::new(Int8Array::from(vec![kind; rows.len()])) as ArrayRef,
        Arc::new(Int32Array::from(vec![-1; rows.len()])) as ArrayRef,
    ]);
    let mut fields = payload.schema().fields().to_vec();
    fields.extend([
        Arc::new(Field::new(OWNED_TIMESTAMP_V1, DataType::Int64, true)),
        Arc::new(Field::new(ROW_KIND, DataType::Int8, false)),
        Arc::new(Field::new(INPUT_ROW, DataType::Int32, false)),
    ]);
    RecordBatch::try_new(Arc::new(Schema::new(fields)), columns).unwrap()
}
fn run(
    context: &Arc<NativeExecutionContext>,
    input: RecordBatch,
    event: Option<ControlEvent>,
) -> Result<Vec<RecordBatch>> {
    let mut stream = if let Some(event) = event {
        context.start_control(vec![input], &[(3, event)])
    } else {
        context.start(vec![input])
    }?;
    let mut output = Vec::new();
    while let Some(batch) = context.runtime().block_on(stream.next()) {
        output.push(batch?);
    }
    Ok(output)
}
fn rows(output: &[RecordBatch]) -> usize {
    output.iter().map(RecordBatch::num_rows).sum()
}

#[test]
fn calc_window_calc_drains_watermark_chains_without_jvm_handoffs_or_eof_flushes() {
    let broker = Arc::new(TestBroker::new(256 << 20));
    let context = context(broker.clone(), None, None);
    let input = input(&[(1, 1, 2000), (1, 2, 4000)], INSERT);
    assert_eq!(rows(&run(&context, input.clone(), None).unwrap()), 0);
    for event in [ControlEvent::BeforeCheckpoint(1), ControlEvent::EndInput] {
        assert_eq!(
            rows(&run(&context, input.slice(0, 0), Some(event)).unwrap()),
            0
        );
    }
    let output = run(
        &context,
        input.slice(0, 0),
        Some(ControlEvent::Watermark(i64::MAX)),
    )
    .unwrap();
    assert_eq!(rows(&output), 4);
    for batch in output.iter().filter(|batch| batch.num_rows() > 0) {
        assert_eq!(batch.column(4).null_count(), batch.num_rows());
        assert_eq!(batch.schema().field(4).name(), OWNED_TIMESTAMP_V1);
        assert!(batch
            .column(5)
            .as_any()
            .downcast_ref::<Int8Array>()
            .unwrap()
            .values()
            .iter()
            .all(|&kind| kind == INSERT));
        assert!(batch
            .column(6)
            .as_any()
            .downcast_ref::<Int32Array>()
            .unwrap()
            .values()
            .iter()
            .all(|&row| row == -1));
    }
    assert_eq!(
        context.metric_snapshot().unwrap(),
        [4, 4, 4, 3, 2, 4, 2, 2, 2, 1, 0, 2]
    );
    drop(context);
    assert!(broker.reserved() > 0);
    drop(output);
    assert_eq!(broker.reserved(), 0);
}

#[test]
fn common_state_restore_requires_a_flink_watermark_and_handles_both_backends() {
    let broker = Arc::new(TestBroker::new(256 << 20));
    let source = context(broker.clone(), None, None);
    let batch = input(&[(1, 2, 2000)], INSERT);
    run(&source, batch.clone(), None).unwrap();
    assert_eq!(
        rows(
            &run(
                &source,
                batch.slice(0, 0),
                Some(ControlEvent::Watermark(1999))
            )
            .unwrap()
        ),
        1
    );
    let snapshots = (0..128)
        .map(|group| source.snapshot_state(3, group).unwrap())
        .collect::<Vec<_>>();
    drop(source);
    let missing = context(broker.clone(), None, None);
    assert!(missing
        .restore_state(3, 0, &snapshots[0])
        .unwrap_err()
        .to_string()
        .contains("union-operator watermark"));
    drop(missing);
    for rocks in [false, true] {
        if rocks && std::env::var("STREAMFUSION_TEST_ROCKSDB_PLUGIN").is_err() {
            continue;
        }
        let directory = tempfile::tempdir().unwrap();
        let restored = context(
            broker.clone(),
            Some(1999),
            rocks.then_some(directory.path()),
        );
        for (group, bytes) in snapshots.iter().enumerate() {
            restored.restore_state(3, group as u32, bytes).unwrap();
        }
        run(&restored, input(&[(7, 13, -2000)], INSERT), None).unwrap();
        assert_eq!(
            rows(
                &run(
                    &restored,
                    batch.slice(0, 0),
                    Some(ControlEvent::Watermark(1999))
                )
                .unwrap()
            ),
            0
        );
        assert_eq!(
            rows(
                &run(
                    &restored,
                    batch.slice(0, 0),
                    Some(ControlEvent::Watermark(i64::MAX))
                )
                .unwrap()
            ),
            2
        );
    }
    drop(snapshots);
    assert_eq!(broker.reserved(), 0);
}

#[test]
fn malformed_clock_bindings_and_invalid_input_fail_before_reuse() {
    let broker = Arc::new(TestBroker::new(256 << 20));
    let memory = HostMemoryReservation::new(broker.clone(), "window binding validation");
    let mut unbound =
        NativeExecutionContext::new(&plan().encode_to_vec(), memory.datafusion_pool(256 << 20))
            .unwrap();
    let baseline = broker.reserved();
    let mut invalid = resources(Some(1999), None);
    invalid.protocol_version = 2;
    assert!(unbound
        .install_state(&invalid.encode_to_vec(), memory.sibling("invalid"))
        .is_err());
    assert_eq!(broker.reserved(), baseline);
    unbound
        .install_state(
            &resources(None, None).encode_to_vec(),
            memory.sibling("valid"),
        )
        .unwrap();
    let context = Arc::new(unbound);
    assert!(run(&context, input(&[(1, 1, 2000)], DELETE), None).is_err());
    assert!(run(&context, input(&[(1, 1, 2000)], INSERT), None).is_err());
    drop(context);
    assert_eq!(broker.reserved(), 0);
}

#[test]
fn cancelling_a_window_control_stream_requires_recovery_and_preserves_output_ownership() {
    let broker = Arc::new(TestBroker::new(256 << 20));
    let context = context(broker.clone(), None, None);
    let batch = input(
        &(0..1100).map(|key| (key, 1, 2000)).collect::<Vec<_>>(),
        INSERT,
    );
    run(&context, batch.clone(), None).unwrap();
    let mut stream = context
        .start_control(
            vec![batch.slice(0, 0)],
            &[(3, ControlEvent::Watermark(i64::MAX))],
        )
        .unwrap();
    let held = loop {
        let batch = context.runtime().block_on(stream.next()).unwrap().unwrap();
        if batch.num_rows() != 0 {
            break batch;
        }
    };
    assert_eq!(held.num_rows(), OUTPUT_ROWS);
    drop(stream);
    assert!(context.snapshot_state(3, 0).is_err());
    assert!(context.start(vec![batch.slice(0, 0)]).is_err());
    drop(context);
    assert!(broker.reserved() > 0);
    assert_eq!(held.column(4).null_count(), OUTPUT_ROWS);
    drop(held);
    assert_eq!(broker.reserved(), 0);
}
