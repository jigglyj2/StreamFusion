// Copyright 2026 StreamFusion Authors
// Licensed under the Apache License, Version 2.0.

use super::*;
use crate::execution_context::NativeExecutionContext;
use crate::memory_pool::tests_support::TestBroker;
use arrow::array::TimestampMillisecondArray;
use futures::StreamExt;
use prost::Message;

fn calc(id: u64, child: proto::Operator, width: u32) -> proto::Operator {
    proto::Operator {
        plan_node_id: id,
        operator: Some(proto::operator::Operator::Calc(Box::new(proto::Calc {
            input: Some(Box::new(child)),
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
fn context() -> (Arc<NativeExecutionContext>, Arc<TestBroker>, SchemaRef) {
    let kernel = crate::planner::operators::local_window_aggregate::tests::processor(false);
    let schema = kernel.input_schema.clone();
    let mut local_plan = kernel.plan.clone();
    local_plan.input = Some(Box::new(calc(
        2,
        proto::Operator {
            plan_node_id: 1,
            operator: Some(proto::operator::Operator::Input(proto::Input::default())),
            ..Default::default()
        },
        3,
    )));
    let local = proto::Operator {
        plan_node_id: 3,
        operator: Some(proto::operator::Operator::LocalWindowAggregate(Box::new(
            local_plan,
        ))),
        ..Default::default()
    };
    let broker = Arc::new(TestBroker::new(256 << 20));
    let memory = HostMemoryReservation::new(broker.clone(), "shared local window test");
    let plan = proto::NativePlan {
        protocol_version: 2,
        root: Some(calc(4, local, 4)),
    };
    let mut context =
        NativeExecutionContext::new(&plan.encode_to_vec(), memory.datafusion_pool(256 << 20))
            .unwrap();
    context
        .install_task_resources(
            &resources(3).encode_to_vec(),
            memory.sibling("task resources"),
        )
        .unwrap();
    (Arc::new(context), broker, schema)
}
fn resources(id: u64) -> proto::NativeTaskBindings {
    proto::NativeTaskBindings {
        protocol_version: 1,
        bindings: vec![proto::NativeTaskBinding {
            plan_node_id: id,
            resource: Some(proto::native_task_binding::Resource::LocalWindowBuffer(
                proto::NativeLocalWindowBuffer {
                    flink_buffer_memory_bytes: 3 << 20,
                    flink_page_bytes: 32 << 10,
                },
            )),
        }],
    }
}

fn input(schema: &SchemaRef, rows: &[(i64, i64, i64)], kind: i8) -> RecordBatch {
    let mut fields = schema.fields().to_vec();
    fields.extend([
        Arc::new(Field::new(OWNED_TIMESTAMP_V1, DataType::Int64, true)),
        Arc::new(Field::new(ROW_KIND, DataType::Int8, false)),
        Arc::new(Field::new(INPUT_ROW, DataType::Int32, false)),
    ]);
    RecordBatch::try_new(
        Arc::new(Schema::new(fields)),
        vec![
            Arc::new(Int64Array::from_iter_values(rows.iter().map(|r| r.0))) as ArrayRef,
            Arc::new(Int64Array::from_iter_values(rows.iter().map(|r| r.1))) as ArrayRef,
            Arc::new(TimestampMillisecondArray::from_iter_values(
                rows.iter().map(|r| r.2),
            )) as ArrayRef,
            Arc::new(Int64Array::from_iter_values(rows.iter().map(|r| r.2 + 123))) as ArrayRef,
            Arc::new(Int8Array::from(vec![kind; rows.len()])) as ArrayRef,
            Arc::new(Int32Array::from(vec![-1; rows.len()])) as ArrayRef,
        ],
    )
    .unwrap()
}
fn run(
    context: &Arc<NativeExecutionContext>,
    batch: RecordBatch,
    event: Option<ControlEvent>,
) -> Result<Vec<RecordBatch>> {
    let mut stream = if let Some(event) = event {
        context.start_control(vec![batch], &[(3, event)])
    } else {
        context.start(vec![batch])
    }?;
    let mut batches = Vec::new();
    while let Some(batch) = context.runtime().block_on(stream.next()) {
        batches.push(batch?);
    }
    Ok(batches)
}
fn rows(batches: &[RecordBatch]) -> usize {
    batches.iter().map(RecordBatch::num_rows).sum()
}

#[test]
fn calc_window_calc_shares_controls_owned_envelopes_and_logical_stage_counters() {
    let (context, broker, schema) = context();
    let batch = input(
        &schema,
        &[(9, 5, 5000), (2, 7, 1000), (9, 11, 5001)],
        INSERT,
    );
    assert_eq!(rows(&run(&context, batch.clone(), None).unwrap()), 0);
    assert_eq!(
        rows(
            &run(
                &context,
                batch.slice(0, 0),
                Some(ControlEvent::Watermark(1998))
            )
            .unwrap()
        ),
        0
    );
    let output = run(
        &context,
        batch.slice(0, 0),
        Some(ControlEvent::Watermark(1999)),
    )
    .unwrap();
    assert_eq!(rows(&output), 2);
    let batch = output.iter().find(|batch| batch.num_rows() != 0).unwrap();
    assert_eq!(
        batch
            .column(0)
            .as_any()
            .downcast_ref::<Int64Array>()
            .unwrap()
            .values()
            .as_ref(),
        &[9, 2]
    );
    assert_eq!(batch.column(4).null_count(), 2);
    assert_eq!(batch.schema().field(4).name(), OWNED_TIMESTAMP_V1);
    assert_eq!(
        batch
            .column(5)
            .as_any()
            .downcast_ref::<Int8Array>()
            .unwrap()
            .values()
            .as_ref(),
        &[0, 0]
    );
    assert_eq!(
        batch
            .column(6)
            .as_any()
            .downcast_ref::<Int32Array>()
            .unwrap()
            .values()
            .as_ref(),
        &[-1, -1]
    );
    assert_eq!(
        context.metric_snapshot().unwrap(),
        vec![4, 2, 2, 3, 3, 2, 2, 3, 3, 1, 0, 3]
    );
    assert!(context
        .snapshot_state(3, 0)
        .unwrap_err()
        .to_string()
        .contains("no shared snapshot"));
    let (wire, credit) = context.control_capabilities().unwrap();
    let capabilities = proto::NativeControlCapabilities::decode(wire.as_slice()).unwrap();
    assert_eq!(capabilities.stages.len(), 1);
    assert_eq!(capabilities.stages[0].plan_node_id, 3);
    assert!(
        capabilities.stages[0].watermark
            && capabilities.stages[0].before_checkpoint
            && capabilities.stages[0].end_input
    );
    drop((wire, credit));
    drop(context);
    assert!(broker.reserved() > 0);
    drop(output);
    assert_eq!(broker.reserved(), 0);
}

#[test]
fn prebarrier_chunks_drain_through_downstream_calc_without_eof_flushing() {
    let (context, broker, schema) = context();
    let batch = input(
        &schema,
        &(0..5000).map(|key| (key, key, 1000)).collect::<Vec<_>>(),
        INSERT,
    );
    assert_eq!(rows(&run(&context, batch.clone(), None).unwrap()), 0);
    assert_eq!(
        rows(&run(&context, batch.slice(0, 0), Some(ControlEvent::EndInput)).unwrap()),
        0
    );
    let output = run(
        &context,
        batch.slice(0, 0),
        Some(ControlEvent::BeforeCheckpoint(7)),
    )
    .unwrap();
    assert_eq!(rows(&output), 5000);
    assert!(output.iter().all(|batch| batch.num_rows() <= OUTPUT_ROWS));
    assert_eq!(
        context.metric_snapshot().unwrap(),
        vec![4, 5000, 5000, 3, 5000, 5000, 2, 5000, 5000, 1, 0, 5000]
    );
    assert_eq!(
        rows(
            &run(
                &context,
                batch.slice(0, 0),
                Some(ControlEvent::BeforeCheckpoint(8))
            )
            .unwrap()
        ),
        0
    );
    drop((context, output));
    assert_eq!(broker.reserved(), 0);
}

#[test]
fn invalid_rowkind_and_cancelled_partial_flush_require_recovery() {
    for cancel in [false, true] {
        let (context, broker, schema) = context();
        let batch = input(
            &schema,
            &(0..5000).map(|key| (key, key, 1000)).collect::<Vec<_>>(),
            if cancel { INSERT } else { DELETE },
        );
        if cancel {
            drop(run(&context, batch.clone(), None).unwrap());
            let mut stream = context
                .start_control(
                    vec![batch.slice(0, 0)],
                    &[(3, ControlEvent::BeforeCheckpoint(7))],
                )
                .unwrap();
            loop {
                let output = context.runtime().block_on(stream.next()).unwrap().unwrap();
                if output.num_rows() != 0 {
                    break;
                }
            }
            drop(stream);
        } else {
            assert!(run(&context, batch.clone(), None)
                .unwrap_err()
                .to_string()
                .contains("RowKind"));
        }
        assert!(run(&context, batch.slice(0, 0), None)
            .unwrap_err()
            .to_string()
            .contains("failed"));
        assert!(context
            .require_idle()
            .unwrap_err()
            .to_string()
            .contains("recovery"));
        drop(context);
        assert_eq!(broker.reserved(), 0);
    }
}

#[test]
fn resource_binding_rejects_invalid_requests_transactionally() {
    let (original, _, _) = context();
    let wire = original.plan().encode_to_vec();
    for case in 0..9 {
        let broker = Arc::new(TestBroker::new(256 << 20));
        let memory = HostMemoryReservation::new(broker.clone(), "resource validation");
        let mut context =
            NativeExecutionContext::new(&wire, memory.datafusion_pool(256 << 20)).unwrap();
        let baseline = broker.reserved();
        let mut invalid = resources(3);
        match case {
            0 => invalid.protocol_version = 2,
            1 => invalid.bindings[0].plan_node_id = 0,
            2 => invalid.bindings[0].plan_node_id = 2,
            3 => invalid.bindings[0].plan_node_id = 99,
            4 => invalid.bindings[0].resource = None,
            5 => invalid.bindings.push(invalid.bindings[0].clone()),
            6 => {
                invalid.bindings[0].resource =
                    Some(proto::native_task_binding::Resource::LocalWindowBuffer(
                        proto::NativeLocalWindowBuffer {
                            flink_buffer_memory_bytes: 1,
                            flink_page_bytes: 17,
                        },
                    ))
            }
            7 => invalid.bindings.clear(),
            _ => {
                invalid.bindings[0].resource =
                    Some(proto::native_task_binding::Resource::LocalWindowBuffer(
                        proto::NativeLocalWindowBuffer {
                            flink_buffer_memory_bytes: u64::MAX,
                            flink_page_bytes: 32 << 10,
                        },
                    ))
            }
        }
        assert!(context
            .install_task_resources(&invalid.encode_to_vec(), memory.sibling("invalid"))
            .is_err());
        assert_eq!(
            broker.reserved(),
            baseline,
            "failed request {case} leaked credit"
        );
        context
            .install_task_resources(&resources(3).encode_to_vec(), memory.sibling("valid retry"))
            .unwrap();
        assert!(context
            .install_task_resources(&resources(3).encode_to_vec(), memory.sibling("duplicate"))
            .is_err());
        drop(context);
        assert_eq!(broker.reserved(), 0);
    }
}
