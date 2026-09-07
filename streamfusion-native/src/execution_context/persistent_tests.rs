// Copyright 2026 StreamFusion Authors
// Licensed under the Apache License, Version 2.0

use super::*;
use crate::memory_pool::{
    tests_support::TestBroker, HostMemoryReservation, MemoryReservationBroker,
};
use crate::planner::operators::{
    arrow_handoff_tests::Observe,
    deduplicate::{
        execution_plan::{DeduplicateExec, DeduplicateFactory},
        DeduplicateProcessor,
    },
};
use arrow::array::{Array, ArrayRef, Int32Array, Int64Array};
use arrow::datatypes::{DataType, Field, Schema};
use arrow::record_batch::RecordBatch;
use futures::StreamExt;
use std::sync::atomic::AtomicUsize;

fn reference(index: u32) -> proto::Expression {
    proto::Expression {
        expression: Some(proto::expression::Expression::InputReference(
            proto::InputReference {
                index,
                r#type: None,
            },
        )),
    }
}
fn dedup(id: u64, input: proto::Operator, key: u32) -> proto::Operator {
    proto::Operator {
        plan_node_id: id,
        metric_name: String::new(),
        clear_record_timestamps: false,
        metric_uid: None,
        operator: Some(proto::operator::Operator::Deduplicate(Box::new(
            proto::Deduplicate {
                input: Some(Box::new(input)),
                key_indices: vec![key],
                processing_time: true,
                keep_last: false,
                ..Default::default()
            },
        ))),
    }
}
fn batch() -> RecordBatch {
    RecordBatch::try_new(
        Arc::new(Schema::new(vec![
            Field::new("k1", DataType::Int64, false),
            Field::new("k2", DataType::Int64, false),
            Field::new("__streamfusion_input_row", DataType::Int32, false),
        ])),
        vec![
            Arc::new(Int64Array::from(vec![1, 1, 2, 3])),
            Arc::new(Int64Array::from(vec![10, 20, 10, 30])),
            Arc::new(Int32Array::from(vec![7, 8, 9, 10])),
        ],
    )
    .unwrap()
}
fn serialized(root: proto::Operator) -> Vec<u8> {
    proto::NativePlan {
        protocol_version: crate::PLAN_PROTOCOL_VERSION,
        root: Some(root),
    }
    .encode_to_vec()
}

#[test]
fn shared_context_composes_multiple_state_owners_without_operator_pair_rules() {
    let broker = Arc::new(TestBroker::new(32 << 20));
    let memory = HostMemoryReservation::new(broker.clone(), "generic region");
    let first = dedup(
        2,
        proto::Operator {
            plan_node_id: 1,
            metric_name: String::new(),
            clear_record_timestamps: false,
            metric_uid: None,
            operator: Some(proto::operator::Operator::Input(proto::Input {
                input_index: 0,
                schema: None,
            })),
        },
        0,
    );
    let calc = proto::Operator {
        plan_node_id: 3,
        metric_name: String::new(),
        clear_record_timestamps: false,
        metric_uid: None,
        operator: Some(proto::operator::Operator::Calc(Box::new(proto::Calc {
            preserve_input_envelope: false,
            input: Some(Box::new(first.clone())),
            projections: (0..4).map(reference).collect(),
            condition: None,
        }))),
    };
    let root = dedup(4, calc, 1);
    let processors = [first, root.clone()].map(|node| {
        Arc::new(Mutex::new(
            DeduplicateProcessor::new(
                &serialized(node),
                128,
                0,
                127,
                memory.sibling("generic persistent state"),
            )
            .unwrap(),
        ))
    });
    let mut context =
        NativeExecutionContext::new(&serialized(root), memory.datafusion_pool(32 << 20)).unwrap();
    context
        .bind_persistent(vec![
            (2, Arc::new(DeduplicateFactory(processors[0].clone()))),
            (4, Arc::new(DeduplicateFactory(processors[1].clone()))),
        ])
        .unwrap();
    let context = Arc::new(context);
    let mut stream = context.start(vec![batch()]).unwrap();
    let output = context.runtime().block_on(stream.next()).unwrap().unwrap();
    assert_eq!(output.num_rows(), 2);
    assert_eq!(
        output
            .column(3)
            .as_any()
            .downcast_ref::<Int32Array>()
            .unwrap()
            .values()
            .as_ref(),
        &[7, 10]
    );
    assert!(context.require_idle().is_err());
    assert!(context.runtime().block_on(stream.next()).is_none());
    assert_eq!(
        context.metric_snapshot().unwrap(),
        vec![4, 3, 2, 3, 3, 3, 2, 4, 3, 1, 0, 4]
    );
    drop(output);
    drop(stream);
    let mut stream = context.start(vec![batch()]).unwrap();
    let output = context.runtime().block_on(stream.next()).unwrap().unwrap();
    assert_eq!(output.num_rows(), 0);
    assert!(context.runtime().block_on(stream.next()).is_none());
    assert_eq!(
        context.metric_snapshot().unwrap(),
        vec![4, 3, 2, 3, 3, 3, 2, 8, 3, 1, 0, 8]
    );
    drop(output);
    drop(stream);
    drop(context);
    drop(processors);
    drop(memory);
    assert_eq!(broker.reserved(), 0);
}

#[test]
fn persistent_setup_failure_can_retry_but_lazy_stream_error_or_cancel_requires_recovery() {
    for fail in [false, true] {
        let broker = Arc::new(TestBroker::new(32 << 20));
        let memory = HostMemoryReservation::new(broker.clone(), "persistent invocation");
        let node = dedup(
            2,
            proto::Operator {
                plan_node_id: 1,
                metric_name: String::new(),
                clear_record_timestamps: false,
                metric_uid: None,
                operator: Some(proto::operator::Operator::Input(proto::Input::default())),
            },
            0,
        );
        let plan = serialized(node);
        let processor = Arc::new(Mutex::new(
            DeduplicateProcessor::new(&plan, 128, 0, 127, memory.sibling("state")).unwrap(),
        ));
        let mut context =
            NativeExecutionContext::new(&plan, memory.datafusion_pool(32 << 20)).unwrap();
        context
            .bind_persistent(vec![(2, Arc::new(DeduplicateFactory(processor.clone())))])
            .unwrap();
        let context = Arc::new(context);
        let (_, dropped) = super::stream_tests::defer(&context, batch(), fail);
        // Input arity is validated before execution and must leave persistent state reusable.
        assert!(context.start(Vec::new()).is_err());
        context.require_idle().unwrap();
        let mut stream = context.start(vec![batch()]).unwrap();
        let result = context.runtime().block_on(stream.next()).unwrap();
        if fail {
            assert!(result.is_err());
            assert!(context.runtime().block_on(stream.next()).is_none());
        } else {
            assert_eq!(result.unwrap().num_rows(), 3);
            // Even after the last data batch, downstream EOF is required before checkpointing.
        }
        drop(stream);
        assert!(dropped.load(Ordering::Relaxed));
        assert!(context.require_idle().is_err());
        assert!(context.start(vec![batch()]).is_err());
        assert!(context.execute_plan(vec![batch()], |_| Ok(())).is_err());
        drop(context);
        drop(processor);
        drop(memory);
        assert_eq!(broker.reserved(), 0);
    }
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
    fn transfer_to_arrow(&self, bytes: usize) -> Result<()> {
        self.transfers.fetch_add(1, Ordering::Relaxed);
        self.inner.release(bytes)
    }
}
#[tokio::test]
async fn intermediate_dedup_to_calc_preserves_array_owners_without_java_transfer() {
    let broker = Arc::new(Broker {
        inner: TestBroker::new(32 << 20),
        transfers: AtomicUsize::new(0),
    });
    let input = Arc::new(ReusableInputExec::new(batch().schema()));
    input.replace_batch(batch()).unwrap();
    let node = dedup(
        2,
        proto::Operator {
            plan_node_id: 1,
            metric_name: String::new(),
            clear_record_timestamps: false,
            metric_uid: None,
            operator: Some(proto::operator::Operator::Input(proto::Input {
                input_index: 0,
                schema: None,
            })),
        },
        0,
    );
    let processor = Arc::new(Mutex::new(
        DeduplicateProcessor::new(
            &serialized(node),
            128,
            0,
            127,
            HostMemoryReservation::new(broker.clone(), "handoff"),
        )
        .unwrap(),
    ));
    let dedup = Arc::new(DeduplicateExec::new(processor.clone(), input).unwrap());
    let observed: Arc<Mutex<Vec<ArrayRef>>> = Arc::new(Mutex::new(Vec::new()));
    let observer = Arc::new(Observe {
        input: dedup,
        columns: observed.clone(),
    });
    let plan = datafusion::physical_plan::projection::ProjectionExec::try_new(
        (0..4)
            .map(|index| {
                (
                    Arc::new(datafusion::physical_expr::expressions::Column::new(
                        observer.schema().field(index).name(),
                        index,
                    )) as Arc<dyn datafusion::physical_expr::PhysicalExpr>,
                    observer.schema().field(index).name().clone(),
                )
            })
            .collect::<Vec<_>>(),
        observer,
    )
    .unwrap();
    let mut stream = plan.execute(0, Arc::new(TaskContext::default())).unwrap();
    let output = stream.next().await.unwrap().unwrap();
    for (actual, expected) in output.columns().iter().zip(observed.lock().unwrap().iter()) {
        assert!(Arc::ptr_eq(actual, expected));
    }
    assert_eq!(broker.transfers.load(Ordering::Relaxed), 0);
    let retained = output.column(0).slice(0, 1);
    drop(output);
    observed.lock().unwrap().clear();
    assert!(stream.next().await.is_none());
    drop(stream);
    drop(plan);
    drop(processor);
    assert!(
        broker.inner.reserved() > 0,
        "retained output must stay admitted after producer close"
    );
    drop(retained);
    assert_eq!(broker.inner.reserved(), 0);
}
