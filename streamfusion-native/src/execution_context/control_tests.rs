// Copyright 2026 StreamFusion Authors
// Licensed under the Apache License, Version 2.0

use super::*;
use crate::memory_pool::{tests_support::TestBroker, FlinkMemoryPool};
use crate::planner::persistent::control::ControlEvent;
use crate::planner::persistent::unary::{InvocationState, UnaryBatchProcessor, UnaryExec};
use crate::planner::persistent::PersistentOperatorFactory;
use arrow::array::{ArrayRef, Int32Array, Int64Array, Int8Array, StringArray};
use arrow::record_batch::RecordBatch;
use futures::StreamExt;
use std::collections::VecDeque;

struct Buffered {
    invocation: InvocationState,
    output: VecDeque<RecordBatch>,
    controls: usize,
    fail: bool,
    panic: bool,
}
impl UnaryBatchProcessor for Buffered {
    const NAME: &'static str = "TestControlKernel";
    fn invocation(&mut self) -> &mut InvocationState {
        &mut self.invocation
    }
    fn prepare_output_schema(&mut self, schema: SchemaRef) -> Result<SchemaRef> {
        // A preceding native Calc may rename payload fields. Retain fixture buffers under
        // the stage's actual negotiated schema, like an operator-produced output batch.
        for output in &mut self.output {
            *output = RecordBatch::try_new(schema.clone(), output.columns().to_vec())?;
        }
        Ok(schema)
    }
    fn process_batch(&mut self, batch: RecordBatch) -> Result<RecordBatch> {
        Ok(batch)
    }
    fn poll_control(&mut self, _: ControlEvent) -> Result<Option<RecordBatch>> {
        self.controls += 1;
        assert!(!self.panic, "injected control panic");
        if self.fail {
            return Err(DataFusionError::Execution(
                "injected control failure".into(),
            ));
        }
        Ok(self.output.pop_front())
    }
}
struct Factory(Arc<Mutex<Buffered>>, bool);
impl PersistentOperatorFactory for Factory {
    fn supports_control(&self, _: ControlEvent) -> bool {
        self.1
    }
    fn build(
        &self,
        node: &proto::Operator,
        mut children: Vec<Arc<dyn ExecutionPlan>>,
    ) -> Result<Arc<dyn ExecutionPlan>> {
        Ok(Arc::new(
            UnaryExec::new(self.0.clone(), children.remove(0))?.with_node_id(node.plan_node_id),
        ))
    }
}
struct Fixture {
    context: Arc<NativeExecutionContext>,
    kernels: Vec<Arc<Mutex<Buffered>>>,
    batch: RecordBatch,
    broker: Arc<TestBroker>,
}
impl Fixture {
    fn new(supported: bool) -> Self {
        let batch = RecordBatch::try_from_iter(vec![
            ("k", Arc::new(Int64Array::from(vec![1, 2])) as ArrayRef),
            (
                "v",
                Arc::new(StringArray::from(vec![Some("é"), None])) as ArrayRef,
            ),
            (
                "__streamfusion_row_kind",
                Arc::new(Int8Array::from(vec![1, 2])) as ArrayRef,
            ),
            (
                "__streamfusion_input_row",
                Arc::new(Int32Array::from(vec![0, 1])) as ArrayRef,
            ),
        ])
        .unwrap();
        let mut root = proto::Operator {
            plan_node_id: 1,
            operator: Some(proto::operator::Operator::Input(proto::Input::default())),
            ..Default::default()
        };
        let mut bindings: Vec<PersistentBinding> = Vec::new();
        let mut kernels = Vec::new();
        for id in [2, 4] {
            root = proto::Operator {
                plan_node_id: id,
                operator: Some(proto::operator::Operator::GroupAggregate(Box::new(
                    proto::GroupAggregate {
                        input: Some(Box::new(root)),
                        ..Default::default()
                    },
                ))),
                ..Default::default()
            };
            root = proto::Operator {
                plan_node_id: id + 1,
                operator: Some(proto::operator::Operator::Calc(Box::new(proto::Calc {
                    input: Some(Box::new(root)),
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
            };
            let kernel = Arc::new(Mutex::new(Buffered {
                invocation: InvocationState::Idle,
                output: VecDeque::from(vec![batch.clone(), batch.clone()]),
                controls: 0,
                fail: false,
                panic: false,
            }));
            bindings.push((id, Arc::new(Factory(kernel.clone(), supported))));
            kernels.push(kernel);
        }
        let broker = Arc::new(TestBroker::new(8 << 20));
        let pool = Arc::new(FlinkMemoryPool::new(broker.clone(), 8 << 20));
        let bytes = proto::NativePlan {
            protocol_version: 2,
            root: Some(root),
        }
        .encode_to_vec();
        let mut context = NativeExecutionContext::new(&bytes, pool).unwrap();
        context.bind_persistent(bindings).unwrap();
        Self {
            context: Arc::new(context),
            kernels,
            batch,
            broker,
        }
    }
    fn inputs(&self) -> Vec<RecordBatch> {
        vec![self.batch.slice(0, 0)]
    }
    fn drain(&self, events: &[(u64, ControlEvent)]) -> Result<Vec<RecordBatch>> {
        let mut stream = self.context.start_control(self.inputs(), events)?;
        self.context.runtime().block_on(async {
            let mut batches = Vec::new();
            while let Some(batch) = stream.next().await {
                let batch = batch?;
                if batch.num_rows() != 0 {
                    batches.push(batch);
                }
            }
            Ok(batches)
        })
    }
}

#[test]
fn explicit_controls_drain_through_calc_and_state_stages_with_shared_arrow_and_metrics() {
    for event in [
        ControlEvent::Watermark(99),
        ControlEvent::BeforeCheckpoint(7),
        ControlEvent::EndInput,
    ] {
        let f = Fixture::new(true);
        // Ordinary invocation EOF must not touch pending control output.
        let mut ordinary = f.context.start(f.inputs()).unwrap();
        f.context.runtime().block_on(async {
            while let Some(batch) = ordinary.next().await {
                batch.unwrap();
            }
        });
        drop(ordinary);
        assert!(f
            .kernels
            .iter()
            .all(|kernel| kernel.lock().unwrap().controls == 0));
        let before = f.broker.reserved();
        let batches = f.drain(&[(2, event), (4, event)]).unwrap();
        assert_eq!(batches.len(), 4);
        for batch in batches {
            for (actual, expected) in batch.columns().iter().zip(f.batch.columns()) {
                assert!(
                    Arc::ptr_eq(actual, expected),
                    "control handoff copied an Arrow array"
                );
            }
        }
        assert_eq!(
            f.context.metric_snapshot().unwrap(),
            vec![5, 8, 8, 4, 4, 8, 3, 4, 4, 2, 0, 4, 1, 0, 0]
        );
        assert!(f
            .kernels
            .iter()
            .all(|kernel| kernel.lock().unwrap().controls == 3));
        assert_eq!(f.broker.reserved(), before);
        // The invocation-local mailbox is cleared, including after successful control EOF.
        let mut ordinary = f.context.start(f.inputs()).unwrap();
        f.context.runtime().block_on(async {
            while let Some(batch) = ordinary.next().await {
                batch.unwrap();
            }
        });
        drop(ordinary);
        assert!(f
            .kernels
            .iter()
            .all(|kernel| kernel.lock().unwrap().controls == 3));
        let broker = f.broker.clone();
        drop(f);
        assert_eq!(broker.reserved(), 0);
    }
}

#[test]
fn targeting_rejects_unknown_duplicate_unmigrated_and_data_bearing_controls_without_mutation() {
    for supported in [false, true] {
        let f = Fixture::new(supported);
        let before = f.broker.reserved();
        for events in [
            vec![(99, ControlEvent::EndInput)],
            vec![(2, ControlEvent::EndInput); 2],
            vec![(0, ControlEvent::EndInput)],
        ] {
            assert!(f.drain(&events).is_err());
            f.context.require_idle().unwrap();
        }
        assert!(f
            .context
            .start_control(vec![f.batch.clone()], &[(2, ControlEvent::EndInput)])
            .is_err());
        if !supported {
            assert!(f.drain(&[(2, ControlEvent::EndInput)]).is_err());
        }
        assert!(f
            .kernels
            .iter()
            .all(|kernel| kernel.lock().unwrap().controls == 0));
        assert_eq!(f.broker.reserved(), before);
        f.context.require_idle().unwrap();
    }
}

#[test]
fn failed_and_cancelled_control_drains_require_recovery_and_release_reservations() {
    for fail in [false, true] {
        let f = Fixture::new(true);
        f.kernels[0].lock().unwrap().fail = fail;
        if fail {
            assert!(f
                .drain(&[(2, ControlEvent::EndInput)])
                .unwrap_err()
                .to_string()
                .contains("injected control failure"));
        } else {
            let mut stream = f
                .context
                .start_control(f.inputs(), &[(2, ControlEvent::EndInput)])
                .unwrap();
            f.context.runtime().block_on(async {
                loop {
                    if stream.next().await.unwrap().unwrap().num_rows() != 0 {
                        break;
                    }
                }
            });
            drop(stream); // Cancel while the addressed stage still has another output batch.
        }
        assert!(f.context.require_idle().is_err());
        assert!(f.context.start(f.inputs()).is_err());
        let broker = f.broker.clone();
        drop(f);
        assert_eq!(broker.reserved(), 0);
    }
}

#[test]
fn controls_are_stage_addressed_and_admission_denial_is_retryable() {
    let f = Fixture::new(true);
    let hold = f.context.reservation("test occupied Flink memory");
    hold.try_grow((8 << 20) - f.broker.reserved() - 1024)
        .unwrap();
    let before = f.broker.reserved();
    assert!(matches!(
        f.drain(&[(2, ControlEvent::BeforeCheckpoint(7))]),
        Err(DataFusionError::ResourcesExhausted(_))
    ));
    f.context.require_idle().unwrap();
    assert_eq!(f.broker.reserved(), before);
    assert!(f
        .kernels
        .iter()
        .all(|kernel| kernel.lock().unwrap().controls == 0));
    hold.free();
    assert_eq!(
        f.drain(&[(2, ControlEvent::BeforeCheckpoint(7))])
            .unwrap()
            .len(),
        2
    );
    assert_eq!(f.kernels[0].lock().unwrap().controls, 3);
    assert_eq!(f.kernels[1].lock().unwrap().controls, 0);
    assert_eq!(
        f.drain(&[(4, ControlEvent::Watermark(99))]).unwrap().len(),
        2
    );
    assert_eq!(f.kernels[0].lock().unwrap().controls, 3);
    assert_eq!(f.kernels[1].lock().unwrap().controls, 3);
}

#[test]
fn child_control_output_reaches_parent_before_parent_control_and_bad_schema_fails_closed() {
    let f = Fixture::new(true);
    let mut stream = f
        .context
        .start_control(
            f.inputs(),
            &[(2, ControlEvent::EndInput), (4, ControlEvent::EndInput)],
        )
        .unwrap();
    f.context.runtime().block_on(async {
        loop {
            if stream.next().await.unwrap().unwrap().num_rows() != 0 {
                break;
            }
        }
    });
    assert_eq!(f.kernels[0].lock().unwrap().controls, 1);
    assert_eq!(f.kernels[1].lock().unwrap().controls, 0);
    // Schema corruption after negotiation must fail before the next native stage consumes it.
    f.kernels[0].lock().unwrap().output[0] = RecordBatch::try_from_iter(vec![(
        "wrong",
        Arc::new(Int64Array::from(vec![1])) as ArrayRef,
    )])
    .unwrap();
    let error = f
        .context
        .runtime()
        .block_on(stream.next())
        .unwrap()
        .unwrap_err();
    assert!(error.to_string().contains("control output differs"));
    assert!(f.context.require_idle().is_err());
    drop(stream);
    let broker = f.broker.clone();
    drop(f);
    assert_eq!(broker.reserved(), 0);
}

#[test]
fn unwinding_control_kernel_clears_invocation_storage_and_requires_recovery() {
    let f = Fixture::new(true);
    f.kernels[0].lock().unwrap().panic = true;
    let panic = std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| {
        let _ = f.drain(&[(2, ControlEvent::EndInput)]);
    }));
    assert!(panic.is_err());
    assert!(f.context.require_idle().is_err());
    let broker = f.broker.clone();
    drop(f);
    assert_eq!(broker.reserved(), 0);
}

#[test]
fn persistent_stages_reject_owned_envelopes_without_an_explicit_capability() {
    let f = Fixture::new(true);
    let mut fields = f.batch.schema().fields().to_vec();
    fields.insert(
        2,
        Arc::new(arrow::datatypes::Field::new(
            crate::planner::operators::envelope::OWNED_TIMESTAMP_V1,
            arrow::datatypes::DataType::Int64,
            true,
        )),
    );
    let mut columns = f.batch.columns().to_vec();
    columns.insert(2, Arc::new(Int64Array::from(vec![None, None])));
    let input =
        RecordBatch::try_new(Arc::new(arrow::datatypes::Schema::new(fields)), columns).unwrap();
    let before = f.broker.reserved();
    let failure = f
        .context
        .start(vec![input])
        .err()
        .expect("must reject unmigrated envelope");
    assert!(failure
        .to_string()
        .contains("owned-record envelope binding"));
    f.context.require_idle().unwrap();
    assert_eq!(f.broker.reserved(), before);
    assert!(f
        .kernels
        .iter()
        .all(|kernel| kernel.lock().unwrap().controls == 0));
}

mod fanout;
