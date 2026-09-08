// Copyright 2026 StreamFusion Authors
// Licensed under the Apache License, Version 2.0.
use super::*;
use crate::planner::persistent::{
    control::{ControlEvent, ControlEvents},
    unary::{InvocationState, UnaryBatchProcessor, UnaryExec},
    PersistentOperatorFactory,
};
use std::sync::Mutex;

struct Kernel {
    state: InvocationState,
    schema: SchemaRef,
    controls: Vec<ControlEvent>,
    sent: bool,
}
impl UnaryBatchProcessor for Kernel {
    const NAME: &'static str = "RegionControlFixture";
    fn invocation(&mut self) -> &mut InvocationState {
        &mut self.state
    }
    fn prepare_output_schema(&mut self, schema: SchemaRef) -> Result<SchemaRef> {
        self.schema = schema.clone();
        Ok(schema)
    }
    fn process_batch(&mut self, input: RecordBatch) -> Result<RecordBatch> {
        Ok(input)
    }
    fn poll_control(&mut self, event: ControlEvent) -> Result<Option<RecordBatch>> {
        if self.sent {
            self.sent = false;
            return Ok(None);
        }
        self.controls.push(event);
        self.sent = true;
        Ok(Some(RecordBatch::try_new(
            self.schema.clone(),
            batch(99).columns().to_vec(),
        )?))
    }
}
struct Factory(Arc<Mutex<Kernel>>);
impl PersistentOperatorFactory for Factory {
    fn supports_owned_envelope(&self) -> bool {
        true
    }
    fn supports_control(&self, _: ControlEvent) -> bool {
        true
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

#[test]
fn shared_persistent_control_stage_runs_once_before_both_outputs_finish() {
    let mut plan = message();
    let input = calc(&mut plan, 0).input.clone();
    plan.stages[0].operator.as_mut().unwrap().operator = Some(
        proto::operator::Operator::GroupAggregate(Box::new(proto::GroupAggregate {
            input,
            ..Default::default()
        })),
    );
    let pool: Arc<dyn MemoryPool> = Arc::new(GreedyMemoryPool::new(1 << 20));
    let controls = Arc::new(ControlEvents::default());
    let mut task = Arc::try_unwrap(task(pool.clone())).unwrap();
    task = task.with_session_config(SessionConfig::new().with_extension(controls.clone()));
    let task = Arc::new(task);
    let kernel = Arc::new(Mutex::new(Kernel {
        state: InvocationState::Idle,
        schema: batch(0).schema(),
        controls: vec![],
        sent: false,
    }));
    let contract = RegionPlan::decode(&plan.encode_to_vec(), &pool).unwrap();
    let (source, executes) = source(0, 0);
    let region = PhysicalRegion::lower(
        contract,
        vec![source],
        &[(4294967301, Arc::new(Factory(kernel.clone())))],
        pool.clone(),
    )
    .unwrap();
    for (index, event) in [
        ControlEvent::Watermark(10),
        ControlEvent::BeforeCheckpoint(9),
        ControlEvent::EndInput,
    ]
    .into_iter()
    .enumerate()
    {
        controls.install(&[(4294967301, event)], &task).unwrap();
        let clear = controls.clone();
        let mut stream = region
            .start(
                task.clone(),
                Box::new(move |success| {
                    assert!(success);
                    clear.clear().unwrap();
                }),
            )
            .unwrap();
        let outputs = drain(&mut stream, 2).unwrap();
        assert_eq!(outputs[0].len(), outputs[1].len());
        for (left, right) in outputs[0].iter().zip(&outputs[1]) {
            assert_eq!(left.columns(), right.columns());
            for index in 0..4 {
                assert!(Arc::ptr_eq(left.column(index), right.column(index)));
            }
        }
        assert_eq!(outputs[0].len(), 1);
        assert_eq!(kernel.lock().unwrap().controls.len(), index + 1);
        assert_eq!(executes.load(Ordering::Relaxed), index + 1);
        assert_eq!(
            region.metrics(),
            vec![
                4294967301,
                0,
                (index as i64 + 1) * 2,
                4294967302,
                (index as i64 + 1) * 2,
                (index as i64 + 1) * 2,
                4294967303,
                (index as i64 + 1) * 2,
                (index as i64 + 1) * 2
            ]
        );
    }
    drop(region);
    drop(task);
    assert_eq!(pool.reserved(), 0);
}
