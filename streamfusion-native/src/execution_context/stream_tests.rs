// Copyright 2026 StreamFusion Authors
// Licensed under the Apache License, Version 2.0

use super::*;
use crate::memory_pool::tests_support::TestBroker;
use arrow::array::{Int32Array, RecordBatch};
use arrow::datatypes::{DataType, Field, Schema};
use datafusion::common::tree_node::TreeNodeRecursion;
use datafusion::physical_expr::PhysicalExpr;
use datafusion::physical_plan::stream::RecordBatchStreamAdapter;
use datafusion::physical_plan::{
    DisplayAs, DisplayFormatType, PlanProperties, SendableRecordBatchStream,
};
use futures::{StreamExt, TryStreamExt};
use std::sync::atomic::{AtomicBool, AtomicUsize};
use std::sync::Weak;

fn batch(value: i32) -> RecordBatch {
    RecordBatch::try_new(
        Arc::new(Schema::new(vec![Field::new(
            "value",
            DataType::Int32,
            false,
        )])),
        vec![Arc::new(Int32Array::from(vec![value]))],
    )
    .unwrap()
}

fn context() -> (Arc<NativeExecutionContext>, Arc<TestBroker>) {
    let broker = Arc::new(TestBroker::new(16 << 20));
    let plan = proto::NativePlan {
        protocol_version: crate::PLAN_PROTOCOL_VERSION,
        root: Some(proto::Operator {
            plan_node_id: 1,
            metric_name: String::new(),
            clear_record_timestamps: false,
            metric_uid: None,
            operator: Some(proto::operator::Operator::Input(proto::Input::default())),
        }),
    }
    .encode_to_vec();
    let context = Arc::new(
        NativeExecutionContext::new(
            &plan,
            Arc::new(FlinkMemoryPool::new(broker.clone(), 16 << 20)),
        )
        .unwrap(),
    );
    (context, broker)
}

/// Model DataFusion's legal deferred child-execution contract. No operator pair or Java
/// boundary is involved; opening the child is intentionally postponed until the first poll.
#[derive(Debug)]
struct Deferred {
    input: Arc<dyn ExecutionPlan>,
    opens: Arc<AtomicUsize>,
    fail: bool,
    context: Weak<NativeExecutionContext>,
    dropped_while_active: Arc<AtomicBool>,
}

impl DisplayAs for Deferred {
    fn fmt_as(&self, _: DisplayFormatType, f: &mut std::fmt::Formatter) -> std::fmt::Result {
        write!(f, "DeferredTestInput")
    }
}
impl ExecutionPlan for Deferred {
    fn name(&self) -> &str {
        "DeferredTestInput"
    }
    fn properties(&self) -> &Arc<PlanProperties> {
        self.input.properties()
    }
    fn children(&self) -> Vec<&Arc<dyn ExecutionPlan>> {
        vec![&self.input]
    }
    fn apply_expressions(
        &self,
        _: &mut dyn FnMut(&Arc<dyn PhysicalExpr>) -> Result<TreeNodeRecursion>,
    ) -> Result<TreeNodeRecursion> {
        Ok(TreeNodeRecursion::Continue)
    }
    fn with_new_children(
        self: Arc<Self>,
        _: Vec<Arc<dyn ExecutionPlan>>,
    ) -> Result<Arc<dyn ExecutionPlan>> {
        Ok(self)
    }
    fn execute(
        &self,
        partition: usize,
        task: Arc<TaskContext>,
    ) -> Result<SendableRecordBatchStream> {
        let input = self.input.clone();
        let opens = self.opens.clone();
        let fail = self.fail;
        let observer = DropObserver {
            context: self.context.clone(),
            dropped_while_active: self.dropped_while_active.clone(),
        };
        let stream = futures::stream::once(async move {
            opens.fetch_add(1, Ordering::Relaxed);
            if fail {
                return Err(DataFusionError::Execution("deferred child failure".into()));
            }
            input.execute(partition, task)
        })
        .try_flatten();
        // Keep the observer through EOF, so releasing idle before dropping this stream fails.
        Ok(Box::pin(RecordBatchStreamAdapter::new(
            self.schema(),
            stream.map(move |batch| {
                let _ = &observer;
                batch
            }),
        )))
    }
}
struct DropObserver {
    context: Weak<NativeExecutionContext>,
    dropped_while_active: Arc<AtomicBool>,
}
impl Drop for DropObserver {
    fn drop(&mut self) {
        self.dropped_while_active.store(
            self.context
                .upgrade()
                .is_some_and(|context| context.require_idle().is_err()),
            Ordering::Relaxed,
        );
    }
}

pub(super) fn defer(
    context: &Arc<NativeExecutionContext>,
    input: RecordBatch,
    fail: bool,
) -> (Arc<AtomicUsize>, Arc<AtomicBool>) {
    context.execute_plan(vec![input], |_| Ok(())).unwrap();
    let opens = Arc::new(AtomicUsize::new(0));
    let dropped = Arc::new(AtomicBool::new(false));
    let mut cache = context.physical_plan.lock().unwrap();
    let cached = cache.as_mut().unwrap();
    cached.plan = Arc::new(Deferred {
        input: cached.plan.clone(),
        opens: opens.clone(),
        fail,
        context: Arc::downgrade(context),
        dropped_while_active: dropped.clone(),
    });
    (opens, dropped)
}

#[test]
fn lazy_child_keeps_input_until_eof_and_terminal_stream_cannot_clear_the_next_invocation() {
    let (context, broker) = context();
    let (opens, dropped) = defer(&context, batch(0), false);
    let input = batch(7);
    let expected = input.column(0).clone();
    let mut first = context.start(vec![input]).unwrap();
    assert_eq!(opens.load(Ordering::Relaxed), 0);
    // Both APIs must reject before replacing the active invocation's reusable inputs.
    assert!(context.start(vec![batch(99)]).is_err());
    assert!(context.execute_plan(vec![batch(99)], |_| Ok(())).is_err());
    let output = context.runtime().block_on(first.next()).unwrap().unwrap();
    assert!(Arc::ptr_eq(output.column(0), &expected));
    assert!(context.runtime().block_on(first.next()).is_none());
    assert!(dropped.load(Ordering::Relaxed));
    context.require_idle().unwrap();
    let mut next = context.start(vec![batch(8)]).unwrap();
    drop(first);
    assert!(context.require_idle().is_err());
    let actual = context.runtime().block_on(next.next()).unwrap().unwrap();
    assert_eq!(actual, batch(8));
    assert!(context.runtime().block_on(next.next()).is_none());
    drop(next);
    drop(context);
    assert_eq!(broker.reserved(), 0);
}

#[test]
fn cancellation_and_error_drop_work_and_release_input_before_allowing_stateless_retry() {
    for fail in [false, true] {
        let (context, broker) = context();
        let (_, dropped) = defer(&context, batch(0), fail);
        let input = batch(7);
        let weak: Weak<dyn arrow::array::Array> = Arc::downgrade(input.column(0));
        let mut stream = context.start(vec![input]).unwrap();
        assert!(weak.upgrade().is_some());
        if fail {
            assert!(context.runtime().block_on(stream.next()).unwrap().is_err());
            assert!(context.runtime().block_on(stream.next()).is_none());
            assert!(
                weak.upgrade().is_none(),
                "error must release inputs without waiting for stream close"
            );
        }
        drop(stream);
        assert!(dropped.load(Ordering::Relaxed));
        assert!(weak.upgrade().is_none());
        context.require_idle().unwrap();
        drop(context);
        assert_eq!(broker.reserved(), 0);
    }
}

#[test]
fn synchronous_callback_is_exclusive_and_unwinding_releases_input_slots() {
    let (context, broker) = context();
    let input = batch(7);
    let weak = Arc::downgrade(input.column(0));
    let result = std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| {
        context.execute_plan(vec![input], |_| -> Result<()> {
            assert!(context.start(vec![batch(99)]).is_err());
            panic!("test callback panic");
        })
    }));
    assert!(result.is_err());
    assert!(weak.upgrade().is_none());
    context.require_idle().unwrap();
    context.execute_plan(vec![batch(8)], |_| Ok(())).unwrap();
    drop(context);
    assert_eq!(broker.reserved(), 0);
}

#[test]
fn successive_arrivals_reuse_one_synchronous_execution_stream() {
    let (context, broker) = context();
    for value in 0..20 {
        let mut stream = context.start(vec![batch(value)]).unwrap();
        let output = context.runtime().block_on(stream.next()).unwrap().unwrap();
        assert_eq!(output, batch(value));
        assert!(context.runtime().block_on(stream.next()).is_none());
        assert_eq!(context.stream_creations.load(Ordering::Relaxed), 1);
        assert!(context.require_idle().is_ok());
    }
    drop(context);
    assert_eq!(broker.reserved(), 0);
}

mod fanout;
