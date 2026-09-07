// Copyright 2026 StreamFusion Authors
// Licensed under the Apache License, Version 2.0

use super::*;
use crate::memory_pool::{tests_support::TestBroker, FlinkMemoryPool};
use arrow::array::{ArrayRef, Decimal128Array, Int32Array, Int8Array, ListArray, StringArray};
use arrow::datatypes::Int32Type;
use datafusion::datasource::memory::MemorySourceConfig;
use datafusion::execution::runtime_env::RuntimeEnvBuilder;
use datafusion::physical_plan::execution_plan::{ChildrenPropertiesMode, ReplaceChildrenOptions};
use datafusion::physical_plan::stream::RecordBatchStreamAdapter;
use datafusion::prelude::{SessionConfig, SessionContext};
use futures::StreamExt;
use std::sync::atomic::{AtomicUsize, Ordering};

#[derive(Default)]
struct Kernel<const ID: usize> {
    invocation: InvocationState,
    rows: usize,
    batches: usize,
    fail_at: Option<usize>,
    panic_at: Option<usize>,
}
impl<const ID: usize> UnaryBatchProcessor for Kernel<ID> {
    const NAME: &'static str = if ID == 0 {
        "TestFirstKernel"
    } else {
        "TestSecondKernel"
    };
    fn invocation(&mut self) -> &mut InvocationState {
        &mut self.invocation
    }
    fn prepare_output_schema(&mut self, input: SchemaRef) -> Result<SchemaRef> {
        Ok(input)
    }
    fn process_batch(&mut self, batch: RecordBatch) -> Result<RecordBatch> {
        self.batches += 1;
        if self.fail_at == Some(self.batches) {
            return Err(DataFusionError::Execution("injected kernel failure".into()));
        }
        assert_ne!(self.panic_at, Some(self.batches), "injected kernel panic");
        self.rows += batch.num_rows();
        Ok(batch)
    }
}

fn task(limit: usize) -> (Arc<TaskContext>, Arc<TestBroker>) {
    let broker = Arc::new(TestBroker::new(limit));
    let runtime = RuntimeEnvBuilder::new()
        .with_memory_pool(Arc::new(FlinkMemoryPool::new(broker.clone(), limit)))
        .build()
        .unwrap();
    (
        SessionContext::new_with_config_rt(SessionConfig::new(), Arc::new(runtime)).task_ctx(),
        broker,
    )
}
fn batches() -> Vec<RecordBatch> {
    let columns: Vec<(&str, ArrayRef)> = vec![
        (
            "text",
            Arc::new(StringArray::from(vec![
                Some("prefix"),
                None,
                Some("é"),
                Some("wide"),
                Some(""),
            ])),
        ),
        (
            "decimal",
            Arc::new(
                Decimal128Array::from(vec![None, Some(-7), Some(0), None, Some(999)])
                    .with_precision_and_scale(38, 9)
                    .unwrap(),
            ),
        ),
        (
            "nested",
            Arc::new(ListArray::from_iter_primitive::<Int32Type, _, _>(vec![
                None,
                Some(vec![Some(3), None]),
                Some(vec![]),
                None,
                Some(vec![Some(-7)]),
            ])),
        ),
        (
            "__streamfusion_row_kind",
            Arc::new(Int8Array::from(vec![0, 0, 1, 2, 3])),
        ),
        (
            "__streamfusion_input_row",
            Arc::new(Int32Array::from(vec![-1, 8, 2, 13, 9])),
        ),
    ];
    let batch = RecordBatch::try_from_iter(columns).unwrap();
    vec![batch.slice(1, 4), batch.slice(2, 0), batch.slice(2, 2)]
}
fn source(batches: &[RecordBatch]) -> Arc<dyn ExecutionPlan> {
    MemorySourceConfig::try_new_exec(&[batches.to_vec()], batches[0].schema(), None).unwrap()
}

#[tokio::test]
async fn unknown_kernel_types_compose_at_arbitrary_depth_with_shared_arrow_and_persistent_state() {
    let batches = batches();
    for depth in [1, 2, 5, 12] {
        let (context, broker) = task(1 << 20);
        let mut plan = source(&batches);
        let mut first = Vec::new();
        let mut second = Vec::new();
        for index in 0..depth {
            if index % 2 == 0 {
                let kernel = Arc::new(Mutex::new(Kernel::<0>::default()));
                plan = Arc::new(UnaryExec::new(kernel.clone(), plan).unwrap());
                first.push(kernel);
            } else {
                let kernel = Arc::new(Mutex::new(Kernel::<1>::default()));
                plan = Arc::new(UnaryExec::new(kernel.clone(), plan).unwrap());
                second.push(kernel);
            }
        }
        for _ in 0..3 {
            let mut stream = plan.execute(0, context.clone()).unwrap();
            assert!(broker.reserved() >= depth * 1024);
            for expected in &batches {
                let actual = stream.next().await.unwrap().unwrap();
                assert_eq!(actual.num_rows(), expected.num_rows());
                for (actual, expected) in actual.columns().iter().zip(expected.columns()) {
                    assert!(
                        Arc::ptr_eq(actual, expected),
                        "no array reconstruction between stages"
                    );
                }
            }
            assert!(stream.next().await.is_none());
            assert!(stream.next().await.is_none());
            drop(stream);
            assert_eq!(broker.reserved(), 0);
        }
        for kernel in &first {
            let kernel = kernel.lock().unwrap();
            kernel.invocation.require_idle("test").unwrap();
            assert_eq!((kernel.rows, kernel.batches), (18, 9));
        }
        for kernel in &second {
            let kernel = kernel.lock().unwrap();
            kernel.invocation.require_idle("test").unwrap();
            assert_eq!((kernel.rows, kernel.batches), (18, 9));
        }
    }
}

// Exercise constructor, Pending, stream error and teardown independently of any SQL family.
#[derive(Debug)]
struct InputProbe {
    input: Arc<dyn ExecutionPlan>,
    fault: AtomicUsize, // 1: constructor error, 2: poll error, 3: constructor panic
    opens: AtomicUsize,
    drops: Arc<AtomicUsize>,
}
impl DisplayAs for InputProbe {
    fn fmt_as(&self, _: DisplayFormatType, f: &mut fmt::Formatter) -> fmt::Result {
        f.write_str("InputProbe")
    }
}
impl ExecutionPlan for InputProbe {
    fn name(&self) -> &str {
        "InputProbe"
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
        context: Arc<TaskContext>,
    ) -> Result<SendableRecordBatchStream> {
        self.opens.fetch_add(1, Ordering::Relaxed);
        let fault = self.fault.load(Ordering::Relaxed);
        if fault == 1 {
            return Err(DataFusionError::Execution(
                "injected constructor failure".into(),
            ));
        }
        assert_ne!(fault, 3, "injected constructor panic");
        let mut input = self.input.execute(partition, context)?;
        let guard = DropCount(self.drops.clone());
        let mut pending = true;
        Ok(Box::pin(RecordBatchStreamAdapter::new(
            self.schema(),
            futures::stream::poll_fn(move |cx| {
                let _ = &guard;
                if pending {
                    pending = false;
                    cx.waker().wake_by_ref();
                    return Poll::Pending;
                }
                if fault == 2 {
                    return Poll::Ready(Some(Err(DataFusionError::Execution(
                        "injected poll failure".into(),
                    ))));
                }
                input.as_mut().poll_next(cx)
            }),
        )))
    }
}
struct DropCount(Arc<AtomicUsize>);
impl Drop for DropCount {
    fn drop(&mut self) {
        self.0.fetch_add(1, Ordering::Relaxed);
    }
}
fn probe(fault: usize) -> Arc<InputProbe> {
    Arc::new(InputProbe {
        input: source(&batches()),
        fault: AtomicUsize::new(fault),
        opens: AtomicUsize::new(0),
        drops: Arc::new(AtomicUsize::new(0)),
    })
}

#[tokio::test]
async fn setup_denial_and_child_constructor_error_can_retry_without_consuming_state() {
    let kernel = Arc::new(Mutex::new(Kernel::<0>::default()));
    let input = probe(1);
    let plan = UnaryExec::new(kernel.clone(), input.clone()).unwrap();
    let (small, broker) = task(1);
    assert!(plan.execute(0, small).is_err());
    assert_eq!(broker.reserved(), 0);
    assert_eq!(input.opens.load(Ordering::Relaxed), 0);
    kernel
        .lock()
        .unwrap()
        .invocation
        .require_idle("denied")
        .unwrap();
    let (context, broker) = task(1 << 20);
    assert!(plan.execute(0, context.clone()).is_err());
    assert_eq!(broker.reserved(), 0);
    kernel
        .lock()
        .unwrap()
        .invocation
        .require_idle("constructor failed")
        .unwrap();
    input.fault.store(0, Ordering::Relaxed);
    let mut stream = plan.execute(0, context).unwrap();
    while let Some(batch) = stream.next().await {
        batch.unwrap();
    }
    assert_eq!(kernel.lock().unwrap().rows, 6);
    assert_eq!(input.drops.load(Ordering::Relaxed), 1);
    drop(stream);
    assert_eq!(broker.reserved(), 0);
}

#[tokio::test]
async fn cancellation_at_each_pull_boundary_requires_recovery_and_drops_children() {
    for pulls in 0..3 {
        let kernel = Arc::new(Mutex::new(Kernel::<0>::default()));
        let input = probe(0);
        let plan = UnaryExec::new(kernel.clone(), input.clone()).unwrap();
        let (context, broker) = task(1 << 20);
        let mut stream = plan.execute(0, context.clone()).unwrap();
        assert!(plan.execute(0, context.clone()).is_err());
        assert!(kernel
            .lock()
            .unwrap()
            .invocation
            .require_idle("snapshot")
            .is_err());
        if pulls > 0 {
            assert!(stream
                .as_mut()
                .poll_next(&mut Context::from_waker(futures::task::noop_waker_ref()))
                .is_pending());
        }
        if pulls > 1 {
            stream.next().await.unwrap().unwrap();
        }
        drop(stream);
        assert_eq!(input.drops.load(Ordering::Relaxed), 1);
        assert_eq!(broker.reserved(), 0);
        assert!(matches!(
            kernel.lock().unwrap().invocation,
            InvocationState::Failed
        ));
        assert!(plan.execute(0, context).is_err());
    }
}

#[tokio::test]
async fn upstream_and_kernel_errors_are_terminal_and_release_children_immediately() {
    for child_error in [true, false] {
        let kernel = Arc::new(Mutex::new(Kernel::<0> {
            fail_at: (!child_error).then_some(2),
            ..Default::default()
        }));
        let input = probe(if child_error { 2 } else { 0 });
        let plan = UnaryExec::new(kernel.clone(), input.clone()).unwrap();
        let (context, broker) = task(1 << 20);
        let mut stream = plan.execute(0, context.clone()).unwrap();
        if !child_error {
            stream.next().await.unwrap().unwrap();
        }
        let error = stream.next().await.unwrap().unwrap_err();
        assert!(error.to_string().contains(if child_error {
            "poll failure"
        } else {
            "kernel failure"
        }));
        assert_eq!(input.drops.load(Ordering::Relaxed), 1);
        assert!(matches!(
            kernel.lock().unwrap().invocation,
            InvocationState::Failed
        ));
        assert!(stream.next().await.is_none());
        assert!(plan.execute(0, context).is_err());
        drop(stream);
        assert_eq!(broker.reserved(), 0);
    }
}

#[tokio::test]
async fn old_terminal_stream_cannot_release_a_new_invocation() {
    let kernel = Arc::new(Mutex::new(Kernel::<0>::default()));
    let input = probe(0);
    let plan = UnaryExec::new(kernel.clone(), input.clone()).unwrap();
    let (context, broker) = task(1 << 20);
    let mut old = plan.execute(0, context.clone()).unwrap();
    while let Some(batch) = old.next().await {
        batch.unwrap();
    }
    let new = plan.execute(0, context.clone()).unwrap();
    drop(old);
    assert!(matches!(
        kernel.lock().unwrap().invocation,
        InvocationState::Active
    ));
    assert_eq!(input.drops.load(Ordering::Relaxed), 1);
    assert!(plan.execute(0, context).is_err());
    drop(new);
    assert_eq!(broker.reserved(), 0);
}

#[test]
fn constructor_and_kernel_panics_leave_no_reusable_invocation_or_memory_lease() {
    for constructor in [true, false] {
        let kernel = Arc::new(Mutex::new(Kernel::<0> {
            panic_at: (!constructor).then_some(1),
            ..Default::default()
        }));
        let input = probe(if constructor { 3 } else { 0 });
        let plan = UnaryExec::new(kernel.clone(), input.clone()).unwrap();
        let (context, broker) = task(1 << 20);
        let result = std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| {
            let mut stream = plan.execute(0, context.clone()).unwrap();
            futures::executor::block_on(stream.next());
        }));
        assert!(result.is_err());
        assert!(plan.execute(0, context).is_err());
        assert_eq!(broker.reserved(), 0);
        if constructor {
            assert!(matches!(
                kernel.lock().unwrap().invocation,
                InvocationState::Failed
            ));
        } else {
            assert!(kernel.is_poisoned());
            assert_eq!(input.drops.load(Ordering::Relaxed), 1);
        }
    }
}

#[test]
fn repeated_state_owner_in_a_tree_is_rejected_without_locking_across_child_execution() {
    let kernel = Arc::new(Mutex::new(Kernel::<0>::default()));
    let child = Arc::new(UnaryExec::new(kernel.clone(), source(&batches())).unwrap());
    let root = Arc::new(UnaryExec::new(kernel.clone(), child).unwrap());
    let (context, broker) = task(1 << 20);
    assert!(root.execute(1, context.clone()).is_err());
    assert!(root
        .execute(0, context)
        .err()
        .unwrap()
        .to_string()
        .contains("active or failed"));
    assert_eq!(broker.reserved(), 0);
    kernel
        .lock()
        .unwrap()
        .invocation
        .require_idle("setup failure")
        .unwrap();
    assert!(root
        .replace_children(
            vec![],
            ReplaceChildrenOptions::new(ChildrenPropertiesMode::Recompute)
        )
        .is_err());
}

#[test]
fn multi_partition_children_must_be_normalized_by_the_shared_lowerer() {
    let batches = batches();
    let input = MemorySourceConfig::try_new_exec(
        &[batches.clone(), batches.clone()],
        batches[0].schema(),
        None,
    )
    .unwrap();
    let kernel = Arc::new(Mutex::new(Kernel::<0>::default()));
    assert!(UnaryExec::new(kernel.clone(), input)
        .unwrap_err()
        .to_string()
        .contains("normalize children"));
    kernel
        .lock()
        .unwrap()
        .invocation
        .require_idle("rejected child")
        .unwrap();
}
