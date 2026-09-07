// Copyright 2026 StreamFusion Authors
// Licensed under the Apache License, Version 2.0

use super::*;
use crate::memory_pool::{tests_support::TestBroker, FlinkMemoryPool};
use arrow::array::{
    Array, ArrayRef, Decimal128Array, Int32Array, Int8Array, ListArray, StringArray,
};
use arrow::datatypes::Int32Type;
use datafusion::datasource::memory::MemorySourceConfig;
use datafusion::execution::runtime_env::RuntimeEnvBuilder;
use datafusion::physical_plan::stream::RecordBatchStreamAdapter;
use datafusion::prelude::{SessionConfig, SessionContext};
use futures::StreamExt;
use std::sync::atomic::{AtomicUsize, Ordering};

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
fn batch(value: i32) -> RecordBatch {
    let columns: Vec<(&str, ArrayRef)> = vec![
        ("value", Arc::new(Int32Array::from(vec![-1, value]))),
        (
            "text",
            Arc::new(StringArray::from(vec![
                Some("prefix".to_string()),
                (value % 2 == 0).then(|| format!("é-{value}")),
            ])),
        ),
        (
            "amount",
            Arc::new(
                Decimal128Array::from(vec![None, Some(value as i128)])
                    .with_precision_and_scale(38, 9)
                    .unwrap(),
            ),
        ),
        (
            "nested",
            Arc::new(ListArray::from_iter_primitive::<Int32Type, _, _>(vec![
                None,
                Some(vec![Some(value), None]),
            ])),
        ),
        (
            "__streamfusion_row_kind",
            Arc::new(Int8Array::from(vec![0, value as i8])),
        ),
    ];
    RecordBatch::try_from_iter(
        columns
            .into_iter()
            .map(|(name, array)| (name, array.slice(1, 1))),
    )
    .unwrap()
}

#[tokio::test]
async fn round_robin_preserves_batches_and_shares_buffers_without_a_channel_or_concat() {
    let batches = [batch(0), batch(1), batch(2), batch(3)];
    let partitions = vec![
        vec![batches[0].clone(), batches[2].clone()],
        vec![batches[1].clone(), batches[3].clone()],
    ];
    let source = MemorySourceConfig::try_new_exec(&partitions, batches[0].schema(), None).unwrap();
    let plan = single_partition(source);
    assert_eq!(plan.output_partitioning().partition_count(), 1);
    let (context, broker) = task(64 << 10);
    let mut stream = plan.execute(0, context).unwrap();
    assert!(broker.reserved() >= 4096);
    for expected in batches {
        let actual = stream.next().await.unwrap().unwrap();
        for (left, right) in actual.columns().iter().zip(expected.columns()) {
            assert!(Arc::ptr_eq(left, right));
        }
    }
    assert!(stream.next().await.is_none());
    drop(stream);
    assert_eq!(broker.reserved(), 0);
}

#[derive(Debug)]
struct Probe {
    input: Arc<dyn ExecutionPlan>,
    opens: Arc<AtomicUsize>,
    drops: Arc<AtomicUsize>,
    fail_open: Option<usize>,
    fail_poll: Option<usize>,
}
impl DisplayAs for Probe {
    fn fmt_as(&self, _: DisplayFormatType, f: &mut fmt::Formatter) -> fmt::Result {
        write!(f, "Probe")
    }
}
impl ExecutionPlan for Probe {
    fn name(&self) -> &str {
        "Probe"
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
        if self.fail_open == Some(partition) {
            return Err(DataFusionError::Execution("opening failed".into()));
        }
        let guard = DropCounter(self.drops.clone());
        let mut stream = self.input.execute(partition, context)?;
        let fail = self.fail_poll == Some(partition);
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
                if fail {
                    return Poll::Ready(Some(Err(DataFusionError::Execution(
                        "poll failed".into(),
                    ))));
                }
                stream.as_mut().poll_next(cx)
            }),
        )))
    }
}
struct DropCounter(Arc<AtomicUsize>);
impl Drop for DropCounter {
    fn drop(&mut self) {
        self.0.fetch_add(1, Ordering::Relaxed);
    }
}
fn probe(fail_open: Option<usize>, fail_poll: Option<usize>) -> Arc<Probe> {
    Arc::new(Probe {
        input: MemorySourceConfig::try_new_exec(
            &[vec![batch(1)], vec![batch(2)], vec![batch(3)]],
            batch(0).schema(),
            None,
        )
        .unwrap(),
        opens: Arc::new(AtomicUsize::new(0)),
        drops: Arc::new(AtomicUsize::new(0)),
        fail_open,
        fail_poll,
    })
}

#[tokio::test]
async fn denial_open_error_poll_error_and_cancellation_release_every_child() {
    let source = probe(None, None);
    let plan = single_partition(source.clone());
    let (context, broker) = task(1);
    assert!(matches!(
        plan.execute(0, context),
        Err(DataFusionError::ResourcesExhausted(_))
    ));
    assert_eq!(
        source.opens.load(Ordering::Relaxed),
        0,
        "admit controls before opening any child"
    );
    assert_eq!(broker.reserved(), 0);
    let source = probe(Some(2), None);
    let (context, broker) = task(64 << 10);
    assert!(single_partition(source.clone())
        .execute(0, context)
        .is_err());
    assert_eq!(source.drops.load(Ordering::Relaxed), 2);
    assert_eq!(broker.reserved(), 0);
    for fail in [false, true] {
        let source = probe(None, fail.then_some(1));
        let (context, broker) = task(64 << 10);
        let mut stream = single_partition(source.clone())
            .execute(0, context)
            .unwrap();
        if fail {
            assert!(stream.next().await.unwrap().is_ok());
            assert!(stream.next().await.unwrap().is_err());
            assert_eq!(source.drops.load(Ordering::Relaxed), 3);
            assert!(stream.next().await.is_none());
        }
        drop(stream);
        assert_eq!(source.drops.load(Ordering::Relaxed), 3);
        assert_eq!(broker.reserved(), 0);
    }
}

#[test]
fn one_partition_is_an_identity_and_invalid_output_partition_is_rejected() {
    let source: Arc<dyn ExecutionPlan> =
        MemorySourceConfig::try_new_exec(&[vec![batch(1)]], batch(0).schema(), None).unwrap();
    assert!(Arc::ptr_eq(&source, &single_partition(source.clone())));
    let (context, broker) = task(64 << 10);
    assert!(single_partition(probe(None, None))
        .execute(1, context)
        .is_err());
    assert_eq!(broker.reserved(), 0);
}

#[tokio::test]
async fn zero_partitions_return_an_empty_stream_with_the_declared_schema() {
    let source = MemorySourceConfig::try_new_exec(&[], batch(0).schema(), None).unwrap();
    let (context, broker) = task(64 << 10);
    let mut stream = single_partition(source).execute(0, context).unwrap();
    assert_eq!(stream.schema(), batch(0).schema());
    assert!(stream.next().await.is_none());
    drop(stream);
    assert_eq!(broker.reserved(), 0);
}
