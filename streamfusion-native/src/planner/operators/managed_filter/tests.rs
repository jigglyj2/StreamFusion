// Copyright 2026 StreamFusion Authors
// Licensed under the Apache License, Version 2.0.

use super::*;
use crate::allocation_test_support::measure;
use crate::memory_pool::{tests_support::TestBroker, FlinkMemoryPool, MemoryReservationBroker};
use arrow::array::{
    Array, ArrayData, ArrayRef, BooleanArray, Decimal128Array, Int32Array, ListArray, RecordBatch,
    StringArray, StringViewArray, StructArray,
};
use arrow::datatypes::{Field, Int32Type, Schema};
use datafusion::datasource::memory::MemorySourceConfig;
use datafusion::physical_expr::expressions::Column;
use datafusion::physical_plan::filter::FilterExecBuilder;
use datafusion::prelude::SessionContext;
use futures::StreamExt;
use std::sync::atomic::{AtomicUsize, Ordering};

#[derive(Debug)]
struct PeakBroker {
    inner: TestBroker,
    peak: AtomicUsize,
}
impl MemoryReservationBroker for PeakBroker {
    fn try_reserve(&self, bytes: usize) -> Result<bool> {
        let accepted = self.inner.try_reserve(bytes)?;
        if accepted {
            self.peak
                .fetch_max(self.inner.reserved(), Ordering::Relaxed);
        }
        Ok(accepted)
    }
    fn release(&self, bytes: usize) -> Result<()> {
        self.inner.release(bytes)
    }
    fn available(&self) -> Result<Option<usize>> {
        self.inner.available()
    }
}
fn pool(limit: usize) -> (Arc<PeakBroker>, Arc<dyn MemoryPool>) {
    let broker = Arc::new(PeakBroker {
        inner: TestBroker::new(limit),
        peak: AtomicUsize::new(0),
    });
    (
        broker.clone(),
        Arc::new(FlinkMemoryPool::new(broker, limit)),
    )
}
fn filter(batches: Vec<RecordBatch>, projection: Option<Vec<usize>>) -> FilterExec {
    let input =
        MemorySourceConfig::try_new_exec(&[batches.clone()], batches[0].schema(), None).unwrap();
    FilterExecBuilder::new(Arc::new(Column::new("keep", 0)), input)
        .with_batch_size(1)
        .apply_projection(projection)
        .unwrap()
        .build()
        .unwrap()
}
fn fixture(rows: usize, mode: usize) -> RecordBatch {
    let keep = BooleanArray::from_iter((0..rows).map(|i| match mode {
        0 => Some(true),
        1 => Some(false),
        _ => (i % 7 != 0).then_some(i % 3 == 0),
    }));
    let strings = (0..rows)
        .map(|i| (i % 11 != 0).then(|| format!("é-🦀-{i:032}")))
        .collect::<Vec<_>>();
    let text = StringArray::from_iter(strings.iter().map(|v| v.as_deref()));
    let views = StringViewArray::from_iter(strings.iter().map(|v| v.as_deref()));
    let ints = Int32Array::from_iter((0..rows).map(|i| (i % 5 != 0).then_some(i as i32)));
    let list = ListArray::from_iter_primitive::<Int32Type, _, _>(
        (0..rows).map(|i| (i % 13 != 0).then_some(vec![Some(i as i32), None, Some(-(i as i32))])),
    );
    let decimal =
        Decimal128Array::from_iter((0..rows).map(|i| (i % 3 != 0).then_some(i as i128 * 123)))
            .with_precision_and_scale(25, 2)
            .unwrap();
    let fields = vec![Arc::new(Field::new("list", list.data_type().clone(), true))];
    let nested = StructArray::new(fields.into(), vec![Arc::new(list)], None);
    let arrays: Vec<ArrayRef> = vec![
        Arc::new(keep),
        Arc::new(text),
        Arc::new(views),
        Arc::new(ints),
        Arc::new(nested),
        Arc::new(decimal),
    ];
    let fields = arrays
        .iter()
        .enumerate()
        .map(|(i, a)| {
            Field::new(
                if i == 0 {
                    "keep".into()
                } else {
                    format!("v{i}")
                },
                a.data_type().clone(),
                true,
            )
        })
        .collect::<Vec<_>>();
    RecordBatch::try_new(Arc::new(Schema::new(fields)), arrays).unwrap()
}
fn assert_bytes(actual: &ArrayData, expected: &ArrayData) {
    assert_eq!(actual.data_type(), expected.data_type());
    assert_eq!(actual.len(), expected.len());
    assert_eq!(actual.offset(), expected.offset());
    assert_eq!(actual.nulls(), expected.nulls());
    assert_eq!(actual.buffers().len(), expected.buffers().len());
    for (a, e) in actual.buffers().iter().zip(expected.buffers()) {
        assert_eq!(a.as_slice(), e.as_slice());
    }
    assert_eq!(actual.child_data().len(), expected.child_data().len());
    for (a, e) in actual.child_data().iter().zip(expected.child_data()) {
        assert_bytes(a, e);
    }
}

#[test]
fn generated_filter_matches_datafusion_bytes_and_preserves_buffer_ownership() {
    let runtime = tokio::runtime::Builder::new_current_thread()
        .build()
        .unwrap();
    for rows in [0, 1, 257, 4096] {
        for mode in [0, 1, 2] {
            for sliced in [false, true] {
                let input = fixture(rows + usize::from(sliced) * 3, mode);
                let input = if sliced { input.slice(3, rows) } else { input };
                let original = filter(vec![input.clone()], None);
                let context = SessionContext::new().task_ctx();
                let mut expected = original.execute(0, context.clone()).unwrap();
                let expected = runtime.block_on(expected.next());
                let (broker, pool) = pool(64 << 20);
                let managed =
                    ManagedFilterExec::wrap(filter(vec![input.clone()], None), Some(pool)).unwrap();
                let mut stream = managed.execute(0, context).unwrap();
                let actual = runtime.block_on(stream.next());
                assert!(runtime.block_on(stream.next()).is_none());
                drop(stream);
                drop(managed);
                match (actual, expected) {
                    (Some(Ok(actual)), Some(Ok(expected))) => {
                        if actual.num_rows() < input.num_rows() {
                            assert!(broker.inner.reserved() > 0);
                        }
                        assert_eq!(actual.num_rows(), expected.num_rows());
                        for (i, (a, e)) in
                            actual.columns().iter().zip(expected.columns()).enumerate()
                        {
                            assert_bytes(&a.to_data(), &e.to_data());
                            if mode == 0 {
                                for (a, source) in a
                                    .to_data()
                                    .buffers()
                                    .iter()
                                    .zip(input.column(i).to_data().buffers())
                                {
                                    assert_eq!(a.as_ptr(), source.as_ptr());
                                }
                            }
                        }
                    }
                    (None, None) => {}
                    (a, e) => panic!("changed result {a:?} vs {e:?}"),
                }
                assert_eq!(broker.inner.reserved(), 0);
            }
        }
    }
}

#[test]
fn denial_precedes_gather_and_projection_does_not_charge_discarded_payload() {
    let runtime = tokio::runtime::Builder::new_current_thread()
        .build()
        .unwrap();
    let input = RecordBatch::try_from_iter(vec![
        (
            "keep",
            Arc::new(BooleanArray::from_iter((0..8192).map(|i| Some(i % 2 == 0)))) as ArrayRef,
        ),
        (
            "text",
            Arc::new(StringArray::from(vec!["x".repeat(1024); 8192])) as ArrayRef,
        ),
    ])
    .unwrap();
    let context = SessionContext::new().task_ctx();
    let (broker, pool) = pool(1 << 20);
    let plan =
        ManagedFilterExec::wrap(filter(vec![input.clone()], None), Some(pool.clone())).unwrap();
    let mut stream = plan.execute(0, context.clone()).unwrap();
    let (result, observed) = measure(|| runtime.block_on(stream.next()).unwrap());
    assert!(result
        .unwrap_err()
        .to_string()
        .contains("filter gather workspace"));
    assert!(observed.peak < 64 << 10, "{observed:?}");
    drop(stream);
    assert_eq!(broker.inner.reserved(), 0);
    let projected =
        ManagedFilterExec::wrap(filter(vec![input], Some(vec![0])), Some(pool)).unwrap();
    let mut stream = projected.execute(0, context).unwrap();
    let output = runtime.block_on(stream.next()).unwrap().unwrap();
    assert_eq!(output.num_columns(), 1);
    assert_eq!(output.num_rows(), 4096);
    drop(output);
    drop(stream);
    assert_eq!(broker.inner.reserved(), 0);
}

#[test]
fn cancellation_reuse_and_cached_filter_metrics_preserve_invocation_boundaries() {
    let runtime = tokio::runtime::Builder::new_current_thread()
        .build()
        .unwrap();
    let (broker, pool) = pool(4 << 20);
    let plan = ManagedFilterExec::wrap(
        filter(vec![fixture(32, 1), fixture(32, 0)], None),
        Some(pool),
    )
    .unwrap();
    let context = SessionContext::new().task_ctx();
    let cancelled = plan.execute(0, context.clone()).unwrap();
    assert!(plan
        .execute(0, context.clone())
        .err()
        .unwrap()
        .to_string()
        .contains("concurrent"));
    drop(cancelled);
    for run in 1..=3 {
        let mut stream = plan.execute(0, context.clone()).unwrap();
        let output = runtime.block_on(stream.next()).unwrap().unwrap();
        assert_eq!(output.num_rows(), 32);
        if run != 2 {
            assert!(runtime.block_on(stream.next()).is_none());
        }
        drop(stream);
        assert_eq!(broker.inner.reserved(), 0);
        assert_eq!(plan.metrics().unwrap().output_rows(), Some(run * 32));
        drop(output);
        assert_eq!(broker.inner.reserved(), 0);
    }
}

#[test]
fn all_pass_and_all_rejected_wide_batches_need_no_gather_reservation() {
    let runtime = tokio::runtime::Builder::new_current_thread()
        .build()
        .unwrap();
    for keep in [true, false] {
        let input = RecordBatch::try_from_iter(vec![
            (
                "keep",
                Arc::new(BooleanArray::from(vec![keep; 8192])) as ArrayRef,
            ),
            (
                "text",
                Arc::new(StringArray::from(vec!["x".repeat(1024); 8192])) as ArrayRef,
            ),
        ])
        .unwrap();
        let (broker, pool) = pool(0);
        let plan = ManagedFilterExec::wrap(filter(vec![input.clone()], None), Some(pool)).unwrap();
        let mut stream = plan.execute(0, SessionContext::new().task_ctx()).unwrap();
        let result = runtime.block_on(stream.next());
        if keep {
            let output = result.unwrap().unwrap();
            assert_eq!(output, input);
            for (output, input) in output
                .column(1)
                .to_data()
                .buffers()
                .iter()
                .zip(input.column(1).to_data().buffers())
            {
                assert_eq!(output.as_ptr(), input.as_ptr());
            }
        } else {
            assert!(result.is_none());
        }
        assert!(runtime.block_on(stream.next()).is_none());
        assert_eq!(broker.inner.reserved(), 0);
    }
}

#[test]
fn shared_ipc_buffers_do_not_multiply_gather_admission_by_schema_width() {
    let runtime = tokio::runtime::Builder::new_current_thread()
        .build()
        .unwrap();
    let mut columns = vec![(
        "keep".to_string(),
        Arc::new(BooleanArray::from_iter((0..2048).map(|i| Some(i % 2 == 0)))) as ArrayRef,
    )];
    for index in 0..24 {
        columns.push((
            format!("v{index}"),
            Arc::new(StringArray::from(vec!["payload-01234567"; 2048])) as ArrayRef,
        ));
    }
    let input = RecordBatch::try_from_iter(columns).unwrap();
    let frame = crate::exchange::IpcBatchFrame::encode(&input).unwrap();
    let metadata_len = frame.metadata.len();
    let mut bytes = frame.metadata;
    bytes.extend_from_slice(&frame.body);
    let payload_bytes = bytes.len();
    let decoded =
        crate::exchange::IpcBatchFrame::decode_contiguous(bytes, metadata_len, input.schema())
            .unwrap();
    assert!(decoded.get_array_memory_size() > payload_bytes * 10);
    let (broker, pool) = pool(payload_bytes * 4);
    let plan = ManagedFilterExec::wrap(filter(vec![decoded], None), Some(pool)).unwrap();
    let mut stream = plan.execute(0, SessionContext::new().task_ctx()).unwrap();
    let output = runtime.block_on(stream.next()).unwrap().unwrap();
    assert_eq!(output.num_rows(), 1024);
    assert!(runtime.block_on(stream.next()).is_none());
    drop(output);
    drop(stream);
    assert_eq!(broker.inner.reserved(), 0);
}

#[test]
fn replacing_filter_children_keeps_one_admission_transaction() {
    let batch = fixture(512, 2);
    let (broker, pool) = pool(8 << 20);
    let original = ManagedFilterExec::wrap(filter(vec![batch.clone()], None), Some(pool)).unwrap();
    let replacement =
        MemorySourceConfig::try_new_exec(&[vec![batch.clone()]], batch.schema(), None).unwrap();
    let rewritten = original
        .clone()
        .replace_children(
            vec![replacement],
            ReplaceChildrenOptions::new(ChildrenPropertiesMode::Recompute),
        )
        .unwrap();
    let runtime = tokio::runtime::Builder::new_current_thread()
        .build()
        .unwrap();
    let context = SessionContext::new().task_ctx();
    let expected = batch
        .column(0)
        .as_any()
        .downcast_ref::<BooleanArray>()
        .unwrap()
        .true_count();
    for plan in [rewritten, original] {
        let output = runtime
            .block_on(datafusion::physical_plan::collect(plan, context.clone()))
            .unwrap();
        assert_eq!(
            output.iter().map(RecordBatch::num_rows).sum::<usize>(),
            expected
        );
        drop(output);
        assert_eq!(broker.inner.reserved(), 0);
    }
}
