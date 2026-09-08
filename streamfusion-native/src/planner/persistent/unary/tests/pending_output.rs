// Copyright 2026 StreamFusion Authors
// Licensed under the Apache License, Version 2.0.

use super::*;

#[derive(Default)]
struct Chunked {
    invocation: InvocationState,
    pending: Option<(RecordBatch, usize)>,
    fail_pending: bool,
    wrong_schema: bool,
    stall: bool,
    control_seen: Option<ControlEvent>,
}
impl UnaryBatchProcessor for Chunked {
    const NAME: &'static str = "TestChunkedUnary";
    fn invocation(&mut self) -> &mut InvocationState {
        &mut self.invocation
    }
    fn prepare_output_schema(&mut self, input: SchemaRef) -> Result<SchemaRef> {
        Ok(input)
    }
    fn process_batch(&mut self, input: RecordBatch) -> Result<RecordBatch> {
        assert!(
            self.pending.is_none(),
            "next input arrived before cursor drained"
        );
        let first = input.slice(0, input.num_rows().min(1));
        if input.num_rows() > 1 {
            self.pending = Some((input, 1));
        }
        Ok(first)
    }
    fn has_pending_output(&self) -> bool {
        self.pending.is_some()
    }
    fn poll_pending_output(&mut self) -> Result<Option<RecordBatch>> {
        if self.fail_pending {
            return Err(DataFusionError::Execution(
                "injected pending failure".into(),
            ));
        }
        if self.stall {
            return Ok(None);
        }
        let Some((input, row)) = self.pending.take() else {
            return Ok(None);
        };
        let output = if self.wrong_schema {
            input.project(&[0])?
        } else {
            input.slice(row, 1)
        };
        if row + 1 < input.num_rows() {
            self.pending = Some((input, row + 1));
        }
        Ok(Some(output))
    }
    fn poll_control(&mut self, control: ControlEvent) -> Result<Option<RecordBatch>> {
        assert!(self.pending.is_none());
        self.control_seen = Some(control);
        Ok(None)
    }
}

#[tokio::test]
async fn bounded_pending_chunks_drain_before_next_child_batch_or_watermark() {
    let input = batches();
    let upstream = Arc::new(Mutex::new(Kernel::<0>::default()));
    let child = Arc::new(UnaryExec::new(upstream.clone(), source(&input)).unwrap());
    let kernel = Arc::new(Mutex::new(Chunked::default()));
    let plan = UnaryExec::new(kernel.clone(), child)
        .unwrap()
        .with_node_id(9);
    let (context, broker) = task(1 << 20);
    let events = Arc::new(ControlEvents::default());
    let context = SessionContext::new_with_config_rt(
        SessionConfig::new().with_extension(events.clone()),
        context.runtime_env(),
    )
    .task_ctx();
    events
        .install(&[(9, ControlEvent::Watermark(99))], &context)
        .unwrap();
    let mut output = plan.execute(0, context).unwrap();
    for (batch_index, batch) in input.iter().enumerate() {
        for row in 0..batch.num_rows().max(1) {
            let actual = output.next().await.unwrap().unwrap();
            let expected = batch.slice(row, usize::from(batch.num_rows() != 0));
            assert_eq!(actual, expected);
            // Sliced buffers retain their producer; the unary handoff never transposes or
            // concatenates whole batches to produce a bounded output stream.
            assert_eq!(
                actual.column(0).to_data().buffers()[1].as_ptr(),
                expected.column(0).to_data().buffers()[1].as_ptr()
            );
            assert_eq!(upstream.lock().unwrap().batches, batch_index + 1);
            assert!(kernel.lock().unwrap().control_seen.is_none());
        }
    }
    assert!(output.next().await.is_none());
    assert_eq!(
        kernel.lock().unwrap().control_seen,
        Some(ControlEvent::Watermark(99))
    );
    drop(output);
    events.clear().unwrap();
    assert_eq!(broker.reserved(), 0);
}

#[tokio::test]
async fn pending_failure_bad_schema_stall_and_cancellation_require_recovery() {
    for fault in 0..4 {
        let kernel = Arc::new(Mutex::new(Chunked {
            fail_pending: fault == 0,
            wrong_schema: fault == 1,
            stall: fault == 2,
            ..Default::default()
        }));
        let input = probe(0);
        let plan = UnaryExec::new(kernel.clone(), input.clone()).unwrap();
        let (context, broker) = task(1 << 20);
        let mut output = plan.execute(0, context.clone()).unwrap();
        output.next().await.unwrap().unwrap();
        if fault < 3 {
            assert!(output.next().await.unwrap().is_err());
            assert!(output.next().await.is_none());
        }
        drop(output);
        assert_eq!(input.drops.load(Ordering::Relaxed), 1);
        assert!(matches!(
            kernel.lock().unwrap().invocation,
            InvocationState::Failed
        ));
        assert!(plan.execute(0, context).is_err());
        assert_eq!(broker.reserved(), 0);
    }
}
