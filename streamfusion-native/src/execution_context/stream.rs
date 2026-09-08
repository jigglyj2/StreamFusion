// Copyright 2026 StreamFusion Authors
// Licensed under the Apache License, Version 2.0

use super::invocation::InvocationGuard;
use super::{NativeExecutionContext, Ordering};
use crate::memory_pool::HostMemoryReservation;
use crate::planner::persistent::control::ControlEvent;
use arrow::datatypes::SchemaRef;
use arrow::record_batch::RecordBatch;
use datafusion::error::{DataFusionError, Result};
use datafusion::physical_plan::{RecordBatchStream, SendableRecordBatchStream};
use futures::{Stream, StreamExt};
use std::pin::Pin;
use std::sync::Arc;
use std::task::{Context, Poll};

/// One invocation of a shared, retained native tree. No operator-specific tail recognition,
/// runtime, stream driver or metrics traversal belongs in a stateful operator implementation.
pub(crate) struct NativePlanStream {
    stream: Option<SendableRecordBatchStream>,
    schema: SchemaRef,
    context: Arc<NativeExecutionContext>,
    reusable: bool,
    owns_completion: bool,
    polled: bool,
}
impl NativeExecutionContext {
    pub(crate) fn control_capabilities(
        &self,
    ) -> Result<(
        Vec<u8>,
        datafusion::execution::memory_pool::MemoryReservation,
    )> {
        use prost::Message;
        self.require_idle()?;
        let memory = self.reservation("native control capability protobuf");
        memory.try_grow(
            self.persistent
                .len()
                .saturating_mul(256)
                .saturating_add(4096),
        )?;
        let stages = self
            .persistent
            .iter()
            .filter_map(|(id, factory)| {
                let stage = crate::proto::NativeStageControlCapability {
                    plan_node_id: *id,
                    watermark: factory.supports_control(ControlEvent::Watermark(0)),
                    before_checkpoint: factory.supports_control(ControlEvent::BeforeCheckpoint(0)),
                    end_input: factory.supports_control(ControlEvent::EndInput),
                };
                (stage.watermark || stage.before_checkpoint || stage.end_input).then_some(stage)
            })
            .collect();
        Ok((
            crate::proto::NativeControlCapabilities {
                protocol_version: 1,
                stages,
            }
            .encode_to_vec(),
            memory,
        ))
    }

    pub(crate) fn start(self: &Arc<Self>, batches: Vec<RecordBatch>) -> Result<NativePlanStream> {
        self.start_invocation(batches, None)
    }

    /// Flink supplies already-coalesced per-stage events. Empty typed inputs carry schemas,
    /// never data; each addressed native stage drains after its children through the same tree.
    pub(crate) fn start_control(
        self: &Arc<Self>,
        batches: Vec<RecordBatch>,
        events: &[(u64, ControlEvent)],
    ) -> Result<NativePlanStream> {
        self.require_idle()?;
        if batches.iter().any(|batch| batch.num_rows() != 0) {
            return Err(DataFusionError::Plan(
                "native control invocation requires empty input batches".into(),
            ));
        }
        for &(id, event) in events {
            let factory = self
                .persistent
                .iter()
                .find(|(node, _)| *node == id)
                .ok_or_else(|| {
                    DataFusionError::Plan(format!(
                        "native control stage {id} has no persistent binding"
                    ))
                })?;
            if !factory.1.supports_control(event) {
                return Err(DataFusionError::Plan(format!(
                    "native control stage {id} has no migrated binding for {event:?}"
                )));
            }
        }
        self.start_invocation(batches, Some(events))
    }

    fn start_invocation(
        self: &Arc<Self>,
        batches: Vec<RecordBatch>,
        events: Option<&[(u64, ControlEvent)]>,
    ) -> Result<NativePlanStream> {
        let mut invocation = InvocationGuard::begin(self)?;
        if let Some(events) = events {
            self.controls.install(events, &self.task_context())?;
        }
        let plan = self.prepare_plan(batches)?;
        let reusable = self.can_retain_stream(&plan);
        self.set_input_streaming(reusable)?;
        invocation.executing();
        let prior = self
            .retained_stream
            .lock()
            .map_err(|_| DataFusionError::Internal("native stream lock poisoned".into()))?
            .take();
        let stream = match prior {
            Some(stream) if reusable => stream,
            _ => {
                self.stream_creations.fetch_add(1, Ordering::Relaxed);
                plan.execute(0, self.task_context())?
            }
        };
        let schema = stream.schema();
        invocation.transfer_to_stream();
        Ok(NativePlanStream {
            stream: Some(stream),
            schema,
            context: self.clone(),
            reusable,
            owns_completion: true,
            polled: false,
        })
    }

    /// Compatibility edge for a caller requiring exactly one output batch. General production
    /// region consumers use the pull stream; this never concatenates or buffers fan-out output.
    pub(crate) fn execute_single_batch(
        self: &Arc<Self>,
        batches: Vec<RecordBatch>,
        mut edge: HostMemoryReservation,
    ) -> Result<RecordBatch> {
        let mut stream = self.start(batches)?;
        let result = (|| {
            let output = self
                .runtime()
                .block_on(stream.next())
                .transpose()?
                .unwrap_or_else(|| RecordBatch::new_empty(stream.schema()));
            let bytes = output.get_array_memory_size();
            edge.resize(bytes)?;
            if self
                .runtime()
                .block_on(stream.next())
                .transpose()?
                .is_some()
            {
                return Err(DataFusionError::Execution(
                    "single-batch edge received multiple batches; use the native C Stream edge"
                        .into(),
                ));
            }
            edge.transfer_to_arrow(bytes)?;
            Ok(output)
        })();
        if result.is_err() && !self.persistent.is_empty() {
            self.invocation.store(2, Ordering::Release);
        }
        result
    }
}
impl NativePlanStream {
    /// Share one execution without copying Arrow payloads. All consumers must be driven
    /// cooperatively and reach EOF before another mailbox invocation may start.
    pub(crate) fn fan_out(mut self, consumers: usize) -> Result<Vec<SendableRecordBatchStream>> {
        if self.polled || self.stream.is_none() {
            return Err(DataFusionError::Plan(
                "shared native output must be bound before polling".into(),
            ));
        }
        if consumers == 0 {
            return Err(DataFusionError::Plan(
                "shared native output requires consumers".into(),
            ));
        }
        let context = self.context.clone();
        self.owns_completion = false;
        Ok(super::fanout::split(
            Box::pin(self),
            consumers,
            Box::new(move |successful| {
                context.finish_invocation(successful);
            }),
        ))
    }

    fn finish(&self, successful: bool) {
        if self.owns_completion {
            self.context.finish_invocation(successful);
        }
    }
}

impl Stream for NativePlanStream {
    type Item = Result<RecordBatch>;
    fn poll_next(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Option<Self::Item>> {
        self.polled = true;
        let Some(stream) = self.stream.as_mut() else {
            return Poll::Ready(None);
        };
        match stream.as_mut().poll_next(cx) {
            Poll::Ready(None) => {
                drop(self.stream.take());
                self.finish(true);
                Poll::Ready(None)
            }
            Poll::Ready(Some(Err(error))) => {
                drop(self.stream.take());
                self.finish(false);
                Poll::Ready(Some(Err(error)))
            }
            Poll::Pending if self.reusable => {
                // The synchronous input has been exhausted for this mailbox arrival. Retain
                // the execution stream and its operator state for the next batch.
                let stream = self.stream.take();
                *self
                    .context
                    .retained_stream
                    .lock()
                    .unwrap_or_else(|e| e.into_inner()) = stream;
                self.finish(true);
                Poll::Ready(None)
            }
            other => other,
        }
    }
}
impl RecordBatchStream for NativePlanStream {
    fn schema(&self) -> SchemaRef {
        self.schema.clone()
    }
}
impl Drop for NativePlanStream {
    fn drop(&mut self) {
        if self.stream.is_some() {
            drop(self.stream.take());
            self.finish(false);
        }
    }
}
