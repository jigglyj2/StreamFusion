// Copyright 2026 StreamFusion Authors
// Licensed under the Apache License, Version 2.0.

//! Bounded sharing of one native stream. Downstream DataFusion streams must be polled
//! cooperatively: a fast reader cannot run ahead of another reader by buffering its input.
//! Payload arrays and their existing memory owners remain shared; only one batch descriptor
//! is retained here. No independent allocation budget or background execution is introduced.

use arrow::datatypes::SchemaRef;
use arrow::record_batch::RecordBatch;
use datafusion::error::{DataFusionError, Result};
use datafusion::physical_plan::{RecordBatchStream, SendableRecordBatchStream};
use futures::Stream;
use std::pin::Pin;
use std::sync::{Arc, Mutex};
use std::task::{Context, Poll, Waker};

type Completion = Box<dyn FnOnce(bool) + Send>;

pub(crate) fn split(
    source: SendableRecordBatchStream,
    consumers: usize,
    completion: Completion,
) -> Vec<SendableRecordBatchStream> {
    assert!(consumers > 0, "shared stream must have consumers");
    let schema = source.schema();
    let shared = Arc::new(Mutex::new(Shared {
        source: Some(source),
        batch: None,
        pending: vec![false; consumers],
        wakers: vec![None; consumers],
        remaining: consumers,
        eof: false,
        failure: None,
        completion: Some(completion),
    }));
    (0..consumers)
        .map(|index| {
            Box::pin(Reader {
                index,
                shared: shared.clone(),
                schema: schema.clone(),
                finished: false,
            }) as SendableRecordBatchStream
        })
        .collect()
}

struct Shared {
    source: Option<SendableRecordBatchStream>,
    batch: Option<RecordBatch>,
    pending: Vec<bool>,
    wakers: Vec<Option<Waker>>,
    remaining: usize,
    eof: bool,
    failure: Option<Arc<DataFusionError>>,
    completion: Option<Completion>,
}

impl Shared {
    fn wake_readers(&mut self) {
        for waker in &mut self.wakers {
            if let Some(waker) = waker.take() {
                waker.wake();
            }
        }
    }

    fn fail(&mut self, error: DataFusionError) {
        if self.failure.is_some() {
            return;
        }
        self.failure = Some(Arc::new(error));
        // Release work before publishing completion, just as the native invocation guard does.
        self.source = None;
        self.batch = None;
        self.wake_readers();
        if let Some(completion) = self.completion.take() {
            completion(false);
        }
    }
}

impl Drop for Shared {
    fn drop(&mut self) {
        self.source = None;
        self.batch = None;
        if let Some(completion) = self.completion.take() {
            completion(false);
        }
    }
}

struct Reader {
    index: usize,
    shared: Arc<Mutex<Shared>>,
    schema: SchemaRef,
    finished: bool,
}

impl Stream for Reader {
    type Item = Result<RecordBatch>;

    fn poll_next(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Option<Self::Item>> {
        let this = self.get_mut();
        if this.finished {
            return Poll::Ready(None);
        }
        let mut shared = match this.shared.lock() {
            Ok(shared) => shared,
            Err(poisoned) => {
                let mut shared = poisoned.into_inner();
                shared.fail(DataFusionError::Execution(
                    "shared native producer panicked".into(),
                ));
                shared
            }
        };
        loop {
            if let Some(error) = &shared.failure {
                this.finished = true;
                return Poll::Ready(Some(Err(DataFusionError::Shared(error.clone()))));
            }
            if let Some(batch) = &shared.batch {
                if shared.pending[this.index] {
                    let batch = batch.clone();
                    shared.pending[this.index] = false;
                    if !shared.pending.iter().any(|pending| *pending) {
                        shared.batch = None;
                        shared.wake_readers();
                    }
                    return Poll::Ready(Some(Ok(batch)));
                }
                shared.wakers[this.index] = Some(cx.waker().clone());
                return Poll::Pending;
            }
            if shared.eof {
                this.finished = true;
                shared.remaining -= 1;
                if shared.remaining == 0 {
                    if let Some(completion) = shared.completion.take() {
                        completion(true);
                    }
                }
                return Poll::Ready(None);
            }
            match shared.source.as_mut().unwrap().as_mut().poll_next(cx) {
                Poll::Ready(Some(Ok(batch))) => {
                    shared.batch = Some(batch);
                    shared.pending.fill(true);
                    shared.wake_readers();
                }
                Poll::Ready(Some(Err(error))) => shared.fail(error),
                Poll::Ready(None) => {
                    shared.source = None;
                    shared.eof = true;
                    shared.wake_readers();
                }
                Poll::Pending => {
                    shared.wakers[this.index] = Some(cx.waker().clone());
                    return Poll::Pending;
                }
            }
        }
    }
}

impl RecordBatchStream for Reader {
    fn schema(&self) -> SchemaRef {
        self.schema.clone()
    }
}

impl Drop for Reader {
    fn drop(&mut self) {
        if !self.finished {
            self.shared
                .lock()
                .unwrap_or_else(|error| error.into_inner())
                .fail(DataFusionError::Execution(
                    "shared native output consumer was cancelled before EOF".into(),
                ));
        }
    }
}

#[cfg(test)]
mod tests;

#[cfg(test)]
mod datafusion_tests;

#[cfg(test)]
mod memory_tests;
