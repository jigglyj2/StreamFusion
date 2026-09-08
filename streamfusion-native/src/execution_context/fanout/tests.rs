// Copyright 2026 StreamFusion Authors
// Licensed under the Apache License, Version 2.0.

use super::*;
use arrow::array::{ArrayRef, Int32Array, StringArray};
use futures::{FutureExt, StreamExt};
use std::collections::VecDeque;
use std::sync::atomic::{AtomicBool, AtomicUsize, Ordering};

struct Producer {
    batches: VecDeque<RecordBatch>,
    schema: SchemaRef,
    polls: Arc<AtomicUsize>,
    dropped: Arc<AtomicBool>,
    fail: bool,
}
impl Stream for Producer {
    type Item = Result<RecordBatch>;
    fn poll_next(mut self: Pin<&mut Self>, _: &mut Context<'_>) -> Poll<Option<Self::Item>> {
        self.polls.fetch_add(1, Ordering::Relaxed);
        Poll::Ready(match self.batches.pop_front() {
            Some(batch) => Some(Ok(batch)),
            None if self.fail => Some(Err(DataFusionError::Execution("producer failed".into()))),
            None => None,
        })
    }
}
impl RecordBatchStream for Producer {
    fn schema(&self) -> SchemaRef {
        self.schema.clone()
    }
}
impl Drop for Producer {
    fn drop(&mut self) {
        self.dropped.store(true, Ordering::Relaxed);
    }
}
pub(super) fn batch(value: i32) -> RecordBatch {
    RecordBatch::try_from_iter(vec![
        (
            "n",
            Arc::new(Int32Array::from(vec![Some(value), None])) as ArrayRef,
        ),
        (
            "s",
            Arc::new(StringArray::from(vec![Some("é🦀"), None])) as ArrayRef,
        ),
    ])
    .unwrap()
}
pub(super) fn fixture(
    count: usize,
    consumers: usize,
    fail: bool,
) -> (
    Vec<SendableRecordBatchStream>,
    Arc<AtomicUsize>,
    Arc<AtomicUsize>,
    Arc<AtomicBool>,
) {
    let polls = Arc::new(AtomicUsize::new(0));
    let dropped = Arc::new(AtomicBool::new(false));
    let completion = Arc::new(AtomicUsize::new(0));
    let result = completion.clone();
    let source_dropped = dropped.clone();
    let source = Producer {
        batches: (0..count).map(|n| batch(n as i32)).collect(),
        schema: batch(0).schema(),
        polls: polls.clone(),
        dropped: dropped.clone(),
        fail,
    };
    let readers = split(
        Box::pin(source),
        consumers,
        Box::new(move |ok| {
            assert!(source_dropped.load(Ordering::Relaxed));
            assert_eq!(result.swap(if ok { 1 } else { 2 }, Ordering::Relaxed), 0);
        }),
    );
    (readers, polls, completion, dropped)
}

#[test]
fn one_batch_backpressure_preserves_array_identity_and_generated_reader_order() {
    for seed in [3u64, 17, 71] {
        let (mut readers, polls, completion, dropped) = fixture(64, 3, false);
        let first = readers[0].next().now_or_never().unwrap().unwrap().unwrap();
        assert!(readers[0].next().now_or_never().is_none());
        assert_eq!(polls.load(Ordering::Relaxed), 1);
        for reader in &mut readers[1..] {
            let other = reader.next().now_or_never().unwrap().unwrap().unwrap();
            for (left, right) in first.columns().iter().zip(other.columns()) {
                assert!(Arc::ptr_eq(left, right));
            }
        }
        let mut output = vec![vec![first.clone()]; 3];
        let mut ended = [false; 3];
        let mut random = seed;
        let mut steps = 0;
        while ended.iter().any(|ended| !ended) {
            steps += 1;
            assert!(steps < 10000, "shared consumers stopped making progress");
            random = random.wrapping_mul(6364136223846793005).wrapping_add(1);
            let index = (random >> 32) as usize % 3;
            if ended[index] {
                continue;
            }
            match readers[index].next().now_or_never() {
                Some(Some(Ok(batch))) => output[index].push(batch),
                Some(None) => ended[index] = true,
                None => {}
                Some(Some(Err(error))) => panic!("{error}"),
            }
            let lengths = output.iter().map(Vec::len).collect::<Vec<_>>();
            assert!(lengths.iter().max().unwrap() - lengths.iter().min().unwrap() <= 1);
        }
        assert_eq!(completion.load(Ordering::Relaxed), 1);
        assert!(dropped.load(Ordering::Relaxed));
        assert_eq!(polls.load(Ordering::Relaxed), 65);
        for index in 0..64 {
            assert_eq!(output[0][index], batch(index as i32));
            for consumer in 1..3 {
                assert_eq!(output[consumer][index], output[0][index]);
                for (a, b) in output[0][index]
                    .columns()
                    .iter()
                    .zip(output[consumer][index].columns())
                {
                    assert!(Arc::ptr_eq(a, b));
                }
            }
        }
        drop(readers);
        assert_eq!(completion.load(Ordering::Relaxed), 1);
    }
}

#[test]
fn cancellation_releases_the_source_but_keeps_borrowed_arrow_output_alive() {
    let (mut readers, _, completion, dropped) = fixture(5, 2, false);
    let held = readers[0].next().now_or_never().unwrap().unwrap().unwrap();
    let weak = Arc::downgrade(held.column(0));
    drop(readers.remove(1));
    assert_eq!(completion.load(Ordering::Relaxed), 2);
    assert!(dropped.load(Ordering::Relaxed));
    assert!(readers[0]
        .next()
        .now_or_never()
        .unwrap()
        .unwrap()
        .unwrap_err()
        .to_string()
        .contains("cancelled"));
    drop(readers);
    assert_eq!(held, batch(0));
    assert!(weak.upgrade().is_some());
    drop(held);
    assert!(weak.upgrade().is_none());
}

#[test]
fn producer_failure_reaches_every_consumer_once_and_completes_after_cleanup() {
    let (mut readers, _, completion, _) = fixture(2, 3, true);
    for value in 0..2 {
        for reader in &mut readers {
            assert_eq!(
                reader.next().now_or_never().unwrap().unwrap().unwrap(),
                batch(value)
            );
        }
    }
    for reader in &mut readers {
        assert!(reader
            .next()
            .now_or_never()
            .unwrap()
            .unwrap()
            .unwrap_err()
            .to_string()
            .contains("producer failed"));
        assert!(reader.next().now_or_never().unwrap().is_none());
    }
    assert_eq!(completion.load(Ordering::Relaxed), 2);
}

#[test]
fn success_waits_for_every_consumer_eof_even_after_the_producer_finishes() {
    let (mut readers, _, completion, dropped) = fixture(0, 2, false);
    assert!(readers[0].next().now_or_never().unwrap().is_none());
    assert!(dropped.load(Ordering::Relaxed));
    assert_eq!(completion.load(Ordering::Relaxed), 0);
    assert!(readers[1].next().now_or_never().unwrap().is_none());
    assert_eq!(completion.load(Ordering::Relaxed), 1);
}
