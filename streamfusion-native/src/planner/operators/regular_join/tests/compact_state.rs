// Copyright 2026 StreamFusion Authors
// Licensed under the Apache License, Version 2.0.

use super::*;

#[test]
fn compact_batch_flush_does_not_admit_nonexistent_external_page_mutations() {
    use super::coarse_memory::CountingBroker;
    use std::sync::atomic::{AtomicUsize, Ordering};
    let broker = Arc::new(CountingBroker {
        inner: TestBroker::new(32 << 20),
        calls: AtomicUsize::new(0),
        peak: AtomicUsize::new(0),
    });
    let mut join = RegularJoinProcessor::new(
        &plan(proto::RegularJoinType::Inner),
        128,
        0,
        127,
        HostMemoryReservation::new(broker.clone(), "compact batch flush"),
    )
    .unwrap();
    let keys = (0..16_384).collect::<Vec<i64>>();
    let input = batch(
        &keys,
        &vec!["payload"; keys.len()],
        &vec![INSERT; keys.len()],
    );
    broker.calls.store(0, Ordering::Relaxed);
    let before = broker.inner.reserved();
    broker.peak.store(before, Ordering::Relaxed);
    let ((), observed) = crate::allocation_test_support::measure(|| {
        join.begin_streaming_batch(0, input).unwrap();
        assert!(join.next_streaming_batch().unwrap().is_none());
    });
    assert!(observed.peak <= broker.peak.load(Ordering::Relaxed) - before);
    assert!(broker.calls.load(Ordering::Relaxed) < 128);
    let probe = keys.iter().copied().step_by(31).collect::<Vec<_>>();
    let mut count = 0;
    for keys in probe.chunks(128) {
        join.begin_streaming_batch(
            1,
            batch(keys, &vec!["right"; keys.len()], &vec![INSERT; keys.len()]),
        )
        .unwrap();
        while let Some(output) = join.next_streaming_batch().unwrap() {
            count += output.num_rows();
            assert!(output
                .column(1)
                .as_any()
                .downcast_ref::<StringArray>()
                .unwrap()
                .iter()
                .all(|value| value == Some("payload")));
        }
    }
    assert_eq!(count, probe.len());
    drop(join);
    assert_eq!(broker.inner.reserved(), 0);
}

#[test]
fn high_cardinality_small_join_keys_fit_a_coarse_managed_share() {
    let broker = Arc::new(TestBroker::new(36 << 20));
    let mut join = RegularJoinProcessor::new(
        &plan(proto::RegularJoinType::Inner),
        128,
        0,
        127,
        HostMemoryReservation::new(broker.clone(), "high cardinality join"),
    )
    .unwrap();
    let payload = "é".repeat(80);
    for start in (0..100_000).step_by(1024) {
        let end = (start + 1024).min(100_000);
        let keys = (start..end).map(i64::from).collect::<Vec<_>>();
        join.begin_streaming_batch(
            0,
            batch(
                &keys,
                &vec![payload.as_str(); keys.len()],
                &vec![INSERT; keys.len()],
            ),
        )
        .unwrap();
        while let Some(output) = join.next_streaming_batch().unwrap() {
            assert_eq!(output.num_rows(), 0);
        }
    }
    // Probe retained payloads from the opposite side after many independent state insertions.
    let keys = (0..100_000).step_by(997).map(i64::from).collect::<Vec<_>>();
    join.begin_streaming_batch(
        1,
        batch(&keys, &vec!["right"; keys.len()], &vec![INSERT; keys.len()]),
    )
    .unwrap();
    let mut count = 0;
    while let Some(output) = join.next_streaming_batch().unwrap() {
        count += output.num_rows();
        let values = output
            .column(1)
            .as_any()
            .downcast_ref::<StringArray>()
            .unwrap();
        assert!(values.iter().all(|v| v == Some(payload.as_str())));
    }
    assert_eq!(count, keys.len());
    drop(join);
    assert_eq!(broker.reserved(), 0);
}

#[test]
fn completed_batch_releases_decoded_state_before_admitting_backend_write() {
    let broker = Arc::new(TestBroker::new(8 << 20));
    let mut join = RegularJoinProcessor::new(
        &plan(proto::RegularJoinType::Inner),
        128,
        0,
        127,
        HostMemoryReservation::new(broker.clone(), "join write handoff"),
    )
    .unwrap();
    let payload = "é".repeat(80);
    let keys = (0..1024).collect::<Vec<i64>>();
    join.begin_streaming_batch(
        0,
        batch(
            &keys,
            &vec![payload.as_str(); keys.len()],
            &vec![INSERT; keys.len()],
        ),
    )
    .unwrap();
    let mut pressure = HostMemoryReservation::new(broker.clone(), "other operator");
    pressure
        .resize((8 << 20) - broker.reserved() - (7 << 18))
        .unwrap();
    while let Some(output) = join.next_streaming_batch().unwrap() {
        assert_eq!(output.num_rows(), 0);
    }
    drop(pressure);
    let mut count = 0;
    for keys in keys.chunks(128) {
        join.begin_streaming_batch(
            1,
            batch(keys, &vec!["right"; keys.len()], &vec![INSERT; keys.len()]),
        )
        .unwrap();
        while let Some(output) = join.next_streaming_batch().unwrap() {
            count += output.num_rows();
            assert!(output
                .column(1)
                .as_any()
                .downcast_ref::<StringArray>()
                .unwrap()
                .iter()
                .all(|value| value == Some(payload.as_str())));
        }
    }
    assert_eq!(count, keys.len());
    drop(join);
    assert_eq!(broker.reserved(), 0);
}
