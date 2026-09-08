// Copyright 2026 StreamFusion Authors
// Licensed under the Apache License, Version 2.0.

use super::*;

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
