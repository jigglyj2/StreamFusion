// Copyright 2026 StreamFusion Authors
// Licensed under the Apache License, Version 2.0

use super::*;

#[test]
fn streamed_hot_key_changelog_and_canonical_state_match_single_batch_reference() {
    for join_type in [
        proto::RegularJoinType::Inner,
        proto::RegularJoinType::Left,
        proto::RegularJoinType::Right,
        proto::RegularJoinType::Full,
        proto::RegularJoinType::Semi,
        proto::RegularJoinType::Anti,
    ] {
        let broker = Arc::new(TestBroker::new(256 << 20));
        let plan = plan_contract(join_type, true, Some(not_equal_value_condition()));
        let mut reference = RegularJoinProcessor::new(
            &plan,
            128,
            0,
            127,
            HostMemoryReservation::new(broker.clone(), "single batch reference"),
        )
        .unwrap();
        let mut actual = RegularJoinProcessor::new(
            &plan,
            128,
            0,
            127,
            HostMemoryReservation::new(broker.clone(), "streamed join"),
        )
        .unwrap();
        let n = 5003;
        for (side, input) in [
            (0, batch(&vec![1; n], &vec!["left"; n], &vec![INSERT; n])),
            (
                1,
                batch(&[1, 1], &["left", "right"], &[INSERT, UPDATE_AFTER]),
            ),
            (
                1,
                batch(&[1, 1], &["left", "right"], &[DELETE, UPDATE_BEFORE]),
            ),
            (0, batch(&[1, 1], &["left", "absent"], &[DELETE, DELETE])),
        ] {
            let expected = reference.process_arrow(side, input.clone()).unwrap();
            let writes = actual.statistics()[1];
            actual.begin_streaming_batch(side, input).unwrap();
            assert!(actual.snapshot_key_group(0).is_err());
            let mut batches = Vec::new();
            while let Some(output) = actual.next_streaming_batch().unwrap() {
                assert!(output.num_rows() <= BOUNDED_EDGE_OUTPUT_MAX_ROWS);
                batches.push(output);
            }
            let output = arrow::compute::concat_batches(&expected.schema(), &batches).unwrap();
            assert_eq!(output, expected, "{join_type:?}, side={side}");
            assert_eq!(actual.statistics()[1], writes + 1);
            for group in 0..128 {
                assert_eq!(
                    actual.snapshot_key_group(group).unwrap(),
                    reference.snapshot_key_group(group).unwrap()
                );
            }
        }
        drop(actual);
        drop(reference);
        assert_eq!(broker.reserved(), 0);
    }
}

#[test]
fn stream_cancellation_releases_workspace_and_prevents_partial_checkpoint_or_reuse() {
    let broker = Arc::new(TestBroker::new(64 << 20));
    let mut actual = RegularJoinProcessor::new(
        &plan(proto::RegularJoinType::Inner),
        128,
        0,
        127,
        HostMemoryReservation::new(broker.clone(), "cancel join stream"),
    )
    .unwrap();
    actual
        .begin_streaming_batch(
            0,
            batch(
                &vec![1; 10_001],
                &vec!["left"; 10_001],
                &vec![INSERT; 10_001],
            ),
        )
        .unwrap();
    assert!(actual.next_streaming_batch().unwrap().is_none());
    let retained = broker.reserved();
    actual
        .begin_streaming_batch(1, batch(&[1], &["right"], &[INSERT]))
        .unwrap();
    let output = actual.next_streaming_batch().unwrap().unwrap();
    assert!(output.num_rows() <= BOUNDED_EDGE_OUTPUT_MAX_ROWS);
    assert!(broker.reserved() > retained);
    assert!(actual.snapshot_key_group(0).is_err());
    actual.cancel_streaming_batch();
    assert_eq!(broker.reserved(), retained);
    assert!(actual.snapshot_key_group(0).is_err());
    assert!(actual
        .begin_streaming_batch(1, batch(&[1], &["again"], &[INSERT]))
        .is_err());
    drop(output);
    drop(actual);
    assert_eq!(broker.reserved(), 0);
}

#[test]
fn failed_stream_admission_releases_cursor_and_requires_recovery() {
    let broker = Arc::new(TestBroker::new(16 << 20));
    let mut actual = RegularJoinProcessor::new(
        &plan(proto::RegularJoinType::Inner),
        128,
        0,
        127,
        HostMemoryReservation::new(broker.clone(), "denied join stream"),
    )
    .unwrap();
    actual
        .begin_streaming_batch(0, batch(&[1], &["left"], &[INSERT]))
        .unwrap();
    assert!(actual.next_streaming_batch().unwrap().is_none());
    let retained = broker.reserved();
    actual
        .begin_streaming_batch(1, batch(&[1], &["right"], &[INSERT]))
        .unwrap();
    let mut pressure = HostMemoryReservation::new(broker.clone(), "other task consumer");
    pressure.resize((16 << 20) - broker.reserved()).unwrap();
    assert!(matches!(
        actual.next_streaming_batch(),
        Err(DataFusionError::ResourcesExhausted(_))
    ));
    drop(pressure);
    assert_eq!(broker.reserved(), retained);
    assert!(actual.snapshot_key_group(0).is_err());
    drop(actual);
    assert_eq!(broker.reserved(), 0);
}
