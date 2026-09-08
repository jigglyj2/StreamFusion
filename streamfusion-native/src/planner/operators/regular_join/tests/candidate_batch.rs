// Copyright 2026 StreamFusion Authors
// Licensed under the Apache License, Version 2.0

use super::*;

#[test]
fn batched_predicates_preserve_input_order_nulls_and_outer_associations_across_pulls() {
    for side in 0..2 {
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
                HostMemoryReservation::new(broker.clone(), "row predicate reference"),
            )
            .unwrap();
            let mut actual = RegularJoinProcessor::new(
                &plan,
                128,
                0,
                127,
                HostMemoryReservation::new(broker.clone(), "batch predicates"),
            )
            .unwrap();
            let keys = (0..5003)
                .map(|i| if i % 13 == 0 { None } else { Some(i % 11) })
                .collect::<Vec<_>>();
            let values = (0..5003)
                .map(|i| if i % 3 == 0 { "é-other" } else { "é-same" })
                .collect::<Vec<_>>();
            let insert = (0..5003)
                .map(|i| if i % 2 == 0 { INSERT } else { UPDATE_AFTER })
                .collect::<Vec<_>>();
            let retract = (0..5003)
                .map(|i| if i % 2 == 0 { DELETE } else { UPDATE_BEFORE })
                .collect::<Vec<_>>();
            for (port, input) in [
                (
                    1 - side,
                    nullable_batch(
                        &(0..8).map(Some).collect::<Vec<_>>(),
                        &vec!["é-other"; 8],
                        &vec![INSERT; 8],
                    ),
                ),
                (side, nullable_batch(&keys, &values, &insert)),
                (side, nullable_batch(&keys, &values, &retract)),
            ] {
                let expected = reference.process_arrow(port, input.clone()).unwrap();
                actual.begin_streaming_batch(port, input).unwrap();
                let mut batches = Vec::new();
                while let Some(output) = actual.next_streaming_batch().unwrap() {
                    assert!(output.num_rows() <= BOUNDED_EDGE_OUTPUT_MAX_ROWS);
                    batches.push(output);
                }
                let result = arrow::compute::concat_batches(&expected.schema(), &batches).unwrap();
                assert_eq!(result, expected, "{join_type:?} port={port}");
                for key_group in 0..128 {
                    assert_eq!(
                        actual.snapshot_key_group(key_group).unwrap(),
                        reference.snapshot_key_group(key_group).unwrap()
                    );
                }
            }
            drop(actual);
            drop(reference);
            assert_eq!(broker.reserved(), 0);
        }
    }
}

#[test]
fn denied_predicate_batch_releases_workspace_without_flushing_dirty_state() {
    let broker = Arc::new(TestBroker::new(8 << 20));
    let mut join = RegularJoinProcessor::new(
        &plan_contract(
            proto::RegularJoinType::Inner,
            true,
            Some(not_equal_value_condition()),
        ),
        128,
        0,
        127,
        HostMemoryReservation::new(broker.clone(), "denied batch predicate"),
    )
    .unwrap();
    join.process_arrow(0, batch(&[1], &["left"], &[INSERT]))
        .unwrap();
    let writes = join.statistics()[1];
    join.begin_streaming_batch(
        1,
        batch(&vec![1; 1024], &vec!["right"; 1024], &vec![INSERT; 1024]),
    )
    .unwrap();
    let retained = broker.reserved();
    let mut pressure = HostMemoryReservation::new(broker.clone(), "another operator");
    pressure.resize((8 << 20) - retained - 1024).unwrap();
    assert!(matches!(
        join.next_streaming_batch(),
        Err(DataFusionError::ResourcesExhausted(_))
    ));
    assert_eq!(join.statistics()[1], writes);
    assert!(join.snapshot_key_group(0).is_err());
    drop(pressure);
    drop(join);
    assert_eq!(broker.reserved(), 0);
}
