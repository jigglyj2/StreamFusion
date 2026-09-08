// Copyright 2026 StreamFusion Authors
// Licensed under the Apache License, Version 2.0.
use super::*;

#[test]
fn pressure_drains_output_prefixes_and_hot_key_transitions_without_repeating_state_changes() {
    for join_type in [
        proto::RegularJoinType::Inner,
        proto::RegularJoinType::Full,
        proto::RegularJoinType::Semi,
        proto::RegularJoinType::Anti,
    ] {
        for hot in [false, true] {
            let broker = Arc::new(TestBroker::new(128 << 20));
            let mut actual = RegularJoinProcessor::new(
                &plan_contract(join_type, true, Some(not_equal_value_condition())),
                128,
                0,
                127,
                HostMemoryReservation::new(broker.clone(), "pressured join"),
            )
            .unwrap();
            let mut reference = RegularJoinProcessor::new(
                &plan_contract(join_type, true, Some(not_equal_value_condition())),
                128,
                0,
                127,
                HostMemoryReservation::new(Arc::new(TestBroker::new(128 << 20)), "reference join"),
            )
            .unwrap();
            let n = 128;
            let keys = (0..n)
                .map(|i| if hot { 1 } else { i as i64 })
                .collect::<Vec<_>>();
            let left = "é-left".repeat(1000);
            let right = "右-right".repeat(1000);
            let probe_keys = if hot { vec![1] } else { keys.clone() };
            for (arrival, (side, input)) in [
                (0, batch(&keys, &vec![left.as_str(); n], &vec![INSERT; n])),
                (
                    1,
                    batch(
                        &probe_keys,
                        &vec![right.as_str(); probe_keys.len()],
                        &vec![INSERT; probe_keys.len()],
                    ),
                ),
                (
                    1,
                    batch(
                        &probe_keys,
                        &vec![right.as_str(); probe_keys.len()],
                        &vec![DELETE; probe_keys.len()],
                    ),
                ),
            ]
            .into_iter()
            .enumerate()
            {
                let expected = reference.process_arrow(side, input.clone()).unwrap();
                let writes = actual.statistics()[1];
                actual.begin_streaming_batch(side, input).unwrap();
                let mut pressure = HostMemoryReservation::new(broker.clone(), "other consumer");
                if arrival != 0 && expected.num_rows() > 16 {
                    pressure
                        .resize((128 << 20) - broker.reserved() - (512 << 10))
                        .unwrap();
                }
                let mut batches = Vec::new();
                while let Some(output) = actual.next_streaming_batch().unwrap() {
                    batches.push(output);
                    // The competing consumer releases after the first output makes progress;
                    // final state mutation admission is a separate workspace contract.
                    pressure.resize(0).unwrap();
                }
                let output = arrow::compute::concat_batches(&expected.schema(), &batches).unwrap();
                assert_eq!(
                    output, expected,
                    "{join_type:?}, hot={hot}, arrival={arrival}"
                );
                if arrival != 0 && expected.num_rows() > 16 {
                    assert!(
                        batches.len() > 1,
                        "memory pressure must produce smaller outputs"
                    );
                }
                assert_eq!(actual.statistics()[1], writes + 1);
                drop(pressure);
                for group in 0..128 {
                    assert_eq!(
                        actual.snapshot_key_group(group).unwrap(),
                        reference.snapshot_key_group(group).unwrap()
                    );
                }
            }
            drop(actual);
            assert_eq!(broker.reserved(), 0);
        }
    }
}
