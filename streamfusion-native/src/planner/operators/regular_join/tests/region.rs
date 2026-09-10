// Copyright 2026 StreamFusion Authors
// Licensed under the Apache License, Version 2.0

use super::super::region::RegularJoinHandle;
use super::*;

fn identified_plan(join_type: proto::RegularJoinType) -> Vec<u8> {
    let mut plan = proto::NativePlan::decode(plan(join_type).as_slice()).unwrap();
    let root = plan.root.as_mut().unwrap();
    root.plan_node_id = 3;
    let Some(proto::operator::Operator::RegularJoin(join)) = &mut root.operator else {
        unreachable!()
    };
    for (side, child) in [&mut join.left_input, &mut join.right_input]
        .into_iter()
        .enumerate()
    {
        *child = Some(Box::new(proto::Operator {
            plan_node_id: 4 + side as u64,
            metric_name: String::new(),
            clear_record_timestamps: false,
            metric_uid: None,
            operator: Some(proto::operator::Operator::Input(proto::Input {
                input_index: side as u32,
                schema: None,
            })),
        }));
    }
    for id in [2, 1] {
        plan.root = Some(proto::Operator {
            plan_node_id: id,
            metric_name: String::new(),
            clear_record_timestamps: false,
            metric_uid: None,
            operator: Some(proto::operator::Operator::Calc(Box::new(proto::Calc {
                preserve_input_envelope: false,
                input: plan.root.take().map(Box::new),
                projections: (0..if matches!(
                    join_type,
                    proto::RegularJoinType::Semi | proto::RegularJoinType::Anti
                ) {
                    4
                } else {
                    6
                })
                    .map(input_reference)
                    .collect(),
                condition: None,
            }))),
        });
    }
    plan.encode_to_vec()
}

fn handle(bytes: &[u8], broker: &Arc<TestBroker>) -> RegularJoinHandle {
    RegularJoinHandle::new(
        bytes,
        HostMemoryReservation::new(broker.clone(), "region control test"),
        |bare| {
            RegularJoinProcessor::new(
                bare,
                128,
                0,
                127,
                HostMemoryReservation::new(broker.clone(), "region state test"),
            )
        },
    )
    .unwrap()
}

#[test]
fn two_calcs_and_persistent_join_reuse_one_tree_with_changelog_state_and_stage_count_parity() {
    for join_type in [
        proto::RegularJoinType::Inner,
        proto::RegularJoinType::Full,
        proto::RegularJoinType::Semi,
        proto::RegularJoinType::Anti,
    ] {
        let broker = Arc::new(TestBroker::new(128 << 20));
        let bytes = identified_plan(join_type);
        let handle = handle(&bytes, &broker);
        let region = handle.region.as_ref().unwrap();
        let mut oracle = RegularJoinProcessor::new(
            &plan(join_type),
            128,
            0,
            127,
            HostMemoryReservation::new(broker.clone(), "join oracle"),
        )
        .unwrap();
        let mut input_rows = [0i64; 2];
        let mut output_rows = 0i64;
        let mut output_batches = 0u64;
        for (side, input) in [
            (
                0,
                batch(&vec![1; 5003], &vec!["left"; 5003], &vec![INSERT; 5003]),
            ),
            (
                1,
                batch(&[1, 2], &["right", "unmatched"], &[UPDATE_AFTER, INSERT]),
            ),
            (
                1,
                batch(&[1, 2], &["right", "unmatched"], &[UPDATE_BEFORE, DELETE]),
            ),
            (0, batch(&[1], &["left"], &[DELETE])),
        ] {
            input_rows[side] += input.num_rows() as i64;
            let expected = oracle.process_arrow(side, input.clone()).unwrap();
            let mut stream = region.start(side, input).unwrap();
            assert!(handle
                .processor
                .lock()
                .unwrap()
                .snapshot_key_group(0)
                .is_err());
            let mut output = Vec::new();
            while let Some(batch) = stream.next() {
                let batch = batch.unwrap();
                assert!(batch.num_rows() <= 4096);
                output_batches += 1;
                output_rows += batch.num_rows() as i64;
                // Calc renames projected fields; byte-level array values and changelog match.
                output.push(
                    RecordBatch::try_new(expected.schema(), batch.columns().to_vec()).unwrap(),
                );
            }
            drop(stream);
            let actual = arrow::compute::concat_batches(&expected.schema(), &output).unwrap();
            assert_eq!(actual, expected, "{join_type:?} side={side}");
            assert_eq!(
                region.metrics(),
                vec![
                    1,
                    output_rows,
                    output_rows,
                    2,
                    output_rows,
                    output_rows,
                    3,
                    input_rows[0] + input_rows[1],
                    output_rows,
                    4,
                    0,
                    input_rows[0],
                    5,
                    0,
                    input_rows[1]
                ]
            );
            assert_eq!(region.calc_batches(), 2 * output_batches);
            for group in 0..128 {
                assert_eq!(
                    handle
                        .processor
                        .lock()
                        .unwrap()
                        .snapshot_key_group(group)
                        .unwrap(),
                    oracle.snapshot_key_group(group).unwrap()
                );
            }
        }
        drop(handle);
        drop(oracle);
        assert_eq!(broker.reserved(), 0);
    }
}

#[test]
fn region_cancellation_before_poll_and_after_last_batch_requires_recovery() {
    for after_output in [false, true] {
        let broker = Arc::new(TestBroker::new(16 << 20));
        let handle = handle(&identified_plan(proto::RegularJoinType::Full), &broker);
        let region = handle.region.as_ref().unwrap();
        let mut stream = region.start(0, batch(&[1], &["left"], &[INSERT])).unwrap();
        if after_output {
            assert_eq!(stream.next().unwrap().unwrap().num_rows(), 1);
        }
        // Even the last output batch is not EOF: downstream delivery can still fail.
        assert!(handle
            .processor
            .lock()
            .unwrap()
            .snapshot_key_group(0)
            .is_err());
        drop(stream);
        assert!(handle
            .processor
            .lock()
            .unwrap()
            .snapshot_key_group(0)
            .is_err());
        assert!(region.start(1, batch(&[1], &["right"], &[INSERT])).is_err());
        drop(handle);
        assert_eq!(broker.reserved(), 0);
    }
}

#[test]
fn in_flight_tree_owns_processor_and_control_after_java_handle_destruction() {
    let broker = Arc::new(TestBroker::new(16 << 20));
    let handle = handle(&identified_plan(proto::RegularJoinType::Full), &broker);
    let mut stream = handle
        .region
        .as_ref()
        .unwrap()
        .start(0, batch(&[1], &["left"], &[INSERT]))
        .unwrap();
    drop(handle);
    assert_eq!(stream.next().unwrap().unwrap().num_rows(), 1);
    assert!(stream.next().is_none());
    drop(stream);
    assert_eq!(broker.reserved(), 0);
}

#[test]
fn region_admission_denial_does_not_mutate_state_or_leak_an_input_slot() {
    let broker = Arc::new(TestBroker::new(4 << 20));
    let handle = handle(&identified_plan(proto::RegularJoinType::Full), &broker);
    let region = handle.region.as_ref().unwrap();
    let mut pressure = HostMemoryReservation::new(broker.clone(), "competing operator");
    pressure.resize((4 << 20) - broker.reserved()).unwrap();
    assert!(region.start(0, batch(&[1], &["left"], &[INSERT])).is_err());
    drop(pressure);
    handle
        .processor
        .lock()
        .unwrap()
        .snapshot_key_group(0)
        .unwrap();
    let mut stream = region.start(0, batch(&[1], &["left"], &[INSERT])).unwrap();
    assert_eq!(stream.next().unwrap().unwrap().num_rows(), 1);
    assert!(stream.next().is_none());
    drop(stream);
    drop(handle);
    assert_eq!(broker.reserved(), 0);
}

#[test]
fn routing_normalization_shares_payload_and_preencoded_key_buffers() {
    let broker = Arc::new(TestBroker::new(1 << 20));
    let mut memory = HostMemoryReservation::new(broker.clone(), "normalization test");
    let input = batch(&[1, 2], &["é", "wide"], &[INSERT, DELETE]);
    let visible = Arc::new(Schema::new(input.schema().fields()[..2].to_vec()));
    let schema = super::super::region_input::schema(&visible);
    let normalized =
        super::super::region_input::normalize(input.clone(), schema.clone(), &[0], &mut memory)
            .unwrap();
    assert!(Arc::ptr_eq(input.column(0), normalized.column(0)));
    assert!(Arc::ptr_eq(input.column(1), normalized.column(1)));
    let again =
        super::super::region_input::normalize(normalized.clone(), schema, &[0], &mut memory)
            .unwrap();
    for (a, b) in normalized.columns().iter().zip(again.columns()) {
        assert!(Arc::ptr_eq(a, b));
    }
    drop(again);
    drop(normalized);
    drop(memory);
    assert_eq!(broker.reserved(), 0);
}

#[test]
fn persistent_region_rejects_invalid_identities_and_missing_or_misdirected_ports_without_leaks() {
    for invalid in 0..4 {
        let mut plan =
            proto::NativePlan::decode(identified_plan(proto::RegularJoinType::Inner).as_slice())
                .unwrap();
        let root = plan.root.as_mut().unwrap();
        match invalid {
            0 => root.plan_node_id = u64::MAX,
            1 => root.plan_node_id = 3,
            _ => {
                let Some(proto::operator::Operator::Calc(calc)) = root.operator.as_mut() else {
                    unreachable!()
                };
                let Some(proto::operator::Operator::Calc(calc)) =
                    calc.input.as_mut().unwrap().operator.as_mut()
                else {
                    unreachable!()
                };
                let Some(proto::operator::Operator::RegularJoin(join)) =
                    calc.input.as_mut().unwrap().operator.as_mut()
                else {
                    unreachable!()
                };
                if invalid == 2 {
                    join.left_input = None;
                } else {
                    std::mem::swap(&mut join.left_input, &mut join.right_input);
                }
            }
        }
        let broker = Arc::new(TestBroker::new(8 << 20));
        let result = RegularJoinHandle::new(
            &plan.encode_to_vec(),
            HostMemoryReservation::new(broker.clone(), "invalid region control"),
            |bare| {
                RegularJoinProcessor::new(
                    bare,
                    128,
                    0,
                    127,
                    HostMemoryReservation::new(broker.clone(), "invalid region state"),
                )
            },
        );
        assert!(
            result.is_err(),
            "invalid case {invalid} unexpectedly accepted"
        );
        assert_eq!(broker.reserved(), 0);
    }
}

mod slicing;
