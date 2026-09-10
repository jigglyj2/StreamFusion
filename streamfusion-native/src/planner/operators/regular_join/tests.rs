// Copyright 2026 StreamFusion Authors
// Licensed under the Apache License, Version 2.0

use super::*;
use crate::memory_pool::{tests_support::TestBroker, HostMemoryReservation};
use arrow::array::{ArrayRef, Int64Array, Int8Array, ListArray, StringArray};
use arrow::datatypes::Int32Type;
use prost::Message;
mod candidate_batch;
mod coarse_memory;
mod compact_state;
mod input_memory;
mod output_pressure;
mod planning_memory;
mod region;
mod region_input;
mod streaming;
mod timestamp_predicate;
mod wide_predicate;

#[test]
fn residual_candidates_use_bounded_arrow_workspace_and_release_the_mask() {
    let broker = Arc::new(TestBroker::new(8 << 20));
    let mut processor = RegularJoinProcessor::new(
        &plan_contract(
            proto::RegularJoinType::Full,
            true,
            Some(not_equal_value_condition()),
        ),
        128,
        0,
        127,
        HostMemoryReservation::new(broker.clone(), "candidate workspace test"),
    )
    .unwrap();
    let input = batch(&[1], &["left"], &[INSERT]);
    let right = batch(&[1, 1], &["left", "right"], &[INSERT, INSERT]);
    let left_rows = processor.row_converters[0]
        .convert_columns(&input.columns()[..2])
        .unwrap();
    let right_rows = processor.row_converters[1]
        .convert_columns(&right.columns()[..2])
        .unwrap();
    let encoded: [Arc<[u8]>; 2] = [
        Arc::from(right_rows.row(0).data()),
        Arc::from(right_rows.row(1).data()),
    ];
    let candidates = (0..50_003)
        .map(|index| StoredRow {
            id: index as u64,
            row: encoded[index % 2].clone(),
            associations: 0,
        })
        .collect::<Vec<_>>();
    let retained = broker.reserved();
    let mask = processor
        .condition_matches_row(0, &input, 0, left_rows.row(0).data(), &candidates)
        .unwrap();
    assert!(mask
        .iter()
        .enumerate()
        .all(|(index, matched)| matched == (index % 2 == 1)));
    assert_eq!(broker.reserved(), retained + candidates.len());
    drop(mask);
    assert_eq!(broker.reserved(), retained);

    // Null filtering and equality-only evaluation must not allocate a mask at any fan-out.
    let null_input = nullable_batch(&[None], &["left"], &[INSERT]);
    let mask = processor
        .condition_matches_row(0, &null_input, 0, left_rows.row(0).data(), &candidates)
        .unwrap();
    assert!(!mask.iter().any(|matched| matched));
    assert_eq!(broker.reserved(), retained);
    drop(mask);
    processor.residual_condition = None;
    let mask = processor
        .condition_matches_row(0, &input, 0, left_rows.row(0).data(), &candidates)
        .unwrap();
    assert!(mask.iter().all(|matched| matched));
    assert_eq!(broker.reserved(), retained);
    drop(mask);
    drop(processor);
    assert_eq!(broker.reserved(), 0);
}

#[test]
fn tiny_input_cannot_decode_large_historical_state_outside_the_budget() {
    let broker = Arc::new(TestBroker::new(8 << 20));
    let mut processor = RegularJoinProcessor::new(
        &plan(proto::RegularJoinType::Inner),
        128,
        0,
        127,
        HostMemoryReservation::new(broker.clone(), "historical state test"),
    )
    .unwrap();
    let large = "x".repeat(128 * 1024);
    processor
        .process_arrow(0, batch(&[1], &[&large], &[INSERT]))
        .unwrap();
    let reserved = broker.reserved();
    let mut pressure = HostMemoryReservation::new(broker.clone(), "other operator");
    pressure.resize((8 << 20) - reserved - 64 * 1024).unwrap();
    let before = broker.reserved();
    let result = processor.process_arrow(0, batch(&[1], &["small"], &[INSERT]));
    assert!(matches!(
        result,
        Err(DataFusionError::ResourcesExhausted(_))
    ));
    assert_eq!(broker.reserved(), before);
    drop(pressure);
    drop(processor);
    assert_eq!(broker.reserved(), 0);
}

#[test]
fn fanout_rows_share_encoded_payloads() {
    let input: Arc<[u8]> = vec![3; 64 * 1024].into();
    let candidate: Arc<[u8]> = vec![7; 64 * 1024].into();
    let mut state = JoinState {
        right: (0..100)
            .map(|_| StoredRow {
                id: 0,
                row: candidate.clone(),
                associations: 0,
            })
            .collect(),
        ..Default::default()
    };
    let mut output = Vec::new();
    process_change(
        proto::RegularJoinType::Inner,
        0,
        INSERT,
        true,
        &CandidateMatches::constant(100, true),
        input.clone(),
        0,
        &mut state,
        &mut output,
    )
    .unwrap();
    assert_eq!(output.len(), 100);
    for pair in output {
        assert!(Arc::ptr_eq(pair.left.as_ref().unwrap(), &input));
        assert!(Arc::ptr_eq(pair.right.as_ref().unwrap(), &candidate));
    }
}

#[test]
fn full_join_handles_duplicates_and_retractions() {
    let broker = Arc::new(TestBroker::new(64 << 20));
    let mut processor = RegularJoinProcessor::new(
        &plan(proto::RegularJoinType::Full),
        128,
        0,
        127,
        HostMemoryReservation::new(broker.clone(), "regular join test"),
    )
    .unwrap();
    let left = processor
        .process_arrow(0, batch(&[1], &["left"], &[INSERT]))
        .unwrap();
    assert_eq!(kinds(&left), vec![INSERT]);
    let right = processor
        .process_arrow(1, batch(&[1, 1], &["r1", "r2"], &[INSERT, INSERT]))
        .unwrap();
    assert_eq!(kinds(&right), vec![DELETE, INSERT, INSERT]);
    let retract = processor
        .process_arrow(1, batch(&[1, 1], &["r1", "r2"], &[DELETE, DELETE]))
        .unwrap();
    assert_eq!(kinds(&retract), vec![DELETE, DELETE, INSERT]);
    drop(left);
    drop(right);
    drop(retract);
    drop(processor);
    assert_eq!(broker.reserved(), 0);
}

#[test]
fn semi_and_anti_transition_on_first_and_last_match() {
    for (join_type, expected) in [
        (proto::RegularJoinType::Semi, vec![INSERT, DELETE]),
        (proto::RegularJoinType::Anti, vec![DELETE, INSERT]),
    ] {
        let broker = Arc::new(TestBroker::new(64 << 20));
        let mut processor = RegularJoinProcessor::new(
            &plan(join_type),
            128,
            0,
            127,
            HostMemoryReservation::new(broker, "semi anti test"),
        )
        .unwrap();
        let initial = processor
            .process_arrow(0, batch(&[1], &["left"], &[INSERT]))
            .unwrap();
        assert_eq!(
            kinds(&initial),
            if join_type == proto::RegularJoinType::Anti {
                vec![INSERT]
            } else {
                vec![]
            }
        );
        let add = processor
            .process_arrow(1, batch(&[1], &["right"], &[INSERT]))
            .unwrap();
        let remove = processor
            .process_arrow(1, batch(&[1], &["right"], &[DELETE]))
            .unwrap();
        assert_eq!([kinds(&add), kinds(&remove)].concat(), expected);
    }
}

#[test]
fn regular_join_modes_match_flink_transition_contract() {
    for (join_type, first, add, remove) in [
        (
            proto::RegularJoinType::Inner,
            vec![],
            vec![INSERT],
            vec![DELETE],
        ),
        (
            proto::RegularJoinType::Left,
            vec![INSERT],
            vec![DELETE, INSERT],
            vec![DELETE, INSERT],
        ),
        (
            proto::RegularJoinType::Right,
            vec![],
            vec![INSERT],
            vec![DELETE],
        ),
        (
            proto::RegularJoinType::Full,
            vec![INSERT],
            vec![DELETE, INSERT],
            vec![DELETE, INSERT],
        ),
    ] {
        let broker = Arc::new(TestBroker::new(64 << 20));
        let mut processor = RegularJoinProcessor::new(
            &plan(join_type),
            128,
            0,
            127,
            HostMemoryReservation::new(broker, "regular join modes"),
        )
        .unwrap();
        assert_eq!(
            kinds(
                &processor
                    .process_arrow(0, batch(&[1], &["left"], &[INSERT]))
                    .unwrap()
            ),
            first
        );
        assert_eq!(
            kinds(
                &processor
                    .process_arrow(1, batch(&[1], &["right"], &[INSERT]))
                    .unwrap()
            ),
            add
        );
        assert_eq!(
            kinds(
                &processor
                    .process_arrow(1, batch(&[1], &["right"], &[DELETE]))
                    .unwrap()
            ),
            remove
        );
    }
}

#[test]
fn residual_condition_controls_outer_association_transitions() {
    let broker = Arc::new(TestBroker::new(64 << 20));
    let mut processor = RegularJoinProcessor::new(
        &plan_contract(
            proto::RegularJoinType::Left,
            true,
            Some(not_equal_value_condition()),
        ),
        128,
        0,
        127,
        HostMemoryReservation::new(broker.clone(), "regular join residual condition"),
    )
    .unwrap();

    let left = processor
        .process_arrow(0, batch(&[1], &["same"], &[INSERT]))
        .unwrap();
    assert_eq!(kinds(&left), vec![INSERT]);
    let rejected = processor
        .process_arrow(1, batch(&[1], &["same"], &[INSERT]))
        .unwrap();
    assert_eq!(rejected.num_rows(), 0);
    let accepted = processor
        .process_arrow(1, batch(&[1], &["different"], &[INSERT]))
        .unwrap();
    assert_eq!(kinds(&accepted), vec![DELETE, INSERT]);
    let rejected_retract = processor
        .process_arrow(1, batch(&[1], &["same"], &[DELETE]))
        .unwrap();
    assert_eq!(rejected_retract.num_rows(), 0);
    let accepted_retract = processor
        .process_arrow(1, batch(&[1], &["different"], &[DELETE]))
        .unwrap();
    assert_eq!(kinds(&accepted_retract), vec![DELETE, INSERT]);

    drop(left);
    drop(rejected);
    drop(accepted);
    drop(rejected_retract);
    drop(accepted_retract);
    drop(processor);
    assert_eq!(broker.reserved(), 0);
}

#[test]
fn null_filtered_keys_never_match_and_missing_retractions_are_tolerated() {
    let broker = Arc::new(TestBroker::new(64 << 20));
    let mut processor = RegularJoinProcessor::new(
        &plan(proto::RegularJoinType::Full),
        128,
        0,
        127,
        HostMemoryReservation::new(broker, "regular join null keys"),
    )
    .unwrap();
    let left = nullable_batch(&[None], &["left"], &[INSERT]);
    let right = nullable_batch(&[None], &["right"], &[INSERT]);
    assert_eq!(
        kinds(&processor.process_arrow(0, left).unwrap()),
        vec![INSERT]
    );
    assert_eq!(
        kinds(&processor.process_arrow(1, right).unwrap()),
        vec![INSERT]
    );

    // Flink's MapState-based no-unique-key view ignores the absent state
    // record and continues evaluating the transition.
    let missing = processor
        .process_arrow(0, nullable_batch(&[None], &["missing"], &[DELETE]))
        .unwrap();
    assert_eq!(kinds(&missing), vec![DELETE]);
}

#[test]
fn null_safe_keys_match_for_intersect_and_except_join_shapes() {
    let broker = Arc::new(TestBroker::new(64 << 20));
    for (join_type, expected_before, expected_match) in [
        (proto::RegularJoinType::Semi, Vec::new(), vec![INSERT]),
        (proto::RegularJoinType::Anti, vec![INSERT], vec![DELETE]),
    ] {
        let mut processor = RegularJoinProcessor::new(
            &plan_with_filter(join_type, false),
            128,
            0,
            127,
            HostMemoryReservation::new(broker.clone(), "regular join null-safe keys"),
        )
        .unwrap();
        assert_eq!(
            kinds(
                &processor
                    .process_arrow(0, nullable_batch(&[None], &["left"], &[INSERT]))
                    .unwrap()
            ),
            expected_before
        );
        assert_eq!(
            kinds(
                &processor
                    .process_arrow(1, nullable_batch(&[None], &["right"], &[INSERT]))
                    .unwrap()
            ),
            expected_match
        );
    }
}

#[test]
fn residual_join_state_restores_after_rescaling() {
    let broker = Arc::new(TestBroker::new(1 << 30));
    let mut source = processor(broker.clone(), 0, 127);
    source
        .process_arrow(0, batch(&[1, 2], &["a", "b"], &[INSERT, INSERT]))
        .unwrap();
    let snapshots = (0..128)
        .map(|group| source.snapshot_key_group(group).unwrap())
        .collect::<Vec<_>>();
    let mut low = processor(broker.clone(), 0, 63);
    let mut high = processor(broker, 64, 127);
    for (group, snapshot) in snapshots.iter().enumerate() {
        if group < 64 {
            low.restore_key_group(group as u32, snapshot).unwrap();
        } else {
            high.restore_key_group(group as u32, snapshot).unwrap();
        }
    }
    for key in [1, 2] {
        let input = batch(&[key], &["r"], &[INSERT]);
        let encoded = source.group_key(1, &input, 0).unwrap();
        let result = if assign_key_group(&encoded, 128) < 64 {
            low.process_arrow(1, input).unwrap()
        } else {
            high.process_arrow(1, input).unwrap()
        };
        assert_eq!(kinds(&result), vec![DELETE, INSERT]);
    }
}

#[test]
fn residual_join_state_moves_from_memory_to_rocksdb_and_batches_io() {
    let Ok(plugin_path) = std::env::var("STREAMFUSION_TEST_ROCKSDB_PLUGIN") else {
        return;
    };
    let broker = Arc::new(TestBroker::new(1 << 30));
    let mut memory = RegularJoinProcessor::new(
        &plan_contract(
            proto::RegularJoinType::Full,
            true,
            Some(not_equal_value_condition()),
        ),
        128,
        0,
        127,
        HostMemoryReservation::new(broker.clone(), "regular join memory source"),
    )
    .unwrap();
    memory
        .process_arrow(0, batch(&[1, 2], &["a", "b"], &[INSERT, INSERT]))
        .unwrap();
    assert_eq!(memory.statistics(), [1, 1, 0]);
    let snapshots = (0..128)
        .map(|group| memory.snapshot_key_group(group).unwrap())
        .collect::<Vec<_>>();

    let directory = tempfile::tempdir().unwrap();
    let mut rocks = RegularJoinProcessor::new_rocksdb(
        &plan_contract(
            proto::RegularJoinType::Full,
            true,
            Some(not_equal_value_condition()),
        ),
        128,
        0,
        127,
        std::path::Path::new(&plugin_path),
        directory.path(),
        64 << 20,
        HostMemoryReservation::new(broker, "regular join RocksDB scratch"),
    )
    .unwrap();
    for (group, snapshot) in snapshots.iter().enumerate() {
        rocks.restore_key_group(group as u32, snapshot).unwrap();
        assert_eq!(rocks.snapshot_key_group(group as u32).unwrap(), *snapshot);
    }
    let output = rocks
        .process_arrow(1, batch(&[1, 2], &["a", "r2"], &[INSERT, INSERT]))
        .unwrap();
    assert_eq!(kinds(&output), vec![INSERT, DELETE, INSERT]);
    assert_eq!(rocks.statistics(), [1, 1, 0]); // compact keys need only the manifest read
}

#[test]
fn bounded_join_emits_final_insert_only_results_for_every_join_type() {
    for (join_type, expected_rows) in [
        (proto::RegularJoinType::Inner, 1),
        (proto::RegularJoinType::Left, 2),
        (proto::RegularJoinType::Right, 2),
        (proto::RegularJoinType::Full, 3),
        (proto::RegularJoinType::Semi, 1),
        (proto::RegularJoinType::Anti, 1),
    ] {
        let broker = Arc::new(TestBroker::new(64 << 20));
        let mut processor = RegularJoinProcessor::new(
            &bounded_plan(join_type, true, None),
            128,
            0,
            127,
            HostMemoryReservation::new(broker.clone(), "bounded regular join modes"),
        )
        .unwrap();
        assert_eq!(
            processor
                .process_arrow(0, batch(&[1, 2], &["l1", "l2"], &[INSERT, INSERT]))
                .unwrap()
                .num_rows(),
            0
        );
        assert_eq!(
            processor
                .process_arrow(1, batch(&[1, 3], &["r1", "r3"], &[INSERT, INSERT]))
                .unwrap()
                .num_rows(),
            0
        );
        let output = processor.finish_bounded_output().unwrap();
        assert_eq!(output.num_rows(), expected_rows, "{join_type:?}");
        assert_eq!(kinds(&output), vec![INSERT; expected_rows], "{join_type:?}");
        assert_eq!(processor.finish_bounded_output().unwrap().num_rows(), 0);
        drop(output);
        drop(processor);
        assert_eq!(broker.reserved(), 0);
    }
}

#[test]
fn bounded_join_fuses_the_adjacent_calc_tail_in_one_native_plan() {
    let broker = Arc::new(TestBroker::new(64 << 20));
    let mut processor = RegularJoinProcessor::new(
        &bounded_plan_with_identity_calc(proto::RegularJoinType::Inner),
        128,
        0,
        127,
        HostMemoryReservation::new(broker.clone(), "bounded fused Calc join"),
    )
    .unwrap();
    processor
        .process_arrow(0, batch(&[1], &["left"], &[INSERT]))
        .unwrap();
    processor
        .process_arrow(1, batch(&[1], &["right"], &[INSERT]))
        .unwrap();

    let output = processor.finish_bounded_output().unwrap();
    assert_eq!(output.num_rows(), 1);
    assert_eq!(output.num_columns(), 6);
    assert_eq!(kinds(&output), vec![INSERT]);
    assert_eq!(processor.statistics()[2], 1);
    drop(output);
    drop(processor);
    assert_eq!(broker.reserved(), 0);
}

#[test]
fn bounded_join_pushes_a_pure_calc_projection_into_terminal_row_decode() {
    let broker = Arc::new(TestBroker::new(64 << 20));
    let mut processor = RegularJoinProcessor::new(
        &bounded_plan_with_left_projection(),
        128,
        0,
        127,
        HostMemoryReservation::new(broker.clone(), "bounded projected Calc join"),
    )
    .unwrap();
    assert_eq!(processor.bounded_row_required, [true, false]);
    processor
        .process_arrow(0, batch(&[1], &["left"], &[INSERT]))
        .unwrap();
    processor
        .process_arrow(
            1,
            batch(
                &[1, 1, 1],
                &["left", "left", "left"],
                &[INSERT, INSERT, DELETE],
            ),
        )
        .unwrap();

    let output = processor.finish_bounded_output().unwrap();
    assert_eq!(output.num_rows(), 1);
    assert_eq!(output.num_columns(), 4);
    assert_eq!(
        output
            .column(1)
            .as_any()
            .downcast_ref::<StringArray>()
            .unwrap()
            .value(0),
        "left"
    );
    assert_eq!(kinds(&output), vec![INSERT]);
    assert_eq!(processor.statistics()[2], 1);
    drop(output);
    drop(processor);
    assert_eq!(broker.reserved(), 0);
}

#[test]
fn streaming_join_fuses_the_adjacent_calc_tail_in_one_native_plan() {
    let broker = Arc::new(TestBroker::new(64 << 20));
    let mut processor = RegularJoinProcessor::new(
        &streaming_plan_with_identity_calc(proto::RegularJoinType::Inner),
        128,
        0,
        127,
        HostMemoryReservation::new(broker.clone(), "streaming fused Calc join"),
    )
    .unwrap();
    assert_eq!(
        processor
            .process_arrow(0, batch(&[1], &["left"], &[INSERT]))
            .unwrap()
            .num_rows(),
        0
    );
    let output = processor
        .process_arrow(1, batch(&[1], &["right"], &[INSERT]))
        .unwrap();
    assert_eq!(output.num_rows(), 1);
    assert_eq!(output.num_columns(), 6);
    assert_eq!(kinds(&output), vec![INSERT]);
    assert_eq!(processor.statistics()[2], 2);
    drop(output);
    drop(processor);
    assert_eq!(broker.reserved(), 0);
}

#[test]
fn bounded_join_ingests_exchange_frames_without_exporting_empty_arrow_batches() {
    let broker = Arc::new(TestBroker::new(64 << 20));
    let mut processor = RegularJoinProcessor::new(
        &bounded_plan(proto::RegularJoinType::Inner, true, None),
        128,
        0,
        127,
        HostMemoryReservation::new(broker.clone(), "bounded direct exchange join"),
    )
    .unwrap();
    let exchange_plan = exchange_plan();
    for (side, value) in [(0, "left"), (1, "right")] {
        let frame = crate::exchange::IpcBatchFrame::encode(&exchange_batch(1, value)).unwrap();
        let metadata_length = frame.metadata.len();
        let mut payload = frame.metadata;
        payload.extend_from_slice(&frame.body);
        let key_group = assign_key_group(
            &encode_binary_row(&exchange_batch(1, value), 0, &[(0, KeyField::BigInt)]).unwrap(),
            128,
        );
        assert_eq!(
            processor
                .process_bounded_exchange_frame(
                    side,
                    key_group,
                    &exchange_plan,
                    payload,
                    metadata_length,
                )
                .unwrap(),
            1
        );
    }

    let output = processor.finish_bounded_output().unwrap();
    assert_eq!(output.num_rows(), 1);
    assert_eq!(kinds(&output), vec![INSERT]);
    drop(output);
    drop(processor);
    assert_eq!(broker.reserved(), 0);
}

#[test]
fn bounded_join_direct_exchange_transports_opaque_complex_keys() {
    let broker = Arc::new(TestBroker::new(64 << 20));
    let mut processor = RegularJoinProcessor::new(
        &bounded_array_key_plan(),
        128,
        0,
        127,
        HostMemoryReservation::new(broker.clone(), "bounded complex direct exchange join"),
    )
    .unwrap();
    let exchange_plan = array_key_exchange_plan();
    for (side, keys, values) in [
        (0, vec![vec![1, 2]], vec!["left"]),
        (1, vec![vec![9], vec![1, 2]], vec!["miss", "right"]),
    ] {
        let frame =
            crate::exchange::IpcBatchFrame::encode(&array_key_exchange_batch(&keys, &values))
                .unwrap();
        let metadata_length = frame.metadata.len();
        let mut payload = frame.metadata;
        payload.extend_from_slice(&frame.body);
        assert_eq!(
            processor
                .process_bounded_exchange_frame(side, 37, &exchange_plan, payload, metadata_length,)
                .unwrap(),
            keys.len()
        );
    }

    let output = processor.finish_bounded_output().unwrap();
    assert_eq!(output.num_rows(), 1);
    assert_eq!(kinds(&output), vec![INSERT]);
    drop(output);
    drop(processor);
    assert_eq!(broker.reserved(), 0);
}

#[test]
fn bounded_join_applies_retractions_null_filters_and_residual_conditions() {
    let broker = Arc::new(TestBroker::new(64 << 20));
    let mut processor = RegularJoinProcessor::new(
        &bounded_plan(
            proto::RegularJoinType::Full,
            true,
            Some(not_equal_value_condition()),
        ),
        128,
        0,
        127,
        HostMemoryReservation::new(broker, "bounded join contract"),
    )
    .unwrap();
    processor
        .process_arrow(
            0,
            nullable_batch(
                &[Some(1), Some(1), None],
                &["removed", "kept", "null-left"],
                &[INSERT, INSERT, INSERT],
            ),
        )
        .unwrap();
    processor
        .process_arrow(
            0,
            nullable_batch(
                &[Some(1), None],
                &["removed", "absent-null"],
                &[DELETE, DELETE],
            ),
        )
        .unwrap();
    processor
        .process_arrow(
            1,
            nullable_batch(
                &[Some(1), None],
                &["right", "null-right"],
                &[INSERT, INSERT],
            ),
        )
        .unwrap();
    let output = processor.finish_bounded_output().unwrap();
    // kept/right passes the residual predicate; null keys remain two unmatched full-join rows.
    assert_eq!(output.num_rows(), 3);
    assert_eq!(kinds(&output), vec![INSERT; 3]);
}

#[test]
fn state_codec_restores_version_one_rows_without_cached_matchability() {
    let state = JoinState {
        left: vec![StoredRow {
            id: 0,
            row: vec![1, 2, 3].into(),
            associations: 4,
        }],
        right: vec![StoredRow {
            id: 0,
            row: vec![5, 6].into(),
            associations: -2,
        }],
        left_matchable: Some(true),
        right_matchable: Some(false),
        next_row_id: [1, 1],
    };
    let mut legacy = encode_state(&state);
    legacy[4] = LEGACY_STATE_VERSION;
    legacy.drain(5..7);

    let restored = decode_state(&legacy).unwrap();
    assert_eq!(restored.left, state.left);
    assert_eq!(restored.right, state.right);
    assert_eq!(restored.left_matchable, None);
    assert_eq!(restored.right_matchable, None);
}

#[test]
fn bounded_join_chunks_hot_key_output_and_retains_accounted_cursor_memory() {
    let broker = Arc::new(TestBroker::new(256 << 20));
    let mut processor = RegularJoinProcessor::new(
        &bounded_plan(proto::RegularJoinType::Inner, true, None),
        128,
        0,
        127,
        HostMemoryReservation::new(broker.clone(), "bounded join terminal chunks"),
    )
    .unwrap();
    let left_keys = vec![7; 200];
    let left_values = vec!["left"; 200];
    let left_kinds = vec![INSERT; 200];
    let right_keys = vec![7; 100];
    let right_values = vec!["right"; 100];
    let right_kinds = vec![INSERT; 100];
    processor
        .process_arrow(0, batch(&left_keys, &left_values, &left_kinds))
        .unwrap();
    processor
        .process_arrow(1, batch(&right_keys, &right_values, &right_kinds))
        .unwrap();

    let first = processor.finish_bounded_output().unwrap();
    assert_eq!(first.num_rows(), BOUNDED_EDGE_OUTPUT_MAX_ROWS);
    assert!(processor.scratch_reservation.size() > 0);
    let mut rows = first.num_rows();
    loop {
        let next = processor.finish_bounded_output().unwrap();
        assert!(next.num_rows() <= BOUNDED_EDGE_OUTPUT_MAX_ROWS);
        if next.num_rows() == 0 {
            break;
        }
        rows += next.num_rows();
    }
    assert_eq!(rows, 20_000);
    assert_eq!(processor.scratch_reservation.size(), 0);
    drop(first);
    drop(processor);
    assert_eq!(broker.reserved(), 0);
}

#[test]
fn bounded_join_splits_wide_terminal_output_to_fit_the_host_budget() {
    let broker = Arc::new(TestBroker::new(8 << 20));
    let mut processor = RegularJoinProcessor::new(
        &bounded_plan(proto::RegularJoinType::Inner, true, None),
        128,
        0,
        127,
        HostMemoryReservation::new(broker.clone(), "bounded wide output budget"),
    )
    .unwrap();
    let wide = "x".repeat(1024);
    let keys = vec![7; 128];
    let values = vec![wide.as_str(); 128];
    let kinds = vec![INSERT; 128];
    processor
        .process_arrow(0, batch(&keys, &values, &kinds))
        .unwrap();
    processor
        .process_arrow(1, batch(&keys, &values, &kinds))
        .unwrap();

    let mut rows = 0;
    let mut batches = 0;
    loop {
        let output = processor.finish_bounded_output().unwrap();
        if output.num_rows() == 0 {
            break;
        }
        rows += output.num_rows();
        batches += 1;
    }
    assert_eq!(rows, 128 * 128);
    assert!(
        batches > 4,
        "the wide output must be byte-bounded, not only row-bounded"
    );
    drop(processor);
    assert_eq!(broker.reserved(), 0);
}

#[test]
fn bounded_join_restores_key_groups_after_rescaling() {
    let broker = Arc::new(TestBroker::new(1 << 30));
    let mut source = RegularJoinProcessor::new(
        &bounded_plan(proto::RegularJoinType::Inner, true, None),
        128,
        0,
        127,
        HostMemoryReservation::new(broker.clone(), "bounded join rescale source"),
    )
    .unwrap();
    source
        .process_arrow(0, batch(&[1, 2], &["left-1", "left-2"], &[INSERT, INSERT]))
        .unwrap();
    let snapshots = (0..128)
        .map(|group| source.snapshot_key_group(group).unwrap())
        .collect::<Vec<_>>();
    let mut low = RegularJoinProcessor::new(
        &bounded_plan(proto::RegularJoinType::Inner, true, None),
        128,
        0,
        63,
        HostMemoryReservation::new(broker.clone(), "bounded join rescale low"),
    )
    .unwrap();
    let mut high = RegularJoinProcessor::new(
        &bounded_plan(proto::RegularJoinType::Inner, true, None),
        128,
        64,
        127,
        HostMemoryReservation::new(broker, "bounded join rescale high"),
    )
    .unwrap();
    for (group, snapshot) in snapshots.iter().enumerate() {
        if group < 64 {
            low.restore_key_group(group as u32, snapshot).unwrap();
        } else {
            high.restore_key_group(group as u32, snapshot).unwrap();
        }
    }
    for key in [1, 2] {
        let input = batch(&[key], &["right"], &[INSERT]);
        let encoded = source.group_key(1, &input, 0).unwrap();
        if assign_key_group(&encoded, 128) < 64 {
            low.process_arrow(1, input).unwrap();
        } else {
            high.process_arrow(1, input).unwrap();
        }
    }
    assert_eq!(
        low.finish_bounded_output().unwrap().num_rows()
            + high.finish_bounded_output().unwrap().num_rows(),
        2
    );
}

fn processor(broker: Arc<TestBroker>, first: u32, last: u32) -> RegularJoinProcessor {
    RegularJoinProcessor::new(
        &plan_contract(
            proto::RegularJoinType::Left,
            true,
            Some(not_equal_value_condition()),
        ),
        128,
        first,
        last,
        HostMemoryReservation::new(broker, "regular join rescale"),
    )
    .unwrap()
}

pub(super) fn plan(join_type: proto::RegularJoinType) -> Vec<u8> {
    plan_with_filter(join_type, true)
}

fn plan_with_filter(join_type: proto::RegularJoinType, filter_nulls: bool) -> Vec<u8> {
    plan_contract(join_type, filter_nulls, None)
}

fn plan_contract(
    join_type: proto::RegularJoinType,
    filter_nulls: bool,
    residual_condition: Option<proto::Expression>,
) -> Vec<u8> {
    proto::NativePlan {
        protocol_version: crate::PLAN_PROTOCOL_VERSION,
        root: Some(proto::Operator {
            plan_node_id: 0,
            metric_name: String::new(),
            clear_record_timestamps: false,
            metric_uid: None,
            operator: Some(proto::operator::Operator::RegularJoin(Box::new(
                proto::RegularJoin {
                    left_key_indices: vec![0],
                    right_key_indices: vec![0],
                    filter_nulls: vec![filter_nulls],
                    left_schema: Some(schema()),
                    right_schema: Some(schema()),
                    join_type: join_type as i32,
                    left_state_ttl_millis: 0,
                    right_state_ttl_millis: 0,
                    residual_condition,
                    bounded_final_output: false,
                    left_input: None,
                    right_input: None,
                },
            ))),
        }),
    }
    .encode_to_vec()
}

fn bounded_plan(
    join_type: proto::RegularJoinType,
    filter_nulls: bool,
    residual_condition: Option<proto::Expression>,
) -> Vec<u8> {
    let mut native = proto::NativePlan::decode(
        plan_contract(join_type, filter_nulls, residual_condition).as_slice(),
    )
    .unwrap();
    let Some(proto::operator::Operator::RegularJoin(join)) =
        native.root.as_mut().unwrap().operator.as_mut()
    else {
        unreachable!()
    };
    join.bounded_final_output = true;
    native.encode_to_vec()
}

fn bounded_plan_with_identity_calc(join_type: proto::RegularJoinType) -> Vec<u8> {
    let mut native =
        proto::NativePlan::decode(bounded_plan(join_type, true, None).as_slice()).unwrap();
    let join = native.root.take().unwrap();
    native.root = Some(proto::Operator {
        plan_node_id: 2,
        metric_name: String::new(),
        clear_record_timestamps: false,
        metric_uid: None,
        operator: Some(proto::operator::Operator::Calc(Box::new(proto::Calc {
            preserve_input_envelope: false,
            input: Some(Box::new(join)),
            projections: (0..6).map(input_reference).collect(),
            condition: None,
        }))),
    });
    native.encode_to_vec()
}

fn bounded_plan_with_left_projection() -> Vec<u8> {
    let mut native = proto::NativePlan::decode(
        bounded_plan(proto::RegularJoinType::Inner, true, None).as_slice(),
    )
    .unwrap();
    let Some(proto::operator::Operator::RegularJoin(join)) =
        native.root.as_mut().unwrap().operator.as_mut()
    else {
        unreachable!()
    };
    join.left_key_indices = vec![0, 1];
    join.right_key_indices = vec![0, 1];
    join.filter_nulls = vec![true, true];
    let join = native.root.take().unwrap();
    native.root = Some(proto::Operator {
        plan_node_id: 2,
        metric_name: String::new(),
        clear_record_timestamps: false,
        metric_uid: None,
        operator: Some(proto::operator::Operator::Calc(Box::new(proto::Calc {
            preserve_input_envelope: false,
            input: Some(Box::new(join)),
            projections: [0, 1, 4, 5].into_iter().map(input_reference).collect(),
            condition: None,
        }))),
    });
    native.encode_to_vec()
}

fn streaming_plan_with_identity_calc(join_type: proto::RegularJoinType) -> Vec<u8> {
    let mut native = proto::NativePlan::decode(plan(join_type).as_slice()).unwrap();
    let join = native.root.take().unwrap();
    native.root = Some(proto::Operator {
        plan_node_id: 2,
        metric_name: String::new(),
        clear_record_timestamps: false,
        metric_uid: None,
        operator: Some(proto::operator::Operator::Calc(Box::new(proto::Calc {
            preserve_input_envelope: false,
            input: Some(Box::new(join)),
            projections: (0..6).map(input_reference).collect(),
            condition: None,
        }))),
    });
    native.encode_to_vec()
}

fn exchange_plan() -> Vec<u8> {
    let mut transport_schema = schema();
    transport_schema.fields.push(proto::Field {
        name: "__streamfusion_row_kind".to_string(),
        r#type: Some(proto::LogicalType {
            nullable: false,
            r#type: Some(proto::logical_type::Type::Tinyint(
                proto::EmptyType::default(),
            )),
        }),
    });
    proto::NativeExchangePlan {
        protocol_version: crate::PLAN_PROTOCOL_VERSION,
        schema: Some(transport_schema),
        distribution: proto::ExchangeDistribution::Hash as i32,
        key_indices: vec![0],
        max_parallelism: 128,
        transport: proto::ExchangeTransport::ArrowIpcStream as i32,
        metadata_columns: Some(proto::ExchangeMetadataColumns {
            row_kind_index: 2,
            stream_record_timestamp_index: None,
            routing_key_index: None,
        }),
        parallelism: 1,
        preserve_key_groups: true,
        transport_routing_key: false,
    }
    .encode_to_vec()
}

fn bounded_array_key_plan() -> Vec<u8> {
    let mut native = proto::NativePlan::decode(
        bounded_plan(proto::RegularJoinType::Inner, true, None).as_slice(),
    )
    .unwrap();
    let Some(proto::operator::Operator::RegularJoin(join)) =
        native.root.as_mut().unwrap().operator.as_mut()
    else {
        unreachable!()
    };
    join.left_schema = Some(array_key_schema());
    join.right_schema = Some(array_key_schema());
    native.encode_to_vec()
}

fn array_key_exchange_plan() -> Vec<u8> {
    let mut transport_schema = array_key_schema();
    transport_schema.fields.push(proto::Field {
        name: "__streamfusion_row_kind".to_string(),
        r#type: Some(proto::LogicalType {
            nullable: false,
            r#type: Some(proto::logical_type::Type::Tinyint(
                proto::EmptyType::default(),
            )),
        }),
    });
    let routing_key_index = transport_schema.fields.len() as u32;
    proto::NativeExchangePlan {
        protocol_version: crate::PLAN_PROTOCOL_VERSION,
        schema: Some(transport_schema),
        distribution: proto::ExchangeDistribution::Hash as i32,
        key_indices: vec![0],
        max_parallelism: 128,
        transport: proto::ExchangeTransport::ArrowIpcStream as i32,
        metadata_columns: Some(proto::ExchangeMetadataColumns {
            row_kind_index: 2,
            stream_record_timestamp_index: None,
            routing_key_index: Some(routing_key_index),
        }),
        parallelism: 1,
        preserve_key_groups: false,
        transport_routing_key: true,
    }
    .encode_to_vec()
}

fn array_key_schema() -> proto::Schema {
    proto::Schema {
        fields: vec![
            field(
                "key",
                proto::logical_type::Type::Array(Box::new(proto::CollectionType {
                    element_type: Some(Box::new(proto::LogicalType {
                        nullable: true,
                        r#type: Some(proto::logical_type::Type::Integer(
                            proto::EmptyType::default(),
                        )),
                    })),
                })),
            ),
            field(
                "value",
                proto::logical_type::Type::Varchar(proto::EmptyType::default()),
            ),
        ],
    }
}

fn input_reference(index: u32) -> proto::Expression {
    proto::Expression {
        expression: Some(proto::expression::Expression::InputReference(
            proto::InputReference {
                index,
                r#type: None,
            },
        )),
    }
}

fn not_equal_value_condition() -> proto::Expression {
    proto::Expression {
        expression: Some(proto::expression::Expression::Comparison(Box::new(
            proto::Comparison {
                left: Some(Box::new(input_reference(1))),
                right: Some(Box::new(input_reference(3))),
                operator: proto::ComparisonOperator::NotEqual as i32,
            },
        ))),
    }
}

fn schema() -> proto::Schema {
    proto::Schema {
        fields: vec![
            field(
                "key",
                proto::logical_type::Type::Bigint(proto::EmptyType::default()),
            ),
            field(
                "value",
                proto::logical_type::Type::Varchar(proto::EmptyType::default()),
            ),
        ],
    }
}

fn field(name: &str, r#type: proto::logical_type::Type) -> proto::Field {
    proto::Field {
        name: name.to_string(),
        r#type: Some(proto::LogicalType {
            nullable: true,
            r#type: Some(r#type),
        }),
    }
}

pub(super) fn batch(keys: &[i64], values: &[&str], row_kinds: &[i8]) -> RecordBatch {
    RecordBatch::try_from_iter(vec![
        ("key", Arc::new(Int64Array::from(keys.to_vec())) as ArrayRef),
        (
            "value",
            Arc::new(StringArray::from(values.to_vec())) as ArrayRef,
        ),
        (
            "__streamfusion_input_row_kind",
            Arc::new(Int8Array::from(row_kinds.to_vec())) as ArrayRef,
        ),
    ])
    .unwrap()
}

fn nullable_batch(keys: &[Option<i64>], values: &[&str], row_kinds: &[i8]) -> RecordBatch {
    RecordBatch::try_from_iter(vec![
        ("key", Arc::new(Int64Array::from(keys.to_vec())) as ArrayRef),
        (
            "value",
            Arc::new(StringArray::from(values.to_vec())) as ArrayRef,
        ),
        (
            "__streamfusion_input_row_kind",
            Arc::new(Int8Array::from(row_kinds.to_vec())) as ArrayRef,
        ),
    ])
    .unwrap()
}

fn exchange_batch(key: i64, value: &str) -> RecordBatch {
    RecordBatch::try_from_iter(vec![
        ("key", Arc::new(Int64Array::from(vec![key])) as ArrayRef),
        (
            "value",
            Arc::new(StringArray::from(vec![value])) as ArrayRef,
        ),
        (
            "__streamfusion_row_kind",
            Arc::new(Int8Array::from(vec![INSERT])) as ArrayRef,
        ),
    ])
    .unwrap()
}

fn array_key_exchange_batch(keys: &[Vec<i32>], values: &[&str]) -> RecordBatch {
    let keys = ListArray::from_iter_primitive::<Int32Type, _, _>(
        keys.iter()
            .map(|values| Some(values.iter().copied().map(Some).collect::<Vec<_>>())),
    );
    RecordBatch::try_from_iter(vec![
        ("key", Arc::new(keys) as ArrayRef),
        (
            "value",
            Arc::new(StringArray::from(values.to_vec())) as ArrayRef,
        ),
        (
            "__streamfusion_row_kind",
            Arc::new(Int8Array::from(vec![INSERT; values.len()])) as ArrayRef,
        ),
        (
            "__streamfusion_routing_key",
            Arc::new(BinaryArray::from_iter_values(values.iter().map(|value| {
                if *value == "miss" {
                    b"\x09\0\0\0\0\0\0\0".as_slice()
                } else {
                    b"\x01\x02\0\0\0\0\0\0".as_slice()
                }
            }))) as ArrayRef,
        ),
    ])
    .unwrap()
}

fn kinds(batch: &RecordBatch) -> Vec<i8> {
    batch
        .column(batch.num_columns() - 2)
        .as_any()
        .downcast_ref::<Int8Array>()
        .unwrap()
        .values()
        .to_vec()
}
