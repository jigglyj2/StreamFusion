// Copyright 2026 StreamFusion Authors
// Licensed under the Apache License, Version 2.0.

use super::*;

#[test]
fn shared_snapshot_restores_timers_across_backends_and_rescales_with_reset_clock() {
    for rocks in backends() {
        let (source, _, _source_dir) = context(rocks, None, 0, 127);
        run(
            &source,
            input(&[9, 12], &[200, 200], &[b"a", b"b"], &[INSERT, INSERT]),
            empty(),
            None,
        )
        .unwrap();
        run(
            &source,
            empty(),
            input(&[9, 12], &[200, 200], &[b"x", b"y"], &[INSERT, INSERT]),
            None,
        )
        .unwrap();
        run(&source, empty(), empty(), Some(ControlEvent::Watermark(99))).unwrap();
        let snapshots = (0..128)
            .map(|g| source.snapshot_state(3, g).unwrap())
            .collect::<Vec<_>>();
        let target_rocks = !rocks && backends().len() == 2;
        let (lower, _, _lower_dir) = context(target_rocks, None, 0, 63);
        let (upper, _, _upper_dir) = context(target_rocks, None, 64, 127);
        for (group, snapshot) in snapshots.iter().enumerate() {
            let target = if group < 64 { &lower } else { &upper };
            target.restore_state(3, group as u32, snapshot).unwrap();
        }
        let mut actual = Vec::new();
        for target in [&lower, &upper] {
            assert_eq!(target.gauge_snapshot().unwrap().0, [0, 0, i64::MIN]);
            actual.extend(pairs(
                &run(target, empty(), empty(), Some(ControlEvent::Watermark(199))).unwrap(),
            ));
        }
        actual.sort();
        assert_eq!(
            actual,
            vec![
                (b"a".to_vec(), b"x".to_vec()),
                (b"b".to_vec(), b"y".to_vec())
            ]
        );
    }
}

#[test]
fn shared_restore_rejects_changed_contract_and_restored_clock_bindings() {
    let (source, _, _source_dir) = context(false, None, 0, 127);
    let snapshot = source.snapshot_state(3, 0).unwrap();
    let mut entries = crate::state::decode_key_group_snapshot(0, &snapshot).unwrap();
    entries
        .iter_mut()
        .find(|(k, _)| k == SHARED_STATE_KEY)
        .unwrap()
        .1[4] = 1;
    let changed = streamfusion_state_abi::encode_key_group_snapshot(
        0,
        entries.iter().map(|(k, v)| (k.as_slice(), v.as_slice())),
    )
    .unwrap();
    let (wrong_contract, _, _wrong_dir) = context(false, None, 0, 127);
    assert!(wrong_contract
        .restore_state(3, 0, &changed)
        .unwrap_err()
        .to_string()
        .contains("contract/version"));
    let broker = Arc::new(TestBroker::new(256 << 20));
    let memory = HostMemoryReservation::new(broker, "invalid window join clock");
    let mut target =
        NativeExecutionContext::new(&plan().encode_to_vec(), memory.datafusion_pool(256 << 20))
            .unwrap();
    assert!(target
        .install_state(&resources(None, Some(99), 0, 127).encode_to_vec(), memory)
        .unwrap_err()
        .to_string()
        .contains("WindowAggregate binding"));
}

#[test]
fn restored_windows_accept_replayed_rows_before_the_first_watermark() {
    for rocks in backends() {
        let (source, _, _source_dir) = context(rocks, None, 0, 127);
        run(
            &source,
            empty(),
            empty(),
            Some(ControlEvent::Watermark(999)),
        )
        .unwrap();
        let snapshots = (0..128)
            .map(|g| source.snapshot_state(3, g).unwrap())
            .collect::<Vec<_>>();
        let (target, _, _target_dir) = context(rocks, None, 0, 127);
        for (group, bytes) in snapshots.iter().enumerate() {
            target.restore_state(3, group as u32, bytes).unwrap();
        }
        assert_eq!(target.gauge_snapshot().unwrap().0, [0, 0, i64::MIN]);
        run(
            &target,
            input(&[9], &[100], &[b"replayed-left"], &[INSERT]),
            empty(),
            None,
        )
        .unwrap();
        run(
            &target,
            empty(),
            input(&[9], &[100], &[b"replayed-right"], &[INSERT]),
            None,
        )
        .unwrap();
        let rows = run(&target, empty(), empty(), Some(ControlEvent::Watermark(99))).unwrap();
        assert_eq!(
            pairs(&rows),
            vec![(b"replayed-left".to_vec(), b"replayed-right".to_vec())]
        );
        assert_eq!(target.gauge_snapshot().unwrap().0, [0, 0, 99]);
    }
}

#[test]
fn shared_rocks_checkpoint_import_retains_contract_and_timers_with_reset_clock() {
    if backends().len() == 1 {
        return;
    }
    let (source, _, _source_dir) = context(true, None, 0, 127);
    run(
        &source,
        input(&[9], &[200], &[b"a"], &[INSERT]),
        empty(),
        None,
    )
    .unwrap();
    run(
        &source,
        empty(),
        input(&[9], &[200], &[b"x"], &[INSERT]),
        None,
    )
    .unwrap();
    run(&source, empty(), empty(), Some(ControlEvent::Watermark(99))).unwrap();
    let checkpoint_root = tempfile::tempdir().unwrap();
    let checkpoint = checkpoint_root.path().join("checkpoint");
    source.checkpoint_state(3, &checkpoint).unwrap();
    let (target, _, _target_dir) = context(false, None, 0, 127);
    target
        .import_state_checkpoint(
            3,
            std::path::Path::new(&std::env::var("STREAMFUSION_TEST_ROCKSDB_PLUGIN").unwrap()),
            &checkpoint,
            0,
            127,
            8 << 20,
        )
        .unwrap();
    assert_eq!(
        pairs(
            &run(
                &target,
                empty(),
                empty(),
                Some(ControlEvent::Watermark(199))
            )
            .unwrap()
        ),
        vec![(b"a".to_vec(), b"x".to_vec())]
    );
}

#[test]
fn keyless_shared_window_uses_flinks_eight_byte_empty_binary_row_partition() {
    for rocks in backends() {
        let mut plan = plan();
        let Some(proto::operator::Operator::Calc(calc)) = &mut plan.root.as_mut().unwrap().operator
        else {
            unreachable!()
        };
        let Some(proto::operator::Operator::WindowJoin(join)) =
            &mut calc.input.as_mut().unwrap().operator
        else {
            unreachable!()
        };
        join.left_key_indices.clear();
        join.right_key_indices.clear();
        join.filter_nulls.clear();
        let (context, _, _dir) = context_with_plan(plan, rocks, None, 0, 127);
        let left = input(&[9], &[100], &[b"left"], &[INSERT]);
        let empty_key = crate::exchange::encode_binary_row(&left, 0, &[]).unwrap();
        assert_eq!(empty_key, vec![0; 8]); // Flink BinaryRowDataUtil.EMPTY_ROW's fixed header.
        let expected_group = crate::exchange::assign_key_group(&empty_key, 128);
        assert_ne!(expected_group, crate::exchange::assign_key_group(&[], 128));
        run(&context, left, empty(), None).unwrap();
        run(
            &context,
            empty(),
            input(&[12], &[100], &[b"right"], &[INSERT]),
            None,
        )
        .unwrap();
        let mut occupied = Vec::new();
        for group in 0..128 {
            let snapshot = context.snapshot_state(3, group).unwrap();
            let entries = crate::state::decode_key_group_snapshot(group, &snapshot).unwrap();
            if entries.iter().any(|(key, _)| key.first() == Some(&0x91)) {
                occupied.push(group);
            }
        }
        assert_eq!(occupied, vec![expected_group]);
        assert_eq!(
            pairs(
                &run(
                    &context,
                    empty(),
                    empty(),
                    Some(ControlEvent::Watermark(99))
                )
                .unwrap()
            ),
            vec![(b"left".to_vec(), b"right".to_vec())]
        );
    }
}
