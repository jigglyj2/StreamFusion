// Copyright 2026 StreamFusion Authors
// Licensed under the Apache License, Version 2.0.

use super::*;

#[test]
fn physical_input_projection_reordering_preserves_persisted_membership_identity() {
    let owner =
        HostMemoryReservation::new(Arc::new(TestBroker::new(32 << 20)), "input reorder restore");
    let (mut source, _dir, _) = processor(false, false, &owner);
    let seed = input(false, &[(7, Some(10), Some(true), INSERT)]);
    drop(source.process_arrow(seed).unwrap());
    let mut wire = proto::NativePlan::decode(plan(false).as_slice()).unwrap();
    let proto::operator::Operator::GroupAggregate(group) =
        wire.root.as_mut().unwrap().operator.as_mut().unwrap()
    else {
        unreachable!()
    };
    group.grouping_indices[0] = 1;
    for call in &mut group.aggregate_calls {
        if call.input_index == Some(1) {
            call.input_index = Some(0);
        }
    }
    let mut target = GroupAggregateProcessor::new(
        &wire.encode_to_vec(),
        16,
        0,
        15,
        owner.sibling("reordered state"),
    )
    .unwrap();
    for group in 0..16 {
        target
            .restore_key_group(group, &source.snapshot_key_group(group).unwrap())
            .unwrap();
    }
    let next = input(
        false,
        &[
            (7, Some(10), Some(true), INSERT),
            (7, Some(10), Some(true), DELETE),
        ],
    );
    let expected = source.process_arrow(next.clone()).unwrap();
    let actual = target
        .process_arrow(next.project(&[1, 0, 2, 3]).unwrap())
        .unwrap();
    assert_eq!(actual, expected);
    for group in 0..16 {
        assert_eq!(
            target.snapshot_key_group(group).unwrap(),
            source.snapshot_key_group(group).unwrap()
        );
    }
}

#[test]
fn inline_versions_migrate_and_external_members_restore_across_backends() {
    for strings in [false, true] {
        for source_rocks in backends() {
            for target_rocks in backends() {
                for version in [4, 5, 6] {
                    let owner = HostMemoryReservation::new(
                        Arc::new(TestBroker::new(128 << 20)),
                        "membership restore",
                    );
                    let (mut oracle, _oracle_dir, _) = processor(false, strings, &owner);
                    oracle.membership_layout = None;
                    let seed = input(
                        strings,
                        &[
                            (7, Some(10), Some(true), INSERT),
                            (7, Some(20), Some(false), INSERT),
                            (7, Some(10), Some(false), INSERT),
                        ],
                    );
                    drop(oracle.process_arrow(seed.clone()).unwrap());
                    let key = oracle.state_key(&seed, 0).unwrap();
                    assert_eq!(
                        key.key[0], 0,
                        "Flink partition identity is an INSERT BinaryRow"
                    );
                    let refs = [StateKeyRef {
                        key_group: key.key_group,
                        key: &key.key,
                    }];
                    let mut legacy = oracle.state.get_batch(&refs, &owner).unwrap()[0]
                        .as_ref()
                        .unwrap()
                        .to_vec();
                    legacy[4] = version;
                    let (mut source, _source_dir, _) = processor(source_rocks, strings, &owner);
                    source
                        .state
                        .write_batch(vec![StateMutation {
                            key: key.clone(),
                            value: Some(legacy),
                        }])
                        .unwrap();
                    let update = input(
                        strings,
                        &[
                            (7, Some(30), Some(true), INSERT),
                            (7, Some(99), Some(true), DELETE), // signed negative membership survives restore
                        ],
                    );
                    assert_eq!(
                        source.process_arrow(update.clone()).unwrap(),
                        oracle.process_arrow(update).unwrap()
                    );
                    let snapshots = (0..16)
                        .map(|group| source.snapshot_key_group(group).unwrap().to_vec())
                        .collect::<Vec<_>>();
                    let entries = decode_key_group_snapshot(
                        key.key_group,
                        &snapshots[key.key_group as usize],
                    )
                    .unwrap();
                    assert_eq!(entries.len(), 5); // one header, four argument values, each shared by both filters
                    assert!(entries
                        .iter()
                        .find(|(k, _)| *k == key.key)
                        .is_some_and(|(_, v)| is_header(v)));
                    let start = prefix(&key).unwrap();
                    assert!(entries
                        .iter()
                        .filter(|(k, _)| *k != key.key)
                        .all(|(k, _)| k.starts_with(&start)));
                    drop(source);
                    let (mut target, _target_dir, _) = processor(target_rocks, strings, &owner);
                    for (group, snapshot) in snapshots.iter().enumerate() {
                        target.restore_key_group(group as u32, snapshot).unwrap();
                        assert_eq!(
                            &target.snapshot_key_group(group as u32).unwrap().to_vec(),
                            snapshot
                        );
                    }
                    let tail = input(
                        strings,
                        &[
                            (7, Some(99), Some(true), UPDATE_AFTER), // cancel the negative count
                            (7, Some(10), Some(true), DELETE),
                            (7, Some(20), Some(false), UPDATE_BEFORE),
                            (7, Some(10), Some(false), DELETE),
                            (7, Some(30), Some(true), DELETE),
                            (7, Some(99), Some(true), INSERT),
                            (7, Some(99), Some(true), DELETE),
                        ],
                    );
                    assert_eq!(
                        target.process_arrow(tail.clone()).unwrap(),
                        oracle.process_arrow(tail).unwrap()
                    );
                    for group in 0..16 {
                        assert_eq!(
                            target.snapshot_key_group(group).unwrap(),
                            oracle.snapshot_key_group(group).unwrap()
                        );
                    }
                }
            }
        }
    }
}
