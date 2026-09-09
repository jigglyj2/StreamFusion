// Copyright 2026 StreamFusion Authors
// Licensed under the Apache License, Version 2.0.

use super::*;

fn append_plan(strings: bool) -> Vec<u8> {
    let mut plan = proto::NativePlan::decode(plan(strings).as_slice()).unwrap();
    let Some(proto::operator::Operator::GroupAggregate(group)) =
        plan.root.as_mut().unwrap().operator.as_mut()
    else {
        unreachable!()
    };
    group.input_changelog = false;
    for call in &mut group.aggregate_calls {
        call.retractable = false;
    }
    plan.encode_to_vec()
}

#[test]
fn presence_vectors_cover_filter_word_boundaries_and_reject_wrong_formats() {
    for count in [0usize, 1, 7, 8, 9, 63, 64, 65, 128] {
        let counts = (0..count)
            .map(|index| i64::from(index % 3 == 0))
            .collect::<Vec<_>>();
        let encoded = codec::encode_presence(&counts);
        assert_eq!(encoded.len(), 9 + count.div_ceil(8));
        assert_eq!(
            codec::decode_members(&encoded, count, Mode::Presence).unwrap(),
            counts
        );
        assert!(codec::decode_members(&encoded, count, Mode::Counted).is_err());
        assert!(
            codec::decode_members(&codec::encode_counts(&counts), count, Mode::Presence).is_err()
        );
        assert!(codec::decode_members(&encoded, count + 1, Mode::Presence).is_err());
        for end in 0..encoded.len() {
            assert!(codec::decode_members(&encoded[..end], count, Mode::Presence).is_err());
        }
        if count % 8 != 0 {
            let mut noncanonical = encoded.clone();
            *noncanonical.last_mut().unwrap() |= 1 << (count % 8);
            assert!(codec::decode_members(&noncanonical, count, Mode::Presence).is_err());
        }
    }
}

#[test]
fn append_members_are_compact_and_duplicate_batches_only_write_the_group_header() {
    for rocks in backends() {
        for strings in [false, true] {
            let owner = HostMemoryReservation::new(
                Arc::new(TestBroker::new(128 << 20)),
                "append memberships",
            );
            let (mut append, _dir, io) = processor_with_plan(rocks, &owner, &append_plan(strings));
            let (mut counted, _counted_dir, _) = processor(rocks, strings, &owner);
            for start in (0..2048).step_by(128) {
                let rows = (start..start + 128)
                    .map(|value| (7, Some(value), Some(value % 3 == 0), INSERT))
                    .collect::<Vec<_>>();
                let batch = input(strings, &rows);
                assert_eq!(
                    append.process_arrow(batch.clone()).unwrap(),
                    counted.process_arrow(batch).unwrap()
                );
            }
            let duplicate = input(strings, &[(7, Some(3), Some(true), INSERT); 128]);
            let key = append.state_key(&duplicate, 0).unwrap();
            let before = append.snapshot_key_group(key.key_group).unwrap();
            let counted_before = counted.snapshot_key_group(key.key_group).unwrap();
            assert_eq!(
                counted_before.len() - before.len(),
                2048 * 15,
                "two i64 counts become two presence bits"
            );
            let old = decode_key_group_snapshot(key.key_group, &before).unwrap();
            io.reset();
            assert_eq!(
                append.process_arrow(duplicate.clone()).unwrap(),
                counted.process_arrow(duplicate).unwrap()
            );
            assert_eq!(io.read_batches.load(Ordering::Relaxed), 2);
            assert_eq!(io.write_batches.load(Ordering::Relaxed), 1);
            assert_eq!(io.range_reads.load(Ordering::Relaxed), 0);
            let after = append.snapshot_key_group(key.key_group).unwrap();
            let entries = decode_key_group_snapshot(key.key_group, &after).unwrap();
            let (_, header) = entries.iter().find(|(k, _)| k == &key.key).unwrap();
            assert_eq!(
                io.written_bytes.load(Ordering::Relaxed),
                key.key.len() + header.len()
            );
            assert_eq!(header[4], 2);
            assert_eq!(
                old.into_iter()
                    .filter(|(k, _)| k != &key.key)
                    .collect::<Vec<_>>(),
                entries
                    .into_iter()
                    .filter(|(k, _)| k != &key.key)
                    .collect::<Vec<_>>()
            );
        }
    }
}

#[test]
fn append_restore_preserves_presence_and_retains_legacy_counted_groups() {
    for source_rocks in backends() {
        for target_rocks in backends() {
            let owner =
                HostMemoryReservation::new(Arc::new(TestBroker::new(128 << 20)), "append restore");
            for legacy in [false, true] {
                let bytes = if legacy {
                    plan(false)
                } else {
                    append_plan(false)
                };
                let (mut source, _dir, _) = processor_with_plan(source_rocks, &owner, &bytes);
                let seed = input(
                    false,
                    &[
                        (7, Some(1), Some(false), INSERT),
                        (7, Some(1), Some(true), INSERT),
                        (7, None, None, INSERT),
                    ],
                );
                drop(source.process_arrow(seed.clone()).unwrap());
                let key = source.state_key(&seed, 0).unwrap();
                let snapshot = source.snapshot_key_group(key.key_group).unwrap();
                let (mut target, _target_dir, _) =
                    processor_with_plan(target_rocks, &owner, &append_plan(false));
                target.restore_key_group(key.key_group, &snapshot).unwrap();
                assert_eq!(target.snapshot_key_group(key.key_group).unwrap(), snapshot);
                let next = input(
                    false,
                    &[
                        (7, Some(1), Some(false), INSERT),
                        (7, Some(2), Some(true), INSERT),
                    ],
                );
                assert_eq!(
                    target.process_arrow(next.clone()).unwrap(),
                    source.process_arrow(next).unwrap()
                );
                assert_eq!(
                    target.snapshot_key_group(key.key_group).unwrap(),
                    source.snapshot_key_group(key.key_group).unwrap()
                );
                if !legacy {
                    let (mut retractable, _retract_dir, _) = processor(target_rocks, false, &owner);
                    assert!(retractable
                        .restore_key_group(key.key_group, &snapshot)
                        .unwrap_err()
                        .to_string()
                        .contains("retractable plan"));
                    assert!(decode_key_group_snapshot(
                        key.key_group,
                        &retractable.snapshot_key_group(key.key_group).unwrap()
                    )
                    .unwrap()
                    .is_empty());
                }
            }
        }
    }
}

#[test]
fn physical_presence_checkpoint_rejects_retractable_restore_before_processing() {
    use crate::planner::persistent::PersistentOperatorFactory;
    let plugin = std::env::var("STREAMFUSION_TEST_ROCKSDB_PLUGIN").unwrap();
    let owner = HostMemoryReservation::new(
        Arc::new(TestBroker::new(64 << 20)),
        "physical presence restore",
    );
    let (mut source, _source_dir, _) = processor_with_plan(true, &owner, &append_plan(false));
    let seed = input(false, &[(7, Some(1), Some(true), INSERT)]);
    drop(source.process_arrow(seed.clone()).unwrap());
    let key = source.state_key(&seed, 0).unwrap();
    let directory = tempfile::tempdir().unwrap();
    let checkpoint = directory.path().join("checkpoint");
    source.checkpoint(&checkpoint).unwrap();
    let reader =
        RocksPluginKeyedState::open(std::path::Path::new(&plugin), &checkpoint, 0, 15, 1 << 20)
            .unwrap();
    for rocks in backends() {
        let (target, _target_dir, _) = processor(rocks, false, &owner);
        let factory =
            crate::planner::operators::group_aggregate::execution_plan::GroupAggregateFactory(
                Arc::new(std::sync::Mutex::new(target)),
            );
        assert!(factory
            .restore_from_checkpoint(key.key_group, &reader, &owner)
            .unwrap_err()
            .to_string()
            .contains("retractable plan"));
        assert!(decode_key_group_snapshot(
            key.key_group,
            &factory.snapshot(key.key_group).unwrap()
        )
        .unwrap()
        .is_empty());
    }
}
