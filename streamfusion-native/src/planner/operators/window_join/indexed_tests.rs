// Copyright 2026 StreamFusion Authors
// Licensed under the Apache License, Version 2.0

use super::tests::{batch, plan};
use super::*;
use crate::memory_pool::tests_support::TestBroker;
use crate::state::observed_tests::{Io, Observed};
use arrow::array::{Int64Array, TimestampMillisecondArray};
use std::collections::BTreeMap;
use std::sync::atomic::Ordering;

pub(super) fn backends() -> Vec<bool> {
    if std::env::var_os("STREAMFUSION_TEST_ROCKSDB_PLUGIN").is_some() {
        vec![false, true]
    } else {
        vec![false]
    }
}
pub(super) fn other_backend(rocks: bool) -> bool {
    !rocks && std::env::var_os("STREAMFUSION_TEST_ROCKSDB_PLUGIN").is_some()
}

pub(super) fn processor(
    rocks: bool,
    first: u32,
    last: u32,
) -> (
    WindowJoinProcessor,
    Arc<TestBroker>,
    Option<tempfile::TempDir>,
) {
    let broker = Arc::new(TestBroker::new(512 << 20));
    let owner = HostMemoryReservation::new(broker.clone(), "window join indexed test");
    if rocks {
        let plugin = std::env::var("STREAMFUSION_TEST_ROCKSDB_PLUGIN")
            .expect("RocksDB plugin required for indexed state tests");
        let dir = tempfile::tempdir().unwrap();
        let processor = WindowJoinProcessor::new_rocksdb(
            &plan(),
            128,
            first,
            last,
            std::path::Path::new(&plugin),
            dir.path(),
            32 << 20,
            owner,
        )
        .unwrap();
        (processor, broker, Some(dir))
    } else {
        (
            WindowJoinProcessor::new(&plan(), 128, first, last, owner).unwrap(),
            broker,
            None,
        )
    }
}

pub(super) fn observe(processor: &mut WindowJoinProcessor) -> Arc<Io> {
    let replacement =
        Box::new(OrderedMemoryKeyedState::new(0, 0, processor.state_memory()).unwrap());
    let inner = std::mem::replace(&mut processor.state, replacement);
    let io = Arc::new(Io::default());
    processor.state = Box::new(Observed {
        inner,
        io: io.clone(),
    });
    io
}

#[test]
fn incoming_batches_read_only_headers_and_never_rewrite_retained_rows_or_timers() {
    for rocks in backends() {
        let (mut processor, broker, _dir) = processor(rocks, 0, 127);
        let io = observe(&mut processor);
        let payload = vec![17; 4096];
        let mut previous_write_bytes = None;
        for _ in 0..16 {
            io.reset();
            processor
                .process_arrow(
                    0,
                    batch(
                        &[7; 16],
                        &[100; 16],
                        &[payload.as_slice(); 16],
                        &[INSERT; 16],
                    ),
                )
                .unwrap();
            assert_eq!(io.read_batches.load(Ordering::Relaxed), 1);
            assert_eq!(io.range_reads.load(Ordering::Relaxed), 0);
            assert_eq!(io.write_batches.load(Ordering::Relaxed), 1);
            assert_eq!(
                io.read_bytes.load(Ordering::Relaxed),
                if previous_write_bytes.is_some() {
                    29
                } else {
                    0
                }
            );
            let written = io.written_bytes.load(Ordering::Relaxed);
            if let Some(previous) = previous_write_bytes {
                assert_eq!(written, previous);
            }
            previous_write_bytes = Some(written);
        }
        // No per-batch whole timer-set serialization; a checkpoint materializes it once.
        let group = processor.dirty_timer_groups.iter().next().copied().unwrap();
        io.reset();
        let first = processor.snapshot_key_group(group).unwrap();
        assert_eq!(io.write_batches.load(Ordering::Relaxed), 1);
        let second = processor.snapshot_key_group(group).unwrap();
        assert_eq!(io.write_batches.load(Ordering::Relaxed), 1);
        assert_eq!(first, second);
        drop(first);
        drop(second);
        let output = processor.advance_event_time(99).unwrap();
        assert_eq!(output.num_rows(), 256);
        assert_eq!(io.range_reads.load(Ordering::Relaxed), 1);
        assert!(io.scanned_rows.load(Ordering::Relaxed) < 256);
        drop(output);
        drop(processor);
        assert_eq!(broker.reserved(), 0);
    }
}

type Contents = BTreeMap<(i64, i64, usize), Vec<Vec<u8>>>;

fn contents(outputs: &[RecordBatch]) -> Contents {
    let mut contents = Contents::new();
    for output in outputs {
        let sides = output
            .column(6)
            .as_any()
            .downcast_ref::<Int8Array>()
            .unwrap();
        for row in 0..output.num_rows() {
            let side = sides.value(row) as usize;
            let key = output
                .column(side * 3)
                .as_any()
                .downcast_ref::<Int64Array>()
                .unwrap()
                .value(row);
            let end = output
                .column(side * 3 + 1)
                .as_any()
                .downcast_ref::<TimestampMillisecondArray>()
                .unwrap()
                .value(row);
            let value = output
                .column(side * 3 + 2)
                .as_any()
                .downcast_ref::<BinaryArray>()
                .unwrap()
                .value(row)
                .to_vec();
            contents.entry((key, end, side)).or_default().push(value);
        }
    }
    contents
}

#[test]
fn generated_duplicate_arrival_order_survives_backend_switch_and_one_to_two_rescale() {
    for rocks in backends() {
        for seed in 0..3i64 {
            let (mut source, _, _source_dir) = processor(rocks, 0, 127);
            let mut expected = Contents::new();
            for chunk in 0..12 {
                let side = chunk % 2;
                let keys = (0..31)
                    .map(|i| (i * 3 + chunk as i64 + seed) % 7)
                    .collect::<Vec<_>>();
                let ends = (0..31)
                    .map(|i| ((i * 7 + chunk as i64 + seed) % 5 - 2) * 20)
                    .collect::<Vec<_>>();
                let values = (0..31)
                    .map(|i| vec![((i + seed) % 5) as u8; (i % 4) as usize])
                    .collect::<Vec<_>>();
                let refs = values.iter().map(Vec::as_slice).collect::<Vec<_>>();
                let kinds = (0..31)
                    .map(|i| if i % 3 == 0 { UPDATE_AFTER } else { INSERT })
                    .collect::<Vec<_>>();
                source
                    .process_arrow(side, batch(&keys, &ends, &refs, &kinds))
                    .unwrap();
                for ((key, end), value) in keys.into_iter().zip(ends).zip(values) {
                    expected.entry((key, end, side)).or_default().push(value);
                }
            }
            let snapshots = (0..128)
                .map(|g| source.snapshot_key_group(g).unwrap())
                .collect::<Vec<_>>();
            let (mut lower, _, _lower_dir) = processor(other_backend(rocks), 0, 63);
            let (mut upper, _, _upper_dir) = processor(other_backend(rocks), 64, 127);
            for (group, snapshot) in snapshots.iter().enumerate() {
                let target = if group < 64 { &mut lower } else { &mut upper };
                target.restore_key_group(group as u32, snapshot).unwrap();
                assert_eq!(target.snapshot_key_group(group as u32).unwrap(), *snapshot);
            }
            assert_eq!(
                contents(&[
                    lower.advance_event_time(i64::MAX).unwrap(),
                    upper.advance_event_time(i64::MAX).unwrap()
                ]),
                expected
            );
            assert_eq!(lower.statistics()[5] + upper.statistics()[5], 0);
        }
    }
}

#[test]
fn late_retractions_drop_but_on_time_retractions_fail_before_state_mutation() {
    for rocks in backends() {
        let (mut processor, _, _dir) = processor(rocks, 0, 127);
        let io = observe(&mut processor);
        processor.advance_event_time(99).unwrap();
        for side in 0..2 {
            for kind in [UPDATE_BEFORE, DELETE] {
                io.reset();
                assert_eq!(
                    processor
                        .process_arrow(side, batch(&[7], &[100], &[b"late"], &[kind]))
                        .unwrap()
                        .num_rows(),
                    0
                );
                let error = processor
                    .process_arrow(side, batch(&[7], &[101], &[b"on time"], &[kind]))
                    .unwrap_err();
                assert!(error
                    .to_string()
                    .contains("does not support on-time retraction"));
                assert_eq!(io.read_batches.load(Ordering::Relaxed), 0);
                assert_eq!(io.write_batches.load(Ordering::Relaxed), 0);
            }
        }
        assert_eq!(processor.late_records_dropped(), [2, 2]);
    }
}

#[test]
fn maximum_window_end_is_not_late_and_minimum_end_uses_java_wrapping_subtraction() {
    let (mut processor, _, _dir) = processor(false, 0, 127);
    processor.advance_event_time(0).unwrap();
    processor
        .process_arrow(
            0,
            batch(
                &[7, 7],
                &[i64::MIN, i64::MAX],
                &[b"min", b"max"],
                &[INSERT; 2],
            ),
        )
        .unwrap();
    assert_eq!(
        processor
            .advance_event_time(i64::MAX - 1)
            .unwrap()
            .num_rows(),
        1
    );
    assert_eq!(
        processor.advance_event_time(i64::MAX).unwrap().num_rows(),
        1
    );
    // Flink isWindowFired treats MAX_VALUE specially even after the terminal watermark.
    processor
        .process_arrow(0, batch(&[7], &[i64::MAX], &[b"sentinel"], &[INSERT]))
        .unwrap();
    assert_eq!(processor.late_records_dropped(), [0, 0]);
}

#[test]
fn a_payload_larger_than_the_normal_page_budget_is_admitted_as_one_entry() {
    for rocks in backends() {
        let (mut processor, _, _dir) = processor(rocks, 0, 127);
        let payload = vec![42; (1 << 20) + 128];
        processor
            .process_arrow(1, batch(&[7], &[100], &[&payload], &[INSERT]))
            .unwrap();
        let output = processor.advance_event_time(99).unwrap();
        assert_eq!(
            output
                .column(5)
                .as_any()
                .downcast_ref::<BinaryArray>()
                .unwrap()
                .value(0),
            payload
        );
    }
}

#[test]
fn legacy_sfwj2_snapshots_migrate_once_on_both_backends_without_reordering_duplicates() {
    for rocks in backends() {
        let (mut source, _, _dir) = processor(false, 0, 127);
        let input = batch(&[7, 7, 7], &[100; 3], &[b"b", b"a", b"b"], &[INSERT; 3]);
        source.prepare_schema(0, input.schema()).unwrap();
        let group_key = source.group_key(0, &input, 0).unwrap();
        let group = assign_key_group(&group_key, 128);
        let timer_key = window_state_key(group, &group_key, 100);
        let encoded = source.row_converters[0]
            .convert_columns(&input.columns()[..3])
            .unwrap();
        let state = JoinWindowState {
            left: (0..3).map(|i| encoded.row(i).data().to_vec()).collect(),
            right: vec![],
        };
        source
            .state
            .write_batch(vec![StateMutation {
                key: timer_key.clone(),
                value: Some(encode_state(&state)),
            }])
            .unwrap();
        source
            .timers
            .register(
                group,
                TimerDomain::EventTime,
                TimerKey {
                    timestamp: 99,
                    key: timer_key.key,
                    namespace: 100i64.to_le_bytes().to_vec(),
                },
            )
            .unwrap();
        source.dirty_timer_groups.insert(group);
        let legacy = source.snapshot_key_group(group).unwrap();
        let (mut target, _, _target_dir) = processor(rocks, 0, 127);
        target.restore_key_group(group, &legacy).unwrap();
        let migrated = target.snapshot_key_group(group).unwrap();
        assert_ne!(migrated, legacy);
        assert!(crate::state::decode_key_group_snapshot(group, &migrated)
            .unwrap()
            .iter()
            .all(|(k, _)| k[0] != WINDOW_KEY_PREFIX));
        let (mut restored, _, _restored_dir) = processor(other_backend(rocks), 0, 127);
        restored.restore_key_group(group, &migrated).unwrap();
        assert_eq!(restored.snapshot_key_group(group).unwrap(), migrated);
        restored
            .process_arrow(0, batch(&[7], &[100], &[b"c"], &[UPDATE_AFTER]))
            .unwrap();
        assert_eq!(
            contents(&[restored.advance_event_time(99).unwrap()])
                .get(&(7, 100, 0))
                .unwrap(),
            &vec![b"b".to_vec(), b"a".to_vec(), b"b".to_vec(), b"c".to_vec()]
        );
    }
}

#[test]
fn arrow_window_order_and_framed_partitions_are_independent_of_flink_timer_identity() {
    let mut converter = RowConverter::new(vec![SortField::new(DataType::Int64)]).unwrap();
    let ends = [i64::MIN, -100, -1, 0, 1, 100, i64::MAX];
    for partition in [b"a".as_slice(), b"a\0", b"a\xff"] {
        let keys = ends
            .into_iter()
            .map(|end| {
                WindowKeys::new(&window_state_key(7, partition, end), &mut converter).unwrap()
            })
            .collect::<Vec<_>>();
        for adjacent in keys.windows(2) {
            assert!(adjacent[0].header.key < adjacent[1].header.key);
        }
        for other in [b"a".as_slice(), b"a\0", b"a\xff"] {
            if partition == other {
                continue;
            }
            let other = WindowKeys::new(&window_state_key(7, other, 0), &mut converter).unwrap();
            assert!(keys.iter().all(|k| k.header != other.header));
        }
        assert_eq!(keys[0].header.key_group, 7);
    }
}

#[test]
fn future_index_versions_and_mixed_legacy_snapshots_are_rejected_on_restore() {
    for mixed in [false, true] {
        let (mut source, _, _dir) = processor(false, 0, 127);
        source
            .process_arrow(0, batch(&[7], &[100], &[b"value"], &[INSERT]))
            .unwrap();
        let group = source.dirty_timer_groups.iter().next().copied().unwrap();
        let snapshot = source.snapshot_key_group(group).unwrap();
        let mut entries = crate::state::decode_key_group_snapshot(group, &snapshot).unwrap();
        if mixed {
            entries.push((
                window_state_key(group, b"other", 100).key,
                encode_state(&JoinWindowState::default()),
            ));
        } else {
            let (_, value) = entries.iter_mut().find(|(k, _)| k[0] == 0x91).unwrap();
            value[4] = 5;
        }
        let bytes = streamfusion_state_abi::encode_key_group_snapshot(
            group,
            entries.iter().map(|(k, v)| (k.as_slice(), v.as_slice())),
        )
        .unwrap();
        let (mut target, _, _target_dir) = processor(false, 0, 127);
        let error = target
            .restore_key_group(group, &bytes)
            .unwrap_err()
            .to_string();
        assert!(
            error.contains(if mixed {
                "mixes legacy and indexed"
            } else {
                "invalid window join index version"
            }),
            "{error}"
        );
    }
}

#[test]
fn denied_closed_window_workspace_recovers_from_the_last_checkpoint_and_does_not_read_payloads() {
    for rocks in backends() {
        let (mut source, broker, _dir) = processor(rocks, 0, 127);
        source
            .process_arrow(0, batch(&[7], &[100], &[b"recover me"], &[INSERT]))
            .unwrap();
        let group = source.dirty_timer_groups.iter().next().copied().unwrap();
        let snapshot = source.snapshot_key_group(group).unwrap();
        let io = observe(&mut source);
        let mut pressure = HostMemoryReservation::new(broker.clone(), "other native consumers");
        pressure
            .resize(pressure.available_capacity().unwrap().unwrap() - 65536)
            .unwrap();
        let error = source.advance_event_time(99).unwrap_err().to_string();
        assert!(error.contains("Flink denied"), "{error}");
        assert_eq!(io.range_reads.load(Ordering::Relaxed), 0);
        assert_eq!(io.write_batches.load(Ordering::Relaxed), 0);
        drop(pressure);
        drop(source);
        let (mut restored, _, _restore_dir) = processor(other_backend(rocks), 0, 127);
        restored.restore_key_group(group, &snapshot).unwrap();
        assert_eq!(
            contents(&[restored.advance_event_time(99).unwrap()])
                .get(&(7, 100, 0))
                .unwrap(),
            &vec![b"recover me".to_vec()]
        );
        let cleared = restored.snapshot_key_group(group).unwrap();
        let (mut empty, _, _empty_dir) = processor(rocks, 0, 127);
        empty.restore_key_group(group, &cleared).unwrap();
        assert_eq!(empty.advance_event_time(1000).unwrap().num_rows(), 0);
        assert_eq!(empty.statistics()[5], 0);
        drop(snapshot);
        assert_eq!(broker.reserved(), 0);
    }
}
