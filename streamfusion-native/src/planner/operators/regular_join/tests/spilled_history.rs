// Copyright 2026 StreamFusion Authors
// Licensed under the Apache License, Version 2.0.

use super::*;
use crate::state::observed_tests::{Io, Observed};
use std::sync::atomic::Ordering;

#[test]
fn prepared_hot_history_preserves_order_retractions_state_and_batch_io() {
    for rocks in [false, true] {
        for side in 0..2 {
            for residual in [false, true] {
                for legacy in [false, true] {
                    let directory = tempfile::tempdir().unwrap();
                    let spill = tempfile::tempdir().unwrap();
                    let broker = Arc::new(TestBroker::new(1 << 30));
                    let owner =
                        HostMemoryReservation::new(broker.clone(), "prepared history parity");
                    let contract = plan_contract(
                        proto::RegularJoinType::Inner,
                        true,
                        residual.then(not_equal_value_condition),
                    );
                    let mut reference = RegularJoinProcessor::new(
                        &contract,
                        128,
                        0,
                        127,
                        owner.sibling("resident reference"),
                    )
                    .unwrap();
                    let mut actual = if rocks {
                        RegularJoinProcessor::new_rocksdb(
                            &contract,
                            128,
                            0,
                            127,
                            std::path::Path::new(
                                &std::env::var("STREAMFUSION_TEST_ROCKSDB_PLUGIN").unwrap(),
                            ),
                            directory.path(),
                            1 << 20,
                            owner.sibling("RocksDB history"),
                        )
                        .unwrap()
                    } else {
                        RegularJoinProcessor::new(
                            &contract,
                            128,
                            0,
                            127,
                            owner.sibling("memory history"),
                        )
                        .unwrap()
                    };
                    let wide = "h".repeat(4096);
                    let input = batch(
                        &[1, 1, 1, 1, 1, 1, 2, 2],
                        &[
                            "duplicate",
                            "duplicate",
                            "duplicate",
                            "duplicate",
                            "absent",
                            "new",
                            "compact",
                            "compact",
                        ],
                        &[
                            INSERT,
                            DELETE,
                            UPDATE_BEFORE,
                            DELETE,
                            DELETE,
                            UPDATE_AFTER,
                            INSERT,
                            DELETE,
                        ],
                    );
                    actual.prepare_schema(side, input.schema()).unwrap();
                    let logical = actual.group_key(side, &input, 0).unwrap();
                    let group = assign_key_group(&logical, 128);
                    let history = batch(&[1, 1], &[&wide, "duplicate"], &[INSERT; 2]);
                    let encoded = actual.row_converters[1 - side]
                        .convert_columns(&history.columns()[..2])
                        .unwrap();
                    let duplicate = actual.row_converters[side]
                        .convert_columns(&input.columns()[..2])
                        .unwrap();
                    let rows = (0..1501)
                        .map(|id| StoredRow {
                            id,
                            row: Arc::from(encoded.row(if id % 127 == 0 { 1 } else { 0 }).data()),
                            associations: 0,
                        })
                        .collect::<Vec<_>>();
                    let own = (0..2)
                        .map(|id| StoredRow {
                            id,
                            row: Arc::from(duplicate.row(0).data()),
                            associations: 0,
                        })
                        .collect::<Vec<_>>();
                    let (left, right) = if side == 0 { (own, rows) } else { (rows, own) };
                    let value = JoinState {
                        next_row_id: if side == 0 { [2, 1501] } else { [1501, 2] },
                        left,
                        right,
                        ..Default::default()
                    };
                    let entry = StagedState {
                        key: StateKey {
                            key_group: group,
                            key: logical,
                        },
                        value,
                        original: JoinState::default(),
                        original_layout: paged_codec::Layout::Compact,
                        unloaded: None,
                        touched: true,
                    };
                    for processor in [&mut actual, &mut reference] {
                        if legacy {
                            use paged_codec::*;
                            let mut changes = vec![StateMutation {
                                key: manifest_key(&entry.key),
                                value: Some(encode_manifest(&entry.value)),
                            }];
                            for (input, rows) in [&entry.value.left, &entry.value.right]
                                .into_iter()
                                .enumerate()
                            {
                                for (id, rows) in pages(rows) {
                                    changes.push(StateMutation {
                                        key: page_key(&entry.key, input, id),
                                        value: Some(encode_page(rows).unwrap()),
                                    });
                                }
                            }
                            processor.state.write_batch(changes).unwrap();
                        } else {
                            processor
                                .state
                                .write_batch(paged_state::mutations(&entry).unwrap())
                                .unwrap();
                        }
                    }
                    drop(entry);
                    // Mix a compact legacy key into the same prepared batch, independently of the
                    // hot key's persisted layout. Its migration must not lose either side's rows.
                    let logical = actual.group_key(side, &input, 6).unwrap();
                    let compact_key = StateKey {
                        key_group: assign_key_group(&logical, 128),
                        key: logical,
                    };
                    let compact_rows = actual.row_converters[side]
                        .convert_columns(&input.columns()[..2])
                        .unwrap();
                    let row = StoredRow {
                        id: 0,
                        row: Arc::from(compact_rows.row(6).data()),
                        associations: 0,
                    };
                    let compact = JoinState {
                        left: vec![row.clone()],
                        right: vec![row],
                        next_row_id: [1, 1],
                        ..Default::default()
                    };
                    for processor in [&mut actual, &mut reference] {
                        processor
                            .state
                            .write_batch(vec![StateMutation {
                                key: paged_codec::manifest_key(&compact_key),
                                value: Some(paged_codec::encode_compact(&compact).unwrap()),
                            }])
                            .unwrap();
                    }
                    // The complete ordered reference is produced before applying pressure, so it
                    // cannot borrow workspace from the bounded run or affect its reservations.
                    let expected = reference.process_arrow(side, input.clone()).unwrap();
                    let empty = Box::new(
                        MemoryKeyedState::new(0, 127, owner.sibling("replacement")).unwrap(),
                    );
                    let inner = std::mem::replace(&mut actual.state, empty);
                    let io = Arc::new(Io::default());
                    actual.state = Box::new(Observed {
                        inner,
                        io: io.clone(),
                    });
                    actual
                        .set_spill_resources(Some(
                            crate::spill::Resources::new(vec![spill.path().to_path_buf()]).unwrap(),
                        ))
                        .unwrap();
                    let retained = broker.reserved();
                    let mut pressure = owner.sibling("other slot consumers");
                    pressure.resize((1 << 30) - retained - (4 << 20)).unwrap();
                    actual.begin_streaming_batch(side, input).unwrap();
                    assert!(actual
                        .streaming_cursor
                        .as_ref()
                        .unwrap()
                        .uses_spilled_history());
                    assert!(actual.snapshot_key_group(group).is_err());
                    assert_eq!(io.write_batches.load(Ordering::Relaxed), 0);
                    let reads = io.read_batches.load(Ordering::Relaxed);
                    assert_eq!(reads as u64, actual.statistics()[0]);
                    let mut offset = 0;
                    while let Some(output) = actual.next_streaming_batch().unwrap() {
                        assert_eq!(
                            output,
                            expected.slice(offset, output.num_rows()),
                            "rocks={rocks}, side={side}, residual={residual}, offset={offset}"
                        );
                        offset += output.num_rows();
                        assert_eq!(
                            io.read_batches.load(Ordering::Relaxed),
                            reads,
                            "replay must not read RocksDB"
                        );
                    }
                    assert_eq!(offset, expected.num_rows());
                    drop(pressure);
                    for group in 0..128 {
                        assert_eq!(
                            actual.snapshot_key_group(group).unwrap(),
                            reference.snapshot_key_group(group).unwrap(),
                            "canonical group {group}"
                        );
                    }
                    drop((actual, reference, expected, owner));
                    assert_eq!(broker.reserved(), 0);
                    assert_eq!(std::fs::read_dir(spill.path()).unwrap().count(), 0);
                }
            }
        }
    }
}

#[test]
fn prepared_history_cancellation_and_flush_failure_require_recovery_and_release_files() {
    for cancel in [false, true] {
        let spill = tempfile::tempdir().unwrap();
        let broker = Arc::new(TestBroker::new(64 << 20));
        let owner = HostMemoryReservation::new(broker.clone(), "prepared history failure");
        let mut actual = RegularJoinProcessor::new(
            &plan(proto::RegularJoinType::Inner),
            128,
            0,
            127,
            owner.sibling("join"),
        )
        .unwrap();
        let payload = "x".repeat(64);
        actual
            .begin_streaming_batch(
                0,
                batch(
                    &vec![1; 10001],
                    &vec![payload.as_str(); 10001],
                    &vec![INSERT; 10001],
                ),
            )
            .unwrap();
        assert!(actual.next_streaming_batch().unwrap().is_none());
        let baseline = broker.reserved();
        let io = Arc::new(Io::default());
        let empty = Box::new(MemoryKeyedState::new(0, 127, owner.sibling("replacement")).unwrap());
        let inner = std::mem::replace(&mut actual.state, empty);
        actual.state = Box::new(Observed {
            inner,
            io: io.clone(),
        });
        actual
            .set_spill_resources(Some(
                crate::spill::Resources::new(vec![spill.path().to_path_buf()]).unwrap(),
            ))
            .unwrap();
        let mut pressure = owner.sibling("competing task");
        pressure
            .resize((64 << 20) - broker.reserved() - (2 << 20))
            .unwrap();
        let input = batch(&[1], &["right"], &[INSERT]);
        actual.begin_streaming_batch(1, input.clone()).unwrap();
        assert!(actual
            .streaming_cursor
            .as_ref()
            .unwrap()
            .uses_spilled_history());
        let reads = io.read_batches.load(Ordering::Relaxed);
        let first = actual.next_streaming_batch().unwrap().unwrap();
        assert!(first.num_rows() < 10001);
        assert_eq!(io.write_batches.load(Ordering::Relaxed), 0);
        drop(first);
        if cancel {
            actual.cancel_streaming_batch();
            assert_eq!(io.write_batches.load(Ordering::Relaxed), 0);
        } else {
            io.fail_write_batch.store(1, Ordering::Relaxed);
            loop {
                match actual.next_streaming_batch() {
                    Ok(Some(_)) => {}
                    Ok(None) => panic!("flush must fail"),
                    Err(error) => {
                        assert!(error.to_string().contains("injected state write failure"));
                        break;
                    }
                }
            }
        }
        assert_eq!(io.read_batches.load(Ordering::Relaxed), reads);
        assert!(actual.snapshot_key_group(0).is_err());
        assert!(actual.begin_streaming_batch(1, input).is_err());
        assert!(actual.next_streaming_batch().is_err());
        drop(pressure);
        assert_eq!(broker.reserved(), baseline);
        drop((actual, owner));
        assert_eq!(broker.reserved(), 0);
        assert_eq!(std::fs::read_dir(spill.path()).unwrap().count(), 0);
    }
}
