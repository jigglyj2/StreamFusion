// Copyright 2026 StreamFusion Authors
// Licensed under the Apache License, Version 2.0.

use super::*;
use crate::memory_pool::tests_support::TestBroker;
use crate::state::observed_tests::{Io, Observed};
use prost::Message;
use std::sync::atomic::Ordering;

mod append;
mod keys;
mod memory;
mod recovery;

fn plan(strings: bool) -> Vec<u8> {
    let bigint = proto::LogicalType {
        nullable: false,
        r#type: Some(proto::logical_type::Type::Bigint(proto::EmptyType {})),
    };
    let input = proto::LogicalType {
        nullable: true,
        r#type: Some(if strings {
            proto::logical_type::Type::Varchar(proto::EmptyType {})
        } else {
            proto::logical_type::Type::Bigint(proto::EmptyType {})
        }),
    };
    let mut calls = [None, Some(2)]
        .map(|filter_index| proto::AggregateCall {
            function: proto::AggregateFunction::Count as i32,
            input_index: Some(1),
            input_type: Some(input.clone()),
            output_type: Some(bigint.clone()),
            filter_index,
            distinct: true,
            retractable: true,
            accumulator_type: None,
        })
        .to_vec();
    calls.push(proto::AggregateCall {
        function: proto::AggregateFunction::CountStar as i32,
        output_type: Some(bigint),
        retractable: true,
        ..Default::default()
    });
    proto::NativePlan {
        protocol_version: crate::PLAN_PROTOCOL_VERSION,
        root: Some(proto::Operator {
            operator: Some(proto::operator::Operator::GroupAggregate(Box::new(
                proto::GroupAggregate {
                    grouping_indices: vec![0],
                    aggregate_calls: calls,
                    input_changelog: true,
                    generate_update_before: true,
                    ..Default::default()
                },
            ))),
            ..Default::default()
        }),
    }
    .encode_to_vec()
}

fn input(strings: bool, rows: &[(i64, Option<i64>, Option<bool>, i8)]) -> RecordBatch {
    let values = rows
        .iter()
        .map(|(_, value, _, _)| {
            value.map(|value| {
                if strings {
                    AggregateValue::Bytes(format!("é\0-{value}").into_bytes())
                } else {
                    AggregateValue::Int(value as i128)
                }
            })
        })
        .collect::<Vec<_>>();
    let columns = vec![
        (
            "k",
            Arc::new(Int64Array::from_iter_values(rows.iter().map(|row| row.0))) as ArrayRef,
        ),
        (
            "v",
            aggregate_array(
                &values,
                &if strings {
                    DataType::Utf8
                } else {
                    DataType::Int64
                },
            )
            .unwrap(),
        ),
        (
            "selected",
            Arc::new(BooleanArray::from_iter(rows.iter().map(|row| row.2))) as ArrayRef,
        ),
        (
            "__streamfusion_input_row_kind",
            Arc::new(Int8Array::from_iter_values(rows.iter().map(|row| row.3))) as ArrayRef,
        ),
    ];
    let schema = Arc::new(Schema::new(
        columns
            .iter()
            .map(|(name, values)| {
                Field::new(
                    *name,
                    values.data_type().clone(),
                    *name == "v" || *name == "selected",
                )
            })
            .collect::<Vec<_>>(),
    ));
    RecordBatch::try_new(
        schema,
        columns.into_iter().map(|(_, array)| array).collect(),
    )
    .unwrap()
}

fn processor(
    rocks: bool,
    strings: bool,
    owner: &HostMemoryReservation,
) -> (GroupAggregateProcessor, tempfile::TempDir, Arc<Io>) {
    processor_with_plan(rocks, owner, &plan(strings))
}

fn processor_with_plan(
    rocks: bool,
    owner: &HostMemoryReservation,
    plan_bytes: &[u8],
) -> (GroupAggregateProcessor, tempfile::TempDir, Arc<Io>) {
    let dir = tempfile::tempdir().unwrap();
    let state: Box<dyn KeyedState> = if rocks {
        Box::new(
            RocksPluginKeyedState::open_for_owner(
                std::path::Path::new(&std::env::var("STREAMFUSION_TEST_ROCKSDB_PLUGIN").unwrap()),
                dir.path(),
                0,
                15,
                8 << 20,
                owner,
            )
            .unwrap(),
        )
    } else {
        Box::new(MemoryKeyedState::new(0, 15, owner.sibling("member test state")).unwrap())
    };
    let io = Arc::new(Io::default());
    let processor = GroupAggregateProcessor::with_state(
        plan_bytes,
        16,
        0,
        15,
        Box::new(Observed {
            inner: state,
            io: io.clone(),
        }),
        owner.sibling("member test scratch"),
    )
    .unwrap();
    assert!(processor.membership_layout.is_some());
    (processor, dir, io)
}

fn backends() -> Vec<bool> {
    if std::env::var("STREAMFUSION_TEST_ROCKSDB_PLUGIN").is_ok() {
        vec![false, true]
    } else {
        vec![false]
    }
}

#[test]
fn batch_reads_and_writes_only_requested_members_not_the_group_history() {
    for rocks in backends() {
        let broker = Arc::new(TestBroker::new(64 << 20));
        let owner = HostMemoryReservation::new(broker.clone(), "point membership test");
        let (mut processor, _dir, io) = processor(rocks, false, &owner);
        for start in (0..10_000).step_by(500) {
            let rows = (start..start + 500)
                .map(|value| (7, Some(value), Some(true), INSERT))
                .collect::<Vec<_>>();
            drop(processor.process_arrow(input(false, &rows)).unwrap());
        }
        io.reset();
        drop(
            processor
                .process_arrow(input(false, &[(7, Some(3), Some(true), INSERT)]))
                .unwrap(),
        );
        assert_eq!(io.read_batches.load(Ordering::Relaxed), 2);
        assert_eq!(io.write_batches.load(Ordering::Relaxed), 1);
        assert_eq!(io.range_reads.load(Ordering::Relaxed), 0);
        assert!(io.read_bytes.load(Ordering::Relaxed) < 256);
        assert!(io.written_bytes.load(Ordering::Relaxed) < 256);
        // Denial must leave the group header and membership entries unchanged together.
        let key = processor
            .state_key(&input(false, &[(7, Some(3), Some(true), INSERT)]), 0)
            .unwrap();
        let before = processor
            .snapshot_key_group(key.key_group)
            .unwrap()
            .to_vec();
        let mut pressure = owner.sibling("test competing consumer");
        pressure
            .resize((64 << 20) - broker.reserved() - (32 << 10))
            .unwrap();
        io.reset();
        assert!(processor
            .process_arrow(input(false, &[(7, Some(99_999), Some(true), INSERT)]))
            .is_err());
        assert_eq!(io.write_batches.load(Ordering::Relaxed), 0);
        drop(pressure);
        assert_eq!(
            processor
                .snapshot_key_group(key.key_group)
                .unwrap()
                .as_ref(),
            before
        );
        drop(processor);
        drop(owner);
        assert_eq!(broker.reserved(), 0);
    }
}

#[test]
fn partial_membership_matches_inline_compute_for_filters_signed_counts_and_group_recreation() {
    for strings in [false, true] {
        for rocks in backends() {
            let owner = HostMemoryReservation::new(
                Arc::new(TestBroker::new(64 << 20)),
                "membership parity",
            );
            let (mut actual, _dir, _) = processor(rocks, strings, &owner);
            let (mut inline, _inline_dir, _) = processor(false, strings, &owner);
            inline.membership_layout = None;
            for seed in [3, 29, 197] {
                let mut rows = Vec::new();
                for cycle in 0..19 {
                    let key = cycle % 3;
                    let value = seed + cycle;
                    rows.extend([
                        (key, Some(value), Some(true), INSERT),
                        (key, Some(-value), Some(false), DELETE),
                        (key, Some(value), None, UPDATE_BEFORE),
                        (key, Some(-value), Some(true), UPDATE_AFTER),
                        (key, None, None, INSERT),
                        (key, Some(-value), Some(true), DELETE),
                        (key, None, None, DELETE),
                    ]);
                }
                for chunk in rows.chunks(seed as usize) {
                    let batch = input(strings, chunk);
                    assert_eq!(
                        actual.process_arrow(batch.clone()).unwrap(),
                        inline.process_arrow(batch).unwrap()
                    );
                }
            }
            // Every group was deleted. Neither header nor external member may survive.
            for group in 0..16 {
                assert_eq!(
                    actual.snapshot_key_group(group).unwrap(),
                    inline.snapshot_key_group(group).unwrap()
                );
            }
        }
    }
}

#[test]
fn invalid_external_versions_and_count_vectors_are_rejected() {
    assert!(header_bytes(b"SFGD").is_err());
    assert!(header_bytes(b"SFGD\x03").is_err());
    for counts in [vec![], vec![0], vec![1, -1, i64::MAX, i64::MIN]] {
        let encoded = codec::encode_counts(&counts);
        assert_eq!(
            codec::decode_counts(&encoded, counts.len()).unwrap(),
            counts
        );
        assert!(codec::decode_counts(&encoded, counts.len() + 1).is_err());
        for end in 0..encoded.len() {
            assert!(codec::decode_counts(&encoded[..end], counts.len()).is_err());
        }
        let mut unknown = encoded.clone();
        unknown[4] = 2;
        assert!(codec::decode_counts(&unknown, counts.len()).is_err());
    }
}
