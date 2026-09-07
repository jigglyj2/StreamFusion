// Copyright 2026 StreamFusion Authors
// Licensed under the Apache License, Version 2.0.

use super::*;
use crate::memory_pool::{tests_support::TestBroker, HostMemoryReservation};
use arrow::array::{ArrayRef, Int64Array, StringArray};
use prost::Message;

#[test]
fn three_input_inner_join_preserves_row_kinds_and_duplicates() {
    let broker = Arc::new(TestBroker::new(64 << 20));
    let mut processor = MultiJoinProcessor::new(
        &plan(),
        128,
        0,
        127,
        HostMemoryReservation::new(broker.clone(), "multi-join test"),
    )
    .unwrap();
    assert_eq!(
        processor
            .process_arrow(0, batch(&[1], &["a"], &[INSERT]))
            .unwrap()
            .num_rows(),
        0
    );
    assert_eq!(
        processor
            .process_arrow(1, batch(&[1, 1], &["b", "b2"], &[INSERT, INSERT]))
            .unwrap()
            .num_rows(),
        0
    );
    let joined = processor
        .process_arrow(2, batch(&[1], &["c"], &[UPDATE_AFTER]))
        .unwrap();
    assert_eq!(joined.num_rows(), 2);
    assert_eq!(kinds(&joined), vec![UPDATE_AFTER, UPDATE_AFTER]);
    let retract = processor
        .process_arrow(1, batch(&[1], &["b"], &[UPDATE_BEFORE]))
        .unwrap();
    assert_eq!(retract.num_rows(), 1);
    assert_eq!(kinds(&retract), vec![UPDATE_BEFORE]);
    assert_eq!(processor.statistics(), [4, 4]);
    drop(joined);
    drop(retract);
    drop(processor);
    assert_eq!(broker.reserved(), 0);
}

#[test]
fn absent_retraction_does_not_emit_a_phantom_join() {
    let broker = Arc::new(TestBroker::new(64 << 20));
    let mut processor = MultiJoinProcessor::new(
        &plan(),
        128,
        0,
        127,
        HostMemoryReservation::new(broker, "multi-join absent retract"),
    )
    .unwrap();
    processor
        .process_arrow(0, batch(&[1], &["a"], &[INSERT]))
        .unwrap();
    processor
        .process_arrow(1, batch(&[1], &["b"], &[INSERT]))
        .unwrap();
    assert_eq!(
        processor
            .process_arrow(2, batch(&[1], &["missing"], &[DELETE]))
            .unwrap()
            .num_rows(),
        0
    );
}

#[test]
fn chained_left_joins_retract_and_restore_null_padding() {
    let broker = Arc::new(TestBroker::new(64 << 20));
    let mut processor = MultiJoinProcessor::new(
        &plan_with_types([
            proto::RegularJoinType::Inner,
            proto::RegularJoinType::Left,
            proto::RegularJoinType::Left,
        ]),
        128,
        0,
        127,
        HostMemoryReservation::new(broker, "multi-join left transitions"),
    )
    .unwrap();
    assert_eq!(
        kinds(
            &processor
                .process_arrow(0, batch(&[1], &["a"], &[INSERT]))
                .unwrap()
        ),
        vec![INSERT]
    );
    assert_eq!(
        kinds(
            &processor
                .process_arrow(1, batch(&[1], &["b"], &[INSERT]))
                .unwrap()
        ),
        vec![DELETE, INSERT]
    );
    assert_eq!(
        kinds(
            &processor
                .process_arrow(2, batch(&[1], &["c"], &[INSERT]))
                .unwrap()
        ),
        vec![DELETE, INSERT]
    );
    assert_eq!(
        kinds(
            &processor
                .process_arrow(2, batch(&[1], &["c"], &[DELETE]))
                .unwrap()
        ),
        vec![DELETE, INSERT]
    );
    assert_eq!(
        kinds(
            &processor
                .process_arrow(1, batch(&[1], &["b"], &[DELETE]))
                .unwrap()
        ),
        vec![DELETE, INSERT]
    );
    assert_eq!(
        kinds(
            &processor
                .process_arrow(0, batch(&[1], &["a"], &[DELETE]))
                .unwrap()
        ),
        vec![DELETE]
    );
}

#[test]
fn null_condition_values_do_not_match() {
    let broker = Arc::new(TestBroker::new(64 << 20));
    let mut processor = MultiJoinProcessor::new(
        &plan(),
        128,
        0,
        127,
        HostMemoryReservation::new(broker, "multi-join null condition"),
    )
    .unwrap();
    processor
        .process_arrow(0, nullable_batch(&[1], &["a"], &[INSERT], &[None]))
        .unwrap();
    processor
        .process_arrow(1, batch(&[1], &["b"], &[INSERT]))
        .unwrap();
    assert_eq!(
        processor
            .process_arrow(2, batch(&[1], &["c"], &[INSERT]))
            .unwrap()
            .num_rows(),
        0
    );
}

#[test]
fn rejects_unadmitted_batch_memory_and_releases_the_reservation() {
    let broker = Arc::new(TestBroker::new(8_192));
    let mut processor = MultiJoinProcessor::new(
        &plan(),
        128,
        0,
        127,
        HostMemoryReservation::new(broker.clone(), "multi-join constrained memory"),
    )
    .unwrap();
    let failure = processor
        .process_arrow(0, batch(&[1], &["a"], &[INSERT]))
        .unwrap_err();
    assert!(failure.to_string().contains("Flink denied"));
    drop(processor);
    assert_eq!(broker.reserved(), 0);
}

#[test]
fn canonical_state_restores_after_rescaling() {
    let broker = Arc::new(TestBroker::new(1 << 30));
    let mut source = processor(broker.clone(), 0, 127);
    source
        .process_arrow(0, batch(&[1, 2], &["a", "x"], &[INSERT, INSERT]))
        .unwrap();
    source
        .process_arrow(1, batch(&[1, 2], &["b", "y"], &[INSERT, INSERT]))
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
        let input = batch(&[key], &[if key == 1 { "c" } else { "z" }], &[INSERT]);
        let encoded = source.group_key(2, &input, 0).unwrap();
        let result = if assign_key_group(&encoded, 128) < 64 {
            low.process_arrow(2, input).unwrap()
        } else {
            high.process_arrow(2, input).unwrap()
        };
        assert_eq!(kinds(&result), vec![INSERT]);
    }
}

#[test]
fn canonical_state_moves_from_memory_to_rocksdb_with_batched_io() {
    let Ok(plugin_path) = std::env::var("STREAMFUSION_TEST_ROCKSDB_PLUGIN") else {
        return;
    };
    let broker = Arc::new(TestBroker::new(1 << 30));
    let mut memory = processor(broker.clone(), 0, 127);
    memory
        .process_arrow(0, batch(&[1, 2], &["a", "x"], &[INSERT, INSERT]))
        .unwrap();
    memory
        .process_arrow(1, batch(&[1, 2], &["b", "y"], &[INSERT, INSERT]))
        .unwrap();
    assert_eq!(memory.statistics(), [2, 2]);
    let snapshots = (0..128)
        .map(|group| memory.snapshot_key_group(group).unwrap())
        .collect::<Vec<_>>();

    let directory = tempfile::tempdir().unwrap();
    let mut rocks = MultiJoinProcessor::new_rocksdb(
        &plan(),
        128,
        0,
        127,
        std::path::Path::new(&plugin_path),
        directory.path(),
        64 << 20,
        HostMemoryReservation::new(broker, "multi-join RocksDB scratch"),
    )
    .unwrap();
    for (group, snapshot) in snapshots.iter().enumerate() {
        rocks.restore_key_group(group as u32, snapshot).unwrap();
        assert_eq!(rocks.snapshot_key_group(group as u32).unwrap(), *snapshot);
    }
    let output = rocks
        .process_arrow(2, batch(&[1, 2], &["c", "z"], &[INSERT, INSERT]))
        .unwrap();
    assert_eq!(kinds(&output), vec![INSERT, INSERT]);
    assert_eq!(rocks.statistics(), [1, 1]);
}

fn processor(broker: Arc<TestBroker>, first: u32, last: u32) -> MultiJoinProcessor {
    MultiJoinProcessor::new(
        &plan(),
        128,
        first,
        last,
        HostMemoryReservation::new(broker, "multi-join rescale"),
    )
    .unwrap()
}

fn plan() -> Vec<u8> {
    plan_with_types([
        proto::RegularJoinType::Inner,
        proto::RegularJoinType::Inner,
        proto::RegularJoinType::Inner,
    ])
}

fn plan_with_types(join_types: [proto::RegularJoinType; 3]) -> Vec<u8> {
    let input = || proto::MultiJoinInput {
        schema: Some(schema()),
        common_key_indices: vec![0],
        state_retention_millis: 0,
    };
    proto::NativePlan {
        protocol_version: crate::PLAN_PROTOCOL_VERSION,
        root: Some(proto::Operator {
            plan_node_id: 0,
            metric_name: String::new(),
            clear_record_timestamps: false,
            metric_uid: None,
            operator: Some(proto::operator::Operator::MultiJoin(proto::MultiJoin {
                inputs: vec![input(), input(), input()],
                join_types: join_types.into_iter().map(|kind| kind as i32).collect(),
                equi_conditions: vec![
                    proto::MultiJoinEquiCondition {
                        depth: 1,
                        left_input_index: 0,
                        left_field_index: 0,
                        right_field_index: 0,
                    },
                    proto::MultiJoinEquiCondition {
                        depth: 2,
                        left_input_index: 1,
                        left_field_index: 0,
                        right_field_index: 0,
                    },
                ],
            })),
        }),
    }
    .encode_to_vec()
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

fn batch(keys: &[i64], values: &[&str], row_kinds: &[i8]) -> RecordBatch {
    let conditions = keys
        .iter()
        .map(|key| Some(key.to_le_bytes().to_vec()))
        .collect::<Vec<_>>();
    nullable_batch(keys, values, row_kinds, &conditions)
}

fn nullable_batch(
    keys: &[i64],
    values: &[&str],
    row_kinds: &[i8],
    conditions: &[Option<Vec<u8>>],
) -> RecordBatch {
    let condition_refs = conditions
        .iter()
        .map(|value| value.as_deref())
        .collect::<Vec<_>>();
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
        (
            "__streamfusion_condition_0",
            Arc::new(BinaryArray::from(condition_refs)) as ArrayRef,
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

#[test]
fn one_append_rewrites_only_the_dirty_tail_page_and_directory() {
    let broker = Arc::new(TestBroker::new(32 << 20));
    let mut processor = processor(broker.clone(), 0, 127);
    let seed = batch(&vec![1; 700], &vec!["history"; 700], &vec![INSERT; 700]);
    drop(processor.process_arrow(0, seed.clone()).unwrap());
    let group = assign_key_group(&processor.group_key(0, &seed, 0).unwrap(), 128);
    let before = streamfusion_state_abi::decode_key_group_snapshot(
        group,
        &processor.snapshot_key_group(group).unwrap(),
    )
    .unwrap()
    .into_iter()
    .collect::<std::collections::BTreeMap<_, _>>();
    drop(
        processor
            .process_arrow(0, batch(&[1], &["new"], &[INSERT]))
            .unwrap(),
    );
    let after = streamfusion_state_abi::decode_key_group_snapshot(
        group,
        &processor.snapshot_key_group(group).unwrap(),
    )
    .unwrap()
    .into_iter()
    .collect::<std::collections::BTreeMap<_, _>>();
    let changed_pages = after
        .iter()
        .filter(|(key, value)| key[0] == 1 && before.get(*key) != Some(*value))
        .count();
    assert_eq!(changed_pages, 1);
    assert_eq!(after.keys().filter(|key| key[0] == 1).count(), 3);
    drop(processor);
    assert_eq!(broker.reserved(), 0);
}

#[test]
fn cartesian_fanout_drains_bounded_batches_on_both_backends_without_early_checkpoint() {
    let plugin = std::env::var("STREAMFUSION_TEST_ROCKSDB_PLUGIN").ok();
    for rocks in [false, true] {
        if rocks && plugin.is_none() {
            continue;
        }
        let directory = tempfile::tempdir().unwrap();
        let broker = Arc::new(TestBroker::new(16 << 20));
        let mut processor = if rocks {
            MultiJoinProcessor::new_rocksdb(
                &plan(),
                128,
                0,
                127,
                std::path::Path::new(plugin.as_ref().unwrap()),
                directory.path(),
                1 << 20,
                HostMemoryReservation::new(broker.clone(), "paged multi-join"),
            )
            .unwrap()
        } else {
            processor(broker.clone(), 0, 127)
        };
        for port in [0, 1] {
            drop(
                processor
                    .process_arrow(
                        port,
                        batch(&vec![1; 300], &vec!["row"; 300], &vec![INSERT; 300]),
                    )
                    .unwrap(),
            );
        }
        processor
            .start_arrow_stream(2, batch(&[1], &["active"], &[INSERT]))
            .unwrap();
        assert!(processor.snapshot_key_group(0).is_err());
        let mut retained = None;
        let mut rows = 0;
        let mut pulls = 0;
        while let Some(output) = processor.next_arrow_batch().unwrap() {
            assert!(output.num_rows() <= 4096);
            rows += output.num_rows();
            pulls += 1;
            assert!(kinds(&output).iter().all(|kind| *kind == INSERT));
            if retained.is_none() {
                retained = Some(output);
            }
        }
        assert_eq!(rows, 90_000);
        assert!(pulls > 1);
        assert!(processor.snapshot_key_group(0).is_ok());
        drop(processor);
        assert!(broker.reserved() > 0);
        drop(retained);
        assert_eq!(broker.reserved(), 0);
    }
}
