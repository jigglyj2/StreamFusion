// Copyright 2026 StreamFusion Authors
// Licensed under the Apache License, Version 2.0.

use super::*;
use crate::memory_pool::{tests_support::TestBroker, HostMemoryReservation};
use arrow::array::{Int32Array, StringArray};
use prost::Message;

fn schema() -> SchemaRef {
    Arc::new(Schema::new(vec![
        Field::new("key", DataType::Int32, false),
        Field::new("value", DataType::Utf8, true),
    ]))
}

pub(super) fn plan() -> Vec<u8> {
    let schema = proto::Schema {
        fields: vec![
            proto::Field {
                name: "key".to_string(),
                r#type: Some(proto::LogicalType {
                    nullable: false,
                    r#type: Some(proto::logical_type::Type::Integer(
                        proto::EmptyType::default(),
                    )),
                }),
            },
            proto::Field {
                name: "value".to_string(),
                r#type: Some(proto::LogicalType {
                    nullable: true,
                    r#type: Some(proto::logical_type::Type::Varchar(
                        proto::EmptyType::default(),
                    )),
                }),
            },
        ],
    };
    let mut output = schema.clone();
    output.fields.push(proto::Field {
        name: "row_num".to_string(),
        r#type: Some(proto::LogicalType {
            nullable: false,
            r#type: Some(proto::logical_type::Type::Bigint(
                proto::EmptyType::default(),
            )),
        }),
    });
    proto::NativePlan {
        protocol_version: 1,
        root: Some(proto::Operator {
            plan_node_id: 0,
            metric_name: String::new(),
            clear_record_timestamps: false,
            metric_uid: None,
            operator: Some(proto::operator::Operator::TopN(Box::new(proto::TopN {
                input: Some(Box::new(proto::Operator {
                    plan_node_id: 0,
                    metric_name: String::new(),
                    clear_record_timestamps: false,
                    metric_uid: None,
                    operator: Some(proto::operator::Operator::Input(proto::Input::default())),
                })),
                partition_key_indices: vec![0],
                sort_key_indices: vec![1],
                primary_key_indices: vec![],
                rank_start: 1,
                rank_end: Some(2),
                variable_rank_end_index: None,
                output_rank_number: true,
                generate_update_before: true,
                strategy: proto::TopNStrategy::AppendFast as i32,
                input_schema: Some(schema),
                state_ttl_millis: 0,
                sort_ascending: vec![false],
                sort_nulls_last: vec![true],
                output_schema: Some(output),
                rank_type: proto::TopNRankType::RowNumber as i32,
                bounded_final_output: false,
                physical_input_semantics: false,
            }))),
        }),
    }
    .encode_to_vec()
}

pub(super) fn batch(keys: Vec<i32>, values: Vec<&str>) -> RecordBatch {
    let rows = keys.len();
    batch_with_kinds(keys, values, vec![INSERT; rows])
}

fn batch_with_kinds(keys: Vec<i32>, values: Vec<&str>, kinds: Vec<i8>) -> RecordBatch {
    let rows = keys.len();
    assert_eq!(values.len(), rows);
    assert_eq!(kinds.len(), rows);
    RecordBatch::try_new(
        Arc::new(Schema::new(vec![
            Field::new("key", DataType::Int32, false),
            Field::new("value", DataType::Utf8, true),
            Field::new(INPUT_KIND_COLUMN, DataType::Int8, false),
        ])),
        vec![
            Arc::new(Int32Array::from(keys)),
            Arc::new(StringArray::from(values)),
            Arc::new(Int8Array::from(kinds)),
        ],
    )
    .unwrap()
}

fn top_one_plan(generate_update_before: bool) -> Vec<u8> {
    let bytes = plan();
    let mut native = proto::NativePlan::decode(bytes.as_slice()).unwrap();
    let Some(proto::operator::Operator::TopN(top_n)) =
        native.root.as_mut().and_then(|root| root.operator.as_mut())
    else {
        unreachable!()
    };
    top_n.rank_end = Some(1);
    top_n.output_rank_number = false;
    top_n.generate_update_before = generate_update_before;
    top_n.output_schema = top_n.input_schema.clone();
    native.encode_to_vec()
}

fn output_kinds(batch: &RecordBatch) -> Vec<i8> {
    batch
        .column(batch.num_columns() - 1)
        .as_any()
        .downcast_ref::<Int8Array>()
        .unwrap()
        .values()
        .to_vec()
}

#[test]
fn append_fast_top_one_uses_flinks_update_changelog() {
    for (generate_update_before, expected) in [
        (false, vec![INSERT, UPDATE_AFTER]),
        (true, vec![INSERT, UPDATE_BEFORE, UPDATE_AFTER]),
    ] {
        let broker = Arc::new(TestBroker::new(64 << 20));
        let mut processor = TopNProcessor::new(
            &top_one_plan(generate_update_before),
            128,
            0,
            127,
            HostMemoryReservation::new(broker, "fast top-one changelog"),
        )
        .unwrap();
        let first = processor
            .process_arrow(batch(vec![1], vec!["a"]), 1)
            .unwrap();
        let replacement = processor
            .process_arrow(batch(vec![1], vec!["z"]), 2)
            .unwrap();
        assert_eq!(
            [output_kinds(&first), output_kinds(&replacement)].concat(),
            expected
        );
    }
}

fn bounded_rank_plan() -> Vec<u8> {
    let mut native = proto::NativePlan::decode(plan().as_slice()).unwrap();
    let Some(proto::operator::Operator::TopN(top_n)) =
        native.root.as_mut().and_then(|root| root.operator.as_mut())
    else {
        unreachable!()
    };
    top_n.rank_start = 2;
    top_n.rank_end = Some(3);
    top_n.generate_update_before = false;
    top_n.rank_type = proto::TopNRankType::Rank as i32;
    top_n.bounded_final_output = true;
    top_n.physical_input_semantics = true;
    native.encode_to_vec()
}

#[test]
fn bounded_rank_retains_cutoff_ties_row_kinds_and_canonical_state() {
    let broker = Arc::new(TestBroker::new(64 << 20));
    let mut source = TopNProcessor::new(
        &bounded_rank_plan(),
        128,
        0,
        127,
        HostMemoryReservation::new(broker.clone(), "bounded rank source"),
    )
    .unwrap();
    let input = batch_with_kinds(
        vec![2, 1, 1, 2, 1, 1, 2],
        vec!["y", "b", "c", "z", "a", "b", "z"],
        vec![
            DELETE,
            UPDATE_BEFORE,
            INSERT,
            UPDATE_AFTER,
            DELETE,
            UPDATE_AFTER,
            INSERT,
        ],
    );
    assert_eq!(source.process_arrow(input, 0).unwrap().num_rows(), 0);

    let mut restored = TopNProcessor::new(
        &bounded_rank_plan(),
        128,
        0,
        127,
        HostMemoryReservation::new(broker, "bounded rank restored"),
    )
    .unwrap();
    for key_group in 0..128 {
        let snapshot = source.snapshot_key_group(key_group).unwrap();
        restored.restore_key_group(key_group, &snapshot).unwrap();
    }
    let output = restored.finish_bounded().unwrap();
    assert_eq!(output_values(&output), vec!["b", "b", "y"]);
    assert_eq!(
        output
            .column(2)
            .as_any()
            .downcast_ref::<Int64Array>()
            .unwrap()
            .values(),
        &[2, 2, 3]
    );
    assert_eq!(
        output_kinds(&output),
        vec![UPDATE_BEFORE, UPDATE_AFTER, DELETE]
    );
    assert_eq!(restored.finish_bounded().unwrap().num_rows(), 0);
}

#[test]
fn bounded_rank_canonical_state_moves_between_memory_and_rocksdb() {
    let Ok(plugin_path) = std::env::var("STREAMFUSION_TEST_ROCKSDB_PLUGIN") else {
        return;
    };
    let broker = Arc::new(TestBroker::new(1 << 30));
    let mut memory = TopNProcessor::new(
        &bounded_rank_plan(),
        128,
        0,
        127,
        HostMemoryReservation::new(broker.clone(), "bounded rank memory source"),
    )
    .unwrap();
    memory
        .process_arrow(
            batch_with_kinds(
                vec![2, 1, 1, 2, 1, 1, 2],
                vec!["y", "b", "c", "z", "a", "b", "z"],
                vec![
                    DELETE,
                    UPDATE_BEFORE,
                    INSERT,
                    UPDATE_AFTER,
                    DELETE,
                    UPDATE_AFTER,
                    INSERT,
                ],
            ),
            0,
        )
        .unwrap();
    let snapshots = (0..128)
        .map(|key_group| memory.snapshot_key_group(key_group).unwrap())
        .collect::<Vec<_>>();

    let directory = tempfile::tempdir().unwrap();
    let mut rocks = TopNProcessor::new_rocksdb(
        &bounded_rank_plan(),
        128,
        0,
        127,
        std::path::Path::new(&plugin_path),
        directory.path(),
        64 << 20,
        HostMemoryReservation::new(broker.clone(), "bounded rank RocksDB scratch"),
    )
    .unwrap();
    for (key_group, snapshot) in snapshots.iter().enumerate() {
        rocks.restore_key_group(key_group as u32, snapshot).unwrap();
        assert_eq!(
            rocks.snapshot_key_group(key_group as u32).unwrap(),
            *snapshot
        );
    }
    let rocks_snapshots = (0..128)
        .map(|key_group| rocks.snapshot_key_group(key_group).unwrap())
        .collect::<Vec<_>>();

    let mut restored = TopNProcessor::new(
        &bounded_rank_plan(),
        128,
        0,
        127,
        HostMemoryReservation::new(broker, "bounded rank memory restore"),
    )
    .unwrap();
    for (key_group, snapshot) in rocks_snapshots.iter().enumerate() {
        restored
            .restore_key_group(key_group as u32, snapshot)
            .unwrap();
    }
    let output = restored.finish_bounded().unwrap();
    assert_eq!(output_values(&output), vec!["b", "b", "y"]);
    assert_eq!(
        output_kinds(&output),
        vec![UPDATE_BEFORE, UPDATE_AFTER, DELETE]
    );
}

fn limit_plan(strategy: proto::TopNStrategy) -> Vec<u8> {
    let schema = proto::Schema {
        fields: vec![
            proto::Field {
                name: "key".to_string(),
                r#type: Some(proto::LogicalType {
                    nullable: false,
                    r#type: Some(proto::logical_type::Type::Integer(
                        proto::EmptyType::default(),
                    )),
                }),
            },
            proto::Field {
                name: "value".to_string(),
                r#type: Some(proto::LogicalType {
                    nullable: true,
                    r#type: Some(proto::logical_type::Type::Varchar(
                        proto::EmptyType::default(),
                    )),
                }),
            },
        ],
    };
    proto::NativePlan {
        protocol_version: 1,
        root: Some(proto::Operator {
            plan_node_id: 0,
            metric_name: String::new(),
            clear_record_timestamps: false,
            metric_uid: None,
            operator: Some(proto::operator::Operator::TopN(Box::new(proto::TopN {
                input: Some(Box::new(proto::Operator {
                    plan_node_id: 0,
                    metric_name: String::new(),
                    clear_record_timestamps: false,
                    metric_uid: None,
                    operator: Some(proto::operator::Operator::Input(proto::Input::default())),
                })),
                partition_key_indices: vec![],
                sort_key_indices: vec![],
                primary_key_indices: vec![],
                rank_start: 2,
                rank_end: Some(3),
                variable_rank_end_index: None,
                output_rank_number: false,
                generate_update_before: true,
                strategy: strategy as i32,
                input_schema: Some(schema.clone()),
                state_ttl_millis: 0,
                sort_ascending: vec![],
                sort_nulls_last: vec![],
                output_schema: Some(schema),
                rank_type: proto::TopNRankType::RowNumber as i32,
                bounded_final_output: false,
                physical_input_semantics: false,
            }))),
        }),
    }
    .encode_to_vec()
}

fn output_values(batch: &RecordBatch) -> Vec<&str> {
    batch
        .column(1)
        .as_any()
        .downcast_ref::<StringArray>()
        .unwrap()
        .iter()
        .map(|value| value.unwrap())
        .collect()
}

#[test]
fn unordered_global_limit_preserves_input_order_and_offset_across_batches() {
    let broker = Arc::new(TestBroker::new(64 << 20));
    let mut processor = TopNProcessor::new(
        &limit_plan(proto::TopNStrategy::AppendFast),
        128,
        0,
        127,
        HostMemoryReservation::new(broker.clone(), "native limit"),
    )
    .unwrap();

    let first = processor
        .process_arrow(batch(vec![1, 2], vec!["skip", "first"]), 1)
        .unwrap();
    assert_eq!(output_values(&first), vec!["first"]);
    let second = processor
        .process_arrow(batch(vec![3, 4], vec!["second", "ignored"]), 2)
        .unwrap();
    assert_eq!(output_values(&second), vec!["second"]);
    let state_io_after_saturation = processor.statistics()[..4].to_vec();
    let ignored = processor
        .process_arrow(batch(vec![5, 6], vec!["ignored", "ignored"]), 3)
        .unwrap();
    assert_eq!(ignored.num_rows(), 0);
    assert_eq!(processor.statistics()[..4], state_io_after_saturation);
    assert_eq!(processor.statistics()[5], 0);

    let key_group = assign_key_group(&[], 128);
    let snapshot = processor.snapshot_key_group(key_group).unwrap();
    let mut restored = TopNProcessor::new(
        &limit_plan(proto::TopNStrategy::AppendFast),
        128,
        0,
        127,
        HostMemoryReservation::new(broker.clone(), "restored native limit"),
    )
    .unwrap();
    restored.restore_key_group(key_group, &snapshot).unwrap();
    assert!(!restored.is_append_limit_saturated());
    let after_restore = restored
        .process_arrow(batch(vec![7], vec!["ignored"]), 4)
        .unwrap();
    assert_eq!(after_restore.num_rows(), 0);
    assert!(restored.is_append_limit_saturated());
    assert_eq!(restored.statistics()[0], 1);
    assert_eq!(restored.statistics()[1], 1);
}

#[test]
fn unordered_global_limit_accepts_retractions() {
    let broker = Arc::new(TestBroker::new(64 << 20));
    let mut processor = TopNProcessor::new(
        &limit_plan(proto::TopNStrategy::Retract),
        128,
        0,
        127,
        HostMemoryReservation::new(broker, "retractable native limit"),
    )
    .unwrap();
    let initial = processor
        .process_arrow(batch(vec![1, 2, 3], vec!["skip", "first", "second"]), 1)
        .unwrap();
    assert_eq!(output_values(&initial), vec!["first", "second"]);

    let retracted = processor
        .process_arrow(batch_with_kinds(vec![2], vec!["first"], vec![DELETE]), 2)
        .unwrap();
    assert_eq!(retracted.num_rows(), 3);
    assert_eq!(processor.statistics()[6], 0);
}

#[test]
fn processes_arrow_and_restores_canonical_state_after_rescaling() {
    let source_broker = Arc::new(TestBroker::new(64 << 20));
    let mut source = TopNProcessor::new(
        &plan(),
        128,
        0,
        127,
        HostMemoryReservation::new(source_broker.clone(), "top-n source"),
    )
    .unwrap();
    let output = source
        .process_arrow(batch(vec![1, 1, 2], vec!["a", "c", "b"]), 1)
        .unwrap();
    assert_eq!(output.num_rows(), 5);
    assert_eq!(source.statistics()[..4], [1, 1, 2, 2]);

    let key_batch = RecordBatch::try_new(
        schema(),
        vec![
            Arc::new(Int32Array::from(vec![1])) as ArrayRef,
            Arc::new(StringArray::from(vec!["unused"])) as ArrayRef,
        ],
    )
    .unwrap();
    let key = encode_binary_row(&key_batch, 0, &[(0, KeyField::Integer)]).unwrap();
    let key_group = assign_key_group(&key, 128);
    let snapshot = source.snapshot_key_group(key_group).unwrap();
    assert!(source_broker.reserved() > 0);
    drop(output);
    drop(source);
    assert_eq!(source_broker.reserved(), snapshot.len());

    let restored_broker = Arc::new(TestBroker::new(64 << 20));
    let mut restored = TopNProcessor::new(
        &plan(),
        128,
        key_group,
        key_group,
        HostMemoryReservation::new(restored_broker.clone(), "top-n restored"),
    )
    .unwrap();
    restored.restore_key_group(key_group, &snapshot).unwrap();
    drop(snapshot);
    assert_eq!(source_broker.reserved(), 0);
    let output = restored
        .process_arrow(batch(vec![1], vec!["b"]), 2)
        .unwrap();
    assert!(output.num_rows() > 0);
    drop(output);
    drop(restored);
    assert_eq!(restored_broker.reserved(), 0);
}

#[test]
fn canonical_arrow_state_round_trips_payloads() {
    let rows = RecordBatch::try_new(
        schema(),
        vec![
            Arc::new(Int32Array::from(vec![1])) as ArrayRef,
            Arc::new(StringArray::from(vec![Some("payload")])) as ArrayRef,
        ],
    )
    .unwrap();
    let state = StoredState {
        next_sequence: 2,
        rank_end: Some(2),
        last_access_millis: 9,
        sequences: vec![1],
        rows: rows.clone(),
    };
    let converter = row_converter(&schema()).unwrap();
    let decoded = decode_state(
        &encode_state(&state, &converter).unwrap(),
        &schema(),
        &converter,
    )
    .unwrap();
    assert_eq!(decoded.next_sequence, 2);
    assert_eq!(decoded.rank_end, Some(2));
    assert_eq!(decoded.sequences, vec![1]);
    assert_eq!(decoded.rows, rows);
}

#[test]
fn rejects_unadmitted_working_memory_and_releases_every_reservation() {
    let broker = Arc::new(TestBroker::new(4 << 10));
    let mut processor = TopNProcessor::new(
        &plan(),
        1,
        0,
        0,
        HostMemoryReservation::new(broker.clone(), "top-n constrained"),
    )
    .unwrap();
    let payload = "x".repeat(8 << 10);
    let result = processor.process_arrow(batch(vec![1], vec![payload.as_str()]), 1);
    assert!(matches!(
        result,
        Err(DataFusionError::ResourcesExhausted(_))
    ));
    drop(processor);
    assert_eq!(broker.reserved(), 0);
}
