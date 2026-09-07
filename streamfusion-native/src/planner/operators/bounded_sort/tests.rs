// Copyright 2026 StreamFusion Authors
// Licensed under the Apache License, Version 2.0

use super::*;
use crate::memory_pool::{tests_support::TestBroker, HostMemoryReservation};
use arrow::array::{ArrayRef, Int32Array, StringArray};
use prost::Message;

#[test]
fn sorts_counted_rows_and_applies_all_retraction_kinds() {
    let mut processor = new_processor();
    processor
        .process_arrow(batch(
            &[3, 1, 2, 2, 2, 4],
            &["c", "a", "b", "b", "b", "d"],
            &[INSERT, INSERT, INSERT, INSERT, DELETE, UPDATE_AFTER],
        ))
        .unwrap();
    processor
        .process_arrow(batch(&[4], &["d"], &[UPDATE_BEFORE]))
        .unwrap();
    let output = processor.finish().unwrap();
    assert_eq!(integers(&output), vec![1, 2, 3]);
    assert_eq!(strings(&output), vec!["a", "b", "c"]);
    assert_eq!(kinds(&output), vec![INSERT; 3]);
    assert_eq!(processor.statistics()[..5], [2, 2, 5, 5, 0]);
}

#[test]
fn canonical_snapshot_restores_to_an_identical_terminal_sort() {
    let mut source = new_processor();
    source
        .process_arrow(batch(&[4, 1, 3], &["d", "a", "c"], &[INSERT; 3]))
        .unwrap();
    let snapshot = source.snapshot_key_group(0).unwrap();
    let mut restored = new_processor();
    restored.restore_key_group(0, &snapshot).unwrap();
    restored
        .process_arrow(batch(&[2], &["b"], &[INSERT]))
        .unwrap();
    assert_eq!(integers(&restored.finish().unwrap()), vec![1, 2, 3, 4]);
}

#[test]
fn sort_limit_applies_offset_after_counting_duplicates() {
    let mut processor = BoundedSortProcessor::new(
        &limit_plan(2, 5, false),
        0,
        0,
        HostMemoryReservation::new(
            Arc::new(TestBroker::new(64 << 20)),
            "bounded sort limit test",
        ),
    )
    .unwrap();
    processor
        .process_arrow(batch(
            &[1, 1, 2, 3, 4, 5],
            &["a", "a", "b", "c", "d", "e"],
            &[INSERT; 6],
        ))
        .unwrap();
    assert_eq!(integers(&processor.finish().unwrap()), vec![2, 3, 4]);
    assert_eq!(processor.finish().unwrap().num_rows(), 0);
    assert_eq!(processor.statistics()[6], 3);
}

#[test]
fn local_sort_limit_merges_rescaled_owned_key_groups() {
    let source_plan = limit_plan(0, 4, true);
    let mut left = BoundedSortProcessor::new(
        &source_plan,
        0,
        0,
        HostMemoryReservation::new(
            Arc::new(TestBroker::new(64 << 20)),
            "bounded local sort left",
        ),
    )
    .unwrap();
    left.process_arrow(batch(&[5, 1], &["e", "a"], &[INSERT; 2]))
        .unwrap();
    let left_snapshot = left.snapshot_key_group(0).unwrap();

    let mut right = BoundedSortProcessor::new(
        &source_plan,
        1,
        1,
        HostMemoryReservation::new(
            Arc::new(TestBroker::new(64 << 20)),
            "bounded local sort right",
        ),
    )
    .unwrap();
    right
        .process_arrow(batch(&[4, 2], &["d", "b"], &[INSERT; 2]))
        .unwrap();
    let right_snapshot = right.snapshot_key_group(1).unwrap();

    let mut restored = BoundedSortProcessor::new(
        &source_plan,
        0,
        1,
        HostMemoryReservation::new(
            Arc::new(TestBroker::new(64 << 20)),
            "bounded local sort rescaled",
        ),
    )
    .unwrap();
    restored.restore_key_group(0, &left_snapshot).unwrap();
    restored.restore_key_group(1, &right_snapshot).unwrap();
    restored
        .process_arrow(batch(&[3], &["c"], &[INSERT]))
        .unwrap();
    assert_eq!(integers(&restored.finish().unwrap()), vec![1, 2, 3, 4]);
}

#[test]
fn physical_sort_limit_keeps_the_first_rows_at_an_equal_key_cutoff() {
    let mut processor = BoundedSortProcessor::new(
        &physical_limit_plan(0, 2, false),
        0,
        0,
        HostMemoryReservation::new(
            Arc::new(TestBroker::new(64 << 20)),
            "bounded physical sort limit",
        ),
    )
    .unwrap();
    processor
        .process_arrow(batch(
            &[1, 1, 1, 1],
            &["first", "second", "third", "fourth"],
            &[INSERT, DELETE, UPDATE_BEFORE, UPDATE_AFTER],
        ))
        .unwrap();

    let output = processor.finish().unwrap();
    assert_eq!(strings(&output), vec!["first", "second"]);
    assert_eq!(kinds(&output), vec![INSERT, DELETE]);
}

#[test]
fn physical_sort_limit_persists_only_the_bounded_heap_and_skips_unchanged_writes() {
    let mut processor = BoundedSortProcessor::new(
        &physical_limit_plan(0, 2, true),
        0,
        0,
        HostMemoryReservation::new(
            Arc::new(TestBroker::new(64 << 20)),
            "bounded online physical heap",
        ),
    )
    .unwrap();
    processor
        .process_arrow(batch(
            &[1, 2, 3, 4, 5, 6],
            &["a", "b", "c", "d", "e", "f"],
            &[INSERT; 6],
        ))
        .unwrap();
    assert_eq!(
        decode_key_group_snapshot(0, &processor.snapshot_key_group(0).unwrap())
            .unwrap()
            .len(),
        2
    );
    assert_eq!(processor.statistics()[..4], [0, 1, 0, 2]);

    processor
        .process_arrow(batch(&[7, 8], &["g", "h"], &[INSERT; 2]))
        .unwrap();
    assert_eq!(processor.statistics()[..4], [0, 1, 0, 2]);
    assert_eq!(integers(&processor.finish().unwrap()), vec![2, 1]);
}

#[test]
fn canonical_state_moves_between_memory_and_rocksdb_with_batched_io() {
    let Ok(plugin_path) = std::env::var("STREAMFUSION_TEST_ROCKSDB_PLUGIN") else {
        return;
    };
    let broker = Arc::new(TestBroker::new(1 << 30));
    let mut memory = BoundedSortProcessor::new(
        &plan(),
        0,
        0,
        HostMemoryReservation::new(broker.clone(), "bounded sort memory source"),
    )
    .unwrap();
    memory
        .process_arrow(batch(&[3, 1], &["c", "a"], &[INSERT, INSERT]))
        .unwrap();
    assert_eq!(memory.statistics()[..4], [1, 1, 2, 2]);
    let snapshot = memory.snapshot_key_group(0).unwrap();

    let directory = tempfile::tempdir().unwrap();
    let mut rocks = BoundedSortProcessor::new_rocksdb(
        &plan(),
        0,
        0,
        std::path::Path::new(&plugin_path),
        directory.path(),
        64 << 20,
        HostMemoryReservation::new(broker.clone(), "bounded sort RocksDB scratch"),
    )
    .unwrap();
    rocks.restore_key_group(0, &snapshot).unwrap();
    assert_eq!(rocks.snapshot_key_group(0).unwrap(), snapshot);
    rocks.process_arrow(batch(&[2], &["b"], &[INSERT])).unwrap();
    assert_eq!(rocks.statistics()[..4], [1, 1, 1, 1]);
    let rocks_snapshot = rocks.snapshot_key_group(0).unwrap();

    let mut restored = BoundedSortProcessor::new(
        &plan(),
        0,
        0,
        HostMemoryReservation::new(broker, "bounded sort memory restore"),
    )
    .unwrap();
    restored.restore_key_group(0, &rocks_snapshot).unwrap();
    assert_eq!(integers(&restored.finish().unwrap()), vec![1, 2, 3]);
}

#[test]
fn missing_retraction_fails_with_flinks_contract() {
    let mut processor = new_processor();
    let error = processor
        .process_arrow(batch(&[9], &["missing"], &[DELETE]))
        .unwrap_err();
    assert!(error.to_string().contains("RowData not exist!"));
    assert_eq!(processor.statistics()[4], 1);
}

#[test]
fn spills_large_unique_state_and_removes_temporary_files_after_drain() {
    let directory = tempfile::tempdir().unwrap();
    let broker = Arc::new(TestBroker::new(16 << 20));
    let mut processor = BoundedSortProcessor::new(
        &plan(),
        0,
        0,
        HostMemoryReservation::new(broker.clone(), "bounded sort forced spill"),
    )
    .unwrap();
    processor
        .configure_spill_directory(directory.path().to_path_buf())
        .unwrap();
    let label = "x".repeat(256);
    for start in (0..20_000).step_by(100) {
        let values = (start..start + 100).rev().collect::<Vec<_>>();
        processor
            .process_arrow(batch(
                &values,
                &vec![label.as_str(); 100],
                &vec![INSERT; 100],
            ))
            .unwrap();
    }
    let mut expected = 0;
    loop {
        let output = processor.finish().unwrap();
        if output.num_rows() == 0 {
            break;
        }
        assert!(output.num_rows() <= OUTPUT_BATCH_ROWS);
        for value in integers(&output) {
            assert_eq!(value, expected);
            expected += 1;
        }
    }
    assert_eq!(expected, 20_000);
    assert!(processor.spill_statistics()[0] > 0);
    assert!(processor.spill_statistics()[1] > 0);
    drop(processor);
    assert_eq!(broker.reserved(), 0);
    assert_eq!(std::fs::read_dir(directory.path()).unwrap().count(), 0);
}

#[test]
fn drains_terminal_output_in_managed_batches_without_splitting_row_counts() {
    let rows = OUTPUT_BATCH_ROWS + 3_616;
    let mut processor = new_processor();
    processor
        .process_arrow(batch(
            &vec![7; rows],
            &vec!["same"; rows],
            &vec![INSERT; rows],
        ))
        .unwrap();

    let first = processor.finish().unwrap();
    let second = processor.finish().unwrap();
    let exhausted = processor.finish().unwrap();
    assert_eq!(first.num_rows(), OUTPUT_BATCH_ROWS);
    assert_eq!(second.num_rows(), 3_616);
    assert_eq!(exhausted.num_rows(), 0);
    assert!(integers(&first).iter().all(|&value| value == 7));
    assert!(integers(&second).iter().all(|&value| value == 7));
    assert_eq!(processor.statistics()[6], rows as u64);
}

#[test]
fn accounts_state_scratch_and_output_and_releases_on_failure() {
    let broker = Arc::new(TestBroker::new(64 << 20));
    let mut processor = BoundedSortProcessor::new(
        &plan(),
        0,
        0,
        HostMemoryReservation::new(broker.clone(), "bounded sort accounting"),
    )
    .unwrap();
    let empty_state = broker.reserved();
    processor
        .process_arrow(batch(&[2, 1], &["second", "first"], &[INSERT; 2]))
        .unwrap();
    assert!(broker.reserved() > empty_state);
    let output = processor.finish().unwrap();
    assert_eq!(output.num_rows(), 2);
    drop(output);
    drop(processor);
    assert_eq!(broker.reserved(), 0);

    let constrained = Arc::new(TestBroker::new(1 << 10));
    let mut processor = BoundedSortProcessor::new(
        &plan(),
        0,
        0,
        HostMemoryReservation::new(constrained.clone(), "bounded sort constrained"),
    )
    .unwrap();
    let payload = "x".repeat(8 << 10);
    let error = processor
        .process_arrow(batch(&[1], &[payload.as_str()], &[INSERT]))
        .unwrap_err();
    assert!(matches!(error, DataFusionError::ResourcesExhausted(_)));
    drop(processor);
    assert_eq!(constrained.reserved(), 0);
}

fn new_processor() -> BoundedSortProcessor {
    BoundedSortProcessor::new(
        &plan(),
        0,
        0,
        HostMemoryReservation::new(Arc::new(TestBroker::new(64 << 20)), "bounded sort test"),
    )
    .unwrap()
}

fn plan() -> Vec<u8> {
    limit_plan_values(None, None, false)
}

fn limit_plan(start: u64, end: u64, local: bool) -> Vec<u8> {
    limit_plan_values(Some(start), Some(end), local)
}

fn physical_limit_plan(start: u64, end: u64, local: bool) -> Vec<u8> {
    limit_plan_values_with_semantics(Some(start), Some(end), local, true)
}

fn limit_plan_values(start: Option<u64>, end: Option<u64>, local: bool) -> Vec<u8> {
    limit_plan_values_with_semantics(start, end, local, false)
}

fn limit_plan_values_with_semantics(
    start: Option<u64>,
    end: Option<u64>,
    local: bool,
    physical_input_semantics: bool,
) -> Vec<u8> {
    let schema = proto::Schema {
        fields: vec![
            field(
                "number",
                proto::logical_type::Type::Integer(proto::EmptyType::default()),
            ),
            field(
                "label",
                proto::logical_type::Type::Varchar(proto::EmptyType::default()),
            ),
        ],
    };
    proto::NativePlan {
        protocol_version: crate::PLAN_PROTOCOL_VERSION,
        root: Some(proto::Operator {
            plan_node_id: 0,
            metric_name: String::new(),
            clear_record_timestamps: false,
            metric_uid: None,
            operator: Some(proto::operator::Operator::BoundedSort(Box::new(
                proto::BoundedSort {
                    input: None,
                    input_schema: Some(schema),
                    sort_key_indices: vec![0],
                    sort_ascending: vec![true],
                    sort_nulls_last: vec![false],
                    limit_start: start,
                    limit_end: end,
                    use_first_owned_key_group: local,
                    physical_input_semantics,
                    sort_limit_global: !local,
                },
            ))),
        }),
    }
    .encode_to_vec()
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

fn batch(numbers: &[i32], labels: &[&str], row_kinds: &[i8]) -> RecordBatch {
    RecordBatch::try_from_iter(vec![
        (
            "number",
            Arc::new(Int32Array::from(numbers.to_vec())) as ArrayRef,
        ),
        (
            "label",
            Arc::new(StringArray::from(labels.to_vec())) as ArrayRef,
        ),
        (
            INPUT_KIND_COLUMN,
            Arc::new(Int8Array::from(row_kinds.to_vec())) as ArrayRef,
        ),
    ])
    .unwrap()
}

fn integers(batch: &RecordBatch) -> Vec<i32> {
    batch
        .column(0)
        .as_any()
        .downcast_ref::<Int32Array>()
        .unwrap()
        .values()
        .to_vec()
}

fn strings(batch: &RecordBatch) -> Vec<&str> {
    let values = batch
        .column(1)
        .as_any()
        .downcast_ref::<StringArray>()
        .unwrap();
    (0..values.len()).map(|row| values.value(row)).collect()
}

fn kinds(batch: &RecordBatch) -> Vec<i8> {
    batch
        .column(2)
        .as_any()
        .downcast_ref::<Int8Array>()
        .unwrap()
        .values()
        .to_vec()
}
