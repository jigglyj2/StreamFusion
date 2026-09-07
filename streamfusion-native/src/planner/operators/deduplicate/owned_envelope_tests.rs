// Copyright 2026 StreamFusion Authors
// Licensed under the Apache License, Version 2.0.

use super::*;
use crate::memory_pool::tests_support::TestBroker;
use crate::planner::operators::envelope::{INPUT_ROW, OWNED_TIMESTAMP_V1, ROW_KIND};
use arrow::array::{Int64Array, StringArray};
use prost::Message;

fn plan() -> Vec<u8> {
    proto::NativePlan {
        protocol_version: 3,
        root: Some(proto::Operator {
            operator: Some(proto::operator::Operator::Deduplicate(Box::new(
                proto::Deduplicate {
                    key_indices: vec![0],
                    order_index: 2,
                    keep_last: true,
                    generate_insert: true,
                    generate_update_before: true,
                    ..Default::default()
                },
            ))),
            ..Default::default()
        }),
    }
    .encode_to_vec()
}
fn input(time: i64, timestamp: Option<i64>, ordinal: i32) -> RecordBatch {
    let batch = RecordBatch::try_from_iter(vec![
        ("key", Arc::new(Int64Array::from(vec![1])) as ArrayRef),
        (
            "value",
            Arc::new(StringArray::from(vec!["value"])) as ArrayRef,
        ),
        (
            "rowtime",
            Arc::new(TimestampMillisecondArray::from(vec![time])) as ArrayRef,
        ),
        (
            OWNED_TIMESTAMP_V1,
            Arc::new(Int64Array::from(vec![timestamp])) as ArrayRef,
        ),
        (
            ROW_KIND,
            Arc::new(Int8Array::from(vec![INSERT])) as ArrayRef,
        ),
        (
            INPUT_ROW,
            Arc::new(Int32Array::from(vec![ordinal])) as ArrayRef,
        ),
    ])
    .unwrap();
    let mut fields = batch.schema().fields().to_vec();
    fields[3] = Arc::new(Field::new(OWNED_TIMESTAMP_V1, DataType::Int64, true));
    RecordBatch::try_new(Arc::new(Schema::new(fields)), batch.columns().to_vec()).unwrap()
}

#[test]
fn detached_history_uses_trigger_timestamp_and_invalid_ordinals_cannot_mutate_state() {
    for rocks in [false, true] {
        let broker = Arc::new(TestBroker::new(32 << 20));
        let directory = tempfile::tempdir().unwrap();
        let memory = HostMemoryReservation::new(broker.clone(), "dedup owned test");
        let mut processor = if rocks {
            let Some(plugin) = std::env::var_os("STREAMFUSION_TEST_ROCKSDB_PLUGIN") else {
                continue;
            };
            DeduplicateProcessor::new_rocksdb(
                &plan(),
                16,
                0,
                15,
                std::path::Path::new(&plugin),
                directory.path(),
                4 << 20,
                memory,
            )
        } else {
            DeduplicateProcessor::new(&plan(), 16, 0, 15, memory)
        }
        .unwrap();
        // Admission/schema checks precede all state reads or writes and every row is checked,
        // including an arrival which would otherwise lose the rowtime comparison.
        let first = processor
            .process_native(input(10, Some(i64::MIN), -1))
            .unwrap();
        assert_eq!(first.batch.num_columns(), 6);
        assert_eq!(
            first
                .batch
                .column(3)
                .as_any()
                .downcast_ref::<Int64Array>()
                .unwrap()
                .value(0),
            i64::MIN
        );
        drop(first);
        assert!(processor
            .process_native(input(99, None, 0))
            .err()
            .expect("invalid ordinal must fail before mutation")
            .to_string()
            .contains("ordinal -1"));
        let next = processor.process_native(input(20, None, -1)).unwrap();
        assert_eq!(next.batch.num_rows(), 2); // invalid newer row was never stored
        assert_eq!(next.batch.column(3).null_count(), 2);
        assert_eq!(
            next.batch
                .column(4)
                .as_any()
                .downcast_ref::<Int8Array>()
                .unwrap()
                .values(),
            &[UPDATE_BEFORE, UPDATE_AFTER]
        );
        assert_eq!(
            next.batch
                .column(2)
                .as_any()
                .downcast_ref::<TimestampMillisecondArray>()
                .unwrap()
                .values(),
            &[10, 20]
        );
        assert_eq!(
            next.batch
                .column(5)
                .as_any()
                .downcast_ref::<Int32Array>()
                .unwrap()
                .values(),
            &[-1, -1]
        );
        drop(next);
        drop(processor);
        assert_eq!(broker.reserved(), 0);
    }
}

#[test]
fn update_before_contract_requires_an_initial_insert_even_without_insert_sensitivity() {
    for keep_last in [false, true] {
        for insert in [false, true] {
            for before in [false, true] {
                let mut plan = proto::NativePlan::decode(plan().as_slice()).unwrap();
                let Some(proto::operator::Operator::Deduplicate(node)) =
                    plan.root.as_mut().unwrap().operator.as_mut()
                else {
                    unreachable!()
                };
                node.keep_last = keep_last;
                node.generate_insert = insert;
                node.generate_update_before = before;
                let broker = Arc::new(TestBroker::new(32 << 20));
                let mut processor = DeduplicateProcessor::new(
                    &plan.encode_to_vec(),
                    16,
                    0,
                    15,
                    HostMemoryReservation::new(broker.clone(), "rowtime changelog contract"),
                )
                .unwrap();
                let first = processor.process_native(input(10, None, -1)).unwrap();
                let kinds = first
                    .batch
                    .column(4)
                    .as_any()
                    .downcast_ref::<Int8Array>()
                    .unwrap();
                assert_eq!(
                    kinds.values(),
                    &[if insert || before {
                        INSERT
                    } else {
                        UPDATE_AFTER
                    }]
                );
                drop(first);
                let next = processor
                    .process_native(input(if keep_last { 20 } else { 0 }, None, -1))
                    .unwrap();
                let kinds = next
                    .batch
                    .column(4)
                    .as_any()
                    .downcast_ref::<Int8Array>()
                    .unwrap();
                let expected: &[i8] = if before {
                    &[UPDATE_BEFORE, UPDATE_AFTER]
                } else {
                    &[UPDATE_AFTER]
                };
                assert_eq!(kinds.values(), expected);
                drop(next);
                drop(processor);
                assert_eq!(broker.reserved(), 0);
            }
        }
    }
}
