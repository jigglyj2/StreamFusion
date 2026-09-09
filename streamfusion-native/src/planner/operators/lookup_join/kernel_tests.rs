// Copyright 2026 StreamFusion Authors
// Licensed under the Apache License, Version 2.0

use arrow::array::{Array, BooleanArray, Int64Array, RecordBatch, StringArray};
use arrow::compute::concat_batches;
use datafusion::physical_plan::{collect, ExecutionPlan};
use std::sync::Arc;

use super::{test_support::*, LookupJoinExec, LookupTable};
use crate::planner::operators::{identified::IdentifiedExec, reusable_input::ReusableInputExec};

fn replace_key(batch: RecordBatch, key: arrow::array::ArrayRef) -> RecordBatch {
    let mut fields = batch.schema().fields().to_vec();
    fields[0] = Arc::new(arrow::datatypes::Field::new(
        "key",
        key.data_type().clone(),
        true,
    ));
    let mut columns = batch.columns().to_vec();
    columns[0] = key;
    RecordBatch::try_new(Arc::new(arrow::datatypes::Schema::new(fields)), columns).unwrap()
}

#[tokio::test]
async fn every_admitted_key_type_uses_the_same_null_and_duplicate_semantics() {
    use arrow::array::{ArrayRef, BinaryArray, Int16Array, Int32Array, Int8Array};
    let keys: Vec<ArrayRef> = vec![
        Arc::new(BooleanArray::from(vec![
            Some(false),
            Some(true),
            Some(false),
            None,
        ])),
        Arc::new(Int8Array::from(vec![Some(-1), Some(2), Some(-1), None])),
        Arc::new(Int16Array::from(vec![
            Some(i16::MIN),
            Some(i16::MAX),
            Some(i16::MIN),
            None,
        ])),
        Arc::new(Int32Array::from(vec![
            Some(i32::MIN),
            Some(i32::MAX),
            Some(i32::MIN),
            None,
        ])),
        Arc::new(Int64Array::from(vec![
            Some(i64::MIN),
            Some(i64::MAX),
            Some(i64::MIN),
            None,
        ])),
        Arc::new(StringArray::from(vec![
            Some("é\0x"),
            Some(""),
            Some("é\0x"),
            None,
        ])),
        Arc::new(BinaryArray::from(vec![
            Some(&[0, 255][..]),
            Some(&[][..]),
            Some(&[0, 255][..]),
            None,
        ])),
    ];
    for key in keys {
        let (context, broker) = context(8 << 20, 32);
        let side = replace_key(batch(&rows(1, 4, false), false), key.clone());
        let probe = replace_key(batch(&rows(1, 4, false), true), key);
        let table =
            LookupTable::new(side, vec![0], context.runtime_env().memory_pool.clone()).unwrap();
        let input = Arc::new(ReusableInputExec::new(probe.schema()));
        input.replace_batch(probe).unwrap();
        let join = LookupJoinExec::new(table.clone(), input, vec![0]).unwrap();
        let outputs = collect(join.clone(), context.task_ctx()).await.unwrap();
        let output = concat_batches(&join.schema(), &outputs).unwrap();
        assert_eq!(
            output
                .column(5)
                .as_any()
                .downcast_ref::<Int64Array>()
                .unwrap()
                .values()
                .as_ref(),
            &[0, 2, 1, 0, 2]
        );
        assert_eq!(
            output
                .column(2)
                .as_any()
                .downcast_ref::<Int64Array>()
                .unwrap()
                .values()
                .as_ref(),
            &[0, 0, 1, 2, 2]
        );
        drop(output);
        drop(outputs);
        drop(join);
        drop(table);
        assert_eq!(broker.reserved(), 0);
    }
}

#[tokio::test]
async fn generated_datafusion_kernel_lookup_matches_the_arrival_oracle() {
    for seed in [1, 7, 31] {
        for composite in [false, true] {
            for sparse in [false, true] {
                let side = rows(seed, 217, sparse);
                let (context, broker) = context(64 << 20, 31);
                let keys = if composite { vec![0, 1] } else { vec![0] };
                let table = LookupTable::new(
                    batch(&side, false),
                    keys.clone(),
                    context.runtime_env().memory_pool.clone(),
                )
                .unwrap();
                let cache_bytes = broker.reserved();
                let input = Arc::new(ReusableInputExec::new(schema(true)));
                let stage = IdentifiedExec::wrap(41, input.clone());
                let join = LookupJoinExec::new(table.clone(), stage, keys).unwrap();
                let stage = IdentifiedExec::wrap(42, join.clone());
                let mut input_rows = 0;
                let mut output_rows = 0;
                for size in [0, 1, 37, 129, 0, 17] {
                    let mut probe = rows(seed + 3, size, sparse);
                    for row in probe.iter_mut().step_by(7) {
                        row.key = Some(i64::MAX);
                    }
                    input.replace_batch(batch(&probe, true)).unwrap();
                    {
                        let result = collect(stage.clone(), context.task_ctx()).await.unwrap();
                        assert!(result.iter().all(|batch| batch.num_rows() <= 1024));
                        let result = concat_batches(&join.schema(), &result).unwrap();
                        assert_eq!(
                            result,
                            expected(&probe, &side, composite, false, join.schema())
                        );
                        output_rows += result.num_rows() as u64;
                    }
                    input_rows += size as u64;
                    assert_eq!(broker.reserved(), cache_bytes);
                }
                let metrics = stage.downcast_ref::<IdentifiedExec>().unwrap();
                assert_eq!(metrics.input_rows(), input_rows);
                assert_eq!(metrics.output_rows(), output_rows);
                drop(stage);
                drop(join);
                drop(table);
                assert_eq!(broker.reserved(), 0);
            }
        }
    }
}

#[test]
fn collision_comparison_uses_datafusion_three_valued_equality_for_every_key() {
    let (context, _) = context(8 << 20, 32);
    let table = LookupTable::new(
        batch(&rows(1, 17, false), false),
        vec![0, 1],
        context.runtime_env().memory_pool.clone(),
    )
    .unwrap();
    let input = Arc::new(ReusableInputExec::new(schema(true)));
    let join = LookupJoinExec::new(table, input, vec![0, 1]).unwrap();
    let candidates = RecordBatch::try_new(
        join.key_schema.clone(),
        vec![
            Arc::new(Int64Array::from(vec![
                Some(1),
                Some(1),
                None,
                Some(1),
                Some(1),
            ])),
            Arc::new(Int64Array::from(vec![
                Some(1),
                Some(2),
                None,
                Some(1),
                Some(1),
            ])),
            Arc::new(StringArray::from(vec![
                Some("é\0"),
                Some("é\0"),
                Some("é\0"),
                None,
                Some("a"),
            ])),
            Arc::new(StringArray::from(vec![
                Some("é\0"),
                Some("é\0"),
                Some("é\0"),
                Some("é\0"),
                Some("b"),
            ])),
        ],
    )
    .unwrap();
    let result = join
        .condition
        .evaluate(&candidates)
        .unwrap()
        .into_array(5)
        .unwrap();
    assert_eq!(
        result.as_any().downcast_ref::<BooleanArray>().unwrap(),
        &BooleanArray::from(vec![Some(true), Some(false), None, None, Some(false)])
    );
}
