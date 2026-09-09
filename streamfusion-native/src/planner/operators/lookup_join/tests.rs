// Copyright 2026 StreamFusion Authors
// Licensed under the Apache License, Version 2.0

use super::*;
use arrow::array::{Array, ArrayRef, Int32Array, Int64Array, Int8Array, RecordBatch, StringArray};
use arrow::compute::concat_batches;
use arrow::datatypes::{Field, Schema, SchemaRef};
use datafusion::execution::context::{SessionConfig, SessionContext};
use datafusion::execution::runtime_env::RuntimeEnvBuilder;
use datafusion::physical_plan::collect;
use datafusion::physical_plan::memory::MemoryStream;
use datafusion::physical_plan::projection::ProjectionExec;

use crate::memory_pool::{tests_support::TestBroker, FlinkMemoryPool};
use crate::planner::operators::{identified::IdentifiedExec, reusable_input::ReusableInputExec};

#[derive(Clone, Debug)]
struct Row {
    key: Option<i64>,
    text: Option<String>,
    id: i64,
}

fn schema(probe: bool) -> SchemaRef {
    let mut fields = vec![
        Field::new("key", DataType::Int64, true),
        Field::new("text", DataType::Utf8, true),
        Field::new("id", DataType::Int64, false),
    ];
    if probe {
        fields.extend([
            Field::new(envelope::OWNED_TIMESTAMP_V1, DataType::Int64, true),
            Field::new(envelope::ROW_KIND, DataType::Int8, false),
            Field::new(envelope::INPUT_ROW, DataType::Int32, false),
        ]);
    }
    Arc::new(Schema::new(fields))
}

fn batch(rows: &[Row], probe: bool) -> RecordBatch {
    let mut columns: Vec<ArrayRef> = vec![
        Arc::new(Int64Array::from_iter(rows.iter().map(|r| r.key))),
        Arc::new(StringArray::from_iter(
            rows.iter().map(|r| r.text.as_deref()),
        )),
        Arc::new(Int64Array::from_iter_values(rows.iter().map(|r| r.id))),
    ];
    if probe {
        columns.extend([
            Arc::new(Int64Array::from_iter(
                rows.iter().map(|r| (r.id % 3 != 0).then_some(r.id - 100)),
            )) as ArrayRef,
            Arc::new(Int8Array::from_iter_values(
                rows.iter().map(|r| (r.id % 4) as i8),
            )),
            Arc::new(Int32Array::from_iter_values(0..rows.len() as i32)),
        ]);
    }
    RecordBatch::try_new(schema(probe), columns).unwrap()
}

fn snapshot(rows: &[Row], batch_size: usize) -> Arc<LookupSnapshotExec> {
    let batches = rows
        .chunks(batch_size)
        .map(|rows| batch(rows, false))
        .collect();
    LookupSnapshotExec::new(Box::pin(
        MemoryStream::try_new(batches, schema(false), None).unwrap(),
    ))
}

fn context(limit: usize, batch_size: usize) -> (SessionContext, Arc<TestBroker>) {
    let broker = Arc::new(TestBroker::new(limit));
    let pool = Arc::new(FlinkMemoryPool::new(broker.clone(), limit));
    let env = RuntimeEnvBuilder::new()
        .with_memory_pool(pool)
        .build_arc()
        .unwrap();
    (
        SessionContext::new_with_config_rt(SessionConfig::new().with_batch_size(batch_size), env),
        broker,
    )
}

fn rows(seed: i64, n: usize, sparse: bool) -> Vec<Row> {
    (0..n)
        .map(|i| {
            let i = i as i64;
            Row {
                key: (i % 11 != 0)
                    .then_some(((i * 7 + seed) % 19 - 9) * if sparse { 1_000_000_007 } else { 1 }),
                text: (i % 13 != 0).then(|| format!("é\0{}", (i * 3 + seed) % 5)),
                id: i,
            }
        })
        .collect()
}

fn expected(
    probe: &[Row],
    side: &[Row],
    composite: bool,
    outer: bool,
    output_schema: SchemaRef,
) -> RecordBatch {
    // Independent arrival-by-arrival lookup oracle; retain every duplicate in file order.
    let mut pairs = Vec::new();
    for (ordinal, row) in probe.iter().enumerate() {
        let before = pairs.len();
        for value in side {
            if row.key.is_some()
                && row.key == value.key
                && (!composite || (row.text.is_some() && row.text == value.text))
            {
                pairs.push((ordinal as i32, row, Some(value)));
            }
        }
        if outer && before == pairs.len() {
            pairs.push((ordinal as i32, row, None));
        }
    }
    RecordBatch::try_new(
        output_schema,
        vec![
            Arc::new(Int64Array::from_iter(pairs.iter().map(|(_, p, _)| p.key))),
            Arc::new(StringArray::from_iter(
                pairs.iter().map(|(_, p, _)| p.text.as_deref()),
            )),
            Arc::new(Int64Array::from_iter_values(
                pairs.iter().map(|(_, p, _)| p.id),
            )),
            Arc::new(Int64Array::from_iter(
                pairs.iter().map(|(_, _, s)| s.and_then(|r| r.key)),
            )),
            Arc::new(StringArray::from_iter(
                pairs
                    .iter()
                    .map(|(_, _, s)| s.and_then(|r| r.text.as_deref())),
            )),
            Arc::new(Int64Array::from_iter(
                pairs.iter().map(|(_, _, s)| s.map(|r| r.id)),
            )),
            Arc::new(Int64Array::from_iter(
                pairs
                    .iter()
                    .map(|(_, p, _)| (p.id % 3 != 0).then_some(p.id - 100)),
            )),
            Arc::new(Int8Array::from_iter_values(
                pairs.iter().map(|(_, p, _)| (p.id % 4) as i8),
            )),
            Arc::new(Int32Array::from_iter_values(
                pairs.iter().map(|(ordinal, _, _)| *ordinal),
            )),
        ],
    )
    .unwrap()
}

#[tokio::test]
async fn generated_lookup_preserves_duplicates_nulls_metadata_and_cache_across_invocations() {
    for seed in [1, 7, 31] {
        for composite in [false, true] {
            for sparse in [false, true] {
                for source_batch_rows in [1, 17, 1024] {
                    let side = rows(seed, 217, sparse);
                    let source = snapshot(&side, source_batch_rows);
                    let probe = Arc::new(ReusableInputExec::new(schema(true)));
                    let input_stage = IdentifiedExec::wrap(41, probe.clone());
                    let keys = if composite {
                        vec![(0, 0), (1, 1)]
                    } else {
                        vec![(0, 0)]
                    };
                    let join = create(source.clone(), input_stage, &keys, false).unwrap();
                    assert_eq!(join.name(), "HashJoinExec");
                    let lookup_stage = IdentifiedExec::wrap(42, join.clone());
                    let (context, broker) = context(64 << 20, 31);
                    let mut input_count = 0;
                    let mut output_count = 0;
                    let mut retained = None;
                    for size in [0, 1, 37, 129, 0, 17] {
                        let mut input = rows(seed + 3, size, sparse);
                        // Mix hits with definitely missing keys in each probe invocation.
                        for row in input.iter_mut().step_by(7) {
                            row.key = Some(i64::MAX);
                        }
                        probe.replace_batch(batch(&input, true)).unwrap();
                        let output = collect(lookup_stage.clone(), context.task_ctx())
                            .await
                            .unwrap();
                        assert!(output.iter().all(|batch| batch.num_rows() <= 62));
                        let actual = concat_batches(&join.schema(), output.iter()).unwrap();
                        assert_eq!(actual, expected(&input, &side, composite, false, join.schema()),
                                "seed={seed} composite={composite} sparse={sparse} source_batch={source_batch_rows}");
                        input_count += input.len() as u64;
                        output_count += actual.num_rows() as u64;
                        // The build side is consumed exactly once, including an initial empty
                        // probe. Its hash table owns reservation credit until the plan closes.
                        let cache = broker.reserved();
                        assert!(cache > 0);
                        assert_eq!(*retained.get_or_insert(cache), cache);
                        assert_eq!(
                            join.metrics()
                                .unwrap()
                                .sum_by_name("build_input_rows")
                                .unwrap()
                                .as_usize(),
                            side.len()
                        );
                    }
                    let stage = lookup_stage.downcast_ref::<IdentifiedExec>().unwrap();
                    assert_eq!(stage.input_rows(), input_count);
                    assert_eq!(stage.output_rows(), output_count);
                    // This would fail if rebuilding the join after each incoming Arrow batch.
                    assert!(source.execute(0, context.task_ctx()).is_err());
                    drop(lookup_stage);
                    drop(join);
                    drop(source);
                    assert_eq!(broker.reserved(), 0);
                }
            }
        }
    }
}

#[tokio::test]
async fn empty_snapshot_preserves_inner_empty_output() {
    let probe = Arc::new(ReusableInputExec::new(schema(true)));
    let join = create(snapshot(&[], 1), probe.clone(), &[(0, 0)], false).unwrap();
    let (context, broker) = context(1 << 20, 4);
    let input = rows(1, 31, false);
    for _ in 0..2 {
        probe.replace_batch(batch(&input, true)).unwrap();
        let result = collect(join.clone(), context.task_ctx()).await.unwrap();
        let actual = concat_batches(&join.schema(), result.iter()).unwrap();
        assert_eq!(actual, expected(&input, &[], false, false, join.schema()));
    }
    drop(join);
    assert_eq!(broker.reserved(), 0);
}

#[tokio::test]
async fn large_duplicate_fanout_stays_in_bounded_batches_and_fuses_with_projection() {
    let side = (0..40_003)
        .map(|id| Row {
            key: Some(1),
            text: Some(format!("v{id}")),
            id,
        })
        .collect::<Vec<_>>();
    let probe = Arc::new(ReusableInputExec::new(schema(true)));
    let input = vec![Row {
        key: Some(1),
        text: None,
        id: 3,
    }];
    probe.replace_batch(batch(&input, true)).unwrap();
    let join = create(snapshot(&side, 67), probe.clone(), &[(0, 0)], false).unwrap();
    let projected = Arc::new(
        ProjectionExec::try_new(
            vec![(
                Arc::new(Column::new("id", 5)) as Arc<dyn PhysicalExpr>,
                "matched_id".into(),
            )],
            join.clone(),
        )
        .unwrap(),
    );
    let (context, broker) = context(64 << 20, 257);
    let outputs = collect(projected.clone(), context.task_ctx())
        .await
        .unwrap();
    assert!(outputs.len() > 100);
    assert!(outputs.iter().all(|batch| batch.num_rows() <= 514));
    let result = concat_batches(&projected.schema(), outputs.iter()).unwrap();
    assert_eq!(
        result
            .column(0)
            .as_any()
            .downcast_ref::<Int64Array>()
            .unwrap()
            .values()
            .as_ref(),
        &(0..40_003i64).collect::<Vec<_>>()
    );
    drop(projected);
    drop(join);
    assert_eq!(broker.reserved(), 0);
}

#[tokio::test]
async fn denied_build_releases_memory_on_close_and_does_not_reload_the_source() {
    let probe = Arc::new(ReusableInputExec::new(schema(true)));
    probe
        .replace_batch(batch(&rows(1, 1, false), true))
        .unwrap();
    let source = snapshot(&rows(1, 1000, false), 17);
    let join = create(source.clone(), probe, &[(0, 0)], false).unwrap();
    let (context, broker) = context(4096, 32);
    for _ in 0..2 {
        let error = collect(join.clone(), context.task_ctx()).await.unwrap_err();
        assert!(error.to_string().contains("Resources exhausted"), "{error}");
    }
    assert!(source.execute(0, context.task_ctx()).is_err());
    drop(join);
    drop(source);
    assert_eq!(broker.reserved(), 0);
}

#[test]
fn rejects_missing_or_metadata_keys_before_consuming_snapshot() {
    let probe = Arc::new(ReusableInputExec::new(schema(true)));
    for keys in [vec![], vec![(3, 0)], vec![(0, 3)], vec![(0, 1)]] {
        let source = snapshot(&rows(1, 1, false), 1);
        assert!(create(source.clone(), probe.clone(), &keys, false).is_err());
        assert!(source.execute(0, SessionContext::new().task_ctx()).is_ok());
    }
}

#[test]
fn outer_lookup_is_rejected_until_unmatched_arrival_order_is_preserved() {
    let probe = Arc::new(ReusableInputExec::new(schema(true)));
    let source = snapshot(&rows(1, 1, false), 1);
    let error = create(source.clone(), probe, &[(0, 0)], true).unwrap_err();
    assert!(error.to_string().contains("unmatched-row arrival-order"));
    assert!(source.execute(0, SessionContext::new().task_ctx()).is_ok());
}

#[tokio::test]
async fn repeated_datafusion_execution_retains_metric_descriptors_and_is_not_a_production_binding()
{
    let probe = Arc::new(ReusableInputExec::new(schema(true)));
    let join = create(
        snapshot(&rows(1, 31, false), 17),
        probe.clone(),
        &[(0, 0)],
        false,
    )
    .unwrap();
    let (context, broker) = context(1 << 20, 32);
    let mut first_count = None;
    for invocation in 1..=32 {
        probe
            .replace_batch(batch(&rows(1, 7, false), true))
            .unwrap();
        collect(join.clone(), context.task_ctx()).await.unwrap();
        let count = join.metrics().unwrap().iter().count();
        let first = *first_count.get_or_insert(count);
        assert!(first > 0);
        // DataFusion 55's ExecutionPlanMetricsSet::register appends on every execute. Reusing
        // its build hash table alone therefore does not establish bounded task-lifetime memory.
        // A changed upstream behavior should make this test fail and prompt reassessment.
        assert_eq!(count, first * invocation);
    }
    drop(join);
    assert_eq!(broker.reserved(), 0);
}
