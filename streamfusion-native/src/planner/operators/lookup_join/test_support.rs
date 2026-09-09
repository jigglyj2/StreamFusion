// Copyright 2026 StreamFusion Authors
// Licensed under the Apache License, Version 2.0

use super::experiment::LookupSnapshotExec;
use crate::planner::operators::envelope;
use arrow::array::{ArrayRef, Int32Array, Int64Array, Int8Array, RecordBatch, StringArray};
use arrow::datatypes::{DataType, Field, Schema, SchemaRef};
use datafusion::execution::context::{SessionConfig, SessionContext};
use datafusion::execution::runtime_env::RuntimeEnvBuilder;
use datafusion::physical_plan::memory::MemoryStream;
use std::sync::Arc;

use crate::memory_pool::{
    arrow_lease::Registry, tests_support::TestBroker, FlinkMemoryPool, MemoryReservationBroker,
};

#[derive(Debug)]
struct RegisteredBroker {
    inner: Arc<TestBroker>,
    registry: Arc<Registry>,
}
impl MemoryReservationBroker for RegisteredBroker {
    fn lease_registry(&self) -> Option<Arc<Registry>> {
        Some(self.registry.clone())
    }
    fn try_reserve(&self, bytes: usize) -> datafusion::error::Result<bool> {
        self.inner.try_reserve(bytes)
    }
    fn release(&self, bytes: usize) -> datafusion::error::Result<()> {
        self.inner.release(bytes)
    }
    fn available(&self) -> datafusion::error::Result<Option<usize>> {
        self.inner.available()
    }
}

#[derive(Clone, Debug)]
pub(super) struct Row {
    pub(super) key: Option<i64>,
    pub(super) text: Option<String>,
    pub(super) id: i64,
}

pub(super) fn schema(probe: bool) -> SchemaRef {
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

pub(super) fn batch(rows: &[Row], probe: bool) -> RecordBatch {
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

pub(super) fn snapshot(rows: &[Row], batch_size: usize) -> Arc<LookupSnapshotExec> {
    let batches = rows
        .chunks(batch_size)
        .map(|rows| batch(rows, false))
        .collect();
    LookupSnapshotExec::new(Box::pin(
        MemoryStream::try_new(batches, schema(false), None).unwrap(),
    ))
}

pub(super) fn context(limit: usize, batch_size: usize) -> (SessionContext, Arc<TestBroker>) {
    let broker = Arc::new(TestBroker::new(limit));
    let registered = Arc::new(RegisteredBroker {
        inner: broker.clone(),
        registry: Arc::default(),
    });
    let pool = Arc::new(FlinkMemoryPool::new(registered, limit));
    let env = RuntimeEnvBuilder::new()
        .with_memory_pool(pool)
        .build_arc()
        .unwrap();
    (
        SessionContext::new_with_config_rt(SessionConfig::new().with_batch_size(batch_size), env),
        broker,
    )
}

pub(super) fn rows(seed: i64, n: usize, sparse: bool) -> Vec<Row> {
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

pub(super) fn expected(
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
