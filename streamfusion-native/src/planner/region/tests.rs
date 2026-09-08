// Copyright 2026 StreamFusion Authors
// Licensed under the Apache License, Version 2.0.

use super::*;
use crate::proto;
use arrow::array::{ArrayRef, Int32Array, Int64Array, Int8Array};
use arrow::datatypes::SchemaRef;
use arrow::record_batch::RecordBatch;
use datafusion::error::{DataFusionError, Result};
use datafusion::execution::{
    memory_pool::{GreedyMemoryPool, MemoryPool},
    runtime_env::RuntimeEnvBuilder,
    TaskContext,
};
use datafusion::physical_plan::{
    stream::RecordBatchStreamAdapter,
    streaming::{PartitionStream, StreamingTableExec},
    ExecutionPlan, SendableRecordBatchStream,
};
use datafusion::prelude::SessionConfig;
use futures::{FutureExt, StreamExt};
use prost::Message;
use std::collections::HashMap;
use std::sync::{
    atomic::{AtomicUsize, Ordering},
    Arc,
};

mod control;
mod dataflow;
mod lifecycle;
mod memory;

fn message() -> proto::NativeRegionPlan {
    proto::NativeRegionPlan::decode(
        include_bytes!(concat!(
            env!("CARGO_MANIFEST_DIR"),
            "/../streamfusion-proto/src/test/resources/native-region-v1.pb"
        ))
        .as_slice(),
    )
    .unwrap()
}
fn batch(index: i32) -> RecordBatch {
    RecordBatch::try_from_iter(vec![
        (
            "n",
            Arc::new(Int32Array::from(vec![Some(index), None])) as ArrayRef,
        ),
        (
            "__streamfusion_owned_timestamp_v1",
            Arc::new(Int64Array::from(vec![Some(index as i64 * 100), None])) as ArrayRef,
        ),
        (
            "__streamfusion_row_kind",
            Arc::new(Int8Array::from(vec![1, 2])) as ArrayRef,
        ),
        (
            "__streamfusion_input_row",
            Arc::new(Int32Array::from(vec![0, 1])) as ArrayRef,
        ),
    ])
    .unwrap()
}
fn task(pool: Arc<dyn MemoryPool>) -> Arc<TaskContext> {
    Arc::new(TaskContext::new(
        None,
        "region-test".into(),
        SessionConfig::new(),
        HashMap::new(),
        HashMap::new(),
        HashMap::new(),
        HashMap::new(),
        Arc::new(
            RuntimeEnvBuilder::new()
                .with_memory_pool(pool)
                .build()
                .unwrap(),
        ),
    ))
}
#[derive(Debug)]
struct Source {
    schema: SchemaRef,
    count: usize,
    executes: Arc<AtomicUsize>,
    mode: u8,
}
impl PartitionStream for Source {
    fn schema(&self) -> &SchemaRef {
        &self.schema
    }
    fn execute(&self, _: Arc<TaskContext>) -> SendableRecordBatchStream {
        self.executes.fetch_add(1, Ordering::Relaxed);
        let mode = self.mode;
        Box::pin(RecordBatchStreamAdapter::new(
            self.schema.clone(),
            futures::stream::iter((0..self.count).map(move |index| {
                if index == 2 {
                    assert!(mode != 2, "injected region source panic");
                    if mode == 1 {
                        return Err(DataFusionError::Execution(
                            "injected region source failure".into(),
                        ));
                    }
                }
                Ok(batch(index as i32))
            })),
        ))
    }
}
fn source(count: usize, mode: u8) -> (Arc<dyn ExecutionPlan>, Arc<AtomicUsize>) {
    let executes = Arc::new(AtomicUsize::new(0));
    let schema = batch(0).schema();
    let source = Arc::new(Source {
        schema: schema.clone(),
        count,
        mode,
        executes: executes.clone(),
    });
    (
        Arc::new(StreamingTableExec::try_new(schema, vec![source], None, [], false, None).unwrap()),
        executes,
    )
}
fn lower(
    message: &proto::NativeRegionPlan,
    count: usize,
    mode: u8,
) -> (Arc<PhysicalRegion>, Arc<TaskContext>, Arc<AtomicUsize>) {
    let pool: Arc<dyn MemoryPool> = Arc::new(GreedyMemoryPool::new(16 << 20));
    let contract = RegionPlan::decode(&message.encode_to_vec(), &pool).unwrap();
    let (input, executes) = source(count, mode);
    let region = PhysicalRegion::lower(contract, vec![input], &[], pool.clone()).unwrap();
    (region, task(pool), executes)
}
fn calc(message: &mut proto::NativeRegionPlan, index: usize) -> &mut proto::Calc {
    let proto::operator::Operator::Calc(calc) = message.stages[index]
        .operator
        .as_mut()
        .unwrap()
        .operator
        .as_mut()
        .unwrap()
    else {
        unreachable!()
    };
    calc
}
fn drain(output: &mut super::output::RegionOutput, ports: usize) -> Result<Vec<Vec<RecordBatch>>> {
    let mut result = vec![Vec::new(); ports];
    // These synchronous test sources must always make progress. A broken bounded fan-out
    // must fail the test promptly, rather than leaving the test executor deadlocked.
    loop {
        match output
            .next()
            .now_or_never()
            .expect("region outputs stopped making cooperative progress")
        {
            Some(Ok(value)) => result[value.port].push(value.batch),
            Some(Err(error)) => return Err(error),
            None => return Ok(result),
        }
    }
}
