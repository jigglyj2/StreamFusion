// Copyright 2026 StreamFusion Authors
// Licensed under the Apache License, Version 2.0.

use super::*;
use crate::memory_pool::{tests_support::TestBroker, FlinkMemoryPool};
use crate::proto;
use arrow::array::NullArray;
use datafusion::execution::memory_pool::MemoryPool;
use prost::Message;

fn context(broker: &Arc<TestBroker>) -> NativeExecutionContext {
    let pool: Arc<dyn MemoryPool> = Arc::new(FlinkMemoryPool::new(broker.clone(), 16 << 20));
    let plan = proto::NativePlan {
        protocol_version: crate::PLAN_PROTOCOL_VERSION,
        root: Some(proto::Operator {
            plan_node_id: 1,
            operator: Some(proto::operator::Operator::Input(proto::Input::default())),
            ..Default::default()
        }),
    };
    NativeExecutionContext::new(&plan.encode_to_vec(), pool).unwrap()
}

fn input(width: usize) -> (FFI_ArrowArray, FFI_ArrowSchema) {
    let batch = RecordBatch::try_from_iter((0..width).map(|i| {
        (
            format!("null_{i}"),
            Arc::new(NullArray::new(7)) as arrow::array::ArrayRef,
        )
    }))
    .unwrap();
    let data = StructArray::from(batch).to_data();
    (
        FFI_ArrowArray::new(&data),
        FFI_ArrowSchema::try_from(data.data_type()).unwrap(),
    )
}

#[test]
fn input_admission_denial_and_invalid_ordinals_leave_producer_ownership_untouched() {
    let broker = Arc::new(TestBroker::new(16 << 20));
    let context = context(&broker);
    let baseline = broker.reserved();
    let blocker = context.reservation("test exhaust remaining admission");
    blocker.try_grow((16 << 20) - baseline).unwrap();
    let (mut array, mut schema) = input(64);
    assert!(unsafe { import_input(&context, &mut array, &mut schema, 0, 0) }.is_err());
    assert!(!array.is_released());
    assert!(schema.release.is_some());
    drop(blocker);
    assert_eq!(broker.reserved(), baseline);
    assert!(unsafe { import_input(&context, &mut array, &mut schema, 0, usize::MAX) }.is_err());
    assert!(!array.is_released());
    assert!(schema.release.is_some());
    assert_eq!(broker.reserved(), baseline);
    drop(context);
    assert_eq!(broker.reserved(), 0);
}

#[test]
fn bufferless_inputs_reserve_only_ordinal_buffers_across_schema_negotiation_and_reuse() {
    let broker = Arc::new(TestBroker::new(16 << 20));
    let context = context(&broker);
    let (mut array, mut schema) = input(64);
    let (batch, memory) = unsafe { import_input(&context, &mut array, &mut schema, 0, 3) }.unwrap();
    assert!(array.is_released());
    assert!(schema.release.is_none());
    assert_eq!(memory.size(), batch.num_rows() * std::mem::size_of::<i32>());
    assert_eq!(batch.num_columns(), 65);
    assert_eq!(batch.column(0).logical_null_count(), 7);
    assert_eq!(
        batch
            .column(64)
            .as_any()
            .downcast_ref::<Int32Array>()
            .unwrap()
            .values(),
        &[3, 4, 5, 6, 7, 8, 9]
    );
    drop(batch);
    drop(memory);
    let baseline = broker.reserved();
    let (mut array, _schema) = input(64);
    let (batch, memory) =
        unsafe { import_input(&context, &mut array, std::ptr::null_mut(), 0, 0) }.unwrap();
    assert_eq!(memory.size(), batch.num_rows() * std::mem::size_of::<i32>());
    assert_eq!(batch.column(0).logical_null_count(), 7);
    drop(batch);
    drop(memory);
    assert_eq!(broker.reserved(), baseline);
    drop(context);
    assert_eq!(broker.reserved(), 0);
}
