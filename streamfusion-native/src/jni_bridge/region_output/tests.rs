// Copyright 2026 StreamFusion Authors
// Licensed under the Apache License, Version 2.0.
use super::*;
use crate::{
    memory_pool::{tests_support::TestBroker, FlinkMemoryPool},
    proto,
};
use arrow::array::{Array, ArrayRef, Int32Array, Int64Array, Int8Array, StructArray};
use arrow::record_batch::RecordBatch;
use prost::Message;

fn context() -> (Arc<NativeExecutionContext>, Arc<TestBroker>) {
    let mut plan = proto::NativeRegionPlan::decode(
        include_bytes!(concat!(
            env!("CARGO_MANIFEST_DIR"),
            "/../streamfusion-proto/src/test/resources/native-region-v1.pb"
        ))
        .as_slice(),
    )
    .unwrap();
    plan.input_count = 2;
    plan.stages[0].operator.as_mut().unwrap().operator =
        Some(proto::operator::Operator::Union(proto::Union {
            inputs: (0..2)
                .map(|index| proto::Operator {
                    operator: Some(proto::operator::Operator::Input(proto::Input {
                        input_index: index,
                        ..Default::default()
                    })),
                    ..Default::default()
                })
                .collect(),
        }));
    plan.stages[0]
        .inputs
        .push(proto::NativeRegionInputReference {
            source: Some(proto::native_region_input_reference::Source::ExternalInput(
                1,
            )),
        });
    let proto::operator::Operator::Calc(calc) = plan.stages[1]
        .operator
        .as_mut()
        .unwrap()
        .operator
        .as_mut()
        .unwrap()
    else {
        unreachable!()
    };
    calc.projections = vec![proto::Expression {
        expression: Some(proto::expression::Expression::LongLiteral(
            proto::LongLiteral { value: 73 },
        )),
    }];
    let broker = Arc::new(TestBroker::new(16 << 20));
    let pool = Arc::new(FlinkMemoryPool::new(broker.clone(), 16 << 20));
    (
        Arc::new(NativeExecutionContext::new_region(&plan.encode_to_vec(), pool).unwrap()),
        broker,
    )
}
fn input(value: i32) -> (FFI_ArrowArray, FFI_ArrowSchema) {
    let batch = RecordBatch::try_from_iter(vec![
        (
            "n",
            Arc::new(Int32Array::from(vec![Some(-99), Some(value), None])) as ArrayRef,
        ),
        (
            "__streamfusion_row_kind",
            Arc::new(Int8Array::from(vec![0, 1, 2])) as ArrayRef,
        ),
        (
            "__streamfusion_stream_record_timestamp",
            Arc::new(Int64Array::from(vec![Some(-99), Some(123), None])) as ArrayRef,
        ),
    ])
    .unwrap()
    .slice(1, 2);
    let data = StructArray::from(batch).to_data();
    (
        FFI_ArrowArray::new(&data),
        FFI_ArrowSchema::try_from(data.data_type()).unwrap(),
    )
}
fn open(context: &Arc<NativeExecutionContext>, negotiate: bool) -> i64 {
    let (mut left, mut left_schema) = input(11);
    let (mut right, mut right_schema) = input(29);
    let arrays = [&mut left as *mut _ as i64, &mut right as *mut _ as i64];
    let schemas = if negotiate {
        [
            &mut left_schema as *mut _ as i64,
            &mut right_schema as *mut _ as i64,
        ]
    } else {
        [0, 0]
    };
    let (batches, memory) = unsafe {
        super::super::plan_stream::import_inputs(
            context,
            &arrays,
            &schemas,
            context.reservation("region test edge"),
            None,
        )
    }
    .unwrap();
    assert!(left.is_released() && right.is_released());
    let stream = context.start_region(batches).unwrap();
    register(Output::new(context.clone(), stream, memory)).unwrap()
}
#[test]
fn c_data_outputs_negotiate_each_schema_once_preserve_slices_and_outlive_the_stream() {
    let (context, broker) = context();
    for invocation in 0..3 {
        let handle = open(&context, invocation == 0);
        let mut schemas = [None, None];
        let mut output = [vec![], vec![]];
        loop {
            let mut array = FFI_ArrowArray::empty();
            let mut schema = FFI_ArrowSchema::empty();
            let port = unsafe { next(handle, &mut array, &mut schema) }.unwrap();
            if port < 0 {
                assert!(array.is_released());
                assert!(schema.release.is_none());
                break;
            }
            let port = port as usize;
            if schemas[port].is_none() {
                assert!(schema.release.is_some());
                schemas[port] = Some(arrow::datatypes::Schema::try_from(&schema).unwrap());
            } else {
                assert!(schema.release.is_none());
            }
            let data = unsafe {
                arrow::ffi::from_ffi_and_data_type(
                    array,
                    arrow::datatypes::DataType::Struct(
                        schemas[port].as_ref().unwrap().fields().clone(),
                    ),
                )
            }
            .unwrap();
            output[port].push(RecordBatch::from(StructArray::from(data)));
        }
        assert_ne!(schemas[0], schemas[1]);
        close(handle).unwrap();
        close(handle).unwrap();
        context.require_idle().unwrap();
        assert_eq!(output[0].len(), 2);
        for (batch, value) in output[0].iter().zip([11, 29]) {
            assert_eq!(
                batch.column(0).as_ref(),
                &Int32Array::from(vec![Some(value), None])
            );
        }
        assert_eq!(output[1].len(), 2);
        for batch in &output[1] {
            assert_eq!(batch.column(0).as_ref(), &Int64Array::from(vec![73, 73]));
        }
        for (left, right) in output[0].iter().zip(&output[1]) {
            assert_eq!(left.column(1), right.column(1));
            assert_eq!(left.column(2), right.column(2));
            assert_eq!(left.column(3), right.column(3));
            assert_eq!(left.column(3).as_ref(), &Int32Array::from(vec![-1, -1]));
        }
    }
    drop(context);
    assert_eq!(broker.reserved(), 0);
}
#[test]
fn cancelled_or_invalid_c_data_output_closes_the_invocation_before_reuse() {
    for invalid in [false, true] {
        let (context, broker) = context();
        let handle = open(&context, true);
        if invalid {
            let mut schema = FFI_ArrowSchema::empty();
            assert!(unsafe { next(handle, std::ptr::null_mut(), &mut schema) }.is_err());
            assert!(schema.release.is_none());
        }
        close(handle).unwrap();
        assert!(context.require_idle().is_err());
        assert!(unsafe { next(handle, std::ptr::null_mut(), std::ptr::null_mut()) }.is_err());
        drop(context);
        assert_eq!(broker.reserved(), 0);
    }
}

#[test]
fn output_handle_keeps_the_context_alive_and_exported_arrays_survive_close() {
    let (context, broker) = context();
    let handle = open(&context, true);
    drop(context);
    let mut array = FFI_ArrowArray::empty();
    let mut schema = FFI_ArrowSchema::empty();
    assert!(unsafe { next(handle, &mut array, &mut schema) }.unwrap() >= 0);
    close(handle).unwrap();
    let data = unsafe { arrow::ffi::from_ffi(array, &schema) }.unwrap();
    let batch = RecordBatch::from(StructArray::from(data));
    assert_eq!(batch.num_rows(), 2);
    assert_eq!(batch.column(3).as_ref(), &Int32Array::from(vec![-1, -1]));
    drop(batch);
    drop(schema);
    assert_eq!(broker.reserved(), 0);
}
