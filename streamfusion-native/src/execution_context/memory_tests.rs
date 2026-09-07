// Copyright 2026 StreamFusion Authors
// Licensed under the Apache License, Version 2.0

use super::*;
use crate::memory_pool::tests_support::TestBroker;
use arrow::array::Int32Array;
use arrow::datatypes::{DataType, Field, Schema};
use arrow::record_batch::RecordBatch;

fn pool(broker: &Arc<TestBroker>) -> Arc<dyn MemoryPool> {
    Arc::new(FlinkMemoryPool::new(broker.clone(), 16 << 20))
}

fn plan(column: u32) -> Vec<u8> {
    proto::NativePlan {
        protocol_version: crate::PLAN_PROTOCOL_VERSION,
        root: Some(proto::Operator {
            plan_node_id: 1,
            metric_name: String::new(),
            clear_record_timestamps: false,
            metric_uid: None,
            operator: Some(proto::operator::Operator::Calc(Box::new(proto::Calc {
                preserve_input_envelope: false,
                input: Some(Box::new(proto::Operator {
                    plan_node_id: 2,
                    metric_name: String::new(),
                    clear_record_timestamps: false,
                    metric_uid: None,
                    operator: Some(proto::operator::Operator::Input(proto::Input::default())),
                })),
                projections: vec![proto::Expression {
                    expression: Some(proto::expression::Expression::InputReference(
                        proto::InputReference {
                            index: column,
                            r#type: None,
                        },
                    )),
                }],
                condition: None,
            }))),
        }),
    }
    .encode_to_vec()
}

fn batch(width: usize) -> RecordBatch {
    RecordBatch::try_new(
        Arc::new(Schema::new(
            (0..width)
                .map(|index| Field::new(format!("field{index}"), DataType::Int32, false))
                .collect::<Vec<_>>(),
        )),
        (0..width)
            .map(|index| Arc::new(Int32Array::from(vec![index as i32])) as _)
            .collect(),
    )
    .unwrap()
}

#[test]
fn decoding_is_admitted_before_protobuf_allocation_and_failure_releases_it() {
    let denied = Arc::new(TestBroker::new(1));
    let result = NativeExecutionContext::new(&[0xff], pool(&denied));
    assert!(matches!(
        result,
        Err(DataFusionError::ResourcesExhausted(_))
    ));
    assert_eq!(denied.reserved(), 0);
    let allowed = Arc::new(TestBroker::new(16 << 20));
    let result = NativeExecutionContext::new(&[0xff], pool(&allowed));
    assert!(matches!(result, Err(DataFusionError::Plan(_))));
    assert_eq!(allowed.reserved(), 0);
}

#[test]
fn original_metric_names_survive_native_decoding_and_share_plan_lifetime_admission() {
    let mut original = proto::NativePlan::decode(plan(0).as_slice()).unwrap();
    let name = "Calc(select=[résultat])".repeat(4096);
    original.root.as_mut().unwrap().metric_name = name.clone();
    original.root.as_mut().unwrap().metric_uid = Some(String::new());
    let bytes = original.encode_to_vec();
    let denied = Arc::new(TestBroker::new(bytes.len() / 2));
    assert!(NativeExecutionContext::new(&bytes, pool(&denied)).is_err());
    assert_eq!(denied.reserved(), 0);
    let broker = Arc::new(TestBroker::new(16 << 20));
    let context = NativeExecutionContext::new(&bytes, pool(&broker)).unwrap();
    assert_eq!(context.plan().root.as_ref().unwrap().metric_name, name);
    assert_eq!(
        context.plan().root.as_ref().unwrap().metric_uid,
        Some(String::new())
    );
    assert!(broker.reserved() >= name.len());
    drop(context);
    assert_eq!(broker.reserved(), 0);
}

#[test]
fn failed_lowering_rolls_back_admission_and_a_corrected_input_can_retry() {
    let broker = Arc::new(TestBroker::new(16 << 20));
    let context = NativeExecutionContext::new(&plan(1), pool(&broker)).unwrap();
    let baseline = broker.reserved();
    for _ in 0..4 {
        let result = context.execute_plan(vec![batch(1)], |_| Ok(()));
        assert!(result.is_err());
        assert_eq!(broker.reserved(), baseline);
    }
    let first = context.execute_plan(vec![batch(2)], Ok).unwrap();
    let retained = broker.reserved();
    assert!(retained > baseline);
    let second = context.execute_plan(vec![batch(2)], Ok).unwrap();
    assert!(Arc::ptr_eq(&first, &second));
    assert_eq!(broker.reserved(), retained);
    drop(first);
    drop(second);
    drop(context);
    assert_eq!(broker.reserved(), 0);
}

#[test]
fn retained_schema_accounts_nested_fields_and_metadata_and_rejects_growth_transactionally() {
    let broker = Arc::new(TestBroker::new(16 << 20));
    let context = NativeExecutionContext::new(&plan(0), pool(&broker)).unwrap();
    let baseline = broker.reserved();
    let nested = Field::new("x".repeat(32 * 1024), DataType::Utf8, true);
    let schema = Arc::new(Schema::new_with_metadata(
        vec![Field::new(
            "nested",
            DataType::Struct(vec![Arc::new(nested)].into()),
            true,
        )],
        HashMap::from([("metadata".into(), "m".repeat(64 * 1024))]),
    ));
    let pressure = context.reservation("competing input schema consumer");
    pressure
        .try_grow((16 << 20) - broker.reserved() - 4096)
        .unwrap();
    let under_pressure = broker.reserved();
    assert!(context.remember_input_schema(0, schema.clone()).is_err());
    assert!(context.input_schema(0).is_err());
    assert_eq!(broker.reserved(), under_pressure);
    drop(pressure);
    context.remember_input_schema(0, schema.clone()).unwrap();
    let cached = broker.reserved();
    assert!(cached >= baseline + 192 * 1024);
    context.remember_input_schema(0, schema).unwrap();
    assert_eq!(broker.reserved(), cached);
    assert!(context.remember_input_schema(0, batch(1).schema()).is_err());
    assert_eq!(broker.reserved(), cached);
    drop(context);
    assert_eq!(broker.reserved(), 0);
}
