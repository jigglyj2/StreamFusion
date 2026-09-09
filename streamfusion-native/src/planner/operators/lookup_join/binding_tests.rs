// Copyright 2026 StreamFusion Authors
// Licensed under the Apache License, Version 2.0.

use std::sync::Arc;

use arrow::compute::concat_batches;
use arrow::datatypes::{DataType, SchemaRef};
use datafusion::physical_plan::RecordBatchStream;
use futures::StreamExt;
use prost::Message;

use super::{test_support::*, LookupJoinFactory, LookupTable};
use crate::execution_context::NativeExecutionContext;
use crate::planner::persistent::{control::ControlEvent, PersistentOperatorFactory};
use crate::proto;

fn payload_schema(schema: SchemaRef) -> proto::Schema {
    proto::Schema {
        fields: schema
            .fields()
            .iter()
            .map(|field| proto::Field {
                name: field.name().clone(),
                r#type: Some(proto::LogicalType {
                    nullable: field.is_nullable(),
                    r#type: Some(match field.data_type() {
                        DataType::Int64 => proto::logical_type::Type::Bigint(proto::EmptyType {}),
                        DataType::Utf8 => proto::logical_type::Type::Varchar(proto::EmptyType {}),
                        _ => unreachable!(),
                    }),
                }),
            })
            .collect(),
    }
}

fn lookup(composite: bool) -> proto::Operator {
    let payload = payload_schema(schema(false));
    let output = proto::Schema {
        fields: payload
            .fields
            .iter()
            .chain(&payload.fields)
            .cloned()
            .collect(),
    };
    proto::Operator {
        plan_node_id: 2,
        operator: Some(proto::operator::Operator::LookupJoin(Box::new(
            proto::LookupJoin {
                input: Some(Box::new(probe_calc())),
                input_schema: Some(payload.clone()),
                side_schema: Some(payload),
                output_schema: Some(output),
                probe_keys: if composite { vec![0, 1] } else { vec![0] },
                side_keys: if composite { vec![0, 1] } else { vec![0] },
                kind: proto::LookupJoinKind::Inner as i32,
            },
        ))),
        ..Default::default()
    }
}
fn probe_calc() -> proto::Operator {
    let mut node = calc(proto::Operator {
        plan_node_id: 1,
        operator: Some(proto::operator::Operator::Input(proto::Input {
            input_index: 0,
            schema: None,
        })),
        ..Default::default()
    });
    node.plan_node_id = 4;
    let Some(proto::operator::Operator::Calc(spec)) = &mut node.operator else {
        unreachable!()
    };
    spec.projections.truncate(3);
    node
}
fn serialized(root: proto::Operator, version: u32) -> Vec<u8> {
    proto::NativePlan {
        protocol_version: version,
        root: Some(root),
    }
    .encode_to_vec()
}
fn calc(input: proto::Operator) -> proto::Operator {
    proto::Operator {
        plan_node_id: 3,
        operator: Some(proto::operator::Operator::Calc(Box::new(proto::Calc {
            input: Some(Box::new(input)),
            preserve_input_envelope: true,
            projections: (0..6)
                .map(|index| proto::Expression {
                    expression: Some(proto::expression::Expression::InputReference(
                        proto::InputReference {
                            index,
                            r#type: None,
                        },
                    )),
                })
                .collect(),
            condition: None,
        }))),
        ..Default::default()
    }
}

#[test]
fn lookup_binding_composes_with_calc_and_reuses_cache_across_controls() {
    for composite in [false, true] {
        for seed in [1, 7, 31] {
            let (session, broker) = context(64 << 20, 31);
            let pool = session.runtime_env().memory_pool.clone();
            let side = rows(seed, 217, false);
            let table = LookupTable::new(
                batch(&side, false),
                if composite { vec![0, 1] } else { vec![0] },
                pool.clone(),
            )
            .unwrap();
            let node = lookup(composite);
            let factory = Arc::new(LookupJoinFactory::new(&node, table.clone()).unwrap());
            let mut native = NativeExecutionContext::new(&serialized(calc(node), 3), pool).unwrap();
            native.bind_persistent(vec![(2, factory.clone())]).unwrap();
            let native = Arc::new(native);
            let mut input_rows = 0i64;
            let mut output_rows = 0i64;
            let mut baseline = None;
            for size in [1, 37, 129, 0, 17, 37] {
                let probe = rows(seed + 3, size, false);
                let mut stream = native.start(vec![batch(&probe, true)]).unwrap();
                let output_schema = stream.schema();
                let outputs = native.runtime().block_on(async {
                    let mut outputs = Vec::new();
                    while let Some(output) = stream.next().await {
                        outputs.push(output.unwrap());
                    }
                    outputs
                });
                let output = concat_batches(&output_schema, &outputs).unwrap();
                assert_eq!(
                    output,
                    expected(&probe, &side, composite, false, output_schema)
                );
                input_rows += size as i64;
                output_rows += output.num_rows() as i64;
                drop(output);
                drop(outputs);
                drop(stream);
                native.require_idle().unwrap();
                assert_eq!(
                    native.metric_snapshot().unwrap(),
                    vec![
                        3,
                        output_rows,
                        output_rows,
                        2,
                        input_rows,
                        output_rows,
                        4,
                        input_rows,
                        input_rows,
                        1,
                        0,
                        input_rows
                    ]
                );
                // Control traversal must not invalidate the cache, produce rows or rebuild it.
                for event in [
                    ControlEvent::Watermark(100),
                    ControlEvent::BeforeCheckpoint(4),
                    ControlEvent::EndInput,
                ] {
                    let mut control = native
                        .start_control(vec![batch(&[], true)], &[(2, event)])
                        .unwrap();
                    native.runtime().block_on(async {
                        while let Some(output) = control.next().await {
                            assert_eq!(output.unwrap().num_rows(), 0);
                        }
                    });
                }
                assert_eq!(
                    broker.reserved(),
                    *baseline.get_or_insert_with(|| broker.reserved())
                );
            }
            // CSV snapshot is task-local, not checkpointed keyed state.
            assert!(factory.snapshot(0).is_err());
            assert!(!factory.supports_control(ControlEvent::ProcessingTime(0)));
            drop(native);
            drop(factory);
            drop(table);
            assert_eq!(broker.reserved(), 0);
        }
    }
}

#[test]
fn lookup_wire_contract_rejects_old_protocol_and_missing_snapshot() {
    for version in [1, 2] {
        let error = crate::decode_plan(&serialized(lookup(false), version)).unwrap_err();
        assert!(error.to_string().contains("protocol version 3"));
    }
    let (session, broker) = context(64 << 20, 31);
    let native = Arc::new(
        NativeExecutionContext::new(
            &serialized(lookup(false), 3),
            session.runtime_env().memory_pool.clone(),
        )
        .unwrap(),
    );
    let error = native
        .start(vec![batch(&rows(1, 1, false), true)])
        .err()
        .unwrap();
    assert!(error
        .to_string()
        .contains("task-open Arrow snapshot binding"));
    drop(native);
    assert_eq!(broker.reserved(), 0);
}

#[test]
fn lookup_factory_rejects_mismatched_resource_and_unsupported_semantics() {
    let (session, broker) = context(64 << 20, 31);
    let table = LookupTable::new(
        batch(&rows(1, 17, false), false),
        vec![0],
        session.runtime_env().memory_pool.clone(),
    )
    .unwrap();
    let baseline = broker.reserved();
    for case in 0..8 {
        let mut node = lookup(false);
        let Some(proto::operator::Operator::LookupJoin(spec)) = &mut node.operator else {
            unreachable!()
        };
        match case {
            0 => spec.kind = 0,
            1 => spec.kind = 99,
            2 => spec.side_keys = vec![1],
            3 => spec.probe_keys = vec![3],
            4 => spec.probe_keys = vec![1],
            5 => spec.side_schema = None,
            6 => spec
                .output_schema
                .as_mut()
                .unwrap()
                .fields
                .pop()
                .map(|_| ())
                .unwrap(),
            7 => node.plan_node_id = 0,
            _ => unreachable!(),
        }
        assert!(
            LookupJoinFactory::new(&node, table.clone()).is_err(),
            "case {case}"
        );
        assert_eq!(broker.reserved(), baseline);
    }
    drop(table);
    assert_eq!(broker.reserved(), 0);
}
