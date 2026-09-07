// Copyright 2026 StreamFusion Authors
// Licensed under the Apache License, Version 2.0.

use super::*;
use crate::allocation_test_support::measure;
use crate::execution_context::allocation_tests::wide_plan;
use crate::proto::{self, operator::Operator as Kind};
use prost::Message;

fn node(kind: Kind) -> proto::Operator {
    proto::Operator {
        plan_node_id: 83,
        metric_name: "physical stage 名".repeat(512),
        clear_record_timestamps: false,
        metric_uid: Some("original-stage-uid".into()),
        operator: Some(kind),
    }
}

fn assert_admitted(source: &proto::Operator, expected: &proto::Operator) {
    let (budget, scan) = measure(|| admission(source).unwrap());
    assert_eq!(scan.peak, 0, "borrowed sizing must not allocate");
    let ((copied, encoded), observed) = measure(|| {
        let copied = proto::NativePlan {
            protocol_version: 2,
            root: Some(without_children(source)),
        };
        let encoded = copied.encode_to_vec();
        (copied, encoded)
    });
    assert!(observed.peak <= budget, "{observed:?} budget={budget}");
    assert_eq!(copied.root.as_ref(), Some(expected));
    let decoded = proto::NativePlan::decode(encoded.as_slice()).unwrap();
    assert_eq!(decoded, copied);
}

#[test]
fn unary_binary_and_variadic_copies_preserve_only_their_own_configuration() {
    let (bytes, _) = wide_plan(256, true, 8);
    let child = proto::NativePlan::decode(bytes.as_slice())
        .unwrap()
        .root
        .unwrap();
    let configurations = [
        Kind::Deduplicate(Box::new(proto::Deduplicate {
            key_indices: vec![0, 1, 2],
            order_index: 3,
            keep_last: true,
            generate_insert: true,
            input_changelog: true,
            generate_update_before: true,
            processing_time: true,
            input: None,
        })),
        Kind::RegularJoin(Box::new(proto::RegularJoin {
            left_key_indices: vec![0, 2],
            right_key_indices: vec![2, 0],
            filter_nulls: vec![true, false],
            join_type: proto::RegularJoinType::Full as i32,
            left_state_ttl_millis: 8,
            right_state_ttl_millis: 9,
            bounded_final_output: true,
            ..Default::default()
        })),
        Kind::Union(proto::Union { inputs: vec![] }),
    ];
    for kind in configurations {
        let expected = node(kind);
        let mut source = expected.clone();
        match source.operator.as_mut().unwrap() {
            Kind::Deduplicate(value) => value.input = Some(Box::new(child.clone())),
            Kind::RegularJoin(value) => {
                value.left_input = Some(Box::new(child.clone()));
                value.right_input = Some(Box::new(child.clone()));
            }
            Kind::Union(value) => value.inputs = vec![child.clone(); 16],
            _ => unreachable!(),
        }
        let original = source.encode_to_vec();
        assert_admitted(&source, &expected);
        assert_eq!(admission(&source).unwrap(), admission(&expected).unwrap());
        assert_eq!(
            source.encode_to_vec(),
            original,
            "shared children are unchanged"
        );
    }
}

#[test]
fn schemas_expressions_and_large_payloads_are_admitted_without_traversing_neighbors() {
    for width in [1, 64, 512, 4096] {
        for nested in [false, true] {
            let (bytes, _) = wide_plan(width, nested, 3);
            let tree = proto::NativePlan::decode(bytes.as_slice()).unwrap();
            let mut cursor = tree.root.as_ref().unwrap();
            loop {
                let mut expected = cursor.clone();
                let next = match expected.operator.as_mut().unwrap() {
                    Kind::Calc(calc) => {
                        calc.input = None;
                        let Some(Kind::Calc(source)) = cursor.operator.as_ref() else {
                            unreachable!()
                        };
                        source.input.as_deref()
                    }
                    Kind::Input(_) => None,
                    _ => unreachable!(),
                };
                assert_admitted(cursor, &expected);
                match next {
                    Some(next) => cursor = next,
                    None => break,
                }
            }
        }
    }
    let values = node(Kind::Values(proto::Values {
        schema: None,
        rows: vec![
            proto::ValuesRow {
                values: vec![
                    proto::Expression {
                        expression: Some(proto::expression::Expression::StringLiteral(
                            proto::StringLiteral {
                                value: "payload 名".repeat(16384),
                                ..Default::default()
                            }
                        )),
                    };
                    8
                ],
            };
            4
        ],
    }));
    assert_admitted(&values, &values);
}

#[test]
fn child_depth_does_not_change_a_state_owners_copy_or_admission() {
    let expected = node(Kind::Deduplicate(Box::new(proto::Deduplicate {
        key_indices: (0..1024).collect(),
        order_index: 1024,
        ..Default::default()
    })));
    let budget = admission(&expected).unwrap();
    let mut copies = None;
    for depth in [1, 8, 32] {
        let (bytes, _) = wide_plan(128, true, depth);
        let child = proto::NativePlan::decode(bytes.as_slice())
            .unwrap()
            .root
            .unwrap();
        let mut source = expected.clone();
        let Some(Kind::Deduplicate(config)) = source.operator.as_mut() else {
            unreachable!()
        };
        config.input = Some(Box::new(child));
        assert_eq!(admission(&source).unwrap(), budget);
        let (copy, observed) = measure(|| without_children(&source));
        assert_eq!(copy, expected);
        if let Some((live, peak)) = copies {
            assert_eq!((observed.live, observed.peak), (live, peak));
        }
        copies = Some((observed.live, observed.peak));
    }
}

#[test]
fn checked_size_arithmetic_rejects_overflow() {
    let mut total = usize::MAX;
    assert!(add(&mut total, 1).is_err());
    assert!(add_payload(&mut 0, usize::MAX).is_err());
}

#[test]
fn installing_more_state_owners_has_linear_retained_configuration_cost() {
    use crate::execution_context::NativeExecutionContext;
    use crate::memory_pool::{tests_support::TestBroker, HostMemoryReservation};
    use std::sync::Arc;

    let mut costs = Vec::new();
    for depth in [1, 8, 32] {
        let mut root = proto::Operator {
            plan_node_id: 1,
            operator: Some(Kind::Input(proto::Input::default())),
            ..Default::default()
        };
        let mut bindings = Vec::new();
        for id in 2..=depth + 1 {
            root = proto::Operator {
                plan_node_id: id,
                metric_name: "stage".repeat(1024),
                operator: Some(Kind::Deduplicate(Box::new(proto::Deduplicate {
                    input: Some(Box::new(root)),
                    key_indices: vec![0],
                    processing_time: true,
                    keep_last: false,
                    ..Default::default()
                }))),
                ..Default::default()
            };
            bindings.push(proto::NativeStateBinding {
                plan_node_id: id,
                max_parallelism: 1,
                first_key_group: 0,
                last_key_group: 0,
                backend: Some(proto::native_state_binding::Backend::Memory(
                    proto::NativeMemoryState::default(),
                )),
            });
        }
        let bytes = proto::NativePlan {
            protocol_version: 2,
            root: Some(root),
        }
        .encode_to_vec();
        let options = proto::NativeStateBindings {
            protocol_version: 1,
            bindings,
        }
        .encode_to_vec();
        let broker = Arc::new(TestBroker::new(64 << 20));
        let memory = HostMemoryReservation::new(broker.clone(), "state copy scaling");
        let mut context =
            NativeExecutionContext::new(&bytes, memory.datafusion_pool(64 << 20)).unwrap();
        let before = broker.reserved();
        let (_, observed) = measure(|| context.install_state(&options, memory).unwrap());
        let additional = broker.reserved() - before;
        assert!(
            observed.live as usize <= additional,
            "depth={depth} {observed:?} credit={additional}"
        );
        costs.push(additional);
        drop(context);
        assert_eq!(broker.reserved(), 0);
    }
    assert!(costs[1] <= costs[0] * 8, "{costs:?}");
    assert!(costs[2] <= costs[1] * 4, "{costs:?}");
}
