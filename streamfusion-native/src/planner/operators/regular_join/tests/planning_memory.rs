// Copyright 2026 StreamFusion Authors
// Licensed under the Apache License, Version 2.0.

use super::*;
use crate::allocation_test_support::measure;

#[test]
fn join_constructor_admits_wide_and_nested_configuration_and_releases_it() {
    for width in [1, 64, 512] {
        for nested in [false, true] {
            let mut native =
                proto::NativePlan::decode(plan(proto::RegularJoinType::Full).as_slice()).unwrap();
            let Some(proto::operator::Operator::RegularJoin(join)) =
                native.root.as_mut().unwrap().operator.as_mut()
            else {
                unreachable!()
            };
            let fields = (0..width)
                .map(|i| proto::Field {
                    name: format!("field_{i}_名"),
                    r#type: Some(proto::LogicalType {
                        nullable: true,
                        r#type: Some(proto::logical_type::Type::Varchar(proto::EmptyType {})),
                    }),
                })
                .collect::<Vec<_>>();
            let schema = proto::Schema {
                fields: if nested {
                    vec![proto::Field {
                        name: "nested".into(),
                        r#type: Some(proto::LogicalType {
                            nullable: true,
                            r#type: Some(proto::logical_type::Type::Row(proto::RowType {
                                fields: fields
                                    .into_iter()
                                    .map(|f| proto::RowField {
                                        name: f.name,
                                        r#type: f.r#type,
                                    })
                                    .collect(),
                            })),
                        }),
                    }]
                } else {
                    fields
                },
            };
            join.left_schema = Some(schema.clone());
            join.right_schema = Some(schema);
            let bytes = native.encode_to_vec();
            let broker = Arc::new(TestBroker::new(64 << 20));
            let memory = HostMemoryReservation::new(broker.clone(), "join constructor test");
            let (mut join, observed) =
                measure(|| RegularJoinProcessor::new(&bytes, 128, 0, 127, memory).unwrap());
            assert!(
                observed.peak <= broker.reserved(),
                "width={width} nested={nested} {observed:?} credit={}",
                broker.reserved()
            );
            let credit = broker.reserved();
            join.scratch_reservation.resize(0).unwrap();
            assert_eq!(
                broker.reserved(),
                credit,
                "plan is independent of batch scratch"
            );
            drop(join);
            assert_eq!(broker.reserved(), 0);
        }
    }
}

#[test]
fn denied_join_plan_admission_releases_partially_opened_state() {
    let bytes = plan(proto::RegularJoinType::Inner);
    // Enough to create the 128 empty key-group maps, but not a decoded plan.
    let broker = Arc::new(TestBroker::new(128 * 128));
    let mut pressure = HostMemoryReservation::new(broker.clone(), "unrelated task allocation");
    let state_bytes =
        128 * std::mem::size_of::<hashbrown::HashMap<Vec<u8>, Vec<u8>, ahash::RandomState>>();
    pressure.resize(128 * 128 - state_bytes).unwrap();
    let before = broker.reserved();
    let result = RegularJoinProcessor::new(
        &bytes,
        128,
        0,
        127,
        HostMemoryReservation::new(broker.clone(), "denied constructor"),
    );
    assert!(
        matches!(&result, Err(DataFusionError::ResourcesExhausted(message)) if message.contains("decoded plan"))
    );
    drop(result);
    assert_eq!(broker.reserved(), before);
    drop(pressure);
    assert_eq!(broker.reserved(), 0);
}

#[test]
fn denied_join_schema_admission_releases_decoded_plan_and_state() {
    let bytes = plan(proto::RegularJoinType::Inner);
    let decoded = crate::execution_context::wire_memory::PlanMemory::scan(&bytes)
        .unwrap()
        .decoded()
        .unwrap();
    let state_bytes =
        128 * std::mem::size_of::<hashbrown::HashMap<Vec<u8>, Vec<u8>, ahash::RandomState>>();
    let broker = Arc::new(TestBroker::new(state_bytes + decoded));
    let result = RegularJoinProcessor::new(
        &bytes,
        128,
        0,
        127,
        HostMemoryReservation::new(broker.clone(), "denied schema constructor"),
    );
    assert!(
        matches!(&result, Err(DataFusionError::ResourcesExhausted(message)) if message.contains("planned schemas and codecs"))
    );
    drop(result);
    assert_eq!(broker.reserved(), 0);
}
