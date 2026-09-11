// Copyright 2026 StreamFusion Authors
// Licensed under the Apache License, Version 2.0.

use super::*;
use crate::memory_pool::tests_support::TestBroker;

#[test]
fn input_binding_reuses_schema_and_rejects_unknown_ports_without_retained_credit() {
    let broker = Arc::new(TestBroker::new(16 << 20));
    let plan = proto::NativePlan {
        protocol_version: crate::RECORD_POLICY_PLAN_PROTOCOL_VERSION,
        root: Some(proto::Operator {
            plan_node_id: 1,
            operator: Some(proto::operator::Operator::Input(proto::Input::default())),
            ..Default::default()
        }),
    }
    .encode_to_vec();
    let context = NativeExecutionContext::new(
        &plan,
        Arc::new(FlinkMemoryPool::new(broker.clone(), 16 << 20)),
    )
    .unwrap();
    let base = broker.reserved();
    let exchange = exchange_plan().encode_to_vec();
    for port in [1, 1000] {
        let memory = context.reservation("invalid binding");
        memory.try_grow(65536).unwrap();
        assert!(context
            .bind_exchange_input(port, &exchange, memory)
            .is_err());
        assert_eq!(broker.reserved(), base);
    }
    let memory = context.reservation("malformed binding");
    memory.try_grow(65536).unwrap();
    assert!(context.bind_exchange_input(0, &[255], memory).is_err());
    assert_eq!(broker.reserved(), base);
    let memory = context.reservation("input schema");
    memory.try_grow(65536).unwrap();
    context.bind_exchange_input(0, &exchange, memory).unwrap();
    let schema = context.exchange_input_schema(0).unwrap();
    for _ in 0..20 {
        assert!(Arc::ptr_eq(
            &schema,
            &context.exchange_input_schema(0).unwrap()
        ));
        assert_eq!(broker.reserved(), base + 65536);
    }
    let duplicate = context.reservation("duplicate input schema");
    duplicate.try_grow(65536).unwrap();
    assert!(context
        .bind_exchange_input(0, &exchange, duplicate)
        .is_err());
    assert_eq!(broker.reserved(), base + 65536);
    drop(schema);
    drop(context);
    assert_eq!(broker.reserved(), 0);
}

fn exchange_plan() -> proto::NativeExchangePlan {
    proto::NativeExchangePlan {
        protocol_version: crate::PLAN_PROTOCOL_VERSION,
        schema: Some(proto::Schema {
            fields: vec![
                proto::Field {
                    name: "key".into(),
                    r#type: Some(proto::LogicalType {
                        nullable: false,
                        r#type: Some(proto::logical_type::Type::Integer(proto::EmptyType {})),
                    }),
                },
                proto::Field {
                    name: "__streamfusion_row_kind".into(),
                    r#type: Some(proto::LogicalType {
                        nullable: false,
                        r#type: Some(proto::logical_type::Type::Tinyint(proto::EmptyType {})),
                    }),
                },
            ],
        }),
        distribution: proto::ExchangeDistribution::Hash.into(),
        key_indices: vec![0],
        max_parallelism: 128,
        parallelism: 4,
        preserve_key_groups: true,
        transport_routing_key: false,
        transport: proto::ExchangeTransport::ArrowIpcStream.into(),
        metadata_columns: Some(proto::ExchangeMetadataColumns {
            row_kind_index: 1,
            stream_record_timestamp_index: None,
            routing_key_index: None,
        }),
    }
}
