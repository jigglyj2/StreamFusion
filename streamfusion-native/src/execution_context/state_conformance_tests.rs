// Copyright 2026 StreamFusion Authors
// Licensed under the Apache License, Version 2.0.

//! Shared state-binding lifecycle contract. Fixtures contribute protobuf, not operator handles.
//! Keep failure injection here so every migrated state factory exercises the same admission path.

use super::*;
use crate::memory_pool::{tests_support::TestBroker, HostMemoryReservation};

const LIMIT: usize = 32 << 20;

mod lifecycle;
mod log_configuration;

fn plans() -> Vec<proto::NativePlan> {
    let input = Some(Box::new(proto::Operator {
        plan_node_id: 1,
        operator: Some(proto::operator::Operator::Input(proto::Input::default())),
        ..Default::default()
    }));
    [
        proto::operator::Operator::Deduplicate(Box::new(proto::Deduplicate {
            input: input.clone(),
            key_indices: vec![0],
            processing_time: true,
            keep_last: false,
            ..Default::default()
        })),
        // SELECT DISTINCT uses the ordinary group-aggregate state factory.
        proto::operator::Operator::GroupAggregate(Box::new(proto::GroupAggregate {
            input,
            grouping_indices: vec![0],
            input_changelog: true,
            ..Default::default()
        })),
    ]
    .into_iter()
    .map(|operator| proto::NativePlan {
        protocol_version: crate::RECORD_POLICY_PLAN_PROTOCOL_VERSION,
        root: Some(proto::Operator {
            plan_node_id: 2,
            operator: Some(operator),
            ..Default::default()
        }),
    })
    .collect()
}

fn resources() -> proto::NativeStateBindings {
    proto::NativeStateBindings {
        protocol_version: 1,
        bindings: vec![proto::NativeStateBinding {
            restored_watermark: None,
            plan_node_id: 2,
            max_parallelism: 16,
            first_key_group: 0,
            last_key_group: 15,
            backend: Some(proto::native_state_binding::Backend::Memory(
                proto::NativeMemoryState::default(),
            )),
        }],
    }
}

fn context(plan: &proto::NativePlan, broker: &Arc<TestBroker>) -> NativeExecutionContext {
    NativeExecutionContext::new(
        &plan.encode_to_vec(),
        Arc::new(FlinkMemoryPool::new(broker.clone(), LIMIT)),
    )
    .unwrap()
}

fn install(
    context: &mut NativeExecutionContext,
    broker: &Arc<TestBroker>,
    bytes: &[u8],
) -> Result<()> {
    context.install_state(
        bytes,
        HostMemoryReservation::new(broker.clone(), "conformance state"),
    )
}

#[test]
fn failed_state_binding_is_transactional_and_retry_does_not_consume_the_task_budget() {
    for plan in plans() {
        let broker = Arc::new(TestBroker::new(LIMIT));
        let mut context = context(&plan, &broker);
        let baseline = broker.reserved();
        let valid = resources();
        let mut wrong_protocol = valid.clone();
        wrong_protocol.protocol_version = 99;
        let mut duplicate = valid.clone();
        duplicate.bindings.push(valid.bindings[0].clone());
        let mut range = valid.clone();
        range.bindings[0].last_key_group = 16;
        let mut missing = valid.clone();
        missing.bindings[0].plan_node_id = 100;
        let mut wrong_clock_owner = valid.clone();
        wrong_clock_owner.protocol_version = 3;
        wrong_clock_owner.bindings[0].restored_watermark = Some(1999);
        for bytes in [
            vec![0xff],
            wrong_clock_owner.encode_to_vec(),
            wrong_protocol.encode_to_vec(),
            duplicate.encode_to_vec(),
            range.encode_to_vec(),
            missing.encode_to_vec(),
        ] {
            for attempt in 0..3 {
                assert!(install(&mut context, &broker, &bytes).is_err());
                assert!(context.state_resources.is_none());
                assert!(context.persistent.is_empty());
                context.require_idle().unwrap();
                assert_eq!(
                    broker.reserved(),
                    baseline,
                    "failed binding attempt {attempt}"
                );
            }
        }
        install(&mut context, &broker, &valid.encode_to_vec()).unwrap();
        assert_eq!(context.persistent.len(), 1);
        let installed = broker.reserved();
        assert!(install(&mut context, &broker, &valid.encode_to_vec()).is_err());
        assert_eq!(broker.reserved(), installed);
        drop(context);
        assert_eq!(broker.reserved(), 0);
    }
}

#[test]
fn setup_memory_denial_can_retry_without_partial_state_owners() {
    for plan in plans() {
        let broker = Arc::new(TestBroker::new(LIMIT));
        let mut context = context(&plan, &broker);
        let baseline = broker.reserved();
        let bytes = resources().encode_to_vec();
        // One denial before decode, another after admission while constructing state.
        let setup_bytes = bytes.len() * 4 + 4096;
        for available in [0, setup_bytes] {
            let pressure = context.reservation("competing Flink consumer");
            pressure.try_grow(LIMIT - baseline - available).unwrap();
            let pressured = broker.reserved();
            for _ in 0..3 {
                assert!(matches!(
                    install(&mut context, &broker, &bytes),
                    Err(DataFusionError::ResourcesExhausted(_))
                ));
                assert_eq!(broker.reserved(), pressured);
                assert!(context.state_resources.is_none());
                assert!(context.persistent.is_empty());
                context.require_idle().unwrap();
            }
            drop(pressure);
            assert_eq!(broker.reserved(), baseline);
        }
        install(&mut context, &broker, &bytes).unwrap();
        drop(context);
        assert_eq!(broker.reserved(), 0);
    }
}

#[test]
fn later_factory_failure_releases_already_constructed_state_and_allows_retry() {
    for mut plan in plans() {
        let mut parent = plan.root.as_ref().unwrap().clone();
        parent.plan_node_id = 3;
        let child = plan.root.take().map(Box::new);
        match parent.operator.as_mut().unwrap() {
            proto::operator::Operator::Deduplicate(node) => node.input = child,
            proto::operator::Operator::GroupAggregate(node) => node.input = child,
            _ => unreachable!(),
        }
        plan.root = Some(parent);
        let broker = Arc::new(TestBroker::new(LIMIT));
        let mut context = context(&plan, &broker);
        let baseline = broker.reserved();
        let mut valid = resources();
        let mut second = valid.bindings[0].clone();
        second.plan_node_id = 3;
        valid.bindings.push(second);
        let mut invalid = valid.clone();
        let directory = tempfile::tempdir().unwrap();
        // Validation succeeds and the first owner is created; opening the second plugin fails.
        invalid.bindings[1].backend = Some(proto::native_state_binding::Backend::Rocksdb(
            proto::NativeRocksDbState {
                log_directory: None,
                plugin_path: directory.path().join("missing.so").to_str().unwrap().into(),
                database_path: directory.path().join("database").to_str().unwrap().into(),
                memory_limit: 4 << 20,
            },
        ));
        for _ in 0..3 {
            let error = install(&mut context, &broker, &invalid.encode_to_vec()).unwrap_err();
            assert!(matches!(error, DataFusionError::External(_)), "{error}");
            assert_eq!(broker.reserved(), baseline);
            assert!(context.persistent.is_empty());
            assert!(context.state_resources.is_none());
        }
        install(&mut context, &broker, &valid.encode_to_vec()).unwrap();
        assert_eq!(context.persistent.len(), 2);
        drop(context);
        assert_eq!(broker.reserved(), 0);
    }
}
