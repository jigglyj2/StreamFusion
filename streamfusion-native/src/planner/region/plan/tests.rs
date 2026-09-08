// Copyright 2026 StreamFusion Authors
// Licensed under the Apache License, Version 2.0.

use super::*;
use crate::allocation_test_support::measure;
use datafusion::execution::memory_pool::GreedyMemoryPool;

const GOLDEN: &[u8] = include_bytes!(concat!(
    env!("CARGO_MANIFEST_DIR"),
    "/../streamfusion-proto/src/test/resources/native-region-v1.pb"
));
fn fixture() -> proto::NativeRegionPlan {
    proto::NativeRegionPlan::decode(GOLDEN).unwrap()
}
fn pool(capacity: usize) -> Arc<dyn MemoryPool> {
    Arc::new(GreedyMemoryPool::new(capacity))
}

#[test]
fn java_wire_fixture_resolves_shared_identity_and_output_order() {
    let memory = pool(1 << 20);
    let plan = RegionPlan::decode(GOLDEN, &memory).unwrap();
    assert_eq!(plan.message, fixture());
    assert_eq!(
        plan.inputs,
        [
            vec![RegionInput::External(0)],
            vec![RegionInput::Stage(0)],
            vec![RegionInput::Stage(1)]
        ]
    );
    assert_eq!(plan.outputs, [0, 2]);
    assert_eq!(plan.consumers, [2, 1, 1]);
    assert!(memory.reserved() > GOLDEN.len());
    drop(plan);
    assert_eq!(memory.reserved(), 0);
    assert!(crate::decode_plan(GOLDEN).is_err());
}

#[test]
fn invalid_graphs_fail_before_execution_and_release_admission() {
    let mutations: Vec<Box<dyn Fn(&mut proto::NativeRegionPlan)>> = vec![
        Box::new(|p| p.protocol_version = 2),
        Box::new(|p| p.input_count = 0),
        Box::new(|p| p.input_count = u32::MAX),
        Box::new(|p| p.input_count = 2),
        Box::new(|p| p.stages[0].operator = None),
        Box::new(|p| p.stages[0].operator.as_mut().unwrap().plan_node_id = 0),
        Box::new(|p| p.stages[0].operator.as_mut().unwrap().plan_node_id = u64::MAX),
        Box::new(|p| p.stages[1].operator.as_mut().unwrap().plan_node_id = 4294967301),
        Box::new(|p| {
            for s in &mut p.stages {
                s.operator.as_mut().unwrap().metric_uid = Some("duplicate".into());
            }
        }),
        Box::new(|p| {
            p.stages[0].inputs[0].source = Some(
                proto::native_region_input_reference::Source::StageId(4294967303),
            )
        }),
        Box::new(|p| {
            p.stages[1].inputs[0].source = Some(
                proto::native_region_input_reference::Source::StageId(4294967302),
            )
        }),
        Box::new(|p| p.stages[1].inputs[0] = p.stages[0].inputs[0].clone()),
        Box::new(|p| p.stages[0].inputs[0].source = None),
        Box::new(|p| p.stages[0].inputs.clear()),
        Box::new(|p| p.output_stage_ids = vec![4294967301]),
        Box::new(|p| p.output_stage_ids = vec![4294967303, 4294967303]),
        Box::new(|p| p.output_stage_ids = vec![42]),
        Box::new(|p| p.output_stage_ids.clear()),
        Box::new(|p| {
            let proto::operator::Operator::Calc(c) = p.stages[0]
                .operator
                .as_mut()
                .unwrap()
                .operator
                .as_mut()
                .unwrap()
            else {
                unreachable!()
            };
            c.input.as_mut().unwrap().plan_node_id = 42;
        }),
        Box::new(|p| {
            let proto::operator::Operator::Calc(c) = p.stages[0]
                .operator
                .as_mut()
                .unwrap()
                .operator
                .as_mut()
                .unwrap()
            else {
                unreachable!()
            };
            c.input.as_mut().unwrap().operator =
                Some(proto::operator::Operator::Input(proto::Input {
                    input_index: 1,
                    ..Default::default()
                }));
        }),
        Box::new(|p| {
            let nested = p.stages[1].operator.clone().unwrap();
            let proto::operator::Operator::Calc(c) = p.stages[0]
                .operator
                .as_mut()
                .unwrap()
                .operator
                .as_mut()
                .unwrap()
            else {
                unreachable!()
            };
            c.input = Some(Box::new(nested));
        }),
    ];
    let memory = pool(1 << 20);
    for (index, mutate) in mutations.into_iter().enumerate() {
        let mut message = fixture();
        mutate(&mut message);
        assert!(
            RegionPlan::decode(&message.encode_to_vec(), &memory).is_err(),
            "mutation {index}"
        );
        assert_eq!(memory.reserved(), 0);
    }
}

#[test]
fn admission_covers_wide_shared_graph_and_rejects_before_decode() {
    for width in [3, 64, 512] {
        let mut message = fixture();
        message.stages.truncate(1);
        for index in 1..width {
            let mut stage = message.stages[0].clone();
            stage.operator.as_mut().unwrap().plan_node_id += index as u64;
            stage.inputs[0].source = Some(proto::native_region_input_reference::Source::StageId(
                4294967301,
            ));
            message.stages.push(stage);
        }
        message.output_stage_ids = message
            .stages
            .iter()
            .map(|s| s.operator.as_ref().unwrap().plan_node_id)
            .collect();
        let bytes = message.encode_to_vec();
        let (estimate, allocation) =
            measure(|| PlanMemory::scan_region(&bytes).unwrap().decoded().unwrap());
        assert_eq!(allocation.peak, 0);
        let memory = pool(estimate);
        let (plan, allocation) = measure(|| RegionPlan::decode(&bytes, &memory).unwrap());
        assert!(
            allocation.peak <= estimate,
            "width={width} heap={allocation:?} estimate={estimate}"
        );
        drop(plan);
        assert_eq!(memory.reserved(), 0);
        let small = pool(estimate - 1);
        assert!(matches!(
            RegionPlan::decode(&bytes, &small),
            Err(DataFusionError::ResourcesExhausted(_))
        ));
        assert_eq!(small.reserved(), 0);
    }
}
