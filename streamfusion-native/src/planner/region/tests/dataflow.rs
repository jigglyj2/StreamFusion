// Copyright 2026 StreamFusion Authors
// Licensed under the Apache License, Version 2.0.
use super::*;

#[test]
fn generated_divergent_graphs_compute_shared_stages_once_with_exact_metrics() {
    for width in [2, 3, 7, 19] {
        let mut plan = message();
        plan.stages.truncate(1);
        for index in 1..width {
            let mut stage = plan.stages[0].clone();
            stage.operator.as_mut().unwrap().plan_node_id += index as u64;
            // A binary tree with every stage also exposed as an output exercises several
            // nested shared owners and consumers at different dependency depths.
            stage.inputs[0].source = Some(proto::native_region_input_reference::Source::StageId(
                4294967301 + (index as u64 - 1) / 2,
            ));
            plan.stages.push(stage);
        }
        plan.output_stage_ids = plan
            .stages
            .iter()
            .rev()
            .map(|s| s.operator.as_ref().unwrap().plan_node_id)
            .collect();
        let (region, task, executes) = lower(&plan, 41, 0);
        for invocation in 1..=3 {
            let mut stream = region
                .start(task.clone(), Box::new(|success| assert!(success)))
                .unwrap();
            let outputs = drain(&mut stream, width).unwrap();
            assert_eq!(executes.load(Ordering::Relaxed), invocation);
            for output in &outputs {
                assert_eq!(output.len(), 41);
            }
            for index in 0..41 {
                for output in &outputs {
                    assert_eq!(output[index].columns(), batch(index as i32).columns());
                    for column in 0..4 {
                        assert!(Arc::ptr_eq(
                            output[index].column(column),
                            outputs[0][index].column(column)
                        ));
                    }
                }
            }
            let expected = plan
                .stages
                .iter()
                .flat_map(|s| {
                    [
                        s.operator.as_ref().unwrap().plan_node_id as i64,
                        82 * invocation as i64,
                        82 * invocation as i64,
                    ]
                })
                .collect::<Vec<_>>();
            assert_eq!(region.metrics(), expected);
        }
    }
}

#[test]
fn divergent_datafusion_projections_keep_their_own_schemas_and_envelopes() {
    let mut plan = message();
    calc(&mut plan, 1).projections = vec![proto::Expression {
        expression: Some(proto::expression::Expression::LongLiteral(
            proto::LongLiteral { value: 73 },
        )),
    }];
    let (region, task, executes) = lower(&plan, 17, 0);
    let schemas = region.output_schemas();
    assert_ne!(schemas[0], schemas[1]);
    let mut stream = region
        .start(task, Box::new(|success| assert!(success)))
        .unwrap();
    let outputs = drain(&mut stream, 2).unwrap();
    assert_eq!(executes.load(Ordering::Relaxed), 1);
    for index in 0..17 {
        assert_eq!(outputs[0][index].schema(), schemas[0]);
        assert_eq!(outputs[1][index].schema(), schemas[1]);
        assert_eq!(
            outputs[1][index].column(0).as_ref(),
            &Int64Array::from(vec![73, 73])
        );
        for column in 1..4 {
            assert!(Arc::ptr_eq(
                outputs[0][index].column(column),
                outputs[1][index].column(column)
            ));
        }
    }
}

#[test]
fn reconverging_shared_inputs_are_rejected_before_any_execution() {
    let mut plan = message();
    plan.stages[2].operator.as_mut().unwrap().operator =
        Some(proto::operator::Operator::Union(proto::Union {
            inputs: vec![
                proto::Operator {
                    operator: Some(proto::operator::Operator::Input(proto::Input::default())),
                    ..Default::default()
                },
                proto::Operator {
                    operator: Some(proto::operator::Operator::Input(proto::Input {
                        input_index: 1,
                        ..Default::default()
                    })),
                    ..Default::default()
                },
            ],
        }));
    plan.stages[2]
        .inputs
        .push(proto::NativeRegionInputReference {
            source: Some(proto::native_region_input_reference::Source::StageId(
                4294967301,
            )),
        });
    plan.output_stage_ids = vec![4294967303];
    let pool: Arc<dyn MemoryPool> = Arc::new(GreedyMemoryPool::new(1 << 20));
    let contract = RegionPlan::decode(&plan.encode_to_vec(), &pool).unwrap();
    let (input, executes) = source(1, 0);
    let error = PhysicalRegion::lower(contract, vec![input], &[], pool.clone())
        .err()
        .unwrap();
    assert!(error.to_string().contains("reconverge"));
    assert_eq!(executes.load(Ordering::Relaxed), 0);
    assert_eq!(pool.reserved(), 0);
}

#[test]
fn independent_flink_channels_remain_distinct_through_a_datafusion_union() {
    let mut plan = message();
    plan.stages.truncate(1);
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
    plan.output_stage_ids = vec![4294967301];
    let pool: Arc<dyn MemoryPool> = Arc::new(GreedyMemoryPool::new(1 << 20));
    let contract = RegionPlan::decode(&plan.encode_to_vec(), &pool).unwrap();
    let (left, left_executes) = source(7, 0);
    let (right, right_executes) = source(11, 0);
    let region = PhysicalRegion::lower(contract, vec![left, right], &[], pool.clone()).unwrap();
    let mut stream = region
        .start(task(pool), Box::new(|success| assert!(success)))
        .unwrap();
    let output = drain(&mut stream, 1).unwrap();
    assert_eq!(left_executes.load(Ordering::Relaxed), 1);
    assert_eq!(right_executes.load(Ordering::Relaxed), 1);
    assert_eq!(output[0].len(), 18);
    let mut actual = output[0]
        .iter()
        .map(|batch| {
            batch
                .column(0)
                .as_any()
                .downcast_ref::<Int32Array>()
                .unwrap()
                .value(0)
        })
        .collect::<Vec<_>>();
    actual.sort_unstable();
    let mut expected = (0..7).chain(0..11).collect::<Vec<_>>();
    expected.sort_unstable();
    assert_eq!(actual, expected);
    assert_eq!(region.metrics(), vec![4294967301, 36, 36]);
}
