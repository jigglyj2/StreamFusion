// Copyright 2026 StreamFusion Authors
// Licensed under the Apache License, Version 2.0.

//! Q5's shared global COUNT -> Calc -> attached local MAX, with both resource families
//! bound to one context. Flink topology/recovery tests still own production admission.
use super::*;

fn local() -> proto::Operator {
    let region = region_plan();
    let proto::operator::Operator::WindowAggregate(window) = region.stages[1]
        .operator
        .as_ref()
        .unwrap()
        .operator
        .as_ref()
        .unwrap()
    else {
        unreachable!()
    };
    let bigint = window.aggregate_calls[0].output_type.clone().unwrap();
    proto::Operator {
        plan_node_id: 5,
        operator: Some(proto::operator::Operator::LocalWindowAggregate(Box::new(
            proto::LocalWindowAggregate {
                input: Some(Box::new(slot())),
                grouping_indices: vec![],
                aggregate_calls: vec![
                    proto::AggregateCall {
                        function: proto::AggregateFunction::Max as i32,
                        input_index: Some(1),
                        input_type: Some(bigint.clone()),
                        output_type: Some(bigint.clone()),
                        retractable: false,
                        ..Default::default()
                    },
                    proto::AggregateCall {
                        function: proto::AggregateFunction::CountStar as i32,
                        output_type: Some(bigint.clone()),
                        ..Default::default()
                    },
                ],
                input_changelog: false,
                time_attribute_index: 3,
                kind: proto::WindowKind::Hop as i32,
                size_millis: 6000,
                slide_or_step_millis: 2000,
                offset_millis: 0,
                input_schema: window.output_schema.clone(),
                output_schema: Some(proto::Schema {
                    fields: vec![
                        proto::Field {
                            name: "accumulator".into(),
                            r#type: Some(proto::LogicalType {
                                nullable: false,
                                r#type: Some(proto::logical_type::Type::Binary(
                                    proto::EmptyType {},
                                )),
                            }),
                        },
                        proto::Field {
                            name: "window_start".into(),
                            r#type: Some(bigint.clone()),
                        },
                        proto::Field {
                            name: "slice_end".into(),
                            r#type: Some(bigint),
                        },
                    ],
                }),
                shift_time_zone: "UTC".into(),
                attached_window_start_index: Some(2),
                attached_window_end_index: Some(3),
            },
        ))),
        ..Default::default()
    }
}
fn create(
    broker: Arc<TestBroker>,
    dag: bool,
    rocks: Option<&std::path::Path>,
) -> Arc<NativeExecutionContext> {
    let memory = HostMemoryReservation::new(broker, "mixed window region");
    let mut context = if dag {
        let mut plan = region_plan();
        plan.stages.push(proto::NativeRegionStage {
            operator: Some(local()),
            inputs: vec![reference(4)],
        });
        plan.output_stage_ids = vec![3, 5];
        NativeExecutionContext::new_region(&plan.encode_to_vec(), memory.datafusion_pool(256 << 20))
            .unwrap()
    } else {
        let mut root = local();
        let proto::operator::Operator::LocalWindowAggregate(local) =
            root.operator.as_mut().unwrap()
        else {
            unreachable!()
        };
        local.input = plan().root.map(Box::new);
        NativeExecutionContext::new(
            &proto::NativePlan {
                protocol_version: 3,
                root: Some(root),
            }
            .encode_to_vec(),
            memory.datafusion_pool(256 << 20),
        )
        .unwrap()
    };
    context
        .install_state(
            &resources(None, rocks).encode_to_vec(),
            memory.sibling("global state"),
        )
        .unwrap();
    context
        .install_task_resources(
            &proto::NativeTaskBindings {
                protocol_version: 1,
                bindings: vec![proto::NativeTaskBinding {
                    plan_node_id: 5,
                    resource: Some(proto::native_task_binding::Resource::LocalWindowBuffer(
                        proto::NativeLocalWindowBuffer {
                            flink_buffer_memory_bytes: 3 << 20,
                            flink_page_bytes: 32 << 10,
                        },
                    )),
                }],
            }
            .encode_to_vec(),
            memory,
        )
        .unwrap();
    Arc::new(context)
}
fn run_mixed(
    context: &Arc<NativeExecutionContext>,
    batch: RecordBatch,
    event: Option<ControlEvent>,
    dag: bool,
) -> Result<[Vec<RecordBatch>; 2]> {
    let events = event.map(|event| [(3, event), (5, event)]);
    let mut result = [vec![], vec![]];
    if dag {
        let mut stream = if let Some(events) = events {
            context.start_region_control(vec![batch], &events)?
        } else {
            context.start_region(vec![batch])?
        };
        while let Some(batch) = context.runtime().block_on(stream.next()) {
            let batch = batch?;
            result[batch.port].push(batch.batch);
        }
    } else {
        let mut stream = if let Some(events) = events {
            context.start_control(vec![batch], &events)?
        } else {
            context.start(vec![batch])?
        };
        while let Some(batch) = context.runtime().block_on(stream.next()) {
            result[1].push(batch?);
        }
    }
    context.require_idle()?;
    Ok(result)
}
#[test]
fn one_region_binds_keyed_global_state_and_flink_local_buffer_on_both_backends() {
    for rocks in [false, true] {
        if rocks && std::env::var("STREAMFUSION_TEST_ROCKSDB_PLUGIN").is_err() {
            continue;
        }
        for seed in [3u64, 19, 71] {
            let broker = Arc::new(TestBroker::new(256 << 20));
            let left = tempfile::tempdir().unwrap();
            let right = tempfile::tempdir().unwrap();
            let shared = create(broker.clone(), true, rocks.then_some(left.path()));
            let baseline = create(broker.clone(), false, rocks.then_some(right.path()));
            let raw = context(broker.clone(), None, None);
            let mut random = seed;
            for slice in [2000, 4000, 6000] {
                let partials = (0..31)
                    .map(|_| {
                        random = random.wrapping_mul(6364136223846793005).wrapping_add(1);
                        ((random % 7) as i64, (random % 5 + 1) as i64, slice)
                    })
                    .collect::<Vec<_>>();
                let batch = input(&partials, INSERT);
                assert_eq!(
                    run_mixed(&shared, batch.clone(), None, true).unwrap()[1],
                    run_mixed(&baseline, batch.clone(), None, false).unwrap()[1]
                );
                assert_eq!(rows(&run(&raw, batch, None).unwrap()), 0);
            }
            for event in [
                ControlEvent::BeforeCheckpoint(1),
                ControlEvent::Watermark(3999),
                ControlEvent::BeforeCheckpoint(2),
                ControlEvent::EndInput,
                ControlEvent::Watermark(i64::MAX),
            ] {
                let actual = run_mixed(&shared, input(&[], INSERT), Some(event), true).unwrap();
                let expected =
                    run_mixed(&baseline, input(&[], INSERT), Some(event), false).unwrap();
                assert_eq!(actual[1], expected[1]);
                let expected_raw = run(&raw, input(&[], INSERT), Some(event)).unwrap();
                assert_eq!(actual[0].len(), expected_raw.len());
                for (actual, expected) in actual[0].iter().zip(&expected_raw) {
                    assert_eq!(actual.columns(), expected.columns());
                }
            }
            for metric in shared.metric_snapshot().unwrap().chunks_exact(3) {
                assert!(baseline
                    .metric_snapshot()
                    .unwrap()
                    .chunks_exact(3)
                    .any(|expected| expected == metric));
            }
            assert_eq!(
                shared.input_batch_count(&[3, 5]).unwrap(),
                baseline.input_batch_count(&[3, 5]).unwrap()
            );
            drop(raw);
            drop(shared);
            drop(baseline);
            assert_eq!(broker.reserved(), 0);
        }
    }
}
