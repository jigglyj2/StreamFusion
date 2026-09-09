// Copyright 2026 StreamFusion Authors
// Licensed under the Apache License, Version 2.0.
use super::*;
use crate::planner::operators::envelope::processing_time;

fn plan() -> proto::NativePlan {
    let mut plan = proto::NativePlan::decode(
        super::super::super::tests::plan(proto::WindowKind::Tumble, 1000, 0, false).as_slice(),
    )
    .unwrap();
    let mut window = plan.root.take().unwrap();
    window.plan_node_id = 3;
    let Some(proto::operator::Operator::WindowAggregate(aggregate)) = &mut window.operator else {
        unreachable!()
    };
    aggregate.processing_time = true;
    let clock = aggregate.input_schema.as_mut().unwrap().fields[1]
        .r#type
        .as_mut()
        .unwrap();
    clock.nullable = true;
    clock.r#type = Some(proto::logical_type::Type::TimestampLtz(
        proto::PrecisionType { precision: 3 },
    ));
    aggregate.input = Some(Box::new(proto::Operator {
        plan_node_id: 1,
        operator: Some(proto::operator::Operator::Input(proto::Input::default())),
        ..Default::default()
    }));
    proto::NativePlan {
        protocol_version: 3,
        root: Some(calc(4, window, 4)),
    }
}

fn buffers(version: u32) -> Vec<u8> {
    proto::NativeTaskBindings {
        protocol_version: version,
        bindings: vec![proto::NativeTaskBinding {
            plan_node_id: 3,
            resource: Some(proto::native_task_binding::Resource::LocalWindowBuffer(
                proto::NativeLocalWindowBuffer {
                    flink_buffer_memory_bytes: 3 << 20,
                    flink_page_bytes: 32 << 10,
                },
            )),
        }],
    }
    .encode_to_vec()
}

fn input(keys: Vec<i64>, clocks: Vec<i64>) -> RecordBatch {
    let rows = keys.len();
    let schema = Arc::new(Schema::new(vec![
        Field::new("key", DataType::Int64, false),
        Field::new(
            "ts",
            DataType::Timestamp(arrow::datatypes::TimeUnit::Millisecond, None),
            true,
        ),
        Field::new(OWNED_TIMESTAMP_V1, DataType::Int64, true),
        Field::new(ROW_KIND, DataType::Int8, false),
        Field::new(INPUT_ROW, DataType::Int32, false),
    ]));
    let batch = RecordBatch::try_new(
        schema,
        vec![
            Arc::new(Int64Array::from(keys)),
            Arc::new(TimestampMillisecondArray::from(vec![None; rows])),
            Arc::new(Int64Array::from(vec![Some(123); rows])),
            Arc::new(Int8Array::from(vec![INSERT; rows])),
            Arc::new(Int32Array::from(vec![-1; rows])),
        ],
    )
    .unwrap();
    let clocks = RecordBatch::try_new(
        Arc::new(Schema::new(vec![Field::new(
            processing_time::FIELD,
            DataType::Int64,
            false,
        )])),
        vec![Arc::new(Int64Array::from(clocks))],
    )
    .unwrap();
    processing_time::attach(batch, clocks).unwrap()
}

fn context(
    broker: Arc<TestBroker>,
    rocks: Option<&std::path::Path>,
    restored: Option<i64>,
) -> Arc<NativeExecutionContext> {
    let memory = HostMemoryReservation::new(broker, "processing-time factory");
    let mut context =
        NativeExecutionContext::new(&plan().encode_to_vec(), memory.datafusion_pool(64 << 20))
            .unwrap();
    context
        .install_task_resources(&buffers(2), memory.sibling("buffer resources"))
        .unwrap();
    context
        .install_state(&resources(restored, rocks).encode_to_vec(), memory)
        .unwrap();
    Arc::new(context)
}

fn control(context: &Arc<NativeExecutionContext>, event: ControlEvent) -> Vec<RecordBatch> {
    run(context, input(vec![], vec![]), Some(event)).unwrap()
}
fn count(batches: &[RecordBatch]) -> Vec<i64> {
    batches
        .iter()
        .flat_map(|batch| {
            assert!(processing_time::column(batch).unwrap().is_none());
            assert_eq!(batch.column(4).null_count(), batch.num_rows());
            batch
                .column(1)
                .as_any()
                .downcast_ref::<Int64Array>()
                .unwrap()
                .values()
                .to_vec()
        })
        .collect()
}

#[test]
fn bound_factory_negotiates_clocks_and_drives_buffered_counts_through_the_fused_tree() {
    for rocks in [false, true] {
        let directory = tempfile::tempdir().unwrap();
        let broker = Arc::new(TestBroker::new(64 << 20));
        let context = context(broker.clone(), rocks.then_some(directory.path()), None);
        assert_eq!(context.clock_input_bindings(), [(3, 0)]);
        let (bytes, _credit) = context.control_capabilities().unwrap();
        let caps = proto::NativeControlCapabilities::decode(bytes.as_slice()).unwrap();
        assert_eq!(caps.protocol_version, 3);
        assert!(caps.stages[0].processing_time);
        assert_eq!(caps.stages[0].processing_time_input_port, Some(0));
        drop(_credit);
        assert_eq!(
            rows(&run(&context, input(vec![7; 2], vec![1001; 2]), None).unwrap()),
            0
        );
        assert_eq!(context.processing_time_deadlines().unwrap(), [3, 1999]);
        assert_eq!(
            rows(&control(&context, ControlEvent::Watermark(i64::MAX))),
            0
        );
        assert_eq!(rows(&control(&context, ControlEvent::EndInput)), 0);
        assert_eq!(
            count(&control(&context, ControlEvent::ProcessingTime(1999))),
            [2]
        );
        run(&context, input(vec![7; 3], vec![1; 3]), None).unwrap();
        assert_eq!(
            count(&control(&context, ControlEvent::ProcessingTime(999))),
            [0]
        );
        control(&context, ControlEvent::BeforeCheckpoint(1));
        assert!(context.processing_time_deadlines().unwrap().is_empty());
        run(&context, input(vec![7; 5], vec![1; 5]), None).unwrap();
        assert_eq!(
            count(&control(&context, ControlEvent::ProcessingTime(999))),
            [3]
        );
        assert_eq!(context.gauge_snapshot().unwrap().0, [0, i64::MAX]);
        drop(context);
        assert_eq!(broker.reserved(), 0);
    }
}

#[test]
fn buffer_resources_are_required_versioned_and_transactional_before_state_binding() {
    let broker = Arc::new(TestBroker::new(64 << 20));
    let memory = HostMemoryReservation::new(broker.clone(), "resource failure");
    let mut context =
        NativeExecutionContext::new(&plan().encode_to_vec(), memory.datafusion_pool(64 << 20))
            .unwrap();
    let before = broker.reserved();
    assert!(context
        .install_state(
            &resources(None, None).encode_to_vec(),
            memory.sibling("missing buffer")
        )
        .unwrap_err()
        .to_string()
        .contains("before binding"));
    assert_eq!(broker.reserved(), before);
    assert!(context
        .install_task_resources(&buffers(1), memory.sibling("old protocol"))
        .unwrap_err()
        .to_string()
        .contains("protocol 2"));
    assert_eq!(broker.reserved(), before);
    context
        .install_task_resources(&buffers(2), memory.sibling("buffer"))
        .unwrap();
    context
        .install_state(&resources(None, None).encode_to_vec(), memory)
        .unwrap();
    let context = Arc::new(context);
    run(&context, input(vec![7], vec![1]), None).unwrap();
    assert_eq!(
        count(&control(&context, ControlEvent::ProcessingTime(999))),
        [1]
    );
    drop(context);
    assert_eq!(broker.reserved(), 0);
}

#[test]
fn shared_factory_restores_processing_timers_without_advancing_them_with_the_union_watermark() {
    for rocks in [false, true] {
        let directory = tempfile::tempdir().unwrap();
        let broker = Arc::new(TestBroker::new(64 << 20));
        let source = context(broker.clone(), rocks.then_some(directory.path()), None);
        run(&source, input(vec![7; 4], vec![1001; 4]), None).unwrap();
        control(&source, ControlEvent::BeforeCheckpoint(1));
        let snapshots = (0..128)
            .map(|group| source.snapshot_state(3, group).unwrap())
            .collect::<Vec<_>>();
        drop(source);
        let restored_directory = tempfile::tempdir().unwrap();
        let restored = context(
            broker.clone(),
            (!rocks).then_some(restored_directory.path()),
            Some(i64::MAX),
        );
        for (group, snapshot) in snapshots.iter().enumerate() {
            restored.restore_state(3, group as u32, snapshot).unwrap();
        }
        assert_eq!(restored.processing_time_deadlines().unwrap(), [3, 1999]);
        run(&restored, input(vec![7; 2], vec![1998; 2]), None).unwrap();
        assert_eq!(
            count(&control(&restored, ControlEvent::ProcessingTime(1999))),
            [6]
        );
        drop(restored);
        drop(snapshots);
        assert_eq!(broker.reserved(), 0);
    }
}

#[test]
fn shared_region_clock_owner_emits_one_timer_result_to_both_arrow_exits() {
    let broker = Arc::new(TestBroker::new(64 << 20));
    let memory = HostMemoryReservation::new(broker.clone(), "processing-time region");
    let mut last = plan().root.unwrap();
    let Some(proto::operator::Operator::Calc(calc)) = &mut last.operator else {
        unreachable!()
    };
    let mut owner = *calc.input.take().unwrap();
    calc.input = Some(Box::new(super::region::slot()));
    let Some(proto::operator::Operator::WindowAggregate(window)) = &mut owner.operator else {
        unreachable!()
    };
    window.input = Some(Box::new(super::region::slot()));
    let region = proto::NativeRegionPlan {
        protocol_version: 1,
        input_count: 1,
        stages: vec![
            proto::NativeRegionStage {
                operator: Some(owner),
                inputs: vec![proto::NativeRegionInputReference {
                    source: Some(proto::native_region_input_reference::Source::ExternalInput(
                        0,
                    )),
                }],
            },
            proto::NativeRegionStage {
                operator: Some(last),
                inputs: vec![super::region::reference(3)],
            },
        ],
        output_stage_ids: vec![3, 4],
    };
    let mut context = NativeExecutionContext::new_region(
        &region.encode_to_vec(),
        memory.datafusion_pool(64 << 20),
    )
    .unwrap();
    context
        .install_task_resources(&buffers(2), memory.sibling("buffer"))
        .unwrap();
    context
        .install_state(&resources(None, None).encode_to_vec(), memory)
        .unwrap();
    assert_eq!(context.clock_input_bindings(), [(3, 0)]);
    let context = Arc::new(context);
    super::region::run_region(&context, input(vec![7; 3], vec![1; 3]), None).unwrap();
    let output = super::region::run_region(
        &context,
        input(vec![], vec![]),
        Some(ControlEvent::ProcessingTime(999)),
    )
    .unwrap();
    assert_eq!(count(&output[0]), [3]);
    assert_eq!(count(&output[1]), [3]);
    for (left, right) in output[0].iter().zip(&output[1]) {
        for (a, b) in left.columns().iter().zip(right.columns()) {
            assert!(Arc::ptr_eq(a, b));
        }
    }
    assert!(context.processing_time_deadlines().unwrap().is_empty());
    drop(context);
    drop(output);
    assert_eq!(broker.reserved(), 0);
}
