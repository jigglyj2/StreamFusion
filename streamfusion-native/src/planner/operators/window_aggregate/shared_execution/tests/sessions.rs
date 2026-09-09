// Copyright 2026 StreamFusion Authors
// Licensed under the Apache License, Version 2.0.

use super::*;
use crate::planner::operators::window_aggregate::shared_sessions::tests as fixture;

fn session_plan() -> proto::NativePlan {
    let mut plan = proto::NativePlan::decode(fixture::plan().as_slice()).unwrap();
    let mut window = plan.root.take().unwrap();
    window.plan_node_id = 3;
    let Some(proto::operator::Operator::WindowAggregate(aggregate)) = &mut window.operator else {
        unreachable!()
    };
    aggregate.input = Some(Box::new(calc(
        2,
        proto::Operator {
            plan_node_id: 1,
            operator: Some(proto::operator::Operator::Input(proto::Input::default())),
            ..Default::default()
        },
        2,
    )));
    plan.root = Some(calc(4, window, 4));
    plan.protocol_version = 2;
    plan
}

fn session_input(timestamps: Vec<i64>, kind: i8) -> RecordBatch {
    let rows = timestamps.len();
    let batch = fixture::batch(timestamps);
    let mut columns = batch.columns().to_vec();
    columns.extend([
        Arc::new(Int64Array::from(vec![Some(123); rows])) as ArrayRef,
        Arc::new(Int8Array::from(vec![kind; rows])) as ArrayRef,
        Arc::new(Int32Array::from(vec![-1; rows])) as ArrayRef,
    ]);
    let mut fields = batch.schema().fields().to_vec();
    fields.extend([
        Arc::new(Field::new(OWNED_TIMESTAMP_V1, DataType::Int64, true)),
        Arc::new(Field::new(ROW_KIND, DataType::Int8, false)),
        Arc::new(Field::new(INPUT_ROW, DataType::Int32, false)),
    ]);
    RecordBatch::try_new(Arc::new(Schema::new(fields)), columns).unwrap()
}

#[test]
fn session_calc_tree_uses_common_controls_and_keeps_output_credit_after_close() {
    for rocks in [false, true] {
        let directory = tempfile::tempdir().unwrap();
        let broker = Arc::new(TestBroker::new(256 << 20));
        let memory = HostMemoryReservation::new(broker.clone(), "session shared context");
        let mut context = NativeExecutionContext::new(
            &session_plan().encode_to_vec(),
            memory.datafusion_pool(256 << 20),
        )
        .unwrap();
        context
            .install_state(
                &resources(None, rocks.then_some(directory.path())).encode_to_vec(),
                memory,
            )
            .unwrap();
        let context = Arc::new(context);
        run(&context, session_input(vec![10000], INSERT), None).unwrap();
        let empty = session_input(vec![], INSERT);
        assert_eq!(
            rows(
                &run(
                    &context,
                    empty.clone(),
                    Some(ControlEvent::Watermark(15000))
                )
                .unwrap()
            ),
            0
        );
        run(&context, session_input(vec![0], INSERT), None).unwrap();
        assert_eq!(
            rows(
                &run(
                    &context,
                    empty.clone(),
                    Some(ControlEvent::BeforeCheckpoint(1))
                )
                .unwrap()
            ),
            0
        );
        assert_eq!(
            rows(&run(&context, empty.clone(), Some(ControlEvent::EndInput)).unwrap()),
            0
        );
        let output = run(&context, empty, Some(ControlEvent::Watermark(19999))).unwrap();
        assert_eq!(rows(&output), 1);
        let batch = output.iter().find(|batch| batch.num_rows() == 1).unwrap();
        assert_eq!(
            batch
                .column(1)
                .as_any()
                .downcast_ref::<Int64Array>()
                .unwrap()
                .value(0),
            2
        );
        assert_eq!(batch.column(4).null_count(), 1);
        assert_eq!(
            batch
                .column(5)
                .as_any()
                .downcast_ref::<Int8Array>()
                .unwrap()
                .value(0),
            INSERT
        );
        assert_eq!(
            batch
                .column(6)
                .as_any()
                .downcast_ref::<Int32Array>()
                .unwrap()
                .value(0),
            -1
        );
        drop(context);
        assert!(broker.reserved() > 0);
        drop(output);
        assert_eq!(broker.reserved(), 0);
    }
}
