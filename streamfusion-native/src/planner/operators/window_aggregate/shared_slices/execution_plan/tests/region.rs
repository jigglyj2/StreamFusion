// Copyright 2026 StreamFusion Authors
// Licensed under the Apache License, Version 2.0.
use super::*;

fn region_plan() -> proto::NativeRegionPlan {
    let mut last = plan().root.unwrap();
    let proto::operator::Operator::Calc(calc) = last.operator.as_mut().unwrap() else {
        unreachable!()
    };
    let mut window = *calc.input.take().unwrap();
    calc.input = Some(Box::new(slot()));
    let proto::operator::Operator::WindowAggregate(aggregate) = window.operator.as_mut().unwrap()
    else {
        unreachable!()
    };
    let mut first = *aggregate.input.take().unwrap();
    aggregate.input = Some(Box::new(slot()));
    let proto::operator::Operator::Calc(calc) = first.operator.as_mut().unwrap() else {
        unreachable!()
    };
    calc.input = Some(Box::new(slot()));
    proto::NativeRegionPlan {
        protocol_version: 1,
        input_count: 1,
        stages: vec![
            proto::NativeRegionStage {
                operator: Some(first),
                inputs: vec![proto::NativeRegionInputReference {
                    source: Some(proto::native_region_input_reference::Source::ExternalInput(
                        0,
                    )),
                }],
            },
            proto::NativeRegionStage {
                operator: Some(window),
                inputs: vec![reference(2)],
            },
            proto::NativeRegionStage {
                operator: Some(last),
                inputs: vec![reference(3)],
            },
        ],
        output_stage_ids: vec![3, 4],
    }
}
fn slot() -> proto::Operator {
    proto::Operator {
        operator: Some(proto::operator::Operator::Input(proto::Input::default())),
        ..Default::default()
    }
}
fn reference(id: u64) -> proto::NativeRegionInputReference {
    proto::NativeRegionInputReference {
        source: Some(proto::native_region_input_reference::Source::StageId(id)),
    }
}
fn region_context(
    broker: Arc<TestBroker>,
    watermark: Option<i64>,
    rocks: Option<&std::path::Path>,
) -> Arc<NativeExecutionContext> {
    let memory = HostMemoryReservation::new(broker, "shared window DAG");
    let mut context = NativeExecutionContext::new_region(
        &region_plan().encode_to_vec(),
        memory.datafusion_pool(256 << 20),
    )
    .unwrap();
    context
        .install_state(&resources(watermark, rocks).encode_to_vec(), memory)
        .unwrap();
    Arc::new(context)
}
fn run_region(
    context: &Arc<NativeExecutionContext>,
    input: RecordBatch,
    event: Option<ControlEvent>,
) -> Result<[Vec<RecordBatch>; 2]> {
    let mut output = if let Some(event) = event {
        context.start_region_control(vec![input], &[(3, event)])?
    } else {
        context.start_region(vec![input])?
    };
    let mut result = [vec![], vec![]];
    while let Some(batch) = context.runtime().block_on(output.next()) {
        let batch = batch?;
        result[batch.port].push(batch.batch);
    }
    context.require_idle()?;
    Ok(result)
}
fn parity(expected: &[RecordBatch], outputs: &[Vec<RecordBatch>; 2]) {
    assert_eq!(expected, outputs[1]);
    assert_eq!(outputs[0].len(), outputs[1].len());
    for (left, right) in outputs[0].iter().zip(&outputs[1]) {
        assert_eq!(left.columns(), right.columns());
        for index in 0..left.num_columns() {
            assert!(Arc::ptr_eq(left.column(index), right.column(index)));
        }
    }
}

#[test]
fn shared_window_region_matches_tree_controls_metrics_and_cross_backend_restore() {
    for seed in [3u64, 19, 71] {
        for rocks in [false, true] {
            if rocks && std::env::var("STREAMFUSION_TEST_ROCKSDB_PLUGIN").is_err() {
                continue;
            }
            let directory = tempfile::tempdir().unwrap();
            let broker = Arc::new(TestBroker::new(256 << 20));
            let baseline = context(broker.clone(), None, None);
            let shared = region_context(broker.clone(), None, rocks.then_some(directory.path()));
            let mut random = seed;
            for slice in [2000, 4000, 6000] {
                let partials = (0..31)
                    .map(|_| {
                        random = random.wrapping_mul(6364136223846793005).wrapping_add(1);
                        ((random % 7) as i64, (random % 5 + 1) as i64, slice)
                    })
                    .collect::<Vec<_>>();
                let input = input(&partials, INSERT);
                parity(
                    &run(&baseline, input.clone(), None).unwrap(),
                    &run_region(&shared, input, None).unwrap(),
                );
            }
            for event in [
                ControlEvent::BeforeCheckpoint(7),
                ControlEvent::Watermark(3999),
            ] {
                parity(
                    &run(&baseline, input(&[], INSERT), Some(event)).unwrap(),
                    &run_region(&shared, input(&[], INSERT), Some(event)).unwrap(),
                );
            }
            let baseline_metrics = baseline.metric_snapshot().unwrap();
            for metric in shared.metric_snapshot().unwrap().chunks_exact(3) {
                assert!(baseline_metrics
                    .chunks_exact(3)
                    .any(|expected| expected == metric));
            }
            assert_eq!(
                shared.input_batch_count(&[3]).unwrap(),
                baseline.input_batch_count(&[3]).unwrap()
            );
            for name in ["numLateRecordsDropped", "currentWatermark"] {
                assert_eq!(
                    shared.metric_value(3, name).unwrap(),
                    baseline.metric_value(3, name).unwrap()
                );
            }
            let snapshots = (0..128)
                .map(|group| shared.snapshot_state(3, group).unwrap())
                .collect::<Vec<_>>();
            // Snapshot each physical owner once even though two outputs consume its result.
            for (group, snapshot) in snapshots.iter().enumerate() {
                assert_eq!(
                    snapshot.as_ref(),
                    baseline.snapshot_state(3, group as u32).unwrap().as_ref()
                );
            }
            drop(shared);
            let restored_directory = tempfile::tempdir().unwrap();
            let restored = region_context(
                broker.clone(),
                Some(3999),
                (!rocks)
                    .then_some(restored_directory.path())
                    .filter(|_| std::env::var("STREAMFUSION_TEST_ROCKSDB_PLUGIN").is_ok()),
            );
            for (group, snapshot) in snapshots.iter().enumerate() {
                restored.restore_state(3, group as u32, snapshot).unwrap();
            }
            for event in [ControlEvent::EndInput, ControlEvent::Watermark(i64::MAX)] {
                parity(
                    &run(&baseline, input(&[], INSERT), Some(event)).unwrap(),
                    &run_region(&restored, input(&[], INSERT), Some(event)).unwrap(),
                );
            }
            drop(restored);
            drop(baseline);
            drop(snapshots);
            assert_eq!(broker.reserved(), 0);
        }
    }
}

mod mixed;

#[test]
fn a_partial_shared_window_drain_cannot_snapshot_or_reuse_state() {
    let broker = Arc::new(TestBroker::new(256 << 20));
    let context = region_context(broker.clone(), None, None);
    run_region(&context, input(&[(1, 2, 2000), (2, 7, 4000)], INSERT), None).unwrap();
    let mut output = context
        .start_region_control(
            vec![input(&[], INSERT)],
            &[(3, ControlEvent::Watermark(i64::MAX))],
        )
        .unwrap();
    let held = loop {
        let batch = context
            .runtime()
            .block_on(output.next())
            .unwrap()
            .unwrap()
            .batch;
        if batch.num_rows() > 0 {
            break batch;
        }
    };
    assert!(context.snapshot_state(3, 0).is_err());
    drop(output);
    assert!(context
        .require_idle()
        .unwrap_err()
        .to_string()
        .contains("recovery"));
    assert!(context.snapshot_state(3, 0).is_err());
    drop(context);
    assert!(broker.reserved() > 0);
    drop(held);
    assert_eq!(broker.reserved(), 0);
}
