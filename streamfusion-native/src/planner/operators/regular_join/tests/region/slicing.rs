// Copyright 2026 StreamFusion Authors
// Licensed under the Apache License, Version 2.0.

use super::*;
use crate::planner::operators::regular_join::execution_plan::RegularJoinExec;
use crate::planner::operators::reusable_input::ReusableInputExec;
use datafusion::execution::TaskContext;
use datafusion::physical_plan::ExecutionPlan;
use futures::StreamExt;
use std::sync::Mutex;

#[tokio::test]
async fn large_sliced_input_bounds_staging_and_preserves_changelog_origins_and_state() {
    let broker = Arc::new(TestBroker::new(10 << 20));
    let processor = Arc::new(Mutex::new(
        RegularJoinProcessor::new(
            &plan(proto::RegularJoinType::Full),
            128,
            0,
            127,
            HostMemoryReservation::new(broker.clone(), "segmented state"),
        )
        .unwrap(),
    ));
    let schema = batch(&[], &[], &[]).schema();
    let inputs = [0, 1].map(|_| Arc::new(ReusableInputExec::new(schema.clone())));
    let exec = RegularJoinExec::new(
        processor.clone(),
        vec![inputs[0].clone(), inputs[1].clone()],
    )
    .unwrap();
    let reference_broker = Arc::new(TestBroker::new(128 << 20));
    let mut reference = RegularJoinProcessor::new(
        &plan(proto::RegularJoinType::Full),
        128,
        0,
        127,
        HostMemoryReservation::new(reference_broker.clone(), "unsplit reference"),
    )
    .unwrap();
    let count = 16_387;
    let keys = (0..count as i64 + 2).collect::<Vec<_>>();
    let mut seen = 0;
    for (side, kind, value) in [(0, INSERT, "left-λ"), (0, DELETE, "left-λ")] {
        // Exercise a producer slice as well as the internal state-work slices.
        let input = batch(&keys, &vec![value; keys.len()], &vec![kind; keys.len()]).slice(1, count);
        if kind == INSERT {
            let mut unsplit = RegularJoinProcessor::new(
                &plan(proto::RegularJoinType::Full),
                128,
                0,
                127,
                HostMemoryReservation::new(Arc::new(TestBroker::new(10 << 20)), "unsplit denied"),
            )
            .unwrap();
            assert!(unsplit
                .begin_streaming_batch(side, input.clone())
                .unwrap_err()
                .to_string()
                .contains("Flink denied"));
        }
        let expected = reference.process_arrow(side, input.clone()).unwrap();
        inputs[side].replace_batch(input).unwrap();
        inputs[1 - side]
            .replace_batch(RecordBatch::new_empty(schema.clone()))
            .unwrap();
        let mut stream = exec.execute(0, Arc::new(TaskContext::default())).unwrap();
        let mut offset = 0;
        while let Some(output) = stream.next().await {
            let output = output.unwrap();
            assert!(processor.lock().unwrap().snapshot_key_group(0).is_err());
            let output =
                RecordBatch::try_new(expected.schema(), output.columns().to_vec()).unwrap();
            assert_eq!(output, expected.slice(offset, output.num_rows()));
            offset += output.num_rows();
        }
        drop(stream);
        assert_eq!(offset, expected.num_rows());
        seen += offset;
        for group in 0..128 {
            assert_eq!(
                processor.lock().unwrap().snapshot_key_group(group).unwrap(),
                reference.snapshot_key_group(group).unwrap()
            );
        }
    }
    assert_eq!(seen, count * 2);
    let writes = processor.lock().unwrap().statistics()[1];
    assert!(
        writes > 3 && writes < count as u64 / 32,
        "state writes must stay batched: {writes}"
    );
    drop(exec);
    drop(processor);
    drop(reference);
    assert_eq!(broker.reserved(), 0);
    assert_eq!(reference_broker.reserved(), 0);
}
