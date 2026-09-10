// Copyright 2026 StreamFusion Authors
// Licensed under the Apache License, Version 2.0.

use super::*;

fn string_plan() -> Vec<u8> {
    let mut native = proto::NativePlan::decode(plan(false, true).as_slice()).unwrap();
    let Some(proto::operator::Operator::GroupAggregate(group)) =
        native.root.as_mut().unwrap().operator.as_mut()
    else {
        unreachable!()
    };
    group.aggregate_calls.remove(1); // COUNT(*), MIN(v), MAX(v).
    for call in &mut group.aggregate_calls[1..] {
        call.input_type = Some(logical_varchar(true));
        call.output_type = Some(logical_varchar(true));
    }
    native.encode_to_vec()
}

fn strings(rows: usize, value: &str) -> RecordBatch {
    RecordBatch::try_from_iter(vec![
        ("key", Arc::new(Int64Array::from(vec![7; rows])) as ArrayRef),
        (
            "value",
            Arc::new(StringArray::from(vec![Some(value); rows])) as ArrayRef,
        ),
    ])
    .unwrap()
}

#[test]
fn historical_string_outputs_are_admitted_before_state_changes_on_both_backends() {
    for rocks in [false, true] {
        let plugin = std::env::var("STREAMFUSION_TEST_ROCKSDB_PLUGIN").ok();
        if rocks && plugin.is_none() {
            continue;
        }
        let directory = tempfile::tempdir().unwrap();
        let broker = Arc::new(TestBroker::new(32 << 20));
        let owner = HostMemoryReservation::new(broker.clone(), "historical string extrema");
        let plan = string_plan();
        let mut processor = if rocks {
            GroupAggregateProcessor::new_rocksdb(
                &plan,
                16,
                0,
                15,
                std::path::Path::new(plugin.as_ref().unwrap()),
                directory.path(),
                2 << 20,
                owner,
            )
        } else {
            GroupAggregateProcessor::new(&plan, 16, 0, 15, owner)
        }
        .unwrap();
        assert!(datafusion_compute::Kernels::new(&processor.calls)
            .unwrap()
            .0
            .iter()
            .all(Option::is_some));
        let maximum = "z".repeat(64 << 10);
        let seed = strings(1, &maximum);
        drop(processor.process_arrow(seed.clone()).unwrap());
        let key = processor.state_key(&seed, 0).unwrap();
        let before = processor.snapshot_key_group(key.key_group).unwrap();
        // A tiny input can repeat a large historical result hundreds of times. Input
        // buffer sizing alone must not allow that output to exceed the Flink allowance.
        assert!(processor
            .process_arrow(strings(512, "a"))
            .unwrap_err()
            .to_string()
            .contains("Flink denied"));
        assert_eq!(processor.snapshot_key_group(key.key_group).unwrap(), before);
        let output = processor.process_arrow(strings(16, "a")).unwrap();
        assert_eq!(output.num_rows(), 32);
        let minima = output
            .column(2)
            .as_any()
            .downcast_ref::<StringArray>()
            .unwrap();
        let maxima = output
            .column(3)
            .as_any()
            .downcast_ref::<StringArray>()
            .unwrap();
        assert_eq!(minima.value(0), maximum);
        for row in 0..32 {
            assert_eq!(maxima.value(row), maximum);
            if row != 0 {
                assert_eq!(minima.value(row), "a");
            }
        }
        drop(output);
        drop(processor);
        // The plugin snapshot can also retain bounded page/control headroom.
        assert!(broker.reserved() >= before.len());
        drop(before);
        assert_eq!(broker.reserved(), 0);
    }
}

#[test]
fn large_incoming_extrema_cannot_allocate_unadmitted_retained_state() {
    let broker = Arc::new(TestBroker::new(8 << 20));
    let mut processor = GroupAggregateProcessor::new(
        &string_plan(),
        16,
        0,
        15,
        HostMemoryReservation::new(broker.clone(), "large incoming string"),
    )
    .unwrap();
    let input = strings(1, &"z".repeat(2 << 20));
    assert!(processor
        .process_arrow(input)
        .unwrap_err()
        .to_string()
        .contains("Flink denied"));
    for key_group in 0..16 {
        let snapshot = processor.snapshot_key_group(key_group).unwrap();
        assert_eq!(
            streamfusion_state_abi::validate_key_group_snapshot(key_group, &snapshot).unwrap(),
            0
        );
    }
    drop(processor);
    assert_eq!(broker.reserved(), 0);
}
