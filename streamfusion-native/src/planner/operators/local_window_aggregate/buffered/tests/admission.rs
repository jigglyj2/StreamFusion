// Copyright 2026 StreamFusion Authors
// Licensed under the Apache License, Version 2.0.

use super::*;
use crate::memory_pool::tests_support::TestBroker;

#[test]
fn compute_adapts_to_available_memory_without_emitting_early_partials() {
    let broker = Arc::new(TestBroker::new(64 << 20));
    let mut target = buffer_with_broker(broker.clone());
    let mut reference = buffer();
    let initial = (0..5000).map(|key| (key, 1000)).collect::<Vec<_>>();
    assert!(target.push(input(&target, &initial)).unwrap().is_none());
    assert!(reference
        .push(input(&reference, &initial))
        .unwrap()
        .is_none());
    // More than one normal compute segment, with new groups and repeated keys.
    let rows = (0..4096)
        .map(|key| (key % 3072 + 4000, 1001))
        .collect::<Vec<_>>();
    let batch = input(&target, &rows);
    let large = target.input_workspace(&batch, COMPUTE_ROWS).unwrap();
    let small = target.input_workspace(&batch, 1).unwrap();
    assert!(small < large);
    let mut pressure = HostMemoryReservation::new(broker.clone(), "other operators");
    let remaining = small + (large - small) / 2;
    pressure
        .resize(pressure.available_capacity().unwrap().unwrap() - remaining)
        .unwrap();
    // The old fixed-size request demonstrably fails before any input is consumed.
    assert!(target.kernel.reservation.resize(large).is_err());
    assert!(target.push(batch).unwrap().is_none());
    assert!(!target.has_pending());
    assert!(reference.push(input(&reference, &rows)).unwrap().is_none());
    drop(pressure);
    let first = target.control(ControlEvent::BeforeCheckpoint(1)).unwrap();
    let actual = drain(&mut target, first);
    let first = reference
        .control(ControlEvent::BeforeCheckpoint(1))
        .unwrap();
    assert_eq!(actual, drain(&mut reference, first));
    assert_eq!(actual.iter().map(|row| row.1).sum::<i64>(), 9096);
    drop(target);
    assert_eq!(broker.reserved(), 0);
}

#[test]
fn one_row_budget_denial_does_not_mutate_the_flink_buffer() {
    let broker = Arc::new(TestBroker::new(64 << 20));
    let mut target = buffer_with_broker(broker.clone());
    let batch = input(&target, &[(7, 1000)]);
    let mut pressure = HostMemoryReservation::new(broker.clone(), "other operators");
    pressure
        .resize(pressure.available_capacity().unwrap().unwrap())
        .unwrap();
    assert!(target
        .push(batch)
        .unwrap_err()
        .to_string()
        .contains("Flink denied"));
    assert!(target.order.is_empty());
    assert!(!target.has_pending());
    drop(pressure);
    drop(target);
    assert_eq!(broker.reserved(), 0);
}
