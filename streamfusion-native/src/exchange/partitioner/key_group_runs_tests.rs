// Copyright 2026 StreamFusion Authors
// Licensed under the Apache License, Version 2.0.
use super::*;
use arrow::array::{ArrayRef, Int32Array};

#[test]
fn recovery_runs_preserve_full_input_order_and_share_sliced_payloads() {
    let keys = Arc::new(Int32Array::from(vec![99, 1, 1, 2, 1, 2, 2, 99]));
    let values = Arc::new(Int32Array::from_iter_values(0..8));
    let original = values.values().as_ptr();
    let batch = RecordBatch::try_from_iter(vec![
        ("key", keys as ArrayRef),
        ("ordinal", values as ArrayRef),
    ])
    .unwrap()
    .slice(1, 6);
    let runs = route_batch_by_key_group(batch, &[(0, KeyField::Integer)], 128).unwrap();
    assert_eq!(
        runs.iter().map(|run| run.len).collect::<Vec<_>>(),
        vec![2, 1, 1, 2]
    );
    assert_eq!(runs[0].key_group(), runs[2].key_group());
    assert_ne!(runs[0].key_group(), runs[1].key_group());
    let mut all = Vec::new();
    for run in runs {
        let batch = run.materialize().unwrap();
        let values = batch
            .column(1)
            .as_any()
            .downcast_ref::<Int32Array>()
            .unwrap();
        assert_eq!(
            values.values().as_ptr(),
            original.wrapping_add(1 + run.start)
        );
        all.extend_from_slice(values.values());
    }
    assert_eq!(all, vec![1, 2, 3, 4, 5, 6]);
}

#[test]
fn recurring_key_groups_can_produce_more_frames_than_max_parallelism() {
    let keys = Arc::new(Int32Array::from_iter_values(
        (0..1000).map(|row| 1 + row % 2),
    ));
    let batch = RecordBatch::try_from_iter(vec![("key", keys as ArrayRef)]).unwrap();
    let runs = route_batch_by_key_group(batch, &[(0, KeyField::Integer)], 128).unwrap();
    assert_eq!(runs.len(), 1000);
    assert!(runs.iter().all(|run| run.len == 1));
}
