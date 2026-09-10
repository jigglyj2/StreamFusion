// Copyright 2026 StreamFusion Authors
// Licensed under the Apache License, Version 2.0.

use super::super::region_input::{normalize, schema};
use super::*;

#[test]
fn forwarded_routing_buffers_need_no_second_reservation() {
    let payload = "x".repeat(1 << 20);
    let input = batch(&[1, 2, 3], &[payload.as_str(); 3], &[INSERT; 3]);
    let canonical = schema(&Arc::new(Schema::new(
        input.schema().fields()[..2].to_vec(),
    )));
    let build_broker = Arc::new(TestBroker::new(64 << 10));
    let mut generated = HostMemoryReservation::new(build_broker, "key producer");
    let keyed = normalize(input, canonical.clone(), &[0], &mut generated).unwrap();
    for input in [keyed.clone(), keyed.slice(1, 1)] {
        let broker = Arc::new(TestBroker::new(0));
        let mut memory = HostMemoryReservation::new(broker.clone(), "forwarded keys");
        let output = normalize(input.clone(), canonical.clone(), &[0], &mut memory).unwrap();
        for (input, output) in input.columns().iter().zip(output.columns()) {
            assert!(Arc::ptr_eq(input, output));
        }
        assert_eq!(broker.reserved(), 0);
    }
}

#[test]
fn generated_keys_reserve_only_key_spans_and_preserve_exact_bytes() {
    for key_columns in [vec![], vec![0], vec![1], vec![0, 1]] {
        let text = "é".repeat(2048);
        let input = batch(&[1, 2, 3, 4], &[&text, "", "a", &text], &[INSERT; 4]);
        for input in [input.clone(), input.slice(1, 2)] {
            let canonical = schema(&Arc::new(Schema::new(
                input.schema().fields()[..2].to_vec(),
            )));
            let broker = Arc::new(TestBroker::new(256 << 10));
            let mut memory = HostMemoryReservation::new(broker.clone(), "generated keys");
            let (output, observed) = crate::allocation_test_support::measure(|| {
                normalize(input.clone(), canonical, &key_columns, &mut memory).unwrap()
            });
            assert!(
                observed.peak <= broker.reserved(),
                "{} > {}",
                observed.peak,
                broker.reserved()
            );
            assert!(Arc::ptr_eq(input.column(0), output.column(0)));
            assert!(Arc::ptr_eq(input.column(1), output.column(1)));
            let fields = key_columns
                .iter()
                .map(|&index| {
                    (
                        index as usize,
                        KeyField::from_arrow_type(input.column(index as usize).data_type())
                            .unwrap(),
                    )
                })
                .collect::<Vec<_>>();
            let keys = output
                .column(3)
                .as_any()
                .downcast_ref::<arrow::array::BinaryArray>()
                .unwrap();
            for row in 0..input.num_rows() {
                let expected = if fields.is_empty() {
                    vec![]
                } else {
                    encode_binary_row(&input, row, &fields).unwrap()
                };
                assert_eq!(keys.value(row), expected);
            }
            drop(output);
            drop(memory);
            assert_eq!(broker.reserved(), 0);
        }
    }
}

#[test]
fn key_generation_still_requires_budget_and_invalid_metadata_is_rejected() {
    let input = batch(&[1], &["payload"], &[INSERT]);
    let canonical = schema(&Arc::new(Schema::new(
        input.schema().fields()[..2].to_vec(),
    )));
    let broker = Arc::new(TestBroker::new(0));
    let mut memory = HostMemoryReservation::new(broker.clone(), "denied keys");
    assert!(matches!(
        normalize(input.clone(), canonical.clone(), &[0], &mut memory),
        Err(DataFusionError::ResourcesExhausted(_))
    ));
    let invalid = RecordBatch::try_from_iter([
        ("k", input.column(0).clone()),
        ("v", input.column(1).clone()),
        ("__streamfusion_row_kind", input.column(2).clone()),
        (
            "__streamfusion_key",
            Arc::new(Int64Array::from(vec![1])) as ArrayRef,
        ),
    ])
    .unwrap();
    assert!(matches!(
        normalize(invalid, canonical, &[0], &mut memory),
        Err(DataFusionError::Execution(_))
    ));
    assert_eq!(broker.reserved(), 0);
}
