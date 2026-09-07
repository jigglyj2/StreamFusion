// Copyright 2026 StreamFusion Authors
// Licensed under the Apache License, Version 2.0.

use super::*;
use crate::allocation_test_support::measure;
use crate::execution_context::allocation_tests::wide_plan;
use prost::Message;

#[test]
fn generated_scan_allocates_nothing_and_covers_compact_protobuf_decode() {
    for width in [1, 64, 512, 4096] {
        for nested in [false, true] {
            let (bytes, _) = wide_plan(width, nested, 3);
            let (estimate, scan_heap) = measure(|| PlanMemory::scan(&bytes).unwrap());
            assert_eq!(scan_heap.peak, 0);
            let (decoded, decode_heap) = measure(|| crate::decode_plan(&bytes).unwrap());
            assert!(
                decode_heap.peak <= estimate.decoded().unwrap(),
                "width={width} nested={nested} {decode_heap:?}"
            );
            drop(decoded);
        }
    }
}

#[test]
fn payloads_are_not_mistaken_for_nested_messages_and_unknown_fields_are_skipped() {
    let (bytes, _) = wide_plan(1, false, 1);
    let mut plan = crate::proto::NativePlan::decode(bytes.as_slice()).unwrap();
    plan.root.as_mut().unwrap().metric_name = "x".repeat(8);
    let small = PlanMemory::scan(&plan.encode_to_vec()).unwrap();
    plan.root.as_mut().unwrap().metric_name = "x".repeat(8192);
    let mut bytes = plan.encode_to_vec();
    let large = PlanMemory::scan(&bytes).unwrap();
    assert_eq!(
        large.decoded().unwrap() - small.decoded().unwrap(),
        (8192 - 8) * 2
    );
    assert_eq!(
        large.physical().unwrap() - small.physical().unwrap(),
        (8192 - 8) * 2
    );
    bytes.extend_from_slice(&[0xf8, 0x07, 0x01, 0xfa, 0x07, 0x03, 0xff, 0xff, 0xff]);
    let extended = PlanMemory::scan(&bytes).unwrap();
    assert_eq!(extended.decoded().unwrap(), large.decoded().unwrap());
    crate::proto::NativePlan::decode(bytes.as_slice()).unwrap();
}

#[test]
fn malformed_wire_and_size_overflow_fail_closed() {
    for bytes in [
        vec![0],
        vec![0x80],
        vec![0xff; 11],
        vec![0x12, 0x7f],
        vec![0x0b],
    ] {
        assert!(PlanMemory::scan(&bytes).is_err(), "{bytes:?}");
    }
    assert!(PlanMemory {
        structural: usize::MAX,
        payload: 1
    }
    .decoded()
    .is_err());
    assert!(PlanMemory {
        structural: usize::MAX / 2 + 1,
        payload: 0
    }
    .physical()
    .is_err());
}
