// Copyright 2026 StreamFusion Authors
// Licensed under the Apache License, Version 2.0.
use super::*;
use crate::memory_pool::tests_support::TestBroker;
use arrow::array::StringArray;
use arrow_row::SortField;

#[test]
fn a_wide_winner_is_not_replicated_for_every_later_arrival() {
    let input = Arc::new(
        RecordBatch::try_from_iter(vec![(
            "value",
            Arc::new(StringArray::from_iter_values((0..4096).map(|row| {
                if row == 0 {
                    format!("a{}", "x".repeat(65536))
                } else {
                    "z".to_owned()
                }
            }))) as ArrayRef,
        )])
        .unwrap(),
    );
    let restored = Arc::new(
        RecordBatch::try_from_iter(vec![(
            "value",
            Arc::new(StringArray::from(vec!["b"])) as ArrayRef,
        )])
        .unwrap(),
    );
    let codec = RowConverter::new(vec![SortField::new(DataType::Utf8)]).unwrap();
    let sources = CandidateSources {
        orders: Some(vec![
            codec.convert_columns(input.columns()).unwrap(),
            codec.convert_columns(restored.columns()).unwrap(),
        ]),
        batches: vec![input, restored],
    };
    let groups = vec![GroupWork {
        state_key: top_n_state_key(0, &[]),
        next_sequence: 1,
        rank_end: Some(1),
        candidates: vec![CandidateRef {
            source: 1,
            row: 0,
            sequence: 0,
            kind: INSERT,
        }],
    }];
    let broker = Arc::new(TestBroker::new(4 << 20));
    let owner = HostMemoryReservation::new(broker.clone(), "Top-1 wide winner test");
    let row_groups = vec![0; 4096];
    let (selected, observed) = crate::allocation_test_support::measure(|| {
        winners(&sources, &groups, &row_groups, &owner).unwrap()
    });
    assert!(selected[0]);
    assert!(selected[1..].iter().all(|value| !value));
    // A Binary cumulative MIN would repeat the 64 KiB winner over 4,096 outputs (>256 MiB).
    assert!(observed.peak < 4 << 20, "{}", observed.peak);
    drop(owner);
    assert_eq!(broker.reserved(), 0);
}
