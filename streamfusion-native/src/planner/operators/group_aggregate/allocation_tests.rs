// Copyright 2026 StreamFusion Authors
// Licensed under the Apache License, Version 2.0

use super::accumulator::estimated_value_map_bytes;
use super::*;
use crate::allocation_test_support::measure;

#[test]
fn counted_value_map_admission_covers_measured_sparse_and_dense_heap() {
    for count in [0, 1, 2, 11, 12, 64, 256, 4096] {
        for strings in [false, true] {
            let (map, allocations) = measure(|| {
                let mut map = BTreeMap::new();
                for index in 0..count {
                    let value = if strings {
                        AggregateValue::Bytes(
                            format!("{index:08}-{}", "é".repeat(128)).into_bytes(),
                        )
                    } else {
                        AggregateValue::Int(index as i128)
                    };
                    map.insert(value, 1);
                }
                map
            });
            let admitted = estimated_value_map_bytes(&map);
            assert!(allocations.live >= 0);
            assert!(
                allocations.live as usize <= admitted,
                "count={count} strings={strings} measured={allocations:?} admitted={admitted}"
            );
        }
    }
}

#[test]
fn counted_value_map_admission_covers_retained_nodes_after_retractions() {
    for count in [1, 12, 256, 4096] {
        for remaining in [0, 1, count / 2] {
            let (map, allocations) = measure(|| {
                let mut map = BTreeMap::new();
                for index in 0..count {
                    map.insert(AggregateValue::Int(index), 1);
                }
                for index in remaining..count {
                    map.remove(&AggregateValue::Int(index));
                }
                map
            });
            let admitted = estimated_value_map_bytes(&map);
            assert!(allocations.live >= 0);
            assert!(
                allocations.live as usize <= admitted,
                "count={count} remaining={remaining} measured={allocations:?} admitted={admitted}"
            );
        }
    }
}
