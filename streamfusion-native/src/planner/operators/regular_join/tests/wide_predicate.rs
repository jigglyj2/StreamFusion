// Copyright 2026 StreamFusion Authors
// Licensed under the Apache License, Version 2.0.
use super::coarse_memory::CountingBroker;
use super::*;
use std::sync::atomic::{AtomicUsize, Ordering};

fn join(broker: Arc<CountingBroker>) -> RegularJoinProcessor {
    RegularJoinProcessor::new(
        &plan_contract(
            proto::RegularJoinType::Inner,
            true,
            Some(not_equal_value_condition()),
        ),
        128,
        0,
        127,
        HostMemoryReservation::new(broker, "wide predicate test"),
    )
    .unwrap()
}

#[test]
fn wide_cross_row_and_hot_key_predicates_bound_bytes_without_changing_masks() {
    for side in 0..2 {
        let broker = Arc::new(CountingBroker {
            inner: TestBroker::new(12 << 20),
            calls: AtomicUsize::new(0),
            peak: AtomicUsize::new(0),
        });
        let join = join(broker.clone());
        let a = "a".repeat(16_000);
        let z = "z".repeat(16_000);
        let input = batch(
            &vec![1; 128],
            &(0..128)
                .map(|i| if i % 2 == 0 { a.as_str() } else { z.as_str() })
                .collect::<Vec<_>>(),
            &vec![INSERT; 128],
        );
        let encoded_input = join.row_converters[side]
            .convert_columns(&input.columns()[..2])
            .unwrap();
        let other = batch(&[1, 1], &[&a, &z], &[INSERT; 2]);
        let encoded_other = join.row_converters[1 - side]
            .convert_columns(&other.columns()[..2])
            .unwrap();
        let payloads: [Arc<[u8]>; 2] = [
            Arc::from(encoded_other.row(0).data()),
            Arc::from(encoded_other.row(1).data()),
        ];
        let candidates = |count| {
            (0..count)
                .map(|i| StoredRow {
                    id: i as u64,
                    row: payloads[i % 2].clone(),
                    associations: 0,
                })
                .collect::<Vec<_>>()
        };
        let mut value = JoinState::default();
        if side == 0 {
            value.right = candidates(1);
        } else {
            value.left = candidates(1);
        }
        let state = StagedState {
            key: StateKey {
                key_group: 0,
                key: vec![],
            },
            original: JoinState::default(),
            original_compact: false,
            value,
            touched: false,
        };
        let retained = broker.inner.reserved();
        broker.peak.store(retained, Ordering::Relaxed);
        let mut next = 0;
        let mut pulls = 0;
        while next < input.num_rows() {
            let mut cache = join
                .condition_matches_batch(
                    side,
                    &input,
                    &encoded_input,
                    std::slice::from_ref(&state),
                    &vec![0; 128],
                    next,
                )
                .unwrap();
            assert!(!cache.is_empty());
            while let Some(mask) = cache.pop() {
                assert_eq!(mask.iter().collect::<Vec<_>>(), vec![next % 2 != 0]);
                next += 1;
            }
            pulls += 1;
        }
        assert!(
            pulls > 1,
            "wide pairs are split even below the row-count ceiling"
        );
        assert!(broker.peak.load(Ordering::Relaxed) - retained < 9 << 20);
        assert_eq!(broker.inner.reserved(), retained);
        broker.peak.store(retained, Ordering::Relaxed);
        let hot = candidates(5003);
        let mask = join
            .condition_matches_row(side, &input, 0, encoded_input.row(0).data(), &hot)
            .unwrap();
        for (i, value) in mask.iter().enumerate() {
            assert_eq!(value, i % 2 != 0);
        }
        assert!(broker.peak.load(Ordering::Relaxed) - retained < 9 << 20);
        drop(mask);
        assert_eq!(broker.inner.reserved(), retained);
        drop(join);
        assert_eq!(broker.inner.reserved(), 0);
    }
}

#[test]
fn one_oversized_pair_requires_its_full_credit_but_is_not_rejected_by_the_chunk_quantum() {
    for limit in [8 << 20, 16 << 20] {
        let broker = Arc::new(CountingBroker {
            inner: TestBroker::new(limit),
            calls: AtomicUsize::new(0),
            peak: AtomicUsize::new(0),
        });
        let join = join(broker.clone());
        let input = batch(&[1], &[&"a".repeat(1 << 20)], &[INSERT]);
        let other = batch(&[1], &[&"z".repeat(1 << 20)], &[INSERT]);
        let left = join.row_converters[0]
            .convert_columns(&input.columns()[..2])
            .unwrap();
        let right = join.row_converters[1]
            .convert_columns(&other.columns()[..2])
            .unwrap();
        let candidates = vec![StoredRow {
            id: 1,
            row: Arc::from(right.row(0).data()),
            associations: 0,
        }];
        let result = join.condition_matches_row(0, &input, 0, left.row(0).data(), &candidates);
        if limit == 8 << 20 {
            assert!(matches!(
                result,
                Err(DataFusionError::ResourcesExhausted(_))
            ));
        } else {
            assert_eq!(result.unwrap().iter().collect::<Vec<_>>(), vec![true]);
        }
        drop(join);
        assert_eq!(broker.inner.reserved(), 0);
    }
}
