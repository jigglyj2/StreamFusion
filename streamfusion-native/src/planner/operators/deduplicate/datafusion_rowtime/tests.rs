// Copyright 2026 StreamFusion Authors
// Licensed under the Apache License, Version 2.0
use super::*;
use crate::memory_pool::tests_support::TestBroker;
use crate::state::StateValue;

#[test]
fn interleaved_keys_keep_every_improvement_and_apply_flinks_asymmetric_ties() {
    // Key 1 has restored time 10; key 2 is new. Input order is deliberately not sorted.
    let key_bytes = [vec![1], vec![2]];
    let group_indices = [0, 1, 0, 1, 0, 1, 0, 1, 0, 1];
    let keys = group_indices.map(|index| StateKeyRef {
        key_group: 7,
        key: &key_bytes[index],
    });
    let encoded = encode_value(10, None);
    let existing =
        group_indices.map(|index| (index == 0).then_some(StateValue::Borrowed(&encoded)));
    let times: ArrayRef = Arc::new(TimestampMillisecondArray::from(vec![
        10, 30, 11, 30, 9, 29, 12, 31, 12, 28,
    ]));
    let broker = Arc::new(TestBroker::new(1 << 20));
    let owner = HostMemoryReservation::new(broker.clone(), "cumulative timestamp test");
    for (last, expected) in [
        (
            true,
            vec![
                true, true, true, true, false, false, true, true, true, false,
            ],
        ),
        (
            false,
            vec![
                false, true, false, false, true, true, false, false, false, true,
            ],
        ),
    ] {
        let result = winners(&times, &keys, &existing, last, &owner).unwrap();
        assert_eq!(result.winners, expected);
        assert!(broker.reserved() > 0);
        drop(result);
        assert_eq!(broker.reserved(), 0);
    }
}

#[test]
fn extrema_do_not_need_sentinels_and_empty_input_releases_workspace() {
    let key = StateKeyRef {
        key_group: 0,
        key: &[1],
    };
    let times: ArrayRef = Arc::new(TimestampMillisecondArray::from(vec![
        i64::MIN,
        i64::MAX,
        i64::MIN,
    ]));
    let broker = Arc::new(TestBroker::new(1 << 20));
    let owner = HostMemoryReservation::new(broker.clone(), "timestamp extremes");
    for (last, expected) in [
        (true, vec![true, true, false]),
        (false, vec![true, false, false]),
    ] {
        let result = winners(&times, &[key; 3], &[None, None, None], last, &owner).unwrap();
        assert_eq!(result.winners, expected);
    }
    let empty: ArrayRef = Arc::new(TimestampMillisecondArray::from(Vec::<i64>::new()));
    assert!(winners(&empty, &[], &[], true, &owner)
        .unwrap()
        .winners
        .is_empty());
    assert_eq!(broker.reserved(), 0);
}

#[test]
fn large_order_workspace_is_admitted_before_computation_and_released_on_invalid_time() {
    let key = StateKeyRef {
        key_group: 0,
        key: &[1],
    };
    let broker = Arc::new(TestBroker::new(1 << 20));
    let owner = HostMemoryReservation::new(broker.clone(), "bounded timestamp workspace");
    let times: ArrayRef = Arc::new(TimestampMillisecondArray::from(vec![1; 4096]));
    let keys = vec![key; 4096];
    let existing = vec![None; 4096];
    assert!(matches!(
        winners(&times, &keys, &existing, true, &owner),
        Err(DataFusionError::ResourcesExhausted(_))
    ));
    assert_eq!(broker.reserved(), 0);
    let null: ArrayRef = Arc::new(TimestampMillisecondArray::from(vec![None::<i64>]));
    assert!(winners(&null, &[key], &[None], true, &owner).is_err());
    assert_eq!(broker.reserved(), 0);
}
