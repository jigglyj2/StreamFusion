// Copyright 2026 StreamFusion Authors
// Licensed under the Apache License, Version 2.0.

use super::*;

#[test]
fn prepared_history_preserves_null_filtered_and_null_safe_changelogs() {
    for filter_nulls in [false, true] {
        let spill = tempfile::tempdir().unwrap();
        let broker = Arc::new(TestBroker::new(128 << 20));
        let owner = HostMemoryReservation::new(broker.clone(), "prepared null key parity");
        let contract = plan_with_filter(proto::RegularJoinType::Inner, filter_nulls);
        let mut actual =
            RegularJoinProcessor::new(&contract, 128, 0, 127, owner.sibling("prepared")).unwrap();
        let mut reference =
            RegularJoinProcessor::new(&contract, 128, 0, 127, owner.sibling("resident")).unwrap();
        let value = "x".repeat(64);
        let seed = nullable_batch(
            &vec![None; 10001],
            &vec![value.as_str(); 10001],
            &vec![INSERT; 10001],
        );
        reference.process_arrow(0, seed.clone()).unwrap();
        actual.begin_streaming_batch(0, seed).unwrap();
        assert!(actual.next_streaming_batch().unwrap().is_none());
        actual
            .set_spill_resources(Some(
                crate::spill::Resources::new(vec![spill.path().to_path_buf()]).unwrap(),
            ))
            .unwrap();
        let input = nullable_batch(
            &[None; 4],
            &["right"; 4],
            &[INSERT, UPDATE_AFTER, DELETE, UPDATE_BEFORE],
        );
        let expected = reference.process_arrow(1, input.clone()).unwrap();
        let mut pressure = owner.sibling("competing consumer");
        pressure
            .resize((128 << 20) - broker.reserved() - (2 << 20))
            .unwrap();
        actual.begin_streaming_batch(1, input).unwrap();
        assert!(actual
            .streaming_cursor
            .as_ref()
            .unwrap()
            .uses_spilled_history());
        let mut offset = 0;
        while let Some(batch) = actual.next_streaming_batch().unwrap() {
            assert_eq!(batch, expected.slice(offset, batch.num_rows()));
            offset += batch.num_rows();
        }
        assert_eq!(offset, if filter_nulls { 0 } else { 40004 });
        drop(pressure);
        for group in 0..128 {
            assert_eq!(
                actual.snapshot_key_group(group).unwrap(),
                reference.snapshot_key_group(group).unwrap()
            );
        }
        drop((actual, reference, expected, owner));
        assert_eq!(broker.reserved(), 0);
    }
}
