// Copyright 2026 StreamFusion Authors
// Licensed under the Apache License, Version 2.0.

use super::*;
use crate::memory_pool::tests_support::TestBroker;
use std::sync::Arc;

fn snapshot(count: usize, value_bytes: usize) -> Vec<u8> {
    let mut writer =
        streamfusion_state_abi::SnapshotWriter::new(2, count, 16 + count * (12 + value_bytes))
            .unwrap();
    for key in 0..count as u32 {
        writer
            .append(&key.to_be_bytes(), &vec![key as u8; value_bytes])
            .unwrap();
    }
    writer.finish().unwrap()
}

#[test]
fn canonical_rocks_restore_needs_one_write_page_beyond_the_retained_input() {
    let directory = tempfile::tempdir().unwrap();
    let plugin = std::env::var("STREAMFUSION_TEST_ROCKSDB_PLUGIN").unwrap();
    let broker = Arc::new(TestBroker::new(12 << 20));
    let owner = HostMemoryReservation::new(broker.clone(), "canonical test");
    let bytes = snapshot(8192, 1024);
    let mut input = owner.sibling("snapshot and cache");
    input.resize(bytes.len() + (1 << 20)).unwrap();
    let mut destination = RocksPluginKeyedState::open(
        std::path::Path::new(&plugin),
        directory.path(),
        2,
        3,
        1 << 20,
    )
    .unwrap();
    destination.restore_key_group(2, &bytes, &owner).unwrap();
    assert_eq!(broker.reserved(), input.size());
    for start in (0u32..8192).step_by(128) {
        let keys = (start..start + 128)
            .map(u32::to_be_bytes)
            .collect::<Vec<_>>();
        let keys = keys
            .iter()
            .map(|key| StateKeyRef { key_group: 2, key })
            .collect::<Vec<_>>();
        let values = destination.get_batch(&keys, &owner).unwrap();
        for (offset, value) in values.iter().enumerate() {
            assert_eq!(
                value.as_ref().unwrap().as_ref(),
                vec![(start + offset as u32) as u8; 1024]
            );
        }
    }
    assert!(destination
        .restore_key_group(2, &bytes, &owner)
        .unwrap_err()
        .to_string()
        .contains("restored more than once"));
    drop(destination);
    drop(input);
    assert_eq!(broker.reserved(), 0);
}

#[test]
fn paged_canonical_restore_preserves_bytes_and_validates_before_writes_on_memory_backends() {
    let bytes = snapshot(4096, 128);
    for ordered in [false, true] {
        let broker = Arc::new(TestBroker::new(8 << 20));
        let owner = HostMemoryReservation::new(broker.clone(), "canonical memory test");
        let mut state: Box<dyn KeyedState> = if ordered {
            Box::new(OrderedMemoryKeyedState::new(2, 3, owner.sibling("state")).unwrap())
        } else {
            Box::new(MemoryKeyedState::new(2, 3, owner.sibling("state")).unwrap())
        };
        let baseline = broker.reserved();
        assert!(state
            .restore_key_group(2, &bytes[..bytes.len() - 1], &owner)
            .is_err());
        assert_eq!(broker.reserved(), baseline);
        state.restore_key_group(2, &bytes, &owner).unwrap();
        assert_eq!(
            &*state.snapshot_key_group(2, &owner).unwrap(),
            bytes.as_slice()
        );
        drop(state);
        assert_eq!(broker.reserved(), 0);
    }
}
