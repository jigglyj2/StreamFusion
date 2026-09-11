// Copyright 2026 StreamFusion Authors
// Licensed under the Apache License, Version 2.0.

use super::*;
use crate::memory_pool::{tests_support::TestBroker, HostMemoryReservation};
use datafusion::error::DataFusionError;
use std::sync::Arc;

#[test]
fn admitted_ordered_pages_stop_release_and_resume_on_both_backends() {
    const LIMIT: usize = 16 << 20;
    let plugin = std::env::var("STREAMFUSION_TEST_ROCKSDB_PLUGIN").unwrap();
    let directory = tempfile::tempdir().unwrap();
    for rocks in [false, true] {
        let broker = Arc::new(TestBroker::new(LIMIT));
        let owner = HostMemoryReservation::new(broker.clone(), "admitted range test");
        let mut state: Box<dyn KeyedState> = if rocks {
            Box::new(
                RocksPluginKeyedState::open_for_owner(
                    Path::new(&plugin),
                    directory.path(),
                    2,
                    3,
                    1 << 20,
                    &owner,
                )
                .unwrap(),
            )
        } else {
            Box::new(OrderedMemoryKeyedState::new(2, 3, owner.sibling("state")).unwrap())
        };
        state
            .write_batch(
                (0..20u8)
                    .rev()
                    .map(|key| StateMutation {
                        key: StateKey {
                            key_group: 2,
                            key: vec![key],
                        },
                        value: Some(vec![key; if key == 7 { 32 << 10 } else { 128 }]),
                    })
                    .chain(std::iter::once(StateMutation {
                        key: StateKey {
                            key_group: 3,
                            key: vec![7],
                        },
                        value: Some(vec![255]),
                    }))
                    .collect(),
            )
            .unwrap();
        let baseline = broker.reserved();

        // Exhaust the remaining budget inside the first page. Fetching another RocksDB page
        // after the visitor stops would fail admission; stopping must release all page ownership.
        let mut blocker = owner.sibling("following page must not be fetched");
        let mut calls = 0;
        state
            .visit_range_admitted(2, &[4], Some(&[12]), 3, 1024, &owner, &mut |page| {
                calls += 1;
                assert_eq!(page[0].0, &[4]);
                blocker.resize(LIMIT - broker.reserved())?;
                Ok(false)
            })
            .unwrap();
        assert_eq!(calls, 1);
        drop(blocker);
        assert_eq!(broker.reserved(), baseline);

        let mut next = vec![4];
        let mut seen = Vec::new();
        loop {
            let mut found = false;
            let start = next.clone();
            state
                .visit_range_admitted(2, &start, Some(&[12]), 3, 1024, &owner, &mut |page| {
                    assert!(page.len() <= 3);
                    for (key, value) in page {
                        assert_eq!(value.len(), if key[0] == 7 { 32 << 10 } else { 128 });
                        assert!(value.iter().all(|byte| *byte == key[0]));
                        seen.push(key[0]);
                    }
                    // Appending zero forms an exclusive continuation for these complete byte keys.
                    next = page.last().unwrap().0.to_vec();
                    next.push(0);
                    found = true;
                    Ok(false)
                })
                .unwrap();
            assert_eq!(broker.reserved(), baseline);
            if !found {
                break;
            }
        }
        assert_eq!(seen, (4..12).collect::<Vec<_>>());

        let error = state
            .visit_range_admitted(2, &[7], Some(&[8]), 3, 1024, &owner, &mut |_| {
                Err(DataFusionError::Execution(
                    "injected visitor failure".into(),
                ))
            })
            .unwrap_err();
        assert!(error.to_string().contains("injected visitor failure"));
        assert_eq!(broker.reserved(), baseline);
        state
            .visit_range_admitted(2, &[12], Some(&[4]), 3, 1024, &owner, &mut |_| {
                panic!("reversed range must be empty")
            })
            .unwrap();
        drop(state);
        drop(owner);
        assert_eq!(broker.reserved(), 0);
    }
}
