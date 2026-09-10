// Copyright 2026 StreamFusion Authors
// Licensed under the Apache License, Version 2.0.
use super::*;

#[test]
fn shared_append_top_n_preserves_envelopes_and_restores_ordered_state_across_backends() {
    let plugin = std::env::var("STREAMFUSION_TEST_ROCKSDB_PLUGIN").ok();
    for owned in [false, true] {
        for rocks_first in [false, true] {
            if rocks_first && plugin.is_none() {
                continue;
            }
            let mut plan = plan(owned);
            let Some(proto::operator::Operator::Calc(tail)) =
                &mut plan.root.as_mut().unwrap().operator
            else {
                unreachable!()
            };
            let Some(proto::operator::Operator::TopN(top)) =
                &mut tail.input.as_mut().unwrap().operator
            else {
                unreachable!()
            };
            top.rank_end = Some(3);
            let directory = tempfile::tempdir().unwrap();
            let broker = Arc::new(Broker {
                inner: TestBroker::new(64 << 20),
                transfers: AtomicUsize::new(0),
            });
            let owner = HostMemoryReservation::new(broker.clone(), "shared append context");
            let first_path = directory.path().join("first");
            let first = context(
                &plan,
                &resources(if rocks_first {
                    Some((plugin.as_ref().unwrap(), &first_path))
                } else {
                    None
                }),
                &owner,
            );
            let output = run(
                &first,
                input(
                    owned,
                    vec![1; 4],
                    vec!["a", "m", "z", "zz"],
                    vec![Some(10), None, Some(i64::MIN), Some(i64::MAX)],
                ),
            );
            check(
                &output,
                &[INSERT, INSERT, INSERT, DELETE, INSERT],
                &["a", "m", "z", "a", "zz"],
                owned.then_some(vec![
                    Some(10),
                    None,
                    Some(i64::MIN),
                    Some(i64::MAX),
                    Some(i64::MAX),
                ]),
                if owned {
                    &[-1; 5]
                } else {
                    &[17, 18, 19, 20, 20]
                },
            );
            let snapshots = (0..16)
                .map(|g| first.snapshot_state(3, g).unwrap())
                .collect::<Vec<_>>();
            drop(first);
            let second_path = directory.path().join("second");
            let second = context(
                &plan,
                &resources(if !rocks_first && plugin.is_some() {
                    Some((plugin.as_ref().unwrap(), &second_path))
                } else {
                    None
                }),
                &owner,
            );
            for (g, snapshot) in snapshots.iter().enumerate() {
                second.restore_state(3, g as u32, snapshot).unwrap();
            }
            let next = run(
                &second,
                input(
                    owned,
                    vec![1; 3],
                    vec!["a", "m", "zzz"],
                    vec![None, Some(99), Some(-12)],
                ),
            );
            check(
                &next,
                &[DELETE, INSERT],
                &["m", "zzz"],
                owned.then_some(vec![Some(-12); 2]),
                if owned { &[-1; 2] } else { &[19; 2] },
            );
            drop(next);
            drop(snapshots);
            drop(second);
            drop(owner);
            assert!(broker.inner.reserved() > 0);
            assert_eq!(broker.transfers.load(Ordering::Relaxed), 0);
            drop(output);
            assert_eq!(broker.inner.reserved(), 0);
        }
    }
}
