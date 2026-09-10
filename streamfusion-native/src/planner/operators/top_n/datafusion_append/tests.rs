// Copyright 2026 StreamFusion Authors
// Licensed under the Apache License, Version 2.0.
use super::*;
use crate::memory_pool::tests_support::TestBroker;
use arrow::array::StringArray;

fn sources(input: RecordBatch, restored: RecordBatch) -> CandidateSources {
    let keys = crate::planner::operators::sortable_state::SortKeys::new(
        input.schema().as_ref(),
        &[0, 1],
        &[false, true],
        &[false, true],
    )
    .unwrap()
    .unwrap();
    let batches = vec![Arc::new(input), Arc::new(restored)];
    let orders = Some(
        batches
            .iter()
            .map(|batch| keys.encode(batch.columns()).unwrap())
            .collect(),
    );
    CandidateSources { batches, orders }
}

fn batch(numbers: Vec<Option<i64>>, strings: Vec<&str>) -> RecordBatch {
    RecordBatch::try_from_iter(vec![
        ("number", Arc::new(Int64Array::from(numbers)) as ArrayRef),
        ("text", Arc::new(StringArray::from(strings)) as ArrayRef),
    ])
    .unwrap()
}

fn group(candidates: Vec<CandidateRef>) -> GroupWork {
    GroupWork {
        state_key: top_n_state_key(0, &[]),
        next_sequence: 100,
        rank_end: None,
        candidates,
    }
}

#[test]
fn every_prefix_matches_stable_flink_order_with_nulls_extrema_and_restored_ties() {
    let values = [None, Some(i64::MIN), Some(i64::MAX), Some(1), Some(1)];
    let input = batch(
        (0..128).map(|i| values[i % values.len()]).collect(),
        (0..128)
            .map(|i| if i % 3 == 0 { "a" } else { "b" })
            .collect(),
    );
    let restored = batch(vec![Some(1), None, Some(1), None], vec!["a", "b", "a", "b"]);
    let sources = sources(input, restored);
    let row_groups = (0..128).map(|i| i % 2).collect::<Vec<_>>();
    for limit in [2, 3, 10, 64, 256] {
        let mut groups = vec![
            group(vec![
                CandidateRef {
                    source: 1,
                    row: 0,
                    sequence: 3,
                    kind: INSERT,
                },
                CandidateRef {
                    source: 1,
                    row: 1,
                    sequence: 2,
                    kind: INSERT,
                },
            ]),
            group(vec![
                CandidateRef {
                    source: 1,
                    row: 2,
                    sequence: 3,
                    kind: INSERT,
                },
                CandidateRef {
                    source: 1,
                    row: 3,
                    sequence: 2,
                    kind: INSERT,
                },
            ]),
        ];
        let broker = Arc::new(TestBroker::new(8 << 20));
        let owner = HostMemoryReservation::new(broker.clone(), "append selection test");
        let selection = Selection::new(&sources, &groups, &row_groups, &owner).unwrap();
        let compare = |left: &CandidateRef, right: &CandidateRef| {
            let number = |c: &CandidateRef| {
                let array = sources[c.source]
                    .column(0)
                    .as_any()
                    .downcast_ref::<Int64Array>()
                    .unwrap();
                (!array.is_null(c.row)).then(|| array.value(c.row))
            };
            // SQL DESC NULLS FIRST, then text ASC, then persisted arrival sequence.
            let order = match (number(left), number(right)) {
                (None, None) => Ordering::Equal,
                (None, _) => Ordering::Less,
                (_, None) => Ordering::Greater,
                (Some(l), Some(r)) => r.cmp(&l),
            };
            let text = |c: &CandidateRef| {
                sources[c.source]
                    .column(1)
                    .as_any()
                    .downcast_ref::<StringArray>()
                    .unwrap()
                    .value(c.row)
            };
            order
                .then_with(|| text(left).cmp(text(right)))
                .then_with(|| left.sequence.cmp(&right.sequence))
        };
        let mut expected = groups
            .iter()
            .map(|g| g.candidates.clone())
            .collect::<Vec<_>>();
        for (g, reference) in groups.iter_mut().zip(&mut expected) {
            selection.retain(&mut g.candidates, limit).unwrap();
            reference.sort_by(compare);
            assert_eq!(&g.candidates, reference);
        }
        for (row, &g) in row_groups.iter().enumerate() {
            let candidate = CandidateRef {
                source: 0,
                row,
                sequence: groups[g].next_sequence,
                kind: INSERT,
            };
            groups[g].next_sequence += 1;
            expected[g].push(candidate);
            expected[g].sort_by(compare);
            expected[g].truncate(limit);
            selection
                .insert(&mut groups[g].candidates, candidate, limit)
                .unwrap();
            assert_eq!(
                groups[g].candidates, expected[g],
                "limit={limit}, row={row}"
            );
        }
        assert!(broker.reserved() > 0);
        drop(selection);
        assert_eq!(broker.reserved(), 0);
    }
}

#[test]
fn wide_retained_keys_are_not_copied_by_each_arrivals_sort() {
    let wide = "x".repeat(64 << 10);
    let input = batch(vec![Some(1); 1024], vec!["later"; 1024]);
    let restored = batch(vec![Some(2); 3], vec![&wide; 3]);
    let sources = sources(input, restored);
    let groups = vec![group(
        (0..3)
            .map(|row| CandidateRef {
                source: 1,
                row,
                sequence: row as u64,
                kind: INSERT,
            })
            .collect(),
    )];
    let broker = Arc::new(TestBroker::new(4 << 20));
    let owner = HostMemoryReservation::new(broker.clone(), "wide append selection");
    let (_, allocation) = crate::allocation_test_support::measure(|| {
        let selection = Selection::new(&sources, &groups, &vec![0; 1024], &owner).unwrap();
        let mut retained = groups[0].candidates.clone();
        for row in 0..1024 {
            selection
                .insert(
                    &mut retained,
                    CandidateRef {
                        source: 0,
                        row,
                        sequence: 100 + row as u64,
                        kind: INSERT,
                    },
                    3,
                )
                .unwrap();
            assert_eq!(retained, groups[0].candidates);
        }
    });
    assert!(allocation.peak < 4 << 20, "peak {}", allocation.peak);
    assert_eq!(broker.reserved(), 0);
}

#[test]
fn preparation_denial_and_sequence_overflow_release_workspace() {
    let sources = sources(batch(vec![Some(1)], vec!["a"]), batch(vec![], vec![]));
    let mut groups = vec![group(vec![])];
    let broker = Arc::new(TestBroker::new(1024));
    let owner = HostMemoryReservation::new(broker.clone(), "denied selection");
    assert!(Selection::new(&sources, &groups, &[0], &owner).is_err());
    assert_eq!(broker.reserved(), 0);
    let broker = Arc::new(TestBroker::new(1 << 20));
    let owner = HostMemoryReservation::new(broker.clone(), "overflow selection");
    groups[0].next_sequence = u64::MAX;
    assert!(Selection::new(&sources, &groups, &[0], &owner).is_err());
    assert_eq!(broker.reserved(), 0);
}
