// Copyright 2026 StreamFusion Authors
// Licensed under the Apache License, Version 2.0

use super::*;
use crate::memory_pool::tests_support::TestBroker;

#[test]
fn bounded_transitions_match_original_changelog_and_state_for_every_join_kind() {
    let broker = Arc::new(TestBroker::new(1 << 20));
    let owner = HostMemoryReservation::new(broker.clone(), "transition oracle");
    for join_type in [
        proto::RegularJoinType::Inner,
        proto::RegularJoinType::Left,
        proto::RegularJoinType::Right,
        proto::RegularJoinType::Full,
        proto::RegularJoinType::Semi,
        proto::RegularJoinType::Anti,
    ] {
        for side in 0..2 {
            for kind in [INSERT, UPDATE_BEFORE, UPDATE_AFTER, DELETE] {
                for mask_kind in 0..3 {
                    let rows = || {
                        (0..31)
                            .map(|index| StoredRow {
                                id: index as u64,
                                row: Arc::from(&[index % 5][..]),
                                associations: [0, 1, 2, i32::MAX, i32::MIN][index as usize % 5],
                            })
                            .collect()
                    };
                    let initial = JoinState {
                        left: rows(),
                        right: rows(),
                        ..Default::default()
                    };
                    let matches = match mask_kind {
                        0 => CandidateMatches::constant(31, false, &owner),
                        1 => CandidateMatches::constant(31, true, &owner),
                        _ => CandidateMatches::from_values(
                            (0..31).map(|i| i % 3 == 0).collect(),
                            &owner,
                        ),
                    };
                    let input: Arc<[u8]> = Arc::from(&[2][..]);
                    let accumulate = kind == INSERT || kind == UPDATE_AFTER;
                    let mut expected_state = initial.clone();
                    let mut expected_output = Vec::new();
                    transitions::reference_process_change(
                        join_type,
                        side,
                        kind,
                        accumulate,
                        &matches,
                        input.clone(),
                        19,
                        &mut expected_state,
                        &mut expected_output,
                    )
                    .unwrap();
                    for limit in [2, 3, 7, 4096] {
                        let mut actual_state = initial.clone();
                        let mut actual_output = Vec::new();
                        let mut cursor = change_cursor::ChangeCursor::new(
                            join_type,
                            side,
                            kind,
                            accumulate,
                            input.clone(),
                            19,
                        );
                        for iteration in 0..100 {
                            let mut chunk = Vec::new();
                            let done = cursor
                                .drain(&mut actual_state, &matches, &mut chunk, limit)
                                .unwrap();
                            assert!(chunk.len() <= limit);
                            assert!(done || !chunk.is_empty(), "cursor made no progress");
                            actual_output.extend(chunk);
                            if done {
                                break;
                            }
                            assert!(iteration < 99, "cursor did not terminate");
                        }
                        assert_eq!(actual_output, expected_output, "{join_type:?}, side={side}, kind={kind}, mask={mask_kind}, limit={limit}");
                        assert_eq!(actual_state, expected_state);
                        let mut terminal = Vec::new();
                        assert!(cursor
                            .drain(&mut actual_state, &matches, &mut terminal, limit)
                            .unwrap());
                        assert!(terminal.is_empty());
                    }
                }
            }
        }
    }
    assert_eq!(broker.reserved(), 0);
}
