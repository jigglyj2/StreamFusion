// Copyright 2026 StreamFusion Authors
// Licensed under the Apache License, Version 2.0.
use super::*;
mod oracle;
#[test]
fn bounded_cursor_matches_recursive_inner_and_left_join_order() {
    for seed in 0..20usize {
        for inputs in 2..=4 {
            let plan = proto::MultiJoin {
                join_types: (0..inputs)
                    .map(|input| {
                        if input > 0 && (seed >> input) & 1 != 0 {
                            proto::RegularJoinType::Left as i32
                        } else {
                            proto::RegularJoinType::Inner as i32
                        }
                    })
                    .collect(),
                ..Default::default()
            };
            let state = MultiJoinState {
                inputs: (0..inputs)
                    .map(|input| {
                        (0..(seed + input) % 4)
                            .map(|row| StoredRow {
                                slot: row as u64,
                                row: vec![input as u8, row as u8],
                                condition_values: vec![],
                            })
                            .collect()
                    })
                    .collect(),
            };
            let nulls = vec![vec![255]; inputs];
            for input in 0..inputs {
                let active = StoredRow {
                    slot: 100,
                    row: vec![input as u8, 100],
                    condition_values: vec![],
                };
                for kind in [INSERT, UPDATE_BEFORE, UPDATE_AFTER, DELETE] {
                    let mut expected = Vec::new();
                    oracle::enumerate_join(
                        &plan,
                        &state,
                        &nulls,
                        input,
                        &active,
                        kind,
                        7,
                        &mut expected,
                    );
                    let mut cursor = Cursor::new(input, kind, 7);
                    let mut actual = Vec::new();
                    while let Some(selection) = cursor.next(&plan, &state, &active) {
                        assert!(cursor.frames.len() <= inputs + 1);
                        let rows = selection
                            .rows
                            .iter()
                            .enumerate()
                            .map(|(input, position)| match position {
                                Some(ACTIVE) => active.row.clone(),
                                Some(position) => state.inputs[input][*position].row.clone(),
                                None => nulls[input].clone(),
                            })
                            .collect::<Vec<_>>();
                        actual.push((rows, selection.kind, selection.ordinal));
                    }
                    assert_eq!(
                        actual,
                        expected
                            .into_iter()
                            .map(|row| (row.inputs, row.kind, row.input_ordinal))
                            .collect::<Vec<_>>(),
                        "seed={seed}, inputs={inputs}, active={input}, kind={kind}"
                    );
                }
            }
        }
    }
}
