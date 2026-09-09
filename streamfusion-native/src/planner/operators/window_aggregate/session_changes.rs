// Copyright 2026 StreamFusion Authors
// Licensed under the Apache License, Version 2.0

use super::*;

pub(in crate::planner::operators) fn apply_session_changes(
    sessions: &mut Vec<ActiveSession>,
    changes: Vec<(bool, SessionEvent)>,
    gap: i64,
    calls: &[Call],
    retain_events: bool,
    mut is_late: impl FnMut(i64) -> Result<bool>,
) -> Result<u64> {
    let mut dropped = 0;
    for (accumulate, event) in changes {
        if accumulate {
            let mut start = event.timestamp;
            let mut end = event.timestamp.saturating_add(gap);
            let mut merged_sessions = Vec::new();
            // Re-evaluate after every merge: absorbing one session can expand the namespace
            // enough to overlap another session that did not overlap the incoming row itself.
            // Flink's MergingWindowSet computes this transitive closure.
            while let Some(index) = sessions
                .iter()
                .position(|session| session.start <= end && session.end >= start)
            {
                let merged = sessions.remove(index);
                start = start.min(merged.start);
                end = end.max(merged.end);
                merged_sessions.push(merged);
            }
            // Flink MergingWindowProcessFunction tests the merged namespace, not the
            // incoming event's original window. An older event can extend a live session.
            if is_late(end)? {
                debug_assert!(merged_sessions.is_empty());
                dropped += 1;
                continue;
            }
            let mut accumulator = AccumulatorState::new(calls);
            let mut events = if retain_events {
                Vec::with_capacity(
                    1 + merged_sessions
                        .iter()
                        .map(|session| session.events.len())
                        .sum::<usize>(),
                )
            } else {
                Vec::new()
            };
            // Flink merges existing accumulator namespaces before applying the row that caused
            // the merge. Combining their compact accumulators avoids O(n^2) replay for a session
            // receiving n records while retaining byte-exact retraction data only when needed.
            for merged in merged_sessions {
                accumulator.merge(calls, &merged.accumulator)?;
                if retain_events {
                    events.extend(merged.events);
                }
            }
            accumulator.apply_values(calls, &event.values, true)?;
            if retain_events {
                events.push(event);
                events.sort_by(|left, right| left.timestamp.cmp(&right.timestamp));
            }
            sessions.push(ActiveSession {
                start,
                end,
                accumulator,
                events,
            });
        } else {
            // Retractions retain the matching session namespace. Only an unmatched,
            // expired event can be dropped before its contribution lookup.
            let end = sessions
                .iter()
                .filter(|session| {
                    session.start <= event.timestamp.saturating_add(gap)
                        && session.end >= event.timestamp
                })
                .map(|session| session.end)
                .max()
                .unwrap_or_else(|| event.timestamp.saturating_add(gap));
            if is_late(end)? {
                dropped += 1;
                continue;
            }
            let Some((session_index, event_index)) =
                sessions
                    .iter()
                    .enumerate()
                    .find_map(|(session_index, session)| {
                        session
                            .events
                            .iter()
                            .position(|candidate| candidate == &event)
                            .map(|event_index| (session_index, event_index))
                    })
            else {
                return Err(DataFusionError::Execution(
                    "session window received a retraction without a matching accumulated row"
                        .to_string(),
                ));
            };
            let session = &mut sessions[session_index];
            session.events.remove(event_index);
            session
                .accumulator
                .apply_values(calls, &event.values, false)?;
            // Flink's merging-window operator keeps the namespace of a non-empty session on
            // retraction. It does not shrink or split a session after its bridging row retracts.
            if session.accumulator.row_count == 0 {
                sessions.remove(session_index);
            }
        }
    }
    Ok(dropped)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn late_input_merges_before_expiration_check_without_reordering_arrivals() {
        let calls = vec![Call {
            function: proto::AggregateFunction::CountStar,
            input_index: None,
            input_type: None,
            output_type: DataType::Int64,
            retractable: false,
            filter_index: None,
            distinct: false,
        }];
        let event = |timestamp| {
            (
                true,
                SessionEvent {
                    timestamp,
                    values: vec![None],
                },
            )
        };
        for (timestamps, expected_count, expected_dropped) in
            [(vec![0, 10_000, 0], 2, 1), (vec![10_000, 0, 0], 3, 0)]
        {
            let mut sessions = Vec::new();
            let dropped = apply_session_changes(
                &mut sessions,
                timestamps.into_iter().map(event).collect(),
                10_000,
                &calls,
                false,
                |end| Ok(end - 1 <= 15_000),
            )
            .unwrap();
            assert_eq!(dropped, expected_dropped);
            assert_eq!(sessions.len(), 1);
            assert_eq!((sessions[0].start, sessions[0].end), (0, 20_000));
            assert_eq!(sessions[0].accumulator.row_count, expected_count);
        }
    }
}
