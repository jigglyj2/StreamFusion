// Copyright 2026 StreamFusion Authors
// Licensed under the Apache License, Version 2.0

use super::*;

pub(in crate::planner::operators) fn apply_session_changes(
    sessions: &mut Vec<ActiveSession>,
    changes: Vec<(bool, SessionEvent)>,
    gap: i64,
    calls: &[Call],
    retain_events: bool,
) -> Result<()> {
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
    Ok(())
}
