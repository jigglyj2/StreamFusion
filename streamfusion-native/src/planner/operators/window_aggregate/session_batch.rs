// Copyright 2026 StreamFusion Authors
// Licensed under the Apache License, Version 2.0.

use super::*;

impl WindowAggregateProcessor {
    pub(super) fn process_session_batch(&mut self, batch: &RecordBatch) -> Result<RecordBatch> {
        let grouping_rows = self.encode_grouping_rows(batch)?;
        let timestamp_column = (!self.plan.processing_time)
            .then(|| batch.column(self.plan.time_attribute_index as usize));
        let gap = self.plan.size_millis;
        let progress = if self.plan.processing_time {
            self.current_processing_time
        } else {
            self.current_event_time
        };
        let mut unique = HashMap::<StateKey, usize, RandomState>::with_capacity_and_hasher(
            batch.num_rows(),
            RandomState::new(),
        );
        let mut groups = Vec::<PendingSessionGroup>::new();
        let mut group_key = Vec::new();
        for row in 0..batch.num_rows() {
            let timestamp = if self.plan.processing_time {
                self.to_window_time(self.current_processing_time)?
            } else {
                let Some(timestamp) = timestamp_millis(
                    timestamp_column.expect("event-time window has a timestamp column"),
                    row,
                )?
                else {
                    continue;
                };
                self.to_window_time(timestamp)?
            };
            self.group_key_into(batch, row, &mut group_key)?;
            let key_group = assign_key_group(&group_key, self.max_parallelism);
            let index_key = session_index_key(key_group, &group_key);
            let next = groups.len();
            let index = *unique.entry(index_key).or_insert(next);
            if index == groups.len() {
                groups.push(PendingSessionGroup {
                    key_group,
                    group_key: group_key.clone(),
                    grouping_row: grouping_rows[row].clone(),
                    changes: Vec::new(),
                });
            }
            groups[index].changes.push((
                self.accumulates(batch, row)?,
                SessionEvent {
                    timestamp,
                    values: row_aggregate_values(&self.calls, batch, row)?,
                },
            ));
        }
        if groups.is_empty() {
            return self.empty_output();
        }

        let index_keys = groups
            .iter()
            .map(|group| session_index_key(group.key_group, &group.group_key))
            .collect::<Vec<_>>();
        let index_refs = index_keys
            .iter()
            .map(|key| StateKeyRef {
                key_group: key.key_group,
                key: &key.key,
            })
            .collect::<Vec<_>>();
        let index_values = self
            .state
            .get_batch(&index_refs, &self.scratch_reservation)?;
        let _loaded_state_workspace =
            crate::state::reserve_decoded_values(&index_values, &self.scratch_reservation)?;
        self.state_read_batches = self.state_read_batches.saturating_add(1);
        let old_intervals = index_values
            .into_iter()
            .map(|value| match value {
                Some(value) => decode_session_index(value.as_ref()),
                None => Ok(Vec::new()),
            })
            .collect::<Result<Vec<_>>>()?;
        let session_keys = groups
            .iter()
            .zip(&old_intervals)
            .flat_map(|(group, intervals)| {
                intervals.iter().map(|&(start, end)| {
                    window_state_key(group.key_group, &group.group_key, start, end)
                })
            })
            .collect::<Vec<_>>();
        let session_refs = session_keys
            .iter()
            .map(|key| StateKeyRef {
                key_group: key.key_group,
                key: &key.key,
            })
            .collect::<Vec<_>>();
        let session_values = self
            .state
            .get_batch(&session_refs, &self.scratch_reservation)?;
        let _loaded_state_workspace =
            crate::state::reserve_decoded_values(&session_values, &self.scratch_reservation)?;
        if !session_refs.is_empty() {
            self.state_read_batches = self.state_read_batches.saturating_add(1);
        }
        let mut state_offset = 0usize;
        let mut mutations = Vec::new();
        let mut dirty_timer_groups = BTreeSet::new();
        for ((group, intervals), index_key) in groups.into_iter().zip(old_intervals).zip(index_keys)
        {
            let mut sessions = Vec::new();
            let mut grouping_row = group.grouping_row;
            for &(start, end) in &intervals {
                let state_key = window_state_key(group.key_group, &group.group_key, start, end);
                let value = session_values
                    .get(state_offset)
                    .ok_or_else(|| {
                        DataFusionError::Internal(
                            "session state batch did not match its index".to_string(),
                        )
                    })?
                    .as_ref();
                state_offset += 1;
                if let Some(value) = value {
                    let decoded = decode_session_state(value.as_ref(), &self.calls)?;
                    grouping_row = decoded.0;
                    sessions.push(ActiveSession {
                        start,
                        end,
                        accumulator: decoded.1,
                        events: decoded.2,
                    });
                }
                let timer = TimerKey {
                    timestamp: self.timer_timestamp(end.saturating_sub(1))?,
                    key: state_key.key.clone(),
                    namespace: window_namespace(start, end),
                };
                if self
                    .timers
                    .delete(group.key_group, self.timer_domain(), &timer)?
                {
                    self.timer_deletions = self.timer_deletions.saturating_add(1);
                    dirty_timer_groups.insert(group.key_group);
                }
                mutations.push(StateMutation {
                    key: state_key,
                    value: None,
                });
            }
            let dropped = apply_session_changes(
                &mut sessions,
                group.changes,
                gap,
                &self.calls,
                self.plan.input_changelog,
                |end| Ok(self.timer_timestamp(end.saturating_sub(1))? <= progress),
            )?;
            self.late_records_dropped = self.late_records_dropped.saturating_add(dropped);
            sessions.sort_by_key(|session| (session.start, session.end));
            let mut new_intervals = Vec::with_capacity(sessions.len());
            for session in sessions {
                let start = session.start;
                let end = session.end;
                let state_key = window_state_key(group.key_group, &group.group_key, start, end);
                let timer = TimerKey {
                    timestamp: self.timer_timestamp(end.saturating_sub(1))?,
                    key: state_key.key.clone(),
                    namespace: window_namespace(start, end),
                };
                if self
                    .timers
                    .register(group.key_group, self.timer_domain(), timer)?
                {
                    self.timer_registrations = self.timer_registrations.saturating_add(1);
                    dirty_timer_groups.insert(group.key_group);
                }
                mutations.push(StateMutation {
                    key: state_key,
                    value: Some(encode_session_state(
                        &grouping_row,
                        &session.accumulator,
                        &session.events,
                    )),
                });
                new_intervals.push((start, end));
            }
            mutations.push(StateMutation {
                key: index_key,
                value: (!new_intervals.is_empty()).then(|| encode_session_index(&new_intervals)),
            });
        }
        debug_assert_eq!(state_offset, session_values.len());
        self.append_timer_mutations(&mut mutations, dirty_timer_groups)?;
        self.state.write_batch(mutations)?;
        self.state_write_batches = self.state_write_batches.saturating_add(1);
        self.empty_output()
    }
}
