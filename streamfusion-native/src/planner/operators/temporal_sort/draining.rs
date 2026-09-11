// Copyright 2026 StreamFusion Authors
// Licensed under the Apache License, Version 2.0.

use super::*;
use std::collections::VecDeque;

pub(super) struct Drain {
    pub(super) deadline: i64,
    domain: TimerDomain,
    last_timestamp: i64,
    had_rows: bool,
    groups: VecDeque<Group>,
    markers: Vec<StateMutation>,
    _memory: HostMemoryReservation,
}

struct Group {
    root: StateKey,
    expected: u64,
    read: u64,
}

struct Page {
    groups: Vec<Vec<BufferedRow>>,
    mutations: Vec<StateMutation>,
    rows: usize,
    bytes: usize,
    retained: usize,
    memory: HostMemoryReservation,
}

impl TemporalSortProcessor {
    pub(super) fn require_idle(&self) -> Result<()> {
        self.require_healthy()?;
        if self.pending_drain.is_some() {
            return Err(DataFusionError::Execution(
                "temporal sort output must drain before input, checkpoint or restore".into(),
            ));
        }
        Ok(())
    }

    fn require_healthy(&self) -> Result<()> {
        if self.failed {
            return Err(DataFusionError::Execution(
                "temporal sort failed; recreate the processor and restore its checkpoint".into(),
            ));
        }
        Ok(())
    }

    pub(super) fn advance(&mut self, domain: TimerDomain, progress: i64) -> Result<RecordBatch> {
        self.require_healthy()?;
        if self
            .pending_drain
            .as_ref()
            .is_some_and(|drain| drain.domain != domain || progress < drain.deadline)
        {
            return Err(DataFusionError::Execution(
                "temporal sort cannot change the domain or regress a pending timer".into(),
            ));
        }
        let result = self.advance_page(domain, progress);
        if result.is_err() {
            // Earlier pages may already have been emitted and removed. Never expose a partial
            // callback as a checkpoint or retry it after an output/admission/backend failure.
            self.failed = true;
            self.pending_drain = None;
            let _ = self.scratch_reservation.resize(0);
        }
        result
    }

    fn prepare_drain(&mut self, domain: TimerDomain, progress: i64) -> Result<Option<Drain>> {
        let fired = self
            .timers
            .advance_owned_limited(domain, progress, MAX_TIMERS_PER_OUTPUT)?;
        if fired.is_empty() {
            return Ok(None);
        }
        let mut memory = self
            .scratch_reservation
            .sibling("temporal pending callbacks");
        memory.resize(fired.len().saturating_mul(512))?;
        let mut seen = hashbrown::HashSet::with_hasher(RandomState::new());
        let mut roots = Vec::new();
        for entry in fired.iter() {
            let root = StateKey {
                key_group: entry.key_group,
                key: entry.timer.key.clone(),
            };
            if seen.insert(root.clone()) {
                roots.push(root);
            }
        }
        drop(seen);
        let refs = roots
            .iter()
            .map(|root| StateKeyRef {
                key_group: root.key_group,
                key: &root.key,
            })
            .collect::<Vec<_>>();
        let values = self.state.get_batch(&refs, &self.scratch_reservation)?;
        self.state_read_batches = self.state_read_batches.saturating_add(1);
        let mut groups = VecDeque::new();
        for (root, value) in roots.iter().zip(values) {
            if let Some(value) = value {
                let expected = row_state::next_arrival(value.as_ref())?.ok_or_else(|| {
                    row_state::invalid("legacy groups must migrate during restore")
                })?;
                groups.push_back(Group {
                    root: root.clone(),
                    expected,
                    read: 0,
                });
            }
        }
        self.timers_fired = self.timers_fired.saturating_add(fired.len() as u64);
        Ok(Some(Drain {
            deadline: fired
                .iter()
                .map(|entry| entry.timer.timestamp)
                .min()
                .unwrap(),
            last_timestamp: fired
                .iter()
                .map(|entry| entry.timer.timestamp)
                .max()
                .unwrap(),
            domain,
            had_rows: !groups.is_empty(),
            groups,
            markers: fired
                .iter()
                .map(|entry| StateMutation {
                    key: timer_state::marker_key(entry.key_group, domain, entry.timer.timestamp),
                    value: None,
                })
                .collect(),
            _memory: memory,
        }))
    }

    fn advance_page(&mut self, domain: TimerDomain, progress: i64) -> Result<RecordBatch> {
        let mut drain = match self.pending_drain.take() {
            Some(drain) => {
                self.state_read_batches = self.state_read_batches.saturating_add(1);
                drain
            }
            None => match self.prepare_drain(domain, progress)? {
                Some(drain) => drain,
                None => return Ok(RecordBatch::new_empty(self.output_schema.clone())),
            },
        };
        let mut page = Page {
            groups: Vec::new(),
            mutations: Vec::new(),
            rows: 0,
            bytes: 0,
            retained: 0,
            memory: self
                .scratch_reservation
                .sibling("temporal output page rows and deletions"),
        };
        while let Some(group) = drain.groups.front_mut() {
            let complete =
                page.read_group(self.state.as_ref(), group, &self.scratch_reservation)?;
            if complete {
                if group.read != group.expected {
                    return Err(row_state::invalid(
                        "row count does not match arrival counter",
                    ));
                }
                page.mutations.push(StateMutation {
                    key: group.root.clone(),
                    value: None,
                });
                drain.groups.pop_front();
            }
            if !complete || page.rows >= row_state::PAGE_ROWS || page.bytes >= row_state::PAGE_BYTES
            {
                break;
            }
        }
        // Decode/sort before changing durable state. Only the bounded page is resident; the
        // ordered index preserves global key order and arrival ties across DataFusion batches.
        let output = self.output_row_groups(std::mem::take(&mut page.groups))?;
        if drain.groups.is_empty() {
            page.mutations.append(&mut drain.markers);
            if domain == TimerDomain::EventTime && drain.had_rows {
                page.mutations.push(StateMutation {
                    key: StateKey {
                        key_group: self.key_group,
                        key: LAST_TRIGGER_STATE_KEY.to_vec(),
                    },
                    value: Some(drain.last_timestamp.to_le_bytes().to_vec()),
                });
            }
        }
        self.state
            .write_batch(std::mem::take(&mut page.mutations))?;
        self.state_write_batches = self.state_write_batches.saturating_add(1);
        if drain.groups.is_empty() {
            if domain == TimerDomain::EventTime && drain.had_rows {
                self.last_triggering_timestamp = drain.last_timestamp;
            }
        } else {
            self.pending_drain = Some(drain);
        }
        Ok(output)
    }
}

impl Page {
    fn read_group(
        &mut self,
        state: &dyn KeyedState,
        group: &mut Group,
        owner: &HostMemoryReservation,
    ) -> Result<bool> {
        let prefix = row_state::prefix(&group.root.key)?;
        let end = crate::state::prefix_end(&prefix);
        let mut rows = Vec::new();
        let complete = state.visit_range_admitted(
            group.root.key_group,
            &prefix,
            end.as_deref(),
            row_state::PAGE_ROWS - self.rows,
            row_state::PAGE_BYTES.saturating_sub(self.bytes).max(1),
            owner,
            &mut |entries| {
                let bytes = entries
                    .iter()
                    .map(|(key, value)| key.len().saturating_add(value.len()).saturating_add(256))
                    .sum::<usize>();
                self.retained = self.retained.saturating_add(bytes.saturating_mul(2));
                if self.retained > self.memory.size() {
                    let rounded = self.retained.saturating_add(65535) / 65536 * 65536;
                    self.memory.resize(rounded)?;
                }
                for (key, value) in entries {
                    if !key.starts_with(&prefix) || key.len() < prefix.len() + 8 {
                        return Err(row_state::invalid("row ordering key"));
                    }
                    let ordinal = u64::from_be_bytes(key[key.len() - 8..].try_into().unwrap());
                    if ordinal >= group.expected
                        || !value.starts_with(row_state::ROW_HEADER)
                        || value.len() <= row_state::ROW_HEADER.len()
                    {
                        return Err(row_state::invalid("row entry"));
                    }
                    let kind = value[row_state::ROW_HEADER.len()] as i8;
                    if !matches!(kind, INSERT | UPDATE_BEFORE | UPDATE_AFTER | DELETE) {
                        return Err(row_state::invalid("RowKind"));
                    }
                    rows.push(BufferedRow {
                        kind,
                        sort_key: key[prefix.len()..key.len() - 8].to_vec(),
                        row: value[row_state::ROW_HEADER.len() + 1..].to_vec(),
                    });
                    self.mutations.push(StateMutation {
                        key: StateKey {
                            key_group: group.root.key_group,
                            key: key.to_vec(),
                        },
                        value: None,
                    });
                    self.rows += 1;
                    self.bytes = self
                        .bytes
                        .saturating_add(key.len())
                        .saturating_add(value.len())
                        .saturating_add(100);
                    group.read = group
                        .read
                        .checked_add(1)
                        .ok_or_else(|| row_state::invalid("row count overflow"))?;
                    if group.read > group.expected {
                        return Err(row_state::invalid("row count exceeds arrival counter"));
                    }
                }
                Ok(false)
            },
        )?;
        if !rows.is_empty() {
            self.groups.push(rows);
        }
        Ok(complete)
    }
}

#[cfg(test)]
mod tests;
