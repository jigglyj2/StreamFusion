// Copyright 2026 StreamFusion Authors
// Licensed under the Apache License, Version 2.0

//! Individually indexed window candidates. Input reads only metadata and identities being
//! retracted; watermark scans discard unselected payloads without decoding them.
use super::*;
use crate::planner::operators::sortable_state::{
    prefix, prefix_end, row_key, FORMAT, PAGE_BYTES, PAGE_ROWS,
};
use std::collections::{BTreeMap, VecDeque};

struct Work {
    key: StateKey,
    end: i64,
    next: u64,
    count: u64,
    identities: BTreeMap<Vec<u8>, VecDeque<u64>>,
}
fn header(next: u64, count: u64) -> Vec<u8> {
    let mut bytes = FORMAT.to_vec();
    bytes.extend_from_slice(&next.to_le_bytes());
    bytes.extend_from_slice(&count.to_le_bytes());
    bytes
}
fn read_header(bytes: &[u8]) -> Result<(u64, u64)> {
    if bytes.len() != 22 || !bytes.starts_with(FORMAT) {
        return Err(DataFusionError::Execution(
            "unsupported window-rank ordered state version".into(),
        ));
    }
    Ok((
        u64::from_le_bytes(bytes[6..14].try_into().unwrap()),
        u64::from_le_bytes(bytes[14..22].try_into().unwrap()),
    ))
}
fn identity_prefix(key: &[u8], payload: &[u8]) -> Result<Vec<u8>> {
    let mut p = prefix(3, key)?;
    p.extend_from_slice(
        &u32::try_from(payload.len())
            .map_err(|_| DataFusionError::Execution("window rank payload exceeds UInt32".into()))?
            .to_be_bytes(),
    );
    p.extend_from_slice(payload);
    Ok(p)
}
fn mutation(key: &StateKey, bytes: Vec<u8>, value: Option<Vec<u8>>) -> StateMutation {
    StateMutation {
        key: StateKey {
            key_group: key.key_group,
            key: bytes,
        },
        value,
    }
}
fn candidate_mutations(
    out: &mut Vec<StateMutation>,
    key: &StateKey,
    payload: &[u8],
    order: &[u8],
    sequence: u64,
    insert: bool,
) -> Result<()> {
    out.push(mutation(
        key,
        row_key(&prefix(2, &key.key)?, order, sequence),
        insert.then(|| payload.to_vec()),
    ));
    out.push(mutation(
        key,
        row_key(&identity_prefix(&key.key, payload)?, &[], sequence),
        insert.then(Vec::new),
    ));
    Ok(())
}

pub(super) fn process(
    p: &mut WindowRankProcessor,
    batch: &RecordBatch,
    payloads: &Rows,
) -> Result<RecordBatch> {
    let orders = p.sort_keys.as_ref().unwrap().encode(batch.columns())?;
    let kinds = batch
        .column(p.input_kind_index.unwrap())
        .as_any()
        .downcast_ref::<Int8Array>()
        .ok_or_else(|| {
            DataFusionError::Execution("window rank RowKind metadata is not Arrow Int8".into())
        })?;
    let mut groups = HashMap::<StateKey, usize, RandomState>::with_hasher(RandomState::new());
    let mut keys = Vec::new();
    let mut changes = Vec::new();
    let mut retracts = BTreeMap::<(usize, Vec<u8>), usize>::new();
    for row in 0..batch.num_rows() {
        let Some(end) = timestamp_millis(batch.column(p.plan.window_end_index as usize), row)?
        else {
            continue;
        };
        if p.timer_timestamp(end.saturating_sub(1))? <= p.current_event_time {
            p.late_records_dropped = p.late_records_dropped.saturating_add(1);
            continue;
        }
        let group = group_key(
            batch,
            row,
            p.preencoded_key_index,
            &p.key_fields,
            "window rank",
        )?;
        let key = window_state_key(assign_key_group(&group, p.max_parallelism), &group, end);
        let next = keys.len();
        let i = *groups.entry(key.clone()).or_insert_with(|| {
            keys.push(key);
            next
        });
        let insert = match kinds.value(row) {
            INSERT | UPDATE_AFTER => true,
            DELETE | UPDATE_BEFORE => false,
            k => {
                return Err(DataFusionError::Execution(format!(
                    "unknown Flink RowKind byte {k}"
                )))
            }
        };
        if !insert {
            *retracts
                .entry((i, payloads.row(row).data().to_vec()))
                .or_default() += 1;
        }
        changes.push((i, row, insert));
    }
    if keys.is_empty() {
        return p.empty_output();
    }
    let mut workspace = p
        .scratch_reservation
        .sibling("window rank ordered state pages and mutations");
    workspace.resize(
        PAGE_BYTES
            .saturating_add(batch.get_array_memory_size().saturating_mul(6))
            .saturating_add(
                changes
                    .iter()
                    .map(|(i, _, _)| keys[*i].key.len().saturating_mul(4).saturating_add(512))
                    .sum::<usize>(),
            ),
    )?;
    let refs = keys
        .iter()
        .map(|k| StateKeyRef {
            key_group: k.key_group,
            key: &k.key,
        })
        .collect::<Vec<_>>();
    let values = p.state.get_batch(&refs, &workspace)?;
    let _loaded = crate::state::reserve_decoded_values(&values, &workspace)?;
    p.state_read_batches += 1;
    let mut mutations = Vec::new();
    let mut work = Vec::new();
    for (key, value) in keys.iter().zip(values.iter()) {
        let mut w = Work {
            key: key.clone(),
            end: decode_window_end(&key.key)?,
            next: 0,
            count: 0,
            identities: BTreeMap::new(),
        };
        if let Some(value) = value {
            if value.starts_with(STATE_MAGIC) {
                // Upgrade old snapshots atomically with the first batch touching this window.
                let old = decode_state(value)?;
                workspace.try_grow(value.len().saturating_mul(5))?;
                let parser = p.row_converter.parser();
                let columns = p
                    .row_converter
                    .convert_rows(old.candidates.iter().map(|c| parser.parse(&c.row)))?;
                let sort = p.sort_keys.as_ref().unwrap().encode(&columns)?;
                w.next = old.next_sequence;
                w.count = old.candidates.len() as u64;
                for (i, c) in old.candidates.into_iter().enumerate() {
                    candidate_mutations(
                        &mut mutations,
                        key,
                        &c.row,
                        sort.row(i).data(),
                        c.sequence,
                        true,
                    )?;
                    w.identities.entry(c.row).or_default().push_back(c.sequence);
                }
            } else {
                (w.next, w.count) = read_header(value)?;
            }
        }
        work.push(w);
    }
    drop(values);
    // All backend reads precede row transitions. Fetch at most the batch's deletion count
    // for each identity, preserving oldest-duplicate retraction semantics.
    for ((i, payload), count) in retracts {
        if work[i].identities.contains_key(&payload) {
            continue;
        }
        let start = identity_prefix(&work[i].key.key, &payload)?;
        let end = prefix_end(&start);
        let mut ids = VecDeque::new();
        workspace.try_grow(count.saturating_mul(16))?;
        p.state.visit_range(
            work[i].key.key_group,
            &start,
            end.as_deref(),
            PAGE_ROWS.min(count),
            PAGE_BYTES,
            &mut |page| {
                for (key, _) in page {
                    if key.len() != start.len() + 8 {
                        return Err(DataFusionError::Execution(
                            "invalid window-rank identity key".into(),
                        ));
                    }
                    ids.push_back(u64::from_be_bytes(key[key.len() - 8..].try_into().unwrap()));
                    if ids.len() == count {
                        return Ok(false);
                    }
                }
                Ok(true)
            },
        )?;
        work[i].identities.insert(payload, ids);
    }
    let mut dirty = BTreeSet::new();
    for (i, row, insert) in changes {
        let w = &mut work[i];
        let payload = payloads.row(row);
        let was_empty = w.count == 0;
        let sequence = if insert {
            let seq = w.next;
            w.next = seq.checked_add(1).ok_or_else(|| {
                DataFusionError::Execution("window rank sequence number overflow".into())
            })?;
            w.count += 1;
            w.identities
                .entry(payload.data().to_vec())
                .or_default()
                .push_back(seq);
            seq
        } else {
            let seq = w
                .identities
                .get_mut(payload.data())
                .and_then(VecDeque::pop_front)
                .ok_or_else(|| {
                    DataFusionError::Execution(
                        "window rank received a retraction without a matching row".into(),
                    )
                })?;
            w.count -= 1;
            seq
        };
        candidate_mutations(
            &mut mutations,
            &w.key,
            payload.data(),
            orders.row(row).data(),
            sequence,
            insert,
        )?;
        let timer = TimerKey {
            timestamp: p.timer_timestamp(w.end.saturating_sub(1))?,
            key: w.key.key.clone(),
            namespace: w.end.to_le_bytes().to_vec(),
        };
        if was_empty && w.count != 0 {
            if p.timers
                .register(w.key.key_group, TimerDomain::EventTime, timer)?
            {
                p.timer_registrations += 1;
                dirty.insert(w.key.key_group);
            }
        } else if !was_empty
            && w.count == 0
            && p.timers
                .delete(w.key.key_group, TimerDomain::EventTime, &timer)?
        {
            p.timer_deletions += 1;
            dirty.insert(w.key.key_group);
        }
    }
    for w in work {
        mutations.push(StateMutation {
            key: w.key,
            value: (w.count != 0).then(|| header(w.next, w.count)),
        });
    }
    append_timer_mutations(&p.timers, &mut mutations, dirty, TIMER_STATE_KEY)?;
    p.state.write_batch(mutations)?;
    p.state_write_batches += 1;
    p.empty_output()
}

pub(super) fn fire(
    p: &mut WindowRankProcessor,
    fired: Vec<crate::state::FiredTimer>,
) -> Result<RecordBatch> {
    let mut workspace = p
        .scratch_reservation
        .sibling("window rank ordered watermark pages");
    workspace.resize(PAGE_BYTES)?;
    let refs = fired
        .iter()
        .map(|t| StateKeyRef {
            key_group: t.key_group,
            key: &t.timer.key,
        })
        .collect::<Vec<_>>();
    let values = p.state.get_batch(&refs, &workspace)?;
    let _loaded = crate::state::reserve_decoded_values(&values, &workspace)?;
    p.state_read_batches += 1;
    let mut selected = Vec::new();
    let mut ranks = Vec::new();
    let mut mutations = Vec::new();
    let mut dirty = BTreeSet::new();
    for (i, (timer, value)) in fired.iter().zip(values.iter()).enumerate() {
        let key = StateKey {
            key_group: timer.key_group,
            key: timer.timer.key.clone(),
        };
        dirty.insert(timer.key_group);
        if let Some(value) = value {
            if value.starts_with(STATE_MAGIC) {
                let old = p.output_batch(
                    decode_state(value)?
                        .candidates
                        .into_iter()
                        .map(|c| (i, c))
                        .collect(),
                )?;
                let payload = p
                    .row_converter
                    .convert_columns(&old.columns()[..p.visible_schema.fields().len()])?;
                for (j, row) in payload.iter().enumerate() {
                    selected.push(row.data().to_vec());
                    ranks.push((p.plan.rank_start + j as u64) as i64);
                }
            } else {
                let (_, count) = read_header(value)?;
                let start = prefix(2, &key.key)?;
                let end = prefix_end(&start);
                let mut rank = 0u64;
                p.state.visit_range(
                    key.key_group,
                    &start,
                    end.as_deref(),
                    PAGE_ROWS,
                    PAGE_BYTES,
                    &mut |page| {
                        workspace.try_grow(
                            page.iter().map(|(k, v)| 2 * k.len() + v.len() + 192).sum(),
                        )?;
                        for (k, v) in page {
                            if k.len() < start.len() + 8 {
                                return Err(DataFusionError::Execution(
                                    "truncated window-rank ordered key".into(),
                                ));
                            }
                            rank += 1;
                            if rank >= p.plan.rank_start && rank <= p.plan.rank_end {
                                selected.push(v.to_vec());
                                ranks.push(rank as i64);
                            }
                            let seq = u64::from_be_bytes(k[k.len() - 8..].try_into().unwrap());
                            mutations.push(mutation(&key, k.to_vec(), None));
                            mutations.push(mutation(
                                &key,
                                row_key(&identity_prefix(&key.key, v)?, &[], seq),
                                None,
                            ));
                        }
                        Ok(true)
                    },
                )?;
                if rank != count {
                    return Err(DataFusionError::Execution(
                        "window rank ordered state count mismatch".into(),
                    ));
                }
            }
        }
        mutations.push(StateMutation { key, value: None });
    }
    drop(values);
    append_timer_mutations(&p.timers, &mut mutations, dirty, TIMER_STATE_KEY)?;
    p.state.write_batch(mutations)?;
    p.state_write_batches += 1;
    let parser = p.row_converter.parser();
    workspace.try_grow(selected.iter().map(|v| v.len().saturating_mul(2)).sum())?;
    let mut columns = p
        .row_converter
        .convert_rows(selected.iter().map(|v| parser.parse(v)))?;
    if p.plan.output_rank_number {
        columns.push(Arc::new(Int64Array::from(ranks)));
    }
    columns.push(Arc::new(Int8Array::from(vec![INSERT; selected.len()])));
    let output = RecordBatch::try_new(p.output_schema.clone(), columns)?;
    finish_output(output, 0, &mut p.scratch_reservation)
}
