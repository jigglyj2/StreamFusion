// Copyright 2026 StreamFusion Authors
// Licensed under the Apache License, Version 2.0

//! Shared row-index storage primitives; operator codecs own metadata and row semantics.
use super::sortable_state::{prefix, prefix_end, PAGE_BYTES, PAGE_ROWS};
use crate::memory_pool::HostMemoryReservation;
use crate::state::{KeyedState, StateKey, StateMutation};
use datafusion::error::{DataFusionError, Result};
use std::collections::BTreeMap;

pub(super) type Entries = BTreeMap<Vec<u8>, Vec<u8>>;

pub(super) fn load(
    state: &dyn KeyedState,
    key: &StateKey,
    owner: &mut HostMemoryReservation,
) -> Result<Entries> {
    let start = prefix(0xf0, &key.key)?;
    let end = prefix_end(&start);
    let mut page_credit = owner.sibling("ordered partition scan page");
    page_credit.resize(PAGE_BYTES)?;
    let mut entries = Entries::new();
    state.visit_range(
        key.key_group,
        &start,
        end.as_deref(),
        PAGE_ROWS,
        PAGE_BYTES,
        &mut |page| {
            owner.try_grow(
                page.iter()
                    .map(|(k, v)| k.len().saturating_add(v.len()).saturating_add(192))
                    .sum(),
            )?;
            if page.iter().any(|(k, _)| k.len() < start.len() + 8) {
                return Err(DataFusionError::Execution(
                    "truncated ordered state key".into(),
                ));
            }
            entries.extend(page.iter().map(|(k, v)| (k.to_vec(), v.to_vec())));
            Ok(true)
        },
    )?;
    Ok(entries)
}

pub(super) fn write_delta(
    key: &StateKey,
    old: Entries,
    new: Entries,
    out: &mut Vec<StateMutation>,
) {
    for k in old.keys() {
        if !new.contains_key(k) {
            out.push(StateMutation {
                key: StateKey {
                    key_group: key.key_group,
                    key: k.clone(),
                },
                value: None,
            });
        }
    }
    for (k, v) in new {
        if old.get(&k) != Some(&v) {
            out.push(StateMutation {
                key: StateKey {
                    key_group: key.key_group,
                    key: k,
                },
                value: Some(v),
            });
        }
    }
}

/// Enumerate only operator metadata, excluding indexed row payloads from terminal discovery.
pub(super) fn metadata(
    state: &dyn KeyedState,
    group: u32,
    namespace: u8,
    credit: &mut HostMemoryReservation,
) -> Result<Vec<(Vec<u8>, Vec<u8>)>> {
    let start = [namespace];
    let end = prefix_end(&start);
    let mut page_credit = credit.sibling("ordered state metadata page");
    page_credit.resize(PAGE_BYTES)?;
    let mut entries = Vec::new();
    state.visit_range(
        group,
        &start,
        end.as_deref(),
        PAGE_ROWS,
        PAGE_BYTES,
        &mut |page| {
            credit.try_grow(page.iter().map(|(k, v)| k.len() + v.len() + 64).sum())?;
            entries.extend(page.iter().map(|(k, v)| (k.to_vec(), v.to_vec())));
            Ok(true)
        },
    )?;
    Ok(entries)
}
