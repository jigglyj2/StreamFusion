// Copyright 2026 StreamFusion Authors
// Licensed under the Apache License, Version 2.0

use super::super::ordered_partition;
use super::super::sortable_state::{prefix, row_key, FORMAT};
use super::*;

pub(super) fn load(
    backend: &dyn KeyedState,
    key: &StateKey,
    bytes: &[u8],
    calls: usize,
    credit: &mut HostMemoryReservation,
    orders: Option<&std::collections::BTreeSet<Vec<u8>>>,
) -> Result<OverState> {
    if bytes.starts_with(b"SFOA") {
        return decode_over_state(bytes, calls);
    }
    if bytes.len() != 14 || !bytes.starts_with(FORMAT) {
        return Err(DataFusionError::Execution(
            "unsupported OVER ordered state version".into(),
        ));
    }
    let start = prefix(0xf0, &key.key)?;
    let entries = if let Some(orders) = orders {
        let mut entries = ordered_partition::Entries::new();
        let mut page_credit = credit.sibling("bounded OVER affected order pages");
        page_credit.resize(super::super::sortable_state::PAGE_BYTES)?;
        for order in orders {
            let mut lower = start.clone();
            lower.extend_from_slice(order);
            let upper = super::super::sortable_state::prefix_end(&lower);
            backend.visit_range(
                key.key_group,
                &lower,
                upper.as_deref(),
                super::super::sortable_state::PAGE_ROWS,
                super::super::sortable_state::PAGE_BYTES,
                &mut |page| {
                    credit.try_grow(page.iter().map(|(k, v)| k.len() + v.len() + 192).sum())?;
                    entries.extend(page.iter().map(|(k, v)| (k.to_vec(), v.to_vec())));
                    Ok(true)
                },
            )?;
        }
        entries
    } else {
        ordered_partition::load(backend, key, credit)?
    };
    credit.try_grow(
        entries
            .iter()
            .map(|(k, v)| k.len() + v.len() * 3 + calls * 64 + 192)
            .sum(),
    )?;
    let mut state = OverState::default();
    state.next_id = i64::from_le_bytes(bytes[6..].try_into().unwrap());
    for (k, v) in &entries {
        if k.len() < start.len() + 8 {
            return Err(DataFusionError::Execution(
                "truncated OVER ordered key".into(),
            ));
        }
        let id = (u64::from_be_bytes(k[k.len() - 8..].try_into().unwrap()) ^ (1u64 << 63)) as i64;
        let row = state_codec::decode_index_row(id, v, calls)?;
        state
            .rows
            .entry(k[start.len()..k.len() - 8].to_vec())
            .or_default()
            .push(row);
    }
    state.partial = orders.is_some();
    state.persisted = entries;
    Ok(state)
}

pub(super) fn mutations(
    key: StateKey,
    mut state: OverState,
    keep_empty: bool,
    credit: &mut HostMemoryReservation,
) -> Result<Vec<StateMutation>> {
    let start = prefix(0xf0, &key.key)?;
    credit.try_grow(
        state
            .rows
            .iter()
            .map(|(order, rows)| {
                rows.iter()
                    .map(|r| {
                        start.len()
                            + order.len()
                            + r.payload.len()
                            + (r.contributions.len() + r.output.len()) * 32
                            + 256
                    })
                    .sum::<usize>()
            })
            .sum(),
    )?;
    let mut entries = ordered_partition::Entries::new();
    for (order, rows) in &state.rows {
        for row in rows {
            let key = row_key(&start, order, (row.id as u64) ^ (1u64 << 63));
            if entries
                .insert(key, state_codec::encode_index_row(row))
                .is_some()
            {
                return Err(DataFusionError::Execution(
                    "duplicate OVER ordered row identity".into(),
                ));
            }
        }
    }
    let mut out = Vec::new();
    ordered_partition::write_delta(
        &key,
        std::mem::take(&mut state.persisted),
        entries,
        &mut out,
    );
    let value = if keep_empty || state.partial || !state.rows.is_empty() {
        let mut bytes = FORMAT.to_vec();
        bytes.extend_from_slice(&state.next_id.to_le_bytes());
        Some(bytes)
    } else {
        None
    };
    out.push(StateMutation { key, value });
    Ok(out)
}
