// Copyright 2026 StreamFusion Authors
// Licensed under the Apache License, Version 2.0.

use super::*;

pub(super) fn layout(entry: &StagedState) -> Layout {
    let state = &entry.value;
    if entry.unloaded.is_some() {
        return Layout::Rows;
    }
    if state.left.len() + state.right.len() <= 1 && compact_eligible(state) {
        Layout::Compact
    } else {
        Layout::Rows
    }
}

fn entries(rows: &[StoredRow], layout: Layout) -> impl Iterator<Item = (u64, &[StoredRow])> {
    let rows = if layout == Layout::Compact { &[] } else { rows };
    rows.chunk_by(move |a, b| layout != Layout::Rows && a.id / PAGE_ROWS == b.id / PAGE_ROWS)
        .map(move |rows| {
            (
                if layout == Layout::Rows {
                    rows[0].id
                } else {
                    rows[0].id / PAGE_ROWS
                },
                rows,
            )
        })
}

pub(super) fn changed_rows(
    entry: &StagedState,
    mut visit: impl FnMut(usize, u64, Layout, &[StoredRow]) -> Result<()>,
) -> Result<()> {
    let new_layout = layout(entry);
    if let Some(unloaded) = &entry.unloaded {
        for side in 0..2 {
            for id in unloaded.ids[side].removed_from(&unloaded.original_ids[side]) {
                visit(side, id, Layout::Rows, &[])?;
            }
        }
    }
    for (side, (before, after)) in [
        (&entry.original.left, &entry.value.left),
        (&entry.original.right, &entry.value.right),
    ]
    .into_iter()
    .enumerate()
    {
        if entry.original_layout != new_layout {
            for (id, _) in entries(before, entry.original_layout) {
                visit(side, id, entry.original_layout, &[])?;
            }
            for (id, rows) in entries(after, new_layout) {
                visit(side, id, new_layout, rows)?;
            }
            continue;
        }
        let mut before = entries(before, entry.original_layout).peekable();
        let mut after = entries(after, new_layout).peekable();
        while before.peek().is_some() || after.peek().is_some() {
            let id = match (before.peek(), after.peek()) {
                (Some((a, _)), Some((b, _))) => *a.min(b),
                (Some((a, _)), None) | (None, Some((a, _))) => *a,
                _ => unreachable!(),
            };
            let old = if before.peek().is_some_and(|(i, _)| *i == id) {
                before.next().unwrap().1
            } else {
                &[]
            };
            let new = if after.peek().is_some_and(|(i, _)| *i == id) {
                after.next().unwrap().1
            } else {
                &[]
            };
            if old != new {
                visit(side, id, new_layout, new)?;
            }
        }
    }
    Ok(())
}

pub(super) fn root(
    state: &JoinState,
    layout: Layout,
    unloaded: Option<&[EntryIds; 2]>,
) -> Result<Option<Vec<u8>>> {
    if state.left.is_empty()
        && state.right.is_empty()
        && unloaded.is_none_or(|ids| ids.iter().all(EntryIds::is_empty))
    {
        return Ok(None);
    }
    Ok(Some(match layout {
        Layout::Compact => encode_compact(state)?,
        Layout::Pages => encode_manifest(state),
        Layout::Rows => {
            if unloaded.is_some() {
                encode_rows_with_unloaded(state, unloaded)
            } else {
                encode_rows_manifest(state)
            }
        }
    }))
}

pub(in super::super) fn mutations(entry: &StagedState) -> Result<Vec<StateMutation>> {
    let mut changes = Vec::new();
    changed_rows(entry, |side, id, layout, rows| {
        changes.push(StateMutation {
            key: entry_key(&entry.key, side, id, layout),
            value: if rows.is_empty() {
                None
            } else {
                Some(encode_page(rows)?)
            },
        });
        Ok(())
    })?;
    let changed_layout = entry.original_layout != layout(entry);
    let old = if changed_layout {
        None
    } else {
        root(
            &entry.original,
            entry.original_layout,
            entry.unloaded.as_ref().map(|rows| &rows.original_ids),
        )?
    };
    let new = root(
        &entry.value,
        layout(entry),
        entry.unloaded.as_ref().map(|rows| &rows.ids),
    )?;
    if changed_layout || old != new {
        changes.push(StateMutation {
            key: manifest_key(&entry.key),
            value: new,
        });
    }
    Ok(changes)
}

pub(super) fn workspace(entry: &StagedState) -> usize {
    let mut count = 0usize;
    let mut directory_bytes = 0usize;
    for (state, layout) in [
        (&entry.original, entry.original_layout),
        (&entry.value, layout(entry)),
    ] {
        if state.left.is_empty() && state.right.is_empty() && entry.unloaded.is_none() {
            continue;
        }
        count += 1;
        if layout != Layout::Compact {
            let retained_pages = entry.unloaded.as_ref().map_or(0, |u| {
                u.original_ids
                    .iter()
                    .map(EntryIds::bitmap_count)
                    .sum::<usize>()
            });
            let bytes = 39
                + 16 * (pages(&state.left).count() + pages(&state.right).count() + retained_pages);
            directory_bytes = directory_bytes.saturating_add(bytes.saturating_sub(512));
        }
    }
    changed_rows(entry, |_, _, _, _| {
        count = count.saturating_add(1);
        Ok(())
    })
    .expect("counting mutations is infallible");
    directory_bytes.saturating_add(count.saturating_mul(entry.key.key.len().saturating_add(512)))
}
