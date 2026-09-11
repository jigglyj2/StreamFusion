// Copyright 2026 StreamFusion Authors
// Licensed under the Apache License, Version 2.0.

use super::*;

use crate::state::StateBatchWriter as Writer;

/// Called only after every input transition completes. Writes may span backend pages, but no
/// checkpoint may observe an intermediate page; any error requires task recovery.
pub(in super::super) fn flush(
    state: &mut dyn KeyedState,
    entries: Vec<StagedState>,
    memory: &mut HostMemoryReservation,
    writes: &mut u64,
) -> Result<()> {
    let mut retained = entries
        .capacity()
        .saturating_mul(size_of::<StagedState>())
        .saturating_add(65536);
    for entry in &entries {
        retained = retained.saturating_add(retained_bytes(entry));
    }
    let mut writer = Writer::new(state, memory, retained, writes)?;
    for entry in entries {
        let held = retained_bytes(&entry);
        if entry.touched {
            mutations::changed_rows(&entry, |side, id, layout, rows| {
                let key_bytes = entry.key.key.len().saturating_add(18);
                let value_bytes = if rows.is_empty() {
                    0
                } else {
                    rows.iter().fold(9usize, |bytes, row| {
                        bytes.saturating_add(16).saturating_add(row.row.len())
                    })
                };
                writer.push(key_bytes.saturating_add(value_bytes), 0, || {
                    Ok(StateMutation {
                        key: entry_key(&entry.key, side, id, layout),
                        value: if rows.is_empty() {
                            None
                        } else {
                            Some(encode_page(rows)?)
                        },
                    })
                })
            })?;
            let layout = mutations::layout(&entry);
            // The old and new manifests overlap only for this comparison. Their size follows
            // compressed directory entries, not payload width or the number of queued writes.
            let manifest_bytes = manifest_bound(
                &entry.original,
                entry.original_layout,
                entry.unloaded.as_ref(),
            )
            .saturating_add(manifest_bound(
                &entry.value,
                layout,
                entry.unloaded.as_ref(),
            ));
            writer.admit_extra(manifest_bytes.saturating_mul(4))?;
            let old = mutations::root(
                &entry.original,
                entry.original_layout,
                entry.unloaded.as_ref(),
            )?;
            let new = mutations::root(&entry.value, layout, entry.unloaded.as_ref())?;
            if old != new {
                drop(old);
                let extra = new.as_ref().map_or(0, Vec::capacity);
                writer.push(
                    entry.key.key.len().saturating_add(9).saturating_add(extra),
                    extra,
                    || {
                        Ok(StateMutation {
                            key: manifest_key(&entry.key),
                            value: new,
                        })
                    },
                )?;
            }
        }
        drop(entry);
        writer.release_retained(held);
    }
    writer.finish()
}

fn manifest_bound(state: &JoinState, layout: Layout, unloaded: Option<&UnloadedRows>) -> usize {
    if layout == Layout::Compact {
        return 8192;
    }
    4096usize.saturating_add(
        32usize.saturating_mul(
            pages(&state.left)
                .count()
                .saturating_add(pages(&state.right).count())
                .saturating_add(unloaded.map_or(0, |rows| rows.ids.bitmap_count())),
        ),
    )
}

/// Original and current rows with the same identity normally share their immutable payload.
/// Bound their retained ownership once while keeping both row-vector capacities admitted.
fn retained_bytes(entry: &StagedState) -> usize {
    let mut bytes = entry.key.key.capacity().saturating_add(512).saturating_add(
        entry
            .unloaded
            .as_ref()
            .map_or(0, |rows| rows.ids.allocated_bytes()),
    );
    for (old, new) in [
        (&entry.original.left, &entry.value.left),
        (&entry.original.right, &entry.value.right),
    ] {
        bytes = bytes.saturating_add(
            old.capacity()
                .saturating_add(new.capacity())
                .saturating_mul(size_of::<StoredRow>()),
        );
        bytes = new.iter().fold(bytes, |bytes, row| {
            bytes.saturating_add(row.row.len()).saturating_add(64)
        });
        let mut current = new.iter().peekable();
        for row in old {
            while current
                .peek()
                .is_some_and(|candidate| candidate.id < row.id)
            {
                current.next();
            }
            if !current.peek().is_some_and(|candidate| {
                candidate.id == row.id && Arc::ptr_eq(&candidate.row, &row.row)
            }) {
                bytes = bytes.saturating_add(row.row.len()).saturating_add(64);
            }
        }
    }
    bytes
}

#[cfg(test)]
mod tests;
