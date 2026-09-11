// Copyright 2026 StreamFusion Authors
// Licensed under the Apache License, Version 2.0.

use super::*;

// Leave room for directory mutations alongside the normal 1,024-row input slice.
// The byte ceiling independently bounds wide-row encoding.
const WRITE_ROWS: usize = 4096;
const WRITE_BYTES: usize = 256 << 10;

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
    memory.resize(retained)?;
    let mut writer = Writer {
        state,
        memory,
        retained,
        pending: Vec::new(),
        bytes: 0,
        writes,
    };
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
        writer.retained = writer.retained.saturating_sub(held);
    }
    writer.retained = 0;
    writer.flush(0)?;
    writer.memory.resize(0)?;
    Ok(())
}

struct Writer<'a> {
    state: &'a mut dyn KeyedState,
    memory: &'a mut HostMemoryReservation,
    retained: usize,
    pending: Vec<StateMutation>,
    bytes: usize,
    writes: &'a mut u64,
}

impl Writer<'_> {
    fn bound(&self, bytes: usize, rows: usize, extra: usize) -> usize {
        self.retained
            .saturating_add(bytes.saturating_mul(4))
            .saturating_add(rows.saturating_mul(512))
            .saturating_add(extra)
            .saturating_add(65536)
    }

    fn admit(&mut self, bytes: usize, rows: usize, extra: usize) -> Result<()> {
        let required = self.bound(bytes, rows, extra);
        if required > self.memory.size() {
            let rounded = required
                .checked_add(65535)
                .map_or(required, |bytes| bytes / 65536 * 65536);
            if let Err(error) = self.memory.resize(rounded) {
                if !matches!(error, DataFusionError::ResourcesExhausted(_))
                    || rounded == required
                    || !self.pending.is_empty()
                {
                    return Err(error);
                }
                self.memory.resize(required)?;
            }
        }
        Ok(())
    }

    fn admit_extra(&mut self, extra: usize) -> Result<()> {
        match self.admit(self.bytes, self.pending.len(), extra) {
            Err(DataFusionError::ResourcesExhausted(_)) if !self.pending.is_empty() => {
                self.flush(0)?;
                self.admit(0, 0, extra)
            }
            result => result,
        }
    }

    fn push(
        &mut self,
        bytes: usize,
        extra: usize,
        encode: impl FnOnce() -> Result<StateMutation>,
    ) -> Result<()> {
        if !self.pending.is_empty()
            && (self.pending.len() == WRITE_ROWS || self.bytes.saturating_add(bytes) > WRITE_BYTES)
        {
            self.flush(extra)?;
        }
        match self.admit(
            self.bytes.saturating_add(bytes),
            self.pending.len() + 1,
            extra,
        ) {
            Err(DataFusionError::ResourcesExhausted(_)) if !self.pending.is_empty() => {
                self.flush(extra)?;
                self.admit(bytes, 1, extra)?;
            }
            result => result?,
        }
        let mutation = encode()?;
        self.bytes = self.bytes.saturating_add(
            mutation
                .key
                .key
                .capacity()
                .saturating_add(mutation.value.as_ref().map_or(0, Vec::capacity)),
        );
        self.pending.push(mutation);
        Ok(())
    }

    fn flush(&mut self, extra: usize) -> Result<()> {
        if self.pending.is_empty() {
            return Ok(());
        }
        // Release credit for already-consumed staged groups before backend growth. Admission
        // and release happen per write page, never per individual dirty row. Encoding has
        // finished, so release its scratch allowance before the backend reserves its own
        // persistent growth and write workspace. Keep the pending buffers themselves charged.
        self.memory.resize(
            self.retained
                .saturating_add(self.bytes)
                .saturating_add(
                    self.pending
                        .capacity()
                        .saturating_mul(size_of::<StateMutation>()),
                )
                .saturating_add(extra)
                .saturating_add(65536),
        )?;
        self.state.write_batch(std::mem::take(&mut self.pending))?;
        self.bytes = 0;
        *self.writes = self.writes.saturating_add(1);
        Ok(())
    }
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
