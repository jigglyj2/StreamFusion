// Copyright 2026 StreamFusion Authors
// Licensed under the Apache License, Version 2.0.

use super::write_batch::Writer;
use super::*;
use crate::planner::operators::regular_join::state_codec::StateView;

pub(super) enum Source<'a> {
    Canonical(&'a [u8]),
    Physical(&'a RocksPluginKeyedState),
}

impl Source<'_> {
    fn visit(
        &self,
        group: u32,
        owner: &HostMemoryReservation,
        visitor: &mut dyn FnMut(&[u8], &[u8]) -> Result<()>,
    ) -> Result<()> {
        match self {
            Self::Canonical(bytes) => {
                for (key, value) in streamfusion_state_abi::key_group_snapshot_entries(group, bytes)
                    .map_err(|error| DataFusionError::Execution(error.to_string()))?
                {
                    visitor(key, value)?;
                }
                Ok(())
            }
            Self::Physical(source) => {
                source.visit_key_group_admitted(group, 1024, 256 << 10, owner, &mut |page| {
                    for &(key, value) in page {
                        visitor(key, value)?;
                    }
                    Ok(())
                })
            }
        }
    }

    fn validate(&self, group: u32, owner: &HostMemoryReservation) -> Result<()> {
        if let Self::Canonical(bytes) = self {
            let entries = streamfusion_state_abi::key_group_snapshot_entries(group, bytes)
                .map_err(|error| DataFusionError::Execution(error.to_string()))?;
            let mut memory = owner.sibling("legacy join borrowed key validation");
            memory.resize(entries.len().saturating_mul(size_of::<&[u8]>() * 2))?;
            let mut keys = entries.map(|(key, _)| key).collect::<Vec<_>>();
            keys.sort_unstable();
            if keys.windows(2).any(|pair| pair[0] == pair[1]) {
                return Err(DataFusionError::Execution(
                    "duplicate legacy regular join checkpoint key".into(),
                ));
            }
        }
        // Physical RocksDB keys are unique. Validate every immutable legacy value before the
        // first destination write; no owned row payload or key-group snapshot is constructed.
        self.visit(group, owner, &mut |_, value| {
            StateView::parse(value)?;
            Ok(())
        })
    }
}

pub(super) fn restore(
    destination: &mut dyn KeyedState,
    source: Source<'_>,
    group: u32,
    owner: &HostMemoryReservation,
) -> Result<()> {
    source.validate(group, owner)?;
    crate::state::require_empty_key_group(destination, group, owner)?;
    let mut memory = owner.sibling("legacy join migration write page");
    let mut key_memory = owner.sibling("legacy join migration logical key and compact value");
    let mut writes = 0;
    let mut writer = Writer::new(destination, &mut memory, 0, &mut writes)?;
    source.visit(group, owner, &mut |key, value| {
        let required = key.len().saturating_mul(2).saturating_add(65536);
        if required > key_memory.size() {
            key_memory.resize(required)?;
        }
        let key = StateKey {
            key_group: group,
            key: key.to_vec(),
        };
        migrate_entry(&key, value, &mut writer)
    })?;
    drop(key_memory);
    writer.finish()
}

fn migrate_entry(key: &StateKey, value: &[u8], writer: &mut Writer<'_>) -> Result<()> {
    let view = StateView::parse(value)?;
    let counts = view.rows.each_ref().map(|rows| rows.len() as u64);
    let total = counts[0] + counts[1];
    if total == 0 {
        return Ok(());
    }
    // Only a bounded singleton may be decoded to preserve the historical compact-layout
    // selection exactly. Wide singletons and every larger history remain borrowed.
    if total == 1 && value.len() <= MAX_COMPACT_BYTES {
        let compact = view.to_owned();
        if compact_eligible(&compact) {
            return writer.push(key.key.len().saturating_add(MAX_COMPACT_BYTES), 0, || {
                Ok(StateMutation {
                    key: manifest_key(key),
                    value: Some(encode_compact(&compact)?),
                })
            });
        }
    }
    for (side, rows) in view.rows.iter().enumerate() {
        for (id, associations, row) in rows.iter() {
            writer.push(
                key.key.len().saturating_add(43).saturating_add(row.len()),
                0,
                || {
                    Ok(StateMutation {
                        key: row_key(key, side, id),
                        value: Some(encode_row(id, associations, row)?),
                    })
                },
            )?;
        }
    }
    let manifest_bytes = 39usize.saturating_add(
        counts
            .iter()
            .map(|count| count.div_ceil(PAGE_ROWS) as usize)
            .sum::<usize>()
            .saturating_mul(16),
    );
    writer.push(
        key.key
            .len()
            .saturating_add(1)
            .saturating_add(manifest_bytes),
        0,
        || {
            Ok(StateMutation {
                key: manifest_key(key),
                value: Some(encode_dense_manifest(view.matchable, counts)),
            })
        },
    )
}

#[cfg(test)]
mod tests;
