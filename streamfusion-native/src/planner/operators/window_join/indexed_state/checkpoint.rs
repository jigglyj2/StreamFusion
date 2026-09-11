// Copyright 2026 StreamFusion Authors
// Licensed under the Apache License, Version 2.0.

use super::*;

#[derive(Default)]
struct Encoding {
    legacy: bool,
    indexed: bool,
}

impl Encoding {
    fn validate(&mut self, key: &[u8], value: &[u8]) -> Result<()> {
        match key.first() {
            Some(&WINDOW_KEY_PREFIX) => {
                decode_window_end(key)?;
                self.legacy = true;
            }
            Some(&INDEX) | Some(&PAYLOAD) => {
                validate_indexed_key(key)?;
                if key[0] == INDEX {
                    Header::decode(value)?;
                }
                self.indexed = true;
            }
            _ if key == TIMER_STATE_KEY || key == SHARED_STATE_KEY => {}
            _ => {
                return Err(DataFusionError::Execution(
                    "unknown window join checkpoint key".into(),
                ))
            }
        }
        if self.legacy && self.indexed {
            return Err(DataFusionError::Execution(
                "window join checkpoint mixes legacy and indexed state".into(),
            ));
        }
        Ok(())
    }
}

pub(in super::super) fn restore_canonical(
    state: &mut dyn KeyedState,
    group: u32,
    snapshot: &[u8],
    converter: &mut RowConverter,
    owner: &HostMemoryReservation,
) -> Result<()> {
    let entries = streamfusion_state_abi::key_group_snapshot_entries(group, snapshot)
        .map_err(|error| DataFusionError::Execution(error.to_string()))?;
    let mut encoding = Encoding::default();
    for (key, value) in entries {
        encoding.validate(key, value)?;
    }
    state.restore_key_group(group, snapshot, owner)?;
    if encoding.legacy {
        for (key, value) in streamfusion_state_abi::key_group_snapshot_entries(group, snapshot)
            .map_err(|error| DataFusionError::Execution(error.to_string()))?
        {
            migrate_entry(state, group, key, value, converter, owner)?;
        }
    }
    Ok(())
}

pub(in super::super) fn restore_physical(
    state: &mut dyn KeyedState,
    group: u32,
    source: &dyn KeyedState,
    converter: &mut RowConverter,
    owner: &HostMemoryReservation,
) -> Result<()> {
    // The source is an immutable checkpoint. Validate before installing any target entries;
    // revisit it for legacy migration without retaining a second copy of the key group.
    let mut encoding = Encoding::default();
    source.visit_key_group_admitted(group, 1024, 256 << 10, owner, &mut |page| {
        for (key, value) in page {
            encoding.validate(key, value)?;
        }
        Ok(())
    })?;
    crate::state::import_key_group(state, source, group, owner, &mut |_, _| Ok(()))?;
    if encoding.legacy {
        source.visit_key_group_admitted(group, 1024, 256 << 10, owner, &mut |page| {
            for (key, value) in page {
                migrate_entry(state, group, key, value, converter, owner)?;
            }
            Ok(())
        })?;
    }
    Ok(())
}

/// Migrate one SFWJ/2 whole-window value, preserving duplicate arrival order and timers.
/// Current indexed checkpoints need no decoding workspace; old opaque windows are admitted
/// individually, and failure requires discarding the initialization-only target context.
fn migrate_entry(
    state: &mut dyn KeyedState,
    group: u32,
    key: &[u8],
    value: &[u8],
    converter: &mut RowConverter,
    owner: &HostMemoryReservation,
) -> Result<()> {
    if key.first() != Some(&WINDOW_KEY_PREFIX) {
        return Ok(());
    }
    let mut workspace = owner.sibling("window join checkpoint migration workspace");
    workspace.resize(
        value
            .len()
            .saturating_mul(8)
            .saturating_add(key.len())
            .saturating_add(65536),
    )?;
    let timer = StateKey {
        key_group: group,
        key: key.to_vec(),
    };
    let keys = WindowKeys::new(&timer, converter)?;
    let legacy = decode_state(value)?;
    workspace.resize(
        workspace.size().saturating_add(
            (legacy.left.len() + legacy.right.len())
                .saturating_mul(keys.payload.len().saturating_add(128)),
        ),
    )?;
    let mut mutations = Vec::new();
    let mut header = Header::default();
    for (side, rows) in [legacy.left, legacy.right].into_iter().enumerate() {
        payload_pages::append(&keys, &mut header, side, rows, &mut mutations)?;
    }
    mutations.push(StateMutation {
        key: keys.header,
        value: Some(header.encode()),
    });
    mutations.push(StateMutation {
        key: timer,
        value: None,
    });
    state.write_batch(mutations)
}
