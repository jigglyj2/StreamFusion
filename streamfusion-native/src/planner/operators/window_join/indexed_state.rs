// Copyright 2026 StreamFusion Authors
// Licensed under the Apache License, Version 2.0

//! Append-only payloads, separate from a small window index. Timer identity stays in Flink's
//! original partition encoding; Arrow ordering is exclusively an internal storage concern.

use super::super::sortable_state::{prefix, prefix_end, FORMAT, PAGE_BYTES, PAGE_ROWS};
use super::*;
use arrow::array::Int64Array;

const INDEX: u8 = 0x91;
const PAYLOAD: u8 = 0x92;
const HEADER_MAGIC: &[u8; 4] = b"SFWI";

pub(super) struct WindowKeys {
    pub(super) header: StateKey,
    payload: Vec<u8>,
}

impl WindowKeys {
    pub(super) fn new(timer: &StateKey, converter: &mut RowConverter) -> Result<Self> {
        let end = decode_window_end(&timer.key)?;
        let partition = &timer.key[1..timer.key.len() - 8];
        let order = converter.convert_columns(&[Arc::new(Int64Array::from(vec![end]))])?;
        let mut header = prefix(INDEX, partition)?;
        header.extend_from_slice(order.row(0).data());
        let mut payload = prefix(PAYLOAD, partition)?;
        payload.extend_from_slice(order.row(0).data());
        Ok(Self {
            header: StateKey {
                key_group: timer.key_group,
                key: header,
            },
            payload,
        })
    }

    pub(super) fn payload_key(&self, side: usize, sequence: u64) -> StateKey {
        let mut key = self.payload.clone();
        key.push(side as u8);
        // Stable non-null arrival ordinal is a tie-breaker, separate from the Arrow window key.
        key.extend_from_slice(&sequence.to_be_bytes());
        StateKey {
            key_group: self.header.key_group,
            key,
        }
    }

    pub(super) fn side_prefix(&self, side: usize) -> Vec<u8> {
        let mut key = self.payload.clone();
        key.push(side as u8);
        key
    }
}

pub(super) struct Header {
    pub(super) paged: bool,
    counts: [u64; 2],
    bytes: u64,
}

impl Default for Header {
    fn default() -> Self {
        Self {
            paged: true,
            counts: [0; 2],
            bytes: 0,
        }
    }
}

impl Header {
    pub(super) fn counts(&self) -> [u64; 2] {
        self.counts
    }

    pub(super) fn bytes(&self) -> u64 {
        self.bytes
    }
    pub(super) fn append(&mut self, side: usize, bytes: usize) -> Result<u64> {
        let ordinal = self.counts[side];
        self.counts[side] = ordinal.checked_add(1).ok_or_else(overflow)?;
        self.bytes = self
            .bytes
            .checked_add(u64::try_from(bytes).map_err(|_| overflow())?)
            .ok_or_else(overflow)?;
        Ok(ordinal)
    }

    pub(super) fn encode(&self) -> Vec<u8> {
        let mut bytes = Vec::with_capacity(29);
        bytes.extend_from_slice(HEADER_MAGIC);
        bytes.push(if self.paged { 4 } else { 3 });
        for value in [self.counts[0], self.counts[1], self.bytes] {
            bytes.extend_from_slice(&value.to_le_bytes());
        }
        bytes
    }

    pub(super) fn decode(bytes: &[u8]) -> Result<Self> {
        if bytes.len() != 29 || &bytes[..4] != HEADER_MAGIC || !matches!(bytes[4], 3 | 4) {
            return Err(DataFusionError::Execution(
                "invalid window join index version or length".into(),
            ));
        }
        let value = |offset| u64::from_le_bytes(bytes[offset..offset + 8].try_into().unwrap());
        let header = Self {
            paged: bytes[4] == 4,
            counts: [value(5), value(13)],
            bytes: value(21),
        };
        header.counts[0]
            .checked_add(header.counts[1])
            .ok_or_else(overflow)?;
        Ok(header)
    }

    pub(super) fn workspace_bound(&self, keys: &WindowKeys) -> usize {
        let count =
            usize::try_from(self.counts[0].saturating_add(self.counts[1])).unwrap_or(usize::MAX);
        // Payload copies, Arrow decode/gather, selection vectors and deletion keys. Admission is
        // at the closed-window boundary; the subsequent shared stream will drain output in pages.
        usize::try_from(self.bytes)
            .unwrap_or(usize::MAX)
            .saturating_mul(8)
            .saturating_add(count.saturating_mul(keys.payload.len().saturating_add(256)))
    }
}

fn overflow() -> DataFusionError {
    DataFusionError::ResourcesExhausted("window join index size overflow".into())
}

pub(super) fn read_window(
    state: &dyn KeyedState,
    keys: &WindowKeys,
    header: &Header,
    group: i32,
    rows: &mut Vec<(i32, i8, Vec<u8>)>,
    deletes: &mut Vec<StateMutation>,
) -> Result<()> {
    let mut seen = [0u64; 2];
    let mut bytes = 0u64;
    let end = prefix_end(&keys.payload);
    state.visit_range(
        keys.header.key_group,
        &keys.payload,
        end.as_deref(),
        PAGE_ROWS,
        PAGE_BYTES.max(
            usize::try_from(header.bytes)
                .unwrap_or(usize::MAX)
                .saturating_add(keys.payload.len())
                .saturating_add(128),
        ),
        &mut |page| {
            for &(key, value) in page {
                if key.len() != keys.payload.len() + 9 || !key.starts_with(&keys.payload) {
                    return Err(DataFusionError::Execution(
                        "malformed window join payload key".into(),
                    ));
                }
                let side = key[keys.payload.len()] as usize;
                if side > 1
                    || u64::from_be_bytes(key[key.len() - 8..].try_into().unwrap()) != seen[side]
                {
                    return Err(DataFusionError::Execution(
                        "window join payload order has a gap or invalid side".into(),
                    ));
                }
                let payload = payload_pages::Rows::new(value, header.paged)?;
                seen[side] = seen[side]
                    .checked_add(payload.len() as u64)
                    .ok_or_else(overflow)?;
                bytes = bytes
                    .checked_add(payload.bytes() as u64)
                    .ok_or_else(overflow)?;
                if seen[side] > header.counts[side] || bytes > header.bytes {
                    return Err(DataFusionError::Execution(
                        "window join payload exceeds its index".into(),
                    ));
                }
                rows.extend(payload.iter().map(|row| (group, side as i8, row.to_vec())));
                deletes.push(StateMutation {
                    key: StateKey {
                        key_group: keys.header.key_group,
                        key: key.to_vec(),
                    },
                    value: None,
                });
            }
            Ok(true)
        },
    )?;
    if seen != header.counts || bytes != header.bytes {
        return Err(DataFusionError::Execution(
            "window join payload does not match its index".into(),
        ));
    }
    Ok(())
}

/// Convert the old SFWJ/2 whole-window values once at restore, preserving duplicate arrival
/// order and the original timer identities. Canonical snapshots remain backend independent.
pub(super) fn migrate_legacy(
    state: &mut dyn KeyedState,
    group: u32,
    snapshot: &[u8],
    converter: &mut RowConverter,
    owner: &HostMemoryReservation,
) -> Result<()> {
    let mut workspace = owner.sibling("window join checkpoint migration workspace");
    workspace.resize(snapshot.len().saturating_mul(8).saturating_add(65536))?;
    let entries = crate::state::decode_key_group_snapshot(group, snapshot)?;
    for (key, value) in &entries {
        match key.first() {
            Some(&WINDOW_KEY_PREFIX) => {
                decode_window_end(key)?;
            }
            Some(&INDEX) | Some(&PAYLOAD) => {
                validate_indexed_key(key)?;
                if key[0] == INDEX {
                    Header::decode(value)?;
                }
            }
            _ if key == TIMER_STATE_KEY || key == SHARED_STATE_KEY => {}
            _ => {
                return Err(DataFusionError::Execution(
                    "unknown window join checkpoint key".into(),
                ))
            }
        }
    }
    let has_legacy = entries
        .iter()
        .any(|(k, _)| k.first() == Some(&WINDOW_KEY_PREFIX));
    let has_index = entries
        .iter()
        .any(|(k, _)| matches!(k.first(), Some(&INDEX) | Some(&PAYLOAD)));
    if has_legacy && has_index {
        return Err(DataFusionError::Execution(
            "window join checkpoint mixes legacy and indexed state".into(),
        ));
    }
    if !has_legacy {
        return Ok(());
    }
    let mut mutations = Vec::new();
    let mut bound = workspace.size();
    for (key, value) in entries {
        if key.first() != Some(&WINDOW_KEY_PREFIX) {
            continue;
        }
        let timer = StateKey {
            key_group: group,
            key,
        };
        let keys = WindowKeys::new(&timer, converter)?;
        let legacy = decode_state(&value)?;
        let mut header = Header::default();
        bound = bound.saturating_add(
            (legacy.left.len() + legacy.right.len())
                .saturating_mul(keys.payload.len().saturating_add(128)),
        );
        workspace.resize(bound)?;
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
    }
    state.write_batch(mutations)
}

fn validate_indexed_key(key: &[u8]) -> Result<()> {
    let malformed = || DataFusionError::Execution("invalid window join Arrow key format".into());
    if key.len() < 20 || &key[1..7] != FORMAT {
        return Err(malformed());
    }
    let partition_len = u32::from_be_bytes(key[7..11].try_into().unwrap()) as usize;
    let extra = if key[0] == PAYLOAD { 9 } else { 0 };
    if key.len() != partition_len.saturating_add(20 + extra)
        || key[11 + partition_len] != 1
        || (extra != 0 && key[key.len() - 9] > 1)
    {
        return Err(malformed());
    }
    Ok(())
}
