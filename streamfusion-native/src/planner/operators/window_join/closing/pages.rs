// Copyright 2026 StreamFusion Authors
// Licensed under the Apache License, Version 2.0.

//! Bounded ordered state reads. The complete right side remains admitted; only one left
//! page is decoded at a time, so DataFusion retains Flink's left-major duplicate order.

use super::*;
use crate::planner::operators::sortable_state::{prefix_end, PAGE_BYTES, PAGE_ROWS};

pub(super) struct PayloadEntries {
    starts: Vec<u64>,
    // Entry identities grow with decoded state and outlive the decoding workspace.
    _memory: HostMemoryReservation,
}

pub(super) struct Decoded {
    pub batch: RecordBatch,
    pub bytes: u64,
    pub max_row_bytes: usize,
    pub entries: PayloadEntries,
    pub exhausted: bool,
}

pub(super) fn decode(
    processor: &mut WindowJoinProcessor,
    keys: &WindowKeys,
    header: &Header,
    side: usize,
    first: u64,
    complete: bool,
) -> Result<Decoded> {
    let mut workspace = processor
        .scratch_reservation
        .sibling("window join page decode");
    let start = keys.payload_key(side, first);
    let prefix = keys.side_prefix(side);
    let end = prefix_end(&prefix);
    // Reserve scan headroom before calling either backend. RocksDB independently owns its
    // returned page. Leave room for that page, encoded copies and Arrow decode. The limit
    // derives from the existing allowance; it is not a new deployment memory setting.
    let available = workspace.available_capacity()?.unwrap_or(PAGE_BYTES * 16);
    let page_bytes = usize::try_from(header.bytes())
        .unwrap_or(usize::MAX)
        .saturating_add(PAGE_ROWS.saturating_mul(prefix.len().saturating_add(128)))
        .min(available / 16)
        .max(32 << 10);
    workspace.resize(
        page_bytes
            .saturating_mul(2)
            .saturating_add(4096)
            .saturating_add(
                processor.visible_schemas[side]
                    .fields()
                    .len()
                    .saturating_mul(1024),
            ),
    )?;
    let mut rows = Vec::new();
    let mut starts = Vec::new();
    let mut bytes = 0u64;
    let mut max_row_bytes = 0;
    // A successful range visit reaches its end unless this decoder stopped the visitor.
    // Remember that validation across computation; the active watermark forbids new input.
    let mut exhausted = true;
    processor.state.visit_range(
        keys.header.key_group,
        &start.key,
        end.as_deref(),
        if header.paged && !complete {
            1
        } else {
            PAGE_ROWS
        },
        page_bytes,
        &mut |page| {
            let mut payload_bytes = 0usize;
            let mut row_count = 0usize;
            for &(_, value) in page {
                let payload = payload_pages::Rows::new(value, header.paged)?;
                payload_bytes = payload_bytes
                    .checked_add(payload.bytes())
                    .ok_or_else(invalid_index)?;
                row_count = row_count
                    .checked_add(payload.len())
                    .ok_or_else(invalid_index)?;
            }
            let count = rows.len().saturating_add(row_count);
            if !complete && header.paged && !rows.is_empty() && count > PAGE_ROWS {
                exhausted = false;
                return Ok(false);
            }
            let next = first.checked_add(count as u64).ok_or_else(invalid_index)?;
            bytes = bytes
                .checked_add(payload_bytes as u64)
                .ok_or_else(invalid_index)?;
            if next > header.counts()[side] || bytes > header.bytes() {
                return Err(invalid_index());
            }
            workspace.try_grow(
                payload_bytes
                    .saturating_mul(8)
                    .saturating_add(row_count.saturating_mul(256)),
            )?;
            for &(key, value) in page {
                let expected = first + rows.len() as u64;
                if key.len() != prefix.len() + 8
                    || !key.starts_with(&prefix)
                    || u64::from_be_bytes(key[key.len() - 8..].try_into().unwrap()) != expected
                {
                    return Err(invalid_index());
                }
                // Keep only the validated ordinal, not another copy of each state key.
                // The per-row decode allowance also covers this vector's capacity.
                starts.push(expected);
                let payload = payload_pages::Rows::new(value, header.paged)?;
                for row in payload.iter() {
                    max_row_bytes = max_row_bytes.max(row.len());
                    rows.push(row.to_vec());
                }
            }
            let keep_reading = complete || (header.paged && rows.len() < PAGE_ROWS);
            exhausted &= keep_reading;
            Ok(keep_reading)
        },
    )?;
    processor.state_read_batches = processor.state_read_batches.saturating_add(1);
    if (complete && first + rows.len() as u64 != header.counts()[side])
        || (rows.is_empty() && first != header.counts()[side])
    {
        return Err(invalid_index());
    }
    let converter = &processor.row_converters[side];
    let parser = converter.parser();
    let columns = converter.convert_rows(rows.iter().map(|row| parser.parse(row)))?;
    let batch = RecordBatch::try_new(processor.visible_schemas[side].clone(), columns)?;
    let memory = workspace.split(batch_bytes(&batch)?, "closed window Arrow payload")?;
    let batch = host_batch(batch, memory)?;
    drop(rows);
    let entry_memory = workspace.split(
        starts.capacity().saturating_mul(std::mem::size_of::<u64>()),
        "closed window payload entry identities",
    )?;
    Ok(Decoded {
        batch,
        bytes,
        max_row_bytes,
        entries: PayloadEntries {
            starts,
            _memory: entry_memory,
        },
        exhausted,
    })
}

pub(super) fn invalid_index() -> DataFusionError {
    DataFusionError::Execution("window join payload does not match its index".into())
}

pub(super) fn delete_rows(
    processor: &mut WindowJoinProcessor,
    keys: &WindowKeys,
    side: usize,
    entries: &PayloadEntries,
) -> Result<()> {
    let mut workspace = processor
        .scratch_reservation
        .sibling("window join completed page deletes");
    let key_bytes = keys.side_prefix(side).len().saturating_add(8);
    // Decoding already validated the physical entry starts, including legacy unpaged rows.
    // The acknowledged prefix is immutable until the watermark invocation completes. Derive
    // bounded delete batches directly instead of reading the same payloads a second time.
    for chunk in entries.starts.chunks(PAGE_ROWS) {
        workspace.resize(chunk.len().saturating_mul(key_bytes.saturating_add(128)))?;
        let deletes = chunk
            .iter()
            .map(|&first| StateMutation {
                key: keys.payload_key(side, first),
                value: None,
            })
            .collect();
        processor.state.write_batch(deletes)?;
        processor.state_write_batches = processor.state_write_batches.saturating_add(1);
    }
    Ok(())
}
