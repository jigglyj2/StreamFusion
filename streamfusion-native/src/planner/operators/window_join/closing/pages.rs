// Copyright 2026 StreamFusion Authors
// Licensed under the Apache License, Version 2.0.

//! Bounded ordered state reads. The complete right side remains admitted; only one left
//! page is decoded at a time, so DataFusion retains Flink's left-major duplicate order.

use super::*;
use crate::planner::operators::sortable_state::{prefix_end, PAGE_BYTES, PAGE_ROWS};

pub(super) struct Decoded {
    pub batch: RecordBatch,
    pub bytes: u64,
    pub max_row_bytes: usize,
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
    let mut bytes = 0u64;
    let mut max_row_bytes = 0;
    processor.state.visit_range(
        keys.header.key_group,
        &start.key,
        end.as_deref(),
        PAGE_ROWS,
        page_bytes,
        &mut |page| {
            let page_bytes = page.iter().try_fold(0u64, |sum, (_, value)| {
                sum.checked_add(value.len() as u64)
                    .ok_or_else(invalid_index)
            })?;
            let count = rows.len().saturating_add(page.len());
            let next = first.checked_add(count as u64).ok_or_else(invalid_index)?;
            bytes = bytes.checked_add(page_bytes).ok_or_else(invalid_index)?;
            if next > header.counts()[side] || bytes > header.bytes() {
                return Err(invalid_index());
            }
            workspace.try_grow(
                usize::try_from(page_bytes)
                    .unwrap_or(usize::MAX)
                    .saturating_mul(8)
                    .saturating_add(page.len().saturating_mul(256)),
            )?;
            for &(key, value) in page {
                let expected = first + rows.len() as u64;
                if key.len() != prefix.len() + 8
                    || !key.starts_with(&prefix)
                    || u64::from_be_bytes(key[key.len() - 8..].try_into().unwrap()) != expected
                {
                    return Err(invalid_index());
                }
                max_row_bytes = max_row_bytes.max(value.len());
                rows.push(value.to_vec());
            }
            Ok(complete)
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
    Ok(Decoded {
        batch,
        bytes,
        max_row_bytes,
    })
}

pub(super) fn invalid_index() -> DataFusionError {
    DataFusionError::Execution("window join payload does not match its index".into())
}

pub(super) fn delete_rows(
    processor: &mut WindowJoinProcessor,
    keys: &WindowKeys,
    side: usize,
    first: u64,
    count: u64,
) -> Result<()> {
    let mut workspace = processor
        .scratch_reservation
        .sibling("window join completed page deletes");
    let mut cursor = first;
    let end = first.checked_add(count).ok_or_else(invalid_index)?;
    while cursor < end {
        let size = (end - cursor).min(PAGE_ROWS as u64) as usize;
        workspace.resize(size.saturating_mul(keys.side_prefix(side).len().saturating_add(128)))?;
        let deletes = (cursor..cursor + size as u64)
            .map(|sequence| StateMutation {
                key: keys.payload_key(side, sequence),
                value: None,
            })
            .collect();
        processor.state.write_batch(deletes)?;
        processor.state_write_batches = processor.state_write_batches.saturating_add(1);
        cursor += size as u64;
    }
    Ok(())
}
