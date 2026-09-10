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
                let payload = payload_pages::Rows::new(value, header.paged)?;
                for row in payload.iter() {
                    max_row_bytes = max_row_bytes.max(row.len());
                    rows.push(row.to_vec());
                }
            }
            Ok(complete || (header.paged && rows.len() < PAGE_ROWS))
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
    max_row_bytes: usize,
) -> Result<()> {
    let mut workspace = processor
        .scratch_reservation
        .sibling("window join completed page deletes");
    if count == 0 {
        return Ok(());
    }
    let end = keys.payload_key(side, first.checked_add(count).ok_or_else(invalid_index)?);
    let mut start = keys.payload_key(side, first).key;
    let prefix_bytes = keys.side_prefix(side).len();
    let page_bytes = max_row_bytes
        .saturating_add(prefix_bytes)
        .saturating_add(128)
        .max(32 << 10);
    workspace.resize(
        page_bytes
            .saturating_mul(2)
            .saturating_add(PAGE_ROWS.saturating_mul(prefix_bytes.saturating_add(128))),
    )?;
    loop {
        let mut deletes = Vec::new();
        processor.state.visit_range(
            keys.header.key_group,
            &start,
            Some(&end.key),
            PAGE_ROWS,
            page_bytes,
            &mut |page| {
                for &(key, _) in page {
                    deletes.push(StateMutation {
                        key: StateKey {
                            key_group: keys.header.key_group,
                            key: key.to_vec(),
                        },
                        value: None,
                    });
                }
                Ok(false)
            },
        )?;
        processor.state_read_batches = processor.state_read_batches.saturating_add(1);
        let Some(last) = deletes.last() else {
            break;
        };
        start = last.key.key.clone();
        // The acknowledged prefix is immutable; an inclusive seek after deletion resumes
        // at its successor without retaining a growing list of payload keys.
        processor.state.write_batch(deletes)?;
        processor.state_write_batches = processor.state_write_batches.saturating_add(1);
    }
    Ok(())
}
