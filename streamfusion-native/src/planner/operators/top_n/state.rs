// Copyright 2026 StreamFusion Authors
// Licensed under the Apache License, Version 2.0

use arrow::compute::SortOptions;
use arrow::datatypes::SchemaRef;
#[cfg(test)]
use arrow::record_batch::RecordBatch;
use arrow_row::{RowConverter, SortField};
use datafusion::error::{DataFusionError, Result};

const STATE_MAGIC: &[u8; 4] = b"SFTN";
const LEGACY_STATE_VERSION: u8 = 4;
const STATE_VERSION: u8 = 5;

#[derive(Clone, Debug)]
#[cfg(test)]
pub(super) struct StoredState {
    pub(super) next_sequence: u64,
    pub(super) rank_end: Option<i64>,
    pub(super) last_access_millis: i64,
    pub(super) sequences: Vec<u64>,
    pub(super) rows: RecordBatch,
}

pub(super) struct EncodedStoredState<'a> {
    pub(super) next_sequence: u64,
    pub(super) rank_end: Option<i64>,
    pub(super) last_access_millis: i64,
    pub(super) sequences: Vec<u64>,
    pub(super) row_kinds: Option<Vec<i8>>,
    pub(super) rows: Vec<&'a [u8]>,
}

pub(super) fn row_converter(schema: &SchemaRef) -> Result<RowConverter> {
    let fields = schema
        .fields()
        .iter()
        .map(|field| {
            SortField::new_with_options(
                field.data_type().clone(),
                SortOptions {
                    descending: false,
                    nulls_first: true,
                },
            )
        })
        .collect();
    Ok(RowConverter::new(fields)?)
}

#[cfg(test)]
pub(super) fn encode_state(state: &StoredState, converter: &RowConverter) -> Result<Vec<u8>> {
    if state.sequences.len() != state.rows.num_rows() {
        return Err(DataFusionError::Execution(
            "top-n state sequence count does not match its Arrow rows".to_string(),
        ));
    }
    let rows = converter.convert_columns(state.rows.columns())?;
    encode_state_rows(
        state.next_sequence,
        state.rank_end,
        state.last_access_millis,
        &state.sequences,
        rows.iter(),
    )
}

#[cfg(test)]
pub(super) fn encode_state_rows<'a>(
    next_sequence: u64,
    rank_end: Option<i64>,
    last_access_millis: i64,
    sequences: &[u64],
    rows: impl IntoIterator<Item = arrow_row::Row<'a>>,
) -> Result<Vec<u8>> {
    encode_state_rows_with_kinds(
        next_sequence,
        rank_end,
        last_access_millis,
        sequences,
        None,
        rows,
    )
}

pub(super) fn encode_state_rows_with_kinds<'a>(
    next_sequence: u64,
    rank_end: Option<i64>,
    last_access_millis: i64,
    sequences: &[u64],
    row_kinds: Option<&[i8]>,
    rows: impl IntoIterator<Item = arrow_row::Row<'a>>,
) -> Result<Vec<u8>> {
    if row_kinds.is_some_and(|kinds| kinds.len() != sequences.len()) {
        return Err(DataFusionError::Execution(
            "top-n state RowKind count does not match its Arrow rows".to_string(),
        ));
    }
    let mut bytes = Vec::with_capacity(42usize.saturating_add(sequences.len().saturating_mul(12)));
    bytes.extend_from_slice(STATE_MAGIC);
    bytes.push(STATE_VERSION);
    bytes.extend_from_slice(&next_sequence.to_le_bytes());
    bytes.extend_from_slice(&last_access_millis.to_le_bytes());
    bytes.push(u8::from(rank_end.is_some()));
    bytes.extend_from_slice(&rank_end.unwrap_or_default().to_le_bytes());
    bytes.extend_from_slice(
        &u32::try_from(sequences.len())
            .map_err(|_| {
                DataFusionError::Execution("top-n candidate count exceeds u32".to_string())
            })?
            .to_le_bytes(),
    );
    for sequence in sequences {
        bytes.extend_from_slice(&sequence.to_le_bytes());
    }
    bytes.push(u8::from(row_kinds.is_some()));
    if let Some(kinds) = row_kinds {
        bytes.extend(kinds.iter().map(|kind| *kind as u8));
    }
    let mut row_count = 0usize;
    for row in rows {
        let row = row.data();
        bytes.extend_from_slice(
            &u32::try_from(row.len())
                .map_err(|_| {
                    DataFusionError::Execution("top-n Arrow row is too large".to_string())
                })?
                .to_le_bytes(),
        );
        bytes.extend_from_slice(row);
        row_count += 1;
    }
    if row_count != sequences.len() {
        return Err(DataFusionError::Execution(
            "top-n encoded row count does not match its sequences".to_string(),
        ));
    }
    Ok(bytes)
}

#[cfg(test)]
pub(super) fn decode_state(
    bytes: &[u8],
    expected_schema: &SchemaRef,
    converter: &RowConverter,
) -> Result<StoredState> {
    let encoded = decode_state_rows(bytes)?;
    let parser = converter.parser();
    let columns = converter.convert_rows(encoded.rows.into_iter().map(|row| parser.parse(row)))?;
    let rows = RecordBatch::try_new(expected_schema.clone(), columns)?;
    Ok(StoredState {
        next_sequence: encoded.next_sequence,
        rank_end: encoded.rank_end,
        last_access_millis: encoded.last_access_millis,
        sequences: encoded.sequences,
        rows,
    })
}

pub(super) fn decode_state_rows(bytes: &[u8]) -> Result<EncodedStoredState<'_>> {
    if bytes.len() < 34
        || &bytes[..4] != STATE_MAGIC
        || !matches!(bytes[4], LEGACY_STATE_VERSION | STATE_VERSION)
    {
        return Err(DataFusionError::Execution(
            "invalid native Top-N Arrow state".to_string(),
        ));
    }
    let version = bytes[4];
    let mut offset = 5;
    let next_sequence = read_u64(bytes, &mut offset)?;
    let last_access_millis = read_i64(bytes, &mut offset)?;
    let rank_end_present = read_bytes(bytes, &mut offset, 1)?[0];
    if rank_end_present > 1 {
        return Err(DataFusionError::Execution(
            "native Top-N state has an invalid rank-end marker".to_string(),
        ));
    }
    let rank_end_value = read_i64(bytes, &mut offset)?;
    let rank_end = (rank_end_present == 1).then_some(rank_end_value);
    let count = read_u32(bytes, &mut offset)? as usize;
    let mut sequences = Vec::with_capacity(count);
    for _ in 0..count {
        sequences.push(read_u64(bytes, &mut offset)?);
    }
    let row_kinds = if version >= STATE_VERSION {
        match read_bytes(bytes, &mut offset, 1)?[0] {
            0 => None,
            1 => Some(
                read_bytes(bytes, &mut offset, count)?
                    .iter()
                    .map(|kind| *kind as i8)
                    .collect(),
            ),
            _ => {
                return Err(DataFusionError::Execution(
                    "native Top-N state has an invalid RowKind marker".to_string(),
                ));
            }
        }
    } else {
        None
    };
    let mut encoded_rows = Vec::with_capacity(count);
    for _ in 0..count {
        let length = read_u32(bytes, &mut offset)? as usize;
        encoded_rows.push(read_bytes(bytes, &mut offset, length)?);
    }
    if offset != bytes.len() {
        return Err(DataFusionError::Execution(
            "native Top-N state has trailing bytes".to_string(),
        ));
    }
    Ok(EncodedStoredState {
        next_sequence,
        rank_end,
        last_access_millis,
        sequences,
        row_kinds,
        rows: encoded_rows,
    })
}

fn read_u32(bytes: &[u8], offset: &mut usize) -> Result<u32> {
    Ok(u32::from_le_bytes(read_exact::<4>(bytes, offset)?))
}

fn read_u64(bytes: &[u8], offset: &mut usize) -> Result<u64> {
    Ok(u64::from_le_bytes(read_exact::<8>(bytes, offset)?))
}

fn read_i64(bytes: &[u8], offset: &mut usize) -> Result<i64> {
    Ok(i64::from_le_bytes(read_exact::<8>(bytes, offset)?))
}

fn read_exact<const N: usize>(bytes: &[u8], offset: &mut usize) -> Result<[u8; N]> {
    Ok(read_bytes(bytes, offset, N)?.try_into().unwrap())
}

fn read_bytes<'a>(bytes: &'a [u8], offset: &mut usize, count: usize) -> Result<&'a [u8]> {
    let end = offset.checked_add(count).ok_or_else(|| {
        DataFusionError::Execution("native Top-N state offset overflow".to_string())
    })?;
    let value = bytes
        .get(*offset..end)
        .ok_or_else(|| DataFusionError::Execution("truncated native Top-N state".to_string()))?;
    *offset = end;
    Ok(value)
}
