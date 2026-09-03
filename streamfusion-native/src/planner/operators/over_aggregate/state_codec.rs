// Copyright 2026 StreamFusion Authors
// Licensed under the Apache License, Version 2.0

use std::collections::BTreeMap;

use datafusion::error::{DataFusionError, Result};

use super::AggregateValue;

const MAGIC: &[u8; 4] = b"SFOA";
const VERSION: u8 = 2;
const FIRST_VERSION: u8 = 1;

#[derive(Clone, Debug, PartialEq, Eq)]
pub(super) struct StoredRow {
    pub(super) id: i64,
    pub(super) event_timestamp: i64,
    pub(super) payload: Vec<u8>,
    pub(super) contributions: Vec<Option<AggregateValue>>,
    pub(super) output: Vec<Option<AggregateValue>>,
}

#[derive(Debug, PartialEq, Eq)]
pub(super) struct OverState {
    pub(super) next_id: i64,
    pub(super) rows: BTreeMap<Vec<u8>, Vec<StoredRow>>,
}

impl Default for OverState {
    fn default() -> Self {
        Self {
            next_id: i64::MIN,
            rows: BTreeMap::new(),
        }
    }
}

pub(super) fn encode_state(state: &OverState) -> Vec<u8> {
    let mut bytes = Vec::new();
    bytes.extend_from_slice(MAGIC);
    bytes.push(VERSION);
    bytes.extend_from_slice(&state.next_id.to_le_bytes());
    put_u32(&mut bytes, state.rows.len());
    for (order, rows) in &state.rows {
        put_bytes(&mut bytes, order);
        put_u32(&mut bytes, rows.len());
        for row in rows {
            bytes.extend_from_slice(&row.id.to_le_bytes());
            bytes.extend_from_slice(&row.event_timestamp.to_le_bytes());
            put_bytes(&mut bytes, &row.payload);
            put_values(&mut bytes, &row.contributions);
            put_values(&mut bytes, &row.output);
        }
    }
    bytes
}

pub(super) fn decode_state(bytes: &[u8], call_count: usize) -> Result<OverState> {
    if bytes.len() < 17
        || &bytes[..4] != MAGIC
        || (bytes[4] != FIRST_VERSION && bytes[4] != VERSION)
    {
        return Err(invalid("invalid native OVER aggregate state"));
    }
    let version = bytes[4];
    let mut cursor = Cursor { bytes, offset: 5 };
    let next_id = cursor.i64()?;
    let order_count = cursor.u32()? as usize;
    let mut rows_by_order = BTreeMap::new();
    for _ in 0..order_count {
        let order = cursor.bytes()?.to_vec();
        let row_count = cursor.u32()? as usize;
        let mut rows = Vec::with_capacity(row_count);
        for _ in 0..row_count {
            let id = cursor.i64()?;
            let event_timestamp = if version >= VERSION {
                cursor.i64()?
            } else {
                i64::MIN
            };
            let payload = cursor.bytes()?.to_vec();
            let contributions = cursor.values()?;
            let output = cursor.values()?;
            if contributions.len() != call_count
                || (!output.is_empty() && output.len() != call_count)
            {
                return Err(invalid("OVER aggregate state call count changed"));
            }
            rows.push(StoredRow {
                id,
                event_timestamp,
                payload,
                contributions,
                output,
            });
        }
        if rows_by_order.insert(order, rows).is_some() {
            return Err(invalid("OVER aggregate state has duplicate order keys"));
        }
    }
    if cursor.offset != bytes.len() {
        return Err(invalid("OVER aggregate state has trailing bytes"));
    }
    Ok(OverState {
        next_id,
        rows: rows_by_order,
    })
}

fn put_u32(bytes: &mut Vec<u8>, value: usize) {
    bytes.extend_from_slice(&(value as u32).to_le_bytes());
}

fn put_bytes(bytes: &mut Vec<u8>, value: &[u8]) {
    put_u32(bytes, value.len());
    bytes.extend_from_slice(value);
}

fn put_values(bytes: &mut Vec<u8>, values: &[Option<AggregateValue>]) {
    put_u32(bytes, values.len());
    for value in values {
        match value {
            None => bytes.push(0),
            Some(AggregateValue::Boolean(value)) => {
                bytes.push(1);
                bytes.push(u8::from(*value));
            }
            Some(AggregateValue::Int(value)) => {
                bytes.push(2);
                bytes.extend_from_slice(&value.to_le_bytes());
            }
            Some(AggregateValue::Float32(value)) => {
                bytes.push(3);
                bytes.extend_from_slice(&value.to_le_bytes());
            }
            Some(AggregateValue::Float64(value)) => {
                bytes.push(4);
                bytes.extend_from_slice(&value.to_le_bytes());
            }
            Some(AggregateValue::Bytes(value)) => {
                bytes.push(5);
                put_bytes(bytes, value);
            }
        }
    }
}

struct Cursor<'a> {
    bytes: &'a [u8],
    offset: usize,
}

impl<'a> Cursor<'a> {
    fn take(&mut self, len: usize) -> Result<&'a [u8]> {
        let end = self.offset.checked_add(len).ok_or_else(truncated)?;
        let value = self.bytes.get(self.offset..end).ok_or_else(truncated)?;
        self.offset = end;
        Ok(value)
    }

    fn u8(&mut self) -> Result<u8> {
        Ok(self.take(1)?[0])
    }

    fn u32(&mut self) -> Result<u32> {
        Ok(u32::from_le_bytes(self.take(4)?.try_into().unwrap()))
    }

    fn i64(&mut self) -> Result<i64> {
        Ok(i64::from_le_bytes(self.take(8)?.try_into().unwrap()))
    }

    fn bytes(&mut self) -> Result<&'a [u8]> {
        let len = self.u32()? as usize;
        self.take(len)
    }

    fn values(&mut self) -> Result<Vec<Option<AggregateValue>>> {
        let count = self.u32()? as usize;
        (0..count)
            .map(|_| match self.u8()? {
                0 => Ok(None),
                1 => Ok(Some(AggregateValue::Boolean(self.u8()? != 0))),
                2 => Ok(Some(AggregateValue::Int(i128::from_le_bytes(
                    self.take(16)?.try_into().unwrap(),
                )))),
                3 => Ok(Some(AggregateValue::Float32(u32::from_le_bytes(
                    self.take(4)?.try_into().unwrap(),
                )))),
                4 => Ok(Some(AggregateValue::Float64(u64::from_le_bytes(
                    self.take(8)?.try_into().unwrap(),
                )))),
                5 => Ok(Some(AggregateValue::Bytes(self.bytes()?.to_vec()))),
                tag => Err(invalid(format!("unknown OVER aggregate value tag {tag}"))),
            })
            .collect()
    }
}

fn invalid(message: impl Into<String>) -> DataFusionError {
    DataFusionError::Execution(message.into())
}

fn truncated() -> DataFusionError {
    invalid("truncated native OVER aggregate state")
}

#[cfg(test)]
mod tests {
    use super::*;

    fn encode_first_version(state: &OverState) -> Vec<u8> {
        let mut bytes = Vec::new();
        bytes.extend_from_slice(MAGIC);
        bytes.push(FIRST_VERSION);
        bytes.extend_from_slice(&state.next_id.to_le_bytes());
        put_u32(&mut bytes, state.rows.len());
        for (order, rows) in &state.rows {
            put_bytes(&mut bytes, order);
            put_u32(&mut bytes, rows.len());
            for row in rows {
                bytes.extend_from_slice(&row.id.to_le_bytes());
                put_bytes(&mut bytes, &row.payload);
                put_values(&mut bytes, &row.contributions);
                put_values(&mut bytes, &row.output);
            }
        }
        bytes
    }

    #[test]
    fn canonical_state_round_trips_values_and_rejects_trailing_bytes() {
        let state = OverState {
            next_id: 9,
            rows: BTreeMap::from([(
                vec![1, 2],
                vec![StoredRow {
                    id: 7,
                    event_timestamp: 11,
                    payload: vec![3, 4],
                    contributions: vec![Some(AggregateValue::Int(5)), None],
                    output: vec![Some(AggregateValue::Float64(1.5f64.to_bits())), None],
                }],
            )]),
        };
        let encoded = encode_state(&state);
        assert_eq!(decode_state(&encoded, 2).unwrap(), state);
        let mut malformed = encoded;
        malformed.push(0);
        assert!(decode_state(&malformed, 2).is_err());
    }

    #[test]
    fn restores_first_version_non_time_state_without_an_event_timestamp() {
        let state = OverState {
            next_id: 9,
            rows: BTreeMap::from([(
                vec![1, 2],
                vec![StoredRow {
                    id: 7,
                    event_timestamp: 11,
                    payload: vec![3, 4],
                    contributions: vec![Some(AggregateValue::Int(5))],
                    output: vec![Some(AggregateValue::Int(5))],
                }],
            )]),
        };

        let restored = decode_state(&encode_first_version(&state), 1).unwrap();
        assert_eq!(restored.next_id, state.next_id);
        assert_eq!(restored.rows[&vec![1, 2]][0].event_timestamp, i64::MIN);
        assert_eq!(restored.rows[&vec![1, 2]][0].payload, vec![3, 4]);
        assert_eq!(
            restored.rows[&vec![1, 2]][0].contributions,
            vec![Some(AggregateValue::Int(5))]
        );
    }
}
