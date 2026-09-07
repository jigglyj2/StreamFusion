// Copyright 2026 StreamFusion Authors
// Licensed under the Apache License, Version 2.0

use super::*;

pub(in crate::planner::operators) fn window_state_key(
    key_group: u32,
    group_key: &[u8],
    start: i64,
    end: i64,
) -> StateKey {
    let mut key = Vec::with_capacity(1 + group_key.len() + 16);
    key.push(WINDOW_KEY_PREFIX);
    key.extend_from_slice(group_key);
    key.extend_from_slice(&start.to_be_bytes());
    key.extend_from_slice(&end.to_be_bytes());
    StateKey { key_group, key }
}

pub(in crate::planner::operators) fn session_index_key(
    key_group: u32,
    group_key: &[u8],
) -> StateKey {
    let mut key = Vec::with_capacity(1 + group_key.len());
    key.push(SESSION_INDEX_PREFIX);
    key.extend_from_slice(group_key);
    StateKey { key_group, key }
}

pub(in crate::planner::operators) fn count_index_key(key_group: u32, group_key: &[u8]) -> StateKey {
    let mut key = Vec::with_capacity(1 + group_key.len());
    key.push(COUNT_INDEX_PREFIX);
    key.extend_from_slice(group_key);
    StateKey { key_group, key }
}

pub(in crate::planner::operators) fn count_group_key(key: &[u8]) -> Result<&[u8]> {
    if key.first() != Some(&COUNT_INDEX_PREFIX) {
        return Err(DataFusionError::Execution(
            "count-window index key is malformed".to_string(),
        ));
    }
    Ok(&key[1..])
}

pub(in crate::planner::operators) fn encode_count_index(count: i64) -> Vec<u8> {
    let mut bytes = Vec::with_capacity(13);
    bytes.extend_from_slice(COUNT_INDEX_MAGIC);
    bytes.push(1);
    bytes.extend_from_slice(&count.to_be_bytes());
    bytes
}

pub(in crate::planner::operators) fn decode_count_index(bytes: &[u8]) -> Result<i64> {
    if bytes.len() != 13 || &bytes[..4] != COUNT_INDEX_MAGIC || bytes[4] != 1 {
        return Err(DataFusionError::Execution(
            "count-window index state is corrupt or has an unsupported version".to_string(),
        ));
    }
    Ok(i64::from_be_bytes(
        bytes[5..13].try_into().expect("length checked"),
    ))
}

pub(in crate::planner::operators) fn decode_window_state_key_bounds(
    key: &[u8],
) -> Result<(i64, i64)> {
    if key.len() < 17 || key[0] != WINDOW_KEY_PREFIX {
        return Err(DataFusionError::Execution(
            "window state key is malformed".to_string(),
        ));
    }
    let offset = key.len() - 16;
    let start = i64::from_be_bytes(key[offset..offset + 8].try_into().expect("length checked"));
    let end = i64::from_be_bytes(key[offset + 8..].try_into().expect("length checked"));
    Ok((start, end))
}

pub(in crate::planner::operators) fn group_key_from_window_state_key(key: &[u8]) -> Result<&[u8]> {
    if key.len() < 17 || key[0] != WINDOW_KEY_PREFIX {
        return Err(DataFusionError::Execution(
            "session window state key is malformed".to_string(),
        ));
    }
    Ok(&key[1..key.len() - 16])
}

pub(in crate::planner::operators) fn encode_session_index(intervals: &[(i64, i64)]) -> Vec<u8> {
    let mut bytes = Vec::with_capacity(9 + intervals.len() * 16);
    bytes.extend_from_slice(SESSION_INDEX_MAGIC);
    bytes.push(1);
    bytes.extend_from_slice(&(intervals.len() as u32).to_le_bytes());
    for &(start, end) in intervals {
        bytes.extend_from_slice(&start.to_le_bytes());
        bytes.extend_from_slice(&end.to_le_bytes());
    }
    bytes
}

pub(in crate::planner::operators) fn decode_session_index(bytes: &[u8]) -> Result<Vec<(i64, i64)>> {
    let mut reader = WindowBytesReader::new(bytes);
    if reader.read_exact(4)? != SESSION_INDEX_MAGIC || reader.read_u8()? != 1 {
        return Err(DataFusionError::Execution(
            "invalid native session index state".to_string(),
        ));
    }
    let count = reader.read_u32()? as usize;
    let mut intervals = Vec::with_capacity(count);
    for _ in 0..count {
        intervals.push((reader.read_i64()?, reader.read_i64()?));
    }
    if !reader.is_empty() {
        return Err(DataFusionError::Execution(
            "native session index has trailing bytes".to_string(),
        ));
    }
    Ok(intervals)
}

pub(in crate::planner::operators) fn encode_session_state(
    grouping_row: &[u8],
    accumulator: &AccumulatorState,
    events: &[SessionEvent],
) -> Vec<u8> {
    let aggregate = encode_state(accumulator);
    let mut bytes = Vec::new();
    bytes.extend_from_slice(SESSION_STATE_MAGIC);
    bytes.push(1);
    bytes.extend_from_slice(&(grouping_row.len() as u32).to_le_bytes());
    bytes.extend_from_slice(&(aggregate.len() as u32).to_le_bytes());
    bytes.extend_from_slice(&(events.len() as u32).to_le_bytes());
    bytes.extend_from_slice(grouping_row);
    bytes.extend_from_slice(&aggregate);
    for event in events {
        bytes.extend_from_slice(&event.timestamp.to_le_bytes());
        bytes.extend_from_slice(&(event.values.len() as u32).to_le_bytes());
        for value in &event.values {
            match value {
                None => bytes.push(0),
                Some(value) => {
                    bytes.push(1);
                    encode_session_value(value, &mut bytes);
                }
            }
        }
    }
    bytes
}

pub(in crate::planner::operators) fn decode_session_state(
    bytes: &[u8],
    calls: &[Call],
) -> Result<(Vec<u8>, AccumulatorState, Vec<SessionEvent>)> {
    let mut reader = WindowBytesReader::new(bytes);
    if reader.read_exact(4)? != SESSION_STATE_MAGIC || reader.read_u8()? != 1 {
        return Err(DataFusionError::Execution(
            "invalid native session window state".to_string(),
        ));
    }
    let grouping_length = reader.read_u32()? as usize;
    let aggregate_length = reader.read_u32()? as usize;
    let event_count = reader.read_u32()? as usize;
    let grouping = reader.read_exact(grouping_length)?.to_vec();
    let accumulator = decode_state(reader.read_exact(aggregate_length)?, calls)?;
    let mut events = Vec::with_capacity(event_count);
    for _ in 0..event_count {
        let timestamp = reader.read_i64()?;
        let value_count = reader.read_u32()? as usize;
        if value_count != calls.len() {
            return Err(DataFusionError::Execution(format!(
                "session event has {value_count} aggregate values, expected {}",
                calls.len()
            )));
        }
        let mut values = Vec::with_capacity(value_count);
        for _ in 0..value_count {
            values.push(match reader.read_u8()? {
                0 => None,
                1 => Some(decode_session_value(&mut reader)?),
                other => {
                    return Err(DataFusionError::Execution(format!(
                        "invalid session aggregate value presence {other}"
                    )));
                }
            });
        }
        events.push(SessionEvent { timestamp, values });
    }
    if !reader.is_empty() {
        return Err(DataFusionError::Execution(
            "native session window state has trailing bytes".to_string(),
        ));
    }
    Ok((grouping, accumulator, events))
}

pub(in crate::planner::operators) fn encode_session_value(
    value: &AggregateValue,
    bytes: &mut Vec<u8>,
) {
    match value {
        AggregateValue::Boolean(value) => {
            bytes.push(1);
            bytes.push(*value as u8);
        }
        AggregateValue::Int(value) => {
            bytes.push(2);
            bytes.extend_from_slice(&value.to_le_bytes());
        }
        AggregateValue::Float32(value) => {
            bytes.push(3);
            bytes.extend_from_slice(&value.to_le_bytes());
        }
        AggregateValue::Float64(value) => {
            bytes.push(4);
            bytes.extend_from_slice(&value.to_le_bytes());
        }
        AggregateValue::Bytes(value) => {
            bytes.push(5);
            bytes.extend_from_slice(&(value.len() as u32).to_le_bytes());
            bytes.extend_from_slice(value);
        }
    }
}

fn decode_session_value(reader: &mut WindowBytesReader<'_>) -> Result<AggregateValue> {
    match reader.read_u8()? {
        1 => match reader.read_u8()? {
            0 => Ok(AggregateValue::Boolean(false)),
            1 => Ok(AggregateValue::Boolean(true)),
            other => Err(DataFusionError::Execution(format!(
                "invalid session boolean value {other}"
            ))),
        },
        2 => Ok(AggregateValue::Int(i128::from_le_bytes(
            reader.read_exact(16)?.try_into().unwrap(),
        ))),
        3 => Ok(AggregateValue::Float32(u32::from_le_bytes(
            reader.read_exact(4)?.try_into().unwrap(),
        ))),
        4 => Ok(AggregateValue::Float64(u64::from_le_bytes(
            reader.read_exact(8)?.try_into().unwrap(),
        ))),
        5 => {
            let length = reader.read_u32()? as usize;
            Ok(AggregateValue::Bytes(reader.read_exact(length)?.to_vec()))
        }
        other => Err(DataFusionError::Execution(format!(
            "unknown session aggregate value tag {other}"
        ))),
    }
}

struct WindowBytesReader<'a> {
    bytes: &'a [u8],
    offset: usize,
}

impl<'a> WindowBytesReader<'a> {
    fn new(bytes: &'a [u8]) -> Self {
        Self { bytes, offset: 0 }
    }

    fn read_exact(&mut self, length: usize) -> Result<&'a [u8]> {
        let end = self.offset.checked_add(length).ok_or_else(|| {
            DataFusionError::Execution("native window state length overflow".to_string())
        })?;
        let value = self.bytes.get(self.offset..end).ok_or_else(|| {
            DataFusionError::Execution("truncated native window state".to_string())
        })?;
        self.offset = end;
        Ok(value)
    }

    fn read_u8(&mut self) -> Result<u8> {
        Ok(self.read_exact(1)?[0])
    }

    fn read_u32(&mut self) -> Result<u32> {
        Ok(u32::from_le_bytes(self.read_exact(4)?.try_into().unwrap()))
    }

    fn read_i64(&mut self) -> Result<i64> {
        Ok(i64::from_le_bytes(self.read_exact(8)?.try_into().unwrap()))
    }

    fn is_empty(&self) -> bool {
        self.offset == self.bytes.len()
    }
}

pub(in crate::planner::operators) fn window_namespace(start: i64, end: i64) -> Vec<u8> {
    let mut bytes = Vec::with_capacity(16);
    bytes.extend_from_slice(&start.to_le_bytes());
    bytes.extend_from_slice(&end.to_le_bytes());
    bytes
}

pub(in crate::planner::operators) fn decode_window_namespace(bytes: &[u8]) -> Result<(i64, i64)> {
    if bytes.len() != 16 {
        return Err(DataFusionError::Execution(
            "native window timer namespace has the wrong size".to_string(),
        ));
    }
    Ok((
        i64::from_le_bytes(bytes[..8].try_into().unwrap()),
        i64::from_le_bytes(bytes[8..].try_into().unwrap()),
    ))
}

pub(in crate::planner::operators) fn encode_window_state(
    grouping_row: &[u8],
    accumulator: &AccumulatorState,
) -> Vec<u8> {
    let aggregate = encode_state(accumulator);
    let mut output = Vec::with_capacity(9 + grouping_row.len() + aggregate.len());
    output.extend_from_slice(WINDOW_STATE_MAGIC);
    output.push(WINDOW_STATE_VERSION);
    output.extend_from_slice(&(grouping_row.len() as u32).to_le_bytes());
    output.extend_from_slice(grouping_row);
    output.extend_from_slice(&aggregate);
    output
}

pub(in crate::planner::operators) fn decode_window_state(
    bytes: &[u8],
    calls: &[Call],
) -> Result<(Vec<u8>, AccumulatorState)> {
    if bytes.len() < 9 || &bytes[..4] != WINDOW_STATE_MAGIC {
        return Err(DataFusionError::Execution(
            "invalid native window aggregate state magic".to_string(),
        ));
    }
    if bytes[4] != WINDOW_STATE_VERSION {
        return Err(DataFusionError::Execution(format!(
            "unsupported native window aggregate state version {}",
            bytes[4]
        )));
    }
    let grouping_length = u32::from_le_bytes(bytes[5..9].try_into().unwrap()) as usize;
    let aggregate_offset = 9usize.checked_add(grouping_length).ok_or_else(|| {
        DataFusionError::Execution("native window grouping row length overflow".to_string())
    })?;
    if aggregate_offset > bytes.len() {
        return Err(DataFusionError::Execution(
            "truncated native window grouping row".to_string(),
        ));
    }
    Ok((
        bytes[9..aggregate_offset].to_vec(),
        decode_state(&bytes[aggregate_offset..], calls)?,
    ))
}
