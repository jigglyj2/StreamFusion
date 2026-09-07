// Copyright 2026 StreamFusion Authors
// Licensed under the Apache License, Version 2.0

use std::collections::BTreeMap;

use arrow::datatypes::DataType;
use datafusion::error::{DataFusionError, Result};

use super::{
    proto, value_tag, Accumulator, AccumulatorState, AggregateValue, Call, BOUNDED_STATE_MAGIC,
    BOUNDED_STATE_VERSION, STATE_MAGIC, STATE_VERSION,
};

#[cfg(test)]
mod tests;

pub(in crate::planner::operators) fn encode_state(state: &AccumulatorState) -> Vec<u8> {
    // Split-DISTINCT incremental state commonly has only one active accumulator family for a
    // grouping key. Start with the exact sparse header instead of retaining worst-case capacity
    // in every long-lived in-memory value; populated accumulators grow geometrically as needed.
    let mut bytes = Vec::with_capacity(4 + 1 + 8 + 4 + state.accumulators.len());
    encode_state_into(state, &mut bytes);
    bytes
}

/// Size the canonical representation without allocating or counting B-tree node capacity.
/// The same writer drives both sizing and encoding so new accumulator variants cannot diverge.
pub(super) fn encoded_state_size(state: &AccumulatorState) -> usize {
    let mut size = EncodedSize(0);
    encode_state_into(state, &mut size);
    size.0
}

pub(super) trait StateSink {
    fn push(&mut self, value: u8);
    fn extend_from_slice(&mut self, value: &[u8]);
}

impl StateSink for Vec<u8> {
    fn push(&mut self, value: u8) {
        Vec::push(self, value);
    }
    fn extend_from_slice(&mut self, value: &[u8]) {
        Vec::extend_from_slice(self, value);
    }
}

struct EncodedSize(usize);
impl StateSink for EncodedSize {
    fn push(&mut self, _value: u8) {
        self.0 = self.0.saturating_add(1);
    }
    fn extend_from_slice(&mut self, value: &[u8]) {
        self.0 = self.0.saturating_add(value.len());
    }
}

fn encode_state_into(state: &AccumulatorState, bytes: &mut impl StateSink) {
    bytes.extend_from_slice(STATE_MAGIC);
    bytes.push(STATE_VERSION);
    bytes.extend_from_slice(&state.row_count.to_le_bytes());
    bytes.extend_from_slice(&(state.accumulators.len() as u32).to_le_bytes());
    for accumulator in &state.accumulators {
        if accumulator_is_neutral(accumulator) {
            bytes.push(0);
            continue;
        }
        match accumulator {
            Accumulator::Count(value) => {
                bytes.push(1);
                bytes.extend_from_slice(&value.to_le_bytes());
            }
            Accumulator::DistinctCount { count, values } => {
                bytes.push(5);
                bytes.extend_from_slice(&count.to_le_bytes());
                encode_counted_values(values, bytes);
            }
            Accumulator::Sum { value, count } => {
                bytes.push(2);
                bytes.push(value.is_some() as u8);
                if let Some(value) = value {
                    encode_value(value, bytes);
                }
                bytes.extend_from_slice(&count.to_le_bytes());
            }
            Accumulator::DistinctSum {
                value,
                count,
                values,
            } => {
                bytes.push(6);
                bytes.push(value.is_some() as u8);
                if let Some(value) = value {
                    encode_value(value, bytes);
                }
                bytes.extend_from_slice(&count.to_le_bytes());
                encode_counted_values(values, bytes);
            }
            Accumulator::Average { value, count } => {
                bytes.push(7);
                bytes.push(value.is_some() as u8);
                if let Some(value) = value {
                    encode_value(value, bytes);
                }
                bytes.extend_from_slice(&count.to_le_bytes());
            }
            Accumulator::DistinctAverage {
                value,
                count,
                values,
            } => {
                bytes.push(8);
                bytes.push(value.is_some() as u8);
                if let Some(value) = value {
                    encode_value(value, bytes);
                }
                bytes.extend_from_slice(&count.to_le_bytes());
                encode_counted_values(values, bytes);
            }
            Accumulator::AppendExtremum(value) => {
                bytes.push(4);
                bytes.push(value.is_some() as u8);
                if let Some(value) = value {
                    encode_value(value, bytes);
                }
            }
            Accumulator::Extremum(values) => {
                bytes.push(3);
                bytes.extend_from_slice(&(values.len() as u32).to_le_bytes());
                for (value, count) in values {
                    encode_value(value, bytes);
                    bytes.extend_from_slice(&count.to_le_bytes());
                }
            }
        }
    }
}

pub(super) fn accumulator_is_neutral(accumulator: &Accumulator) -> bool {
    match accumulator {
        Accumulator::Count(value) => *value == 0,
        Accumulator::DistinctCount { count, values } => *count == 0 && values.is_empty(),
        Accumulator::Sum { value, count } | Accumulator::Average { value, count } => {
            *count == 0 && optional_value_is_zero(value.as_ref())
        }
        Accumulator::DistinctSum {
            value,
            count,
            values,
        }
        | Accumulator::DistinctAverage {
            value,
            count,
            values,
        } => *count == 0 && values.is_empty() && optional_value_is_zero(value.as_ref()),
        Accumulator::AppendExtremum(value) => value.is_none(),
        Accumulator::Extremum(values) => values.is_empty(),
    }
}

fn optional_value_is_zero(value: Option<&AggregateValue>) -> bool {
    match value {
        None | Some(AggregateValue::Int(0)) => true,
        Some(AggregateValue::Float32(bits)) => f32::from_bits(*bits) == 0.0,
        Some(AggregateValue::Float64(bits)) => f64::from_bits(*bits) == 0.0,
        Some(AggregateValue::Boolean(_))
        | Some(AggregateValue::Int(_))
        | Some(AggregateValue::Bytes(_)) => false,
    }
}

fn encode_counted_values(values: &BTreeMap<AggregateValue, i64>, bytes: &mut impl StateSink) {
    bytes.extend_from_slice(&(values.len() as u32).to_le_bytes());
    for (value, count) in values {
        encode_value(value, bytes);
        bytes.extend_from_slice(&count.to_le_bytes());
    }
}

pub(in crate::planner::operators) fn decode_state(
    bytes: &[u8],
    calls: &[Call],
) -> Result<AccumulatorState> {
    let mut cursor = Cursor::new(bytes);
    if cursor.read_exact(4)? != STATE_MAGIC {
        return Err(DataFusionError::Execution(
            "group aggregate state has invalid magic".to_string(),
        ));
    }
    let version = cursor.read_u8()?;
    if version < 1 || version > STATE_VERSION {
        return Err(DataFusionError::Execution(format!(
            "group aggregate state version {version} is unsupported"
        )));
    }
    let row_count = cursor.read_i64()?;
    let count = cursor.read_u32()? as usize;
    if count != calls.len() {
        return Err(DataFusionError::Execution(format!(
            "group aggregate state has {count} accumulators, expected {}",
            calls.len()
        )));
    }
    let mut accumulators = Vec::with_capacity(count);
    for call in calls {
        let tag = cursor.read_u8()?;
        let accumulator = match tag {
            0 if version >= 6 => AccumulatorState::new(std::slice::from_ref(call))
                .accumulators
                .into_iter()
                .next()
                .expect("one call creates one accumulator"),
            1 => Accumulator::Count(cursor.read_i64()?),
            2 => Accumulator::Sum {
                value: if version == 1 {
                    Some(AggregateValue::Int(cursor.read_i128()?))
                } else if version == 2 {
                    Some(decode_value(&mut cursor)?)
                } else {
                    match cursor.read_u8()? {
                        0 => None,
                        1 => Some(decode_value(&mut cursor)?),
                        other => {
                            return Err(DataFusionError::Execution(format!(
                                "group aggregate sum presence {other} is invalid"
                            )))
                        }
                    }
                },
                count: cursor.read_i64()?,
            },
            3 => {
                let entries = cursor.read_u32()? as usize;
                let mut values = BTreeMap::new();
                for _ in 0..entries {
                    let value = if version == 1 {
                        AggregateValue::Int(cursor.read_i128()?)
                    } else {
                        decode_value(&mut cursor)?
                    };
                    values.insert(value, cursor.read_i64()?);
                }
                Accumulator::Extremum(values)
            }
            4 => {
                let value = match cursor.read_u8()? {
                    0 => None,
                    1 => Some(if version == 1 {
                        AggregateValue::Int(cursor.read_i128()?)
                    } else {
                        decode_value(&mut cursor)?
                    }),
                    other => {
                        return Err(DataFusionError::Execution(format!(
                            "group aggregate append extremum presence {other} is invalid"
                        )));
                    }
                };
                Accumulator::AppendExtremum(value)
            }
            5 if version >= 4 => Accumulator::DistinctCount {
                count: cursor.read_i64()?,
                values: decode_counted_values(&mut cursor)?,
            },
            6 if version >= 4 => {
                let value = match cursor.read_u8()? {
                    0 => None,
                    1 => Some(decode_value(&mut cursor)?),
                    other => {
                        return Err(DataFusionError::Execution(format!(
                            "group aggregate distinct sum presence {other} is invalid"
                        )))
                    }
                };
                Accumulator::DistinctSum {
                    value,
                    count: cursor.read_i64()?,
                    values: decode_counted_values(&mut cursor)?,
                }
            }
            7 if version >= 5 => Accumulator::Average {
                value: decode_optional_value(&mut cursor, "average")?,
                count: cursor.read_i64()?,
            },
            8 if version >= 5 => Accumulator::DistinctAverage {
                value: decode_optional_value(&mut cursor, "distinct average")?,
                count: cursor.read_i64()?,
                values: decode_counted_values(&mut cursor)?,
            },
            other => {
                return Err(DataFusionError::Execution(format!(
                    "group aggregate state accumulator tag {other} is invalid"
                )));
            }
        };
        let expected = match call.function {
            proto::AggregateFunction::CountStar | proto::AggregateFunction::Count => {
                if call.distinct {
                    5
                } else {
                    1
                }
            }
            proto::AggregateFunction::Sum | proto::AggregateFunction::Sum0 => {
                if call.distinct {
                    6
                } else {
                    2
                }
            }
            proto::AggregateFunction::Avg => {
                if call.distinct {
                    8
                } else {
                    7
                }
            }
            proto::AggregateFunction::Min | proto::AggregateFunction::Max => {
                if call.retractable {
                    3
                } else {
                    4
                }
            }
            _ => unreachable!("validated aggregate function"),
        };
        if tag != 0 && tag != expected {
            return Err(DataFusionError::Execution(
                "group aggregate state does not match its plan".to_string(),
            ));
        }
        validate_accumulator_values(&accumulator, call)?;
        accumulators.push(accumulator);
    }
    if !cursor.is_empty() {
        return Err(DataFusionError::Execution(
            "group aggregate state has trailing bytes".to_string(),
        ));
    }
    Ok(AccumulatorState {
        row_count,
        accumulators,
    })
}

fn decode_optional_value(
    cursor: &mut Cursor<'_>,
    description: &str,
) -> Result<Option<AggregateValue>> {
    match cursor.read_u8()? {
        0 => Ok(None),
        1 => decode_value(cursor).map(Some),
        other => Err(DataFusionError::Execution(format!(
            "group aggregate {description} presence {other} is invalid"
        ))),
    }
}

fn decode_counted_values(cursor: &mut Cursor<'_>) -> Result<BTreeMap<AggregateValue, i64>> {
    let entries = cursor.read_u32()? as usize;
    let mut values = BTreeMap::new();
    for _ in 0..entries {
        let value = decode_value(cursor)?;
        values.insert(value, cursor.read_i64()?);
    }
    Ok(values)
}

fn validate_accumulator_values(accumulator: &Accumulator, call: &Call) -> Result<()> {
    let values: Box<dyn Iterator<Item = &AggregateValue> + '_> = match accumulator {
        Accumulator::Count(_) => return Ok(()),
        Accumulator::DistinctCount { values, .. } => {
            for value in values.keys() {
                if !aggregate_value_matches_type(
                    value,
                    call.input_type
                        .as_ref()
                        .expect("DISTINCT COUNT has input type"),
                ) {
                    return Err(DataFusionError::Execution(format!(
                        "group aggregate distinct value {value:?} does not match its input type"
                    )));
                }
            }
            return Ok(());
        }
        Accumulator::Sum { value, .. } => Box::new(value.iter()),
        Accumulator::DistinctSum { value, values, .. } => {
            for distinct in values.keys() {
                if !aggregate_value_matches_type(
                    distinct,
                    call.input_type
                        .as_ref()
                        .expect("DISTINCT SUM has input type"),
                ) {
                    return Err(DataFusionError::Execution(format!(
                        "group aggregate distinct value {distinct:?} does not match its input type"
                    )));
                }
            }
            Box::new(value.iter())
        }
        Accumulator::Average { value, .. } => Box::new(value.iter()),
        Accumulator::DistinctAverage { value, values, .. } => {
            for distinct in values.keys() {
                if !aggregate_value_matches_type(
                    distinct,
                    call.input_type
                        .as_ref()
                        .expect("DISTINCT AVG has input type"),
                ) {
                    return Err(DataFusionError::Execution(format!(
                        "group aggregate distinct value {distinct:?} does not match its input type"
                    )));
                }
            }
            Box::new(value.iter())
        }
        Accumulator::AppendExtremum(value) => Box::new(value.iter()),
        Accumulator::Extremum(values) => Box::new(values.keys()),
    };
    for value in values {
        let state_type = if call.function == proto::AggregateFunction::Avg {
            call.average_accumulator_type()
        } else {
            call.output_type.clone()
        };
        if !aggregate_value_matches_type(value, &state_type) {
            return Err(DataFusionError::Execution(format!(
                "group aggregate state value {value:?} does not match {state_type}",
            )));
        }
    }
    Ok(())
}

fn aggregate_value_matches_type(value: &AggregateValue, data_type: &DataType) -> bool {
    match value {
        AggregateValue::Boolean(_) => data_type == &DataType::Boolean,
        AggregateValue::Float32(_) => data_type == &DataType::Float32,
        AggregateValue::Float64(_) => data_type == &DataType::Float64,
        AggregateValue::Bytes(_) => data_type == &DataType::Utf8,
        AggregateValue::Int(_) => matches!(
            data_type,
            DataType::Int8
                | DataType::Int16
                | DataType::Int32
                | DataType::Int64
                | DataType::Decimal128(_, _)
                | DataType::Date32
                | DataType::Time32(_)
                | DataType::Time64(_)
                | DataType::Timestamp(_, _)
        ),
    }
}

pub(super) fn encode_value(value: &AggregateValue, bytes: &mut impl StateSink) {
    bytes.push(value_tag(value));
    match value {
        AggregateValue::Boolean(value) => bytes.push(*value as u8),
        AggregateValue::Int(value) => bytes.extend_from_slice(&value.to_le_bytes()),
        AggregateValue::Float32(value) => bytes.extend_from_slice(&value.to_le_bytes()),
        AggregateValue::Float64(value) => bytes.extend_from_slice(&value.to_le_bytes()),
        AggregateValue::Bytes(value) => {
            bytes.extend_from_slice(&(value.len() as u32).to_le_bytes());
            bytes.extend_from_slice(value);
        }
    }
}

fn decode_value(cursor: &mut Cursor<'_>) -> Result<AggregateValue> {
    match cursor.read_u8()? {
        1 => match cursor.read_u8()? {
            0 => Ok(AggregateValue::Boolean(false)),
            1 => Ok(AggregateValue::Boolean(true)),
            other => Err(DataFusionError::Execution(format!(
                "group aggregate Boolean state byte {other} is invalid"
            ))),
        },
        2 => Ok(AggregateValue::Int(cursor.read_i128()?)),
        3 => Ok(AggregateValue::Float32(cursor.read_u32()?)),
        4 => Ok(AggregateValue::Float64(cursor.read_u64()?)),
        5 => {
            let length = cursor.read_u32()? as usize;
            Ok(AggregateValue::Bytes(cursor.read_exact(length)?.to_vec()))
        }
        other => Err(DataFusionError::Execution(format!(
            "group aggregate value state tag {other} is invalid"
        ))),
    }
}

struct Cursor<'a> {
    bytes: &'a [u8],
    offset: usize,
}

impl<'a> Cursor<'a> {
    fn new(bytes: &'a [u8]) -> Self {
        Self { bytes, offset: 0 }
    }

    fn read_exact(&mut self, length: usize) -> Result<&'a [u8]> {
        let end = self.offset.checked_add(length).ok_or_else(|| {
            DataFusionError::Execution("group aggregate state length overflow".to_string())
        })?;
        let value = self.bytes.get(self.offset..end).ok_or_else(|| {
            DataFusionError::Execution("group aggregate state is truncated".to_string())
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

    fn read_u64(&mut self) -> Result<u64> {
        Ok(u64::from_le_bytes(self.read_exact(8)?.try_into().unwrap()))
    }

    fn read_i128(&mut self) -> Result<i128> {
        Ok(i128::from_le_bytes(
            self.read_exact(16)?.try_into().unwrap(),
        ))
    }

    fn is_empty(&self) -> bool {
        self.offset == self.bytes.len()
    }
}

pub(in crate::planner::operators) fn encode_bounded_state(
    grouping_row: &[u8],
    state: &AccumulatorState,
) -> Result<Vec<u8>> {
    let accumulator = encode_state(state);
    let grouping_len = u32::try_from(grouping_row.len()).map_err(|_| {
        DataFusionError::Execution(
            "bounded group aggregate grouping row exceeds the canonical u32 length".to_string(),
        )
    })?;
    let mut bytes = Vec::with_capacity(
        BOUNDED_STATE_MAGIC.len() + 1 + 4 + grouping_row.len() + accumulator.len(),
    );
    bytes.extend_from_slice(BOUNDED_STATE_MAGIC);
    bytes.push(BOUNDED_STATE_VERSION);
    bytes.extend_from_slice(&grouping_len.to_le_bytes());
    bytes.extend_from_slice(grouping_row);
    bytes.extend_from_slice(&accumulator);
    Ok(bytes)
}

pub(in crate::planner::operators) fn decode_bounded_state(
    bytes: &[u8],
    calls: &[Call],
) -> Result<(Vec<u8>, AccumulatorState)> {
    let header = BOUNDED_STATE_MAGIC.len() + 1 + 4;
    if bytes.len() < header || &bytes[..4] != BOUNDED_STATE_MAGIC {
        return Err(DataFusionError::Execution(
            "invalid bounded group aggregate state magic".to_string(),
        ));
    }
    if bytes[4] != BOUNDED_STATE_VERSION {
        return Err(DataFusionError::Execution(format!(
            "unsupported bounded group aggregate state version {}",
            bytes[4]
        )));
    }
    let grouping_len = u32::from_le_bytes(bytes[5..9].try_into().expect("fixed header")) as usize;
    let grouping_end = header.checked_add(grouping_len).ok_or_else(|| {
        DataFusionError::Execution("bounded group aggregate grouping length overflow".to_string())
    })?;
    if grouping_end > bytes.len() {
        return Err(DataFusionError::Execution(
            "truncated bounded group aggregate grouping row".to_string(),
        ));
    }
    Ok((
        bytes[header..grouping_end].to_vec(),
        decode_state(&bytes[grouping_end..], calls)?,
    ))
}
