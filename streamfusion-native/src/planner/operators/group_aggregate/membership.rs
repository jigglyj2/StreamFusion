// Copyright 2026 StreamFusion Authors
// Licensed under the Apache License, Version 2.0.

//! Flink DISTINCT data views: one persisted member with signed counts for calls sharing
//! an argument. Only members needed by the incoming Arrow batch enter the compute workspace.
//! DataFusion still consumes the first/last membership markers and computes every result.

use super::super::sortable_state;
use super::*;

mod batch;
mod codec;
mod memory;
mod prepare;
#[cfg(test)]
mod tests;
pub(super) use codec::{header_bytes, is_header};

struct Member {
    group: usize,
    column: usize,
    value: AggregateValue,
    original: Vec<i64>,
}

pub(super) struct MembershipLayout {
    columns: Vec<MemberColumn>,
}

struct MemberColumn {
    input: usize,
    // Anchor storage to the accumulator position, like the inline state tuple. A planner
    // projection may reorder physical input columns without changing aggregate identity.
    identity: usize,
    calls: Vec<usize>,
    data_type: DataType,
    converter: RowConverter,
}

impl MembershipLayout {
    pub(super) fn eligible(calls: &[Call]) -> bool {
        let distinct = calls
            .iter()
            .filter(|call| call.distinct)
            .collect::<Vec<_>>();
        !distinct.is_empty()
            && distinct.iter().all(|call| {
                datafusion_distinct_count::supports(call)
                    && matches!(
                        call.input_type.as_ref(),
                        Some(
                            DataType::Boolean
                                | DataType::Int8
                                | DataType::Int16
                                | DataType::Int32
                                | DataType::Int64
                                | DataType::Utf8
                                | DataType::Decimal128(_, _)
                                | DataType::Date32
                                | DataType::Time32(_)
                                | DataType::Time64(_)
                                | DataType::Timestamp(_, _)
                        )
                    )
            })
    }

    pub(super) fn new(calls: &[Call]) -> Result<Self> {
        let mut columns: Vec<MemberColumn> = Vec::new();
        for (index, call) in calls.iter().enumerate().filter(|(_, call)| call.distinct) {
            let input = call.input_index.expect("distinct argument");
            let data_type = call.input_type.as_ref().expect("distinct type");
            if let Some(column) = columns.iter_mut().find(|column| column.input == input) {
                if &column.data_type != data_type {
                    return Err(DataFusionError::Plan(
                        "DISTINCT argument has conflicting types".into(),
                    ));
                }
                column.calls.push(index);
            } else {
                columns.push(MemberColumn {
                    input,
                    identity: index,
                    calls: vec![index],
                    data_type: data_type.clone(),
                    converter: RowConverter::new(vec![SortField::new(data_type.clone())])?,
                });
            }
        }
        Ok(Self { columns })
    }

    pub(super) fn decode(&self, bytes: &[u8], calls: &[Call]) -> Result<AccumulatorState> {
        let state = decode_state(header_bytes(bytes)?, calls)?;
        if is_header(bytes) && state.accumulators.iter().any(|accumulator| {
            matches!(accumulator, Accumulator::DistinctCount { values, .. } if !values.is_empty())
        }) {
            return Err(DataFusionError::Execution("external DISTINCT header contains inline membership".into()));
        }
        Ok(state)
    }

    pub(super) fn encode(&self, mut state: AccumulatorState) -> Vec<u8> {
        for accumulator in &mut state.accumulators {
            if let Accumulator::DistinctCount { values, .. } = accumulator {
                values.clear();
            }
        }
        codec::encode_header(&state)
    }
}

fn prefix(key: &StateKey) -> Result<Vec<u8>> {
    // Raw group identities are Flink BinaryRows with INSERT header byte 0. This separate
    // namespace cannot collide with them. The shared framing also pins Arrow's key version.
    sortable_state::prefix(0xd0, &key.key)
}

fn member_key(prefix: &[u8], identity: usize, row: &[u8], key_group: u32) -> StateKey {
    let mut key = Vec::with_capacity(prefix.len() + 4 + row.len());
    key.extend_from_slice(prefix);
    key.extend_from_slice(&(identity as u32).to_be_bytes());
    key.extend_from_slice(row);
    StateKey { key_group, key }
}

fn counts(
    state: Option<&AccumulatorState>,
    column: &MemberColumn,
    value: &AggregateValue,
) -> Vec<i64> {
    column
        .calls
        .iter()
        .map(
            |&index| match state.map(|state| &state.accumulators[index]) {
                Some(Accumulator::DistinctCount { values, .. }) => {
                    values.get(value).copied().unwrap_or(0)
                }
                None => 0,
                _ => unreachable!("validated distinct count"),
            },
        )
        .collect()
}
