// Copyright 2026 StreamFusion Authors
// Licensed under the Apache License, Version 2.0.

//! One reservation for historical aggregate maps, owned payloads and serialized mutations.
//! Scan the current wire format without allocating values; retain the existing conservative
//! allowance for older formats until normal decoding rewrites them in the current format.

use super::*;
use crate::memory_pool::HostMemoryReservation;
use crate::state::StateValue;

pub(in crate::planner::operators::group_aggregate) fn reserve_decoded_values(
    values: &[Option<StateValue<'_>>],
    calls: &[Call],
    owner: &HostMemoryReservation,
) -> Result<HostMemoryReservation> {
    let bytes = values.iter().flatten().try_fold(0usize, |total, value| {
        add(total, workspace(value.as_ref(), calls)?)
    })?;
    let mut reservation = owner.sibling("native aggregate decoded state and mutations");
    reservation.resize(bytes)?;
    Ok(reservation)
}

fn add(left: usize, right: usize) -> Result<usize> {
    left.checked_add(right).ok_or_else(|| {
        DataFusionError::ResourcesExhausted("aggregate decoded workspace overflow".into())
    })
}

fn workspace(bytes: &[u8], calls: &[Call]) -> Result<usize> {
    let mut cursor = Cursor::new(bytes);
    if cursor.read_exact(4)? != STATE_MAGIC {
        return Err(DataFusionError::Execution(
            "group aggregate state has invalid magic".into(),
        ));
    }
    let version = cursor.read_u8()?;
    if version < 1 || version > STATE_VERSION {
        return Err(DataFusionError::Execution(format!(
            "group aggregate state version {version} is unsupported"
        )));
    }
    if version != STATE_VERSION {
        return bytes
            .len()
            .checked_mul(8)
            .and_then(|n| n.checked_add(64))
            .ok_or_else(|| {
                DataFusionError::ResourcesExhausted(
                    "aggregate legacy decoded workspace overflow".into(),
                )
            });
    }
    cursor.read_i64()?;
    let count = cursor.read_u32()? as usize;
    if count != calls.len() {
        return Err(DataFusionError::Execution(
            "group aggregate state accumulator count does not match its plan".into(),
        ));
    }
    // Accumulator vectors and sparse initial nodes are already covered by per-key batch
    // admission. This allowance owns historical entries/payloads plus one exact-size encoding.
    let mut decoded = 0;
    for _ in calls {
        let tag = cursor.read_u8()?;
        match tag {
            0 => {}
            1 => {
                cursor.read_i64()?;
            }
            2 | 6 | 7 | 8 => {
                decoded = add(decoded, optional_value(&mut cursor)?)?;
                cursor.read_i64()?;
                if matches!(tag, 6 | 8) {
                    decoded = add(decoded, counted_values(&mut cursor)?)?;
                }
            }
            3 => {
                decoded = add(decoded, counted_values(&mut cursor)?)?;
            }
            4 => {
                decoded = add(decoded, optional_value(&mut cursor)?)?;
            }
            5 => {
                cursor.read_i64()?;
                decoded = add(decoded, counted_values(&mut cursor)?)?;
            }
            _ => {
                return Err(DataFusionError::Execution(format!(
                    "group aggregate state accumulator tag {tag} is invalid"
                )))
            }
        }
    }
    if !cursor.is_empty() {
        return Err(DataFusionError::Execution(
            "group aggregate state has trailing bytes".into(),
        ));
    }
    add(decoded, bytes.len())
}

fn optional_value(cursor: &mut Cursor<'_>) -> Result<usize> {
    match cursor.read_u8()? {
        0 => Ok(0),
        1 => value(cursor),
        presence => Err(DataFusionError::Execution(format!(
            "group aggregate state presence {presence} is invalid"
        ))),
    }
}

fn counted_values(cursor: &mut Cursor<'_>) -> Result<usize> {
    let count = cursor.read_u32()? as usize;
    let mut bytes = count
        .checked_mul(super::super::accumulator::counted_map_entry_bytes())
        .ok_or_else(|| {
            DataFusionError::ResourcesExhausted("aggregate counted-map workspace overflow".into())
        })?;
    for _ in 0..count {
        bytes = add(bytes, value(cursor)?)?;
        cursor.read_i64()?;
    }
    Ok(bytes)
}

fn value(cursor: &mut Cursor<'_>) -> Result<usize> {
    let length = match cursor.read_u8()? {
        1 => {
            cursor.read_exact(1)?;
            return Ok(0);
        }
        2 => {
            cursor.read_exact(16)?;
            return Ok(0);
        }
        3 => {
            cursor.read_exact(4)?;
            return Ok(0);
        }
        4 => {
            cursor.read_exact(8)?;
            return Ok(0);
        }
        5 => cursor.read_u32()? as usize,
        tag => {
            return Err(DataFusionError::Execution(format!(
                "group aggregate value state tag {tag} is invalid"
            )))
        }
    };
    cursor.read_exact(length)?;
    Ok(length)
}

#[cfg(test)]
mod tests;
