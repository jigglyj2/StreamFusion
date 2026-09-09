// Copyright 2026 StreamFusion Authors
// Licensed under the Apache License, Version 2.0.

//! Canonical maps are sorted and unique. Rust's public FromIterator bulk-builds these
//! instead of performing a root-to-leaf insertion for every historical membership.

use super::*;

pub(super) fn decode_counted_values(
    cursor: &mut Cursor<'_>,
    legacy_integer: bool,
) -> Result<BTreeMap<AggregateValue, i64>> {
    let entries = cursor.read_u32()? as usize;
    // Reject impossible lengths before reserving the staging vector. The shortest value is
    // a Boolean tag/byte plus an i64 count, or an untagged i128 plus count in version one.
    let minimum = if legacy_integer { 24 } else { 10 };
    if entries > (cursor.bytes.len() - cursor.offset) / minimum {
        return Err(DataFusionError::Execution(
            "group aggregate state is truncated".into(),
        ));
    }
    let mut staged: Vec<(AggregateValue, i64)> = Vec::with_capacity(entries);
    let mut strict_order = true;
    for _ in 0..entries {
        let value = if legacy_integer {
            AggregateValue::Int(cursor.read_i128()?)
        } else {
            decode_value(cursor)?
        };
        if staged
            .last()
            .is_some_and(|(previous, _)| previous.cmp(&value) != std::cmp::Ordering::Less)
        {
            strict_order = false;
        }
        staged.push((value, cursor.read_i64()?));
    }
    if strict_order {
        Ok(staged.into_iter().collect())
    } else {
        // Preserve old decoding of unsorted or duplicate entries: first key object, last
        // count. In particular, Flink compares different NaN payloads as equal while the
        // state value's Eq compares bits, so std's Eq-based bulk deduplication is insufficient.
        let mut values = BTreeMap::new();
        for (value, count) in staged {
            values.insert(value, count);
        }
        Ok(values)
    }
}

#[cfg(test)]
mod tests;
