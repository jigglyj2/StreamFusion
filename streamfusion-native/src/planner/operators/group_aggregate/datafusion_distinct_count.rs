// Copyright 2026 StreamFusion Authors
// Licensed under the Apache License, Version 2.0.

//! Flink owns signed DISTINCT membership; DataFusion COUNT consumes its first/last-value markers.
//! A DataFusion distinct set cannot represent unmatched retractions or their later cancellation.
//! This adapter preserves the existing canonical map and checkpoint encoding.

use super::*;
use datafusion::logical_expr::Accumulator as NativeAccumulator;
use datafusion::physical_expr::aggregate::AggregateFunctionExpr;
use datafusion::scalar::ScalarValue;

pub(super) fn supports(call: &Call) -> bool {
    call.distinct && call.function == proto::AggregateFunction::Count
}

/// Caller applies SQL FILTER first. Only a first accumulation or last retraction reaches COUNT.
pub(super) fn membership_change(
    values: &mut BTreeMap<AggregateValue, i64>,
    call: &Call,
    batch: &RecordBatch,
    row: usize,
    accumulate: bool,
) -> Result<bool> {
    let Some(value) = aggregate_value(
        batch
            .column(call.input_index.expect("COUNT argument"))
            .as_ref(),
        row,
    )?
    else {
        return Ok(false);
    };
    let delta = if accumulate { 1 } else { -1 };
    let (previous, current) = match values.entry(value) {
        std::collections::btree_map::Entry::Occupied(mut entry) => {
            let previous = *entry.get();
            let current = previous.wrapping_add(delta);
            if current == 0 {
                entry.remove();
            } else {
                *entry.get_mut() = current;
            }
            (previous, current)
        }
        std::collections::btree_map::Entry::Vacant(entry) => {
            entry.insert(delta);
            (0, delta)
        }
    };
    Ok((accumulate && previous == 0) || (!accumulate && current == 0))
}

pub(super) fn evaluate(native: &mut dyn NativeAccumulator) -> Result<i64> {
    match native.evaluate()? {
        ScalarValue::Int64(Some(count)) => Ok(count),
        _ => Err(DataFusionError::Internal(
            "DataFusion COUNT returned a non-integer".into(),
        )),
    }
}

/// Markers are bounded by the selected run and covered by the caller's batch workspace credit.
/// No input payload is gathered or copied, and no backend access occurs inside this adapter.
pub(super) fn apply_append_batch(
    state: &mut Accumulator,
    call: &Call,
    kernel: &AggregateFunctionExpr,
    batch: &RecordBatch,
    rows: &[usize],
) -> Result<()> {
    let Accumulator::DistinctCount { count, values } = state else {
        return Err(DataFusionError::Internal(
            "expected distinct count state".into(),
        ));
    };
    let mut native = kernel.create_accumulator()?;
    native.merge_batch(&[Arc::new(Int64Array::from(vec![*count]))])?;
    let mut markers = arrow::array::Int8Builder::with_capacity(rows.len());
    for &row in rows {
        let changed = aggregate_filter(call, batch, row)?
            && membership_change(values, call, batch, row, true)?;
        markers.append_option(changed.then_some(1));
    }
    native.update_batch(&[Arc::new(markers.finish())])?;
    *count = evaluate(native.as_mut())?;
    Ok(())
}
