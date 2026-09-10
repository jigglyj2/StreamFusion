// Copyright 2026 StreamFusion Authors
// Licensed under the Apache License, Version 2.0

//! Running extrema preserve every arrival, unlike sorting a batch and selecting one final row.
use super::*;
use arrow::array::{Int64Array, UInt64Array};
use datafusion::common::ScalarValue;
use datafusion::functions_aggregate::min_max::{max_udaf, min_udaf};
use datafusion::logical_expr::{WindowFrame, WindowFrameBound, WindowFrameUnits};
use datafusion::physical_expr::window::{PlainAggregateWindowExpr, WindowExpr};
use datafusion::physical_expr::{
    aggregate::AggregateExprBuilder, expressions::Column, PhysicalSortExpr,
};

pub(super) struct Winners {
    pub(super) orders: Vec<i64>,
    pub(super) winners: Vec<bool>,
    _memory: HostMemoryReservation,
}

pub(super) fn winners(
    times: &ArrayRef,
    keys: &[StateKeyRef<'_>],
    existing: &[Option<crate::state::StateValue<'_>>],
    keep_last: bool,
    owner: &HostMemoryReservation,
) -> Result<Winners> {
    // Fixed-width order/ordinal arrays, grouped input indices, cumulative output and the
    // result mask. No payload copies or key copies: state remains owned by the caller.
    let mut memory = owner.sibling("DataFusion deduplicate cumulative timestamps");
    memory.resize(keys.len().saturating_mul(384).saturating_add(64 << 10))?;
    let orders = (0..keys.len())
        .map(|row| timestamp_millis(times, row))
        .collect::<Result<Vec<_>>>()?;
    let mut groups = HashMap::<StateKeyRef<'_>, Vec<usize>, RandomState>::with_capacity_and_hasher(
        keys.len(),
        RandomState::new(),
    );
    for (row, &key) in keys.iter().enumerate() {
        groups.entry(key).or_default().push(row);
    }
    let schema = Arc::new(Schema::new(vec![
        Field::new("timestamp", DataType::Int64, false),
        Field::new("arrival", DataType::UInt64, false),
    ]));
    let aggregate = Arc::new(
        AggregateExprBuilder::new(
            if keep_last { max_udaf() } else { min_udaf() },
            vec![Arc::new(Column::new("timestamp", 0)) as _],
        )
        .schema(schema.clone())
        .alias("deduplicate_timestamp")
        .build()?,
    );
    let frame = Arc::new(WindowFrame::new_bounds(
        WindowFrameUnits::Rows,
        WindowFrameBound::Preceding(ScalarValue::UInt64(None)),
        WindowFrameBound::CurrentRow,
    ));
    let window = PlainAggregateWindowExpr::new(
        aggregate,
        &[],
        &[PhysicalSortExpr::new(
            Arc::new(Column::new("arrival", 1)),
            arrow::compute::SortOptions::default(),
        )],
        frame,
        None,
    );
    let mut winners = vec![false; keys.len()];
    for rows in groups.values() {
        let prior = existing[rows[0]].as_deref().map(decode_order).transpose()?;
        let values = Int64Array::from_iter_values(
            prior.into_iter().chain(rows.iter().map(|&row| orders[row])),
        );
        let arrival = UInt64Array::from_iter_values(0..values.len() as u64);
        let input =
            RecordBatch::try_new(schema.clone(), vec![Arc::new(values), Arc::new(arrival)])?;
        let output = window.evaluate(&input)?;
        let extrema = output
            .as_any()
            .downcast_ref::<Int64Array>()
            .expect("MIN/MAX Int64");
        let offset = usize::from(prior.is_some());
        for (index, &row) in rows.iter().enumerate() {
            let position = offset + index;
            // Flink keep-last replaces on a tie; keep-first requires strict improvement.
            // The extremum is computed by DataFusion; this comparison selects the Flink
            // changelog event and does not coalesce required intermediate winners.
            winners[row] = if keep_last {
                orders[row] == extrema.value(position)
            } else {
                position == 0 || orders[row] < extrema.value(position - 1)
            };
        }
    }
    Ok(Winners {
        orders,
        winners,
        _memory: memory,
    })
}

#[cfg(test)]
mod tests;
