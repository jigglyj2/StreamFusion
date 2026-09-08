// Copyright 2026 StreamFusion Authors
// Licensed under the Apache License, Version 2.0.

//! Append-only Top-1 is a cumulative minimum over Flink-compatible Arrow sort keys.
//! DataFusion selects winners; the caller owns Flink's per-arrival changelog and state delta.
use super::*;
use arrow::array::UInt64Array;
use datafusion::functions_aggregate::min_max::min_udaf;
use datafusion::logical_expr::{WindowFrame, WindowFrameBound, WindowFrameUnits};
use datafusion::physical_expr::window::{PlainAggregateWindowExpr, WindowExpr};
use datafusion::physical_expr::{
    aggregate::AggregateExprBuilder, expressions::Column, LexOrdering, PhysicalExpr,
    PhysicalSortExpr,
};
use datafusion::physical_plan::sorts::sort::sort_batch;

pub(super) fn compatible(plan: &proto::TopN, sources: &CandidateSources) -> bool {
    plan.strategy == proto::TopNStrategy::AppendFast as i32
        && plan.rank_start == 1
        && plan.rank_end == Some(1)
        && plan.variable_rank_end_index.is_none()
        && rank_type(plan) == proto::TopNRankType::RowNumber
        && !plan.bounded_final_output
        && !plan.physical_input_semantics
        && !plan.sort_key_indices.is_empty()
        && sources.orders.is_some()
}

pub(super) fn winners(
    sources: &CandidateSources,
    groups: &[GroupWork],
    row_groups: &[usize],
    owner: &HostMemoryReservation,
) -> Result<Vec<bool>> {
    let orders = sources.orders.as_ref().expect("compatible sort keys");
    let count = row_groups.len();
    if count == 0 {
        return Ok(Vec::new());
    }
    let key_bytes = orders
        .iter()
        .map(|rows| rows.iter().map(|row| row.data().len()).sum::<usize>())
        .sum::<usize>();
    let mut credit = owner.sibling("DataFusion Top-1 sort keys and cumulative ordinals");
    credit.resize(
        key_bytes
            .saturating_mul(3)
            .saturating_add((count + groups.len()).saturating_mul(256))
            .saturating_add(64 << 10),
    )?;
    let mut tokens = Vec::with_capacity(count + groups.len());
    let mut prior = Vec::with_capacity(groups.len());
    for group in groups {
        if group.candidates.len() > 1 {
            return Err(DataFusionError::Execution(
                "Top-1 state retains more than one candidate".into(),
            ));
        }
        prior.push(group.candidates.first().map(|candidate| {
            let token = tokens.len();
            tokens.push((candidate.source, candidate.row));
            token
        }));
    }
    // Old winners precede new arrivals on equal sort keys. Input order then resolves ties,
    // preserving FastTop1Function's strict improvement check across batches and restore.
    let input_start = tokens.len();
    tokens.extend((0..count).map(|row| (0, row)));
    let keys =
        BinaryArray::from_iter_values(tokens.iter().map(|&(source, row)| orders[source].row(row)));
    let batch = RecordBatch::try_from_iter(vec![
        ("key", Arc::new(keys) as ArrayRef),
        (
            "token",
            Arc::new(UInt64Array::from_iter_values(0..tokens.len() as u64)) as ArrayRef,
        ),
    ])?;
    let ascending = arrow::compute::SortOptions {
        descending: false,
        nulls_first: true,
    };
    let ordering = LexOrdering::new(vec![
        PhysicalSortExpr::new(Arc::new(Column::new("key", 0)), ascending),
        PhysicalSortExpr::new(Arc::new(Column::new("token", 1)), ascending),
    ])
    .expect("two sort columns");
    let sorted = sort_batch(&batch, &ordering, None)?;
    let sorted_tokens = sorted
        .column(1)
        .as_any()
        .downcast_ref::<UInt64Array>()
        .expect("token type");
    let mut priority = vec![0u64; tokens.len()];
    for (rank, token) in sorted_tokens.values().iter().enumerate() {
        priority[*token as usize] = rank as u64;
    }
    drop(sorted);
    drop(batch);
    drop(tokens);

    // Cumulative MIN runs over fixed-width priorities, not variable-width keys: a single
    // wide winning key must not be replicated once for every later input in its partition.
    let schema = Arc::new(Schema::new(vec![
        Field::new("priority", DataType::UInt64, false),
        Field::new("arrival", DataType::UInt64, false),
    ]));
    let args: Vec<Arc<dyn PhysicalExpr>> = vec![Arc::new(Column::new("priority", 0))];
    let aggregate = Arc::new(
        AggregateExprBuilder::new(min_udaf(), args)
            .schema(schema.clone())
            .alias("top_one_priority")
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
            ascending,
        )],
        frame,
        None,
    );
    let mut partition_rows = vec![Vec::new(); groups.len()];
    for (row, &group) in row_groups.iter().enumerate() {
        partition_rows[group].push(row);
    }
    let mut winners = vec![false; count];
    for (group, rows) in partition_rows.iter().enumerate() {
        let scores = prior[group]
            .into_iter()
            .map(|token| priority[token])
            .chain(rows.iter().map(|&row| priority[input_start + row]))
            .collect::<Vec<_>>();
        let arrivals = UInt64Array::from_iter_values(0..scores.len() as u64);
        let input = RecordBatch::try_new(
            schema.clone(),
            vec![Arc::new(UInt64Array::from(scores)), Arc::new(arrivals)],
        )?;
        let output = window.evaluate(&input)?;
        let minima = output
            .as_any()
            .downcast_ref::<UInt64Array>()
            .expect("MIN UInt64");
        let offset = usize::from(prior[group].is_some());
        for (index, &row) in rows.iter().enumerate() {
            winners[row] = minima.value(offset + index) == priority[input_start + row];
        }
    }
    Ok(winners)
}

#[cfg(test)]
mod tests;
