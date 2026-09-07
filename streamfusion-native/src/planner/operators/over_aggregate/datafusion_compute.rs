// Copyright 2026 StreamFusion Authors
// Licensed under the Apache License, Version 2.0

//! Bounded OVER delegates frame traversal and aggregate execution to DataFusion window expressions.
//! Flink retains partition ordering, keyed snapshots, terminal output and changelog envelopes.
use super::super::group_aggregate::{aggregate_array, aggregate_value, flink_udaf::FlinkAggregate};
use super::*;
use arrow::array::BinaryArray;
use datafusion::functions_aggregate::{
    count::count_udaf,
    min_max::{max_udaf, min_udaf},
    sum::sum_udaf,
};
use datafusion::logical_expr::{AggregateUDF, WindowFrame, WindowFrameBound, WindowFrameUnits};
use datafusion::physical_expr::window::{
    PlainAggregateWindowExpr, SlidingAggregateWindowExpr, WindowExpr,
};
use datafusion::physical_expr::{
    aggregate::AggregateExprBuilder, expressions::Column, PhysicalExpr, PhysicalSortExpr,
};
use datafusion::scalar::ScalarValue;

pub(super) fn compatible(calls: &[Call]) -> bool {
    // DataFusion may retract before adding a frame's new rows. Flink's float rounding and
    // decimal-overflow poisoning are order-sensitive; keep that precise frame transition.
    calls.iter().all(|call| {
        if matches!(
            call.function,
            proto::AggregateFunction::Count | proto::AggregateFunction::CountStar
        ) {
            return true;
        }
        if matches!(call.input_type, Some(DataType::Float32 | DataType::Float64)) {
            return false;
        }
        !(matches!(call.input_type, Some(DataType::Decimal128(_, _)))
            && matches!(
                call.function,
                proto::AggregateFunction::Sum
                    | proto::AggregateFunction::Sum0
                    | proto::AggregateFunction::Avg
            ))
    })
}

pub(super) fn evaluate(
    state: &mut OverState,
    calls: &[Call],
    rows_frame: bool,
    preceding: Option<u64>,
    owner: &HostMemoryReservation,
) -> Result<Vec<ChangedRow>> {
    let rows = state.rows.values().flatten().collect::<Vec<_>>();
    if rows.is_empty() {
        return Ok(Vec::new());
    }
    let mut workspace =
        owner.sibling("DataFusion bounded OVER Arrow input, window state and output");
    let bytes = rows.iter().fold(0usize, |bytes, row| {
        bytes.saturating_add(row.payload.len()).saturating_add(
            row.contributions
                .iter()
                .flatten()
                .map(|v| match v {
                    AggregateValue::Bytes(v) => v.len() + 32,
                    _ => 32,
                })
                .sum::<usize>(),
        )
    });
    workspace.resize(
        bytes.saturating_mul(8).saturating_add(
            rows.len()
                .saturating_mul(calls.len() + 1)
                .saturating_mul(64),
        ),
    )?;
    let ordering: ArrayRef = if preceding.is_some() && !rows_frame {
        Arc::new(Int64Array::from_iter_values(
            rows.iter().map(|row| row.event_timestamp),
        ))
    } else {
        Arc::new(BinaryArray::from_iter_values(
            state
                .rows
                .iter()
                .flat_map(|(key, rows)| std::iter::repeat_n(key.as_slice(), rows.len()))
                .collect::<Vec<_>>(),
        ))
    };
    let mut columns = vec![("order".to_string(), ordering)];
    for (i, call) in calls.iter().enumerate() {
        let input = if call.input_index.is_none() {
            Arc::new(Int8Array::from(vec![1; rows.len()])) as ArrayRef
        } else if call.function == proto::AggregateFunction::Count && !call.distinct {
            Arc::new(arrow::array::BooleanArray::from_iter(
                rows.iter()
                    .map(|row| row.contributions[i].as_ref().map(|_| true)),
            )) as ArrayRef
        } else {
            aggregate_array(
                &rows
                    .iter()
                    .map(|row| row.contributions[i].clone())
                    .collect::<Vec<_>>(),
                call.input_type.as_ref().expect("validated aggregate input"),
            )?
        };
        columns.push((format!("value_{i}"), input));
    }
    drop(rows);
    let batch = RecordBatch::try_from_iter(columns)?;
    let order_by = vec![PhysicalSortExpr::new(
        Arc::new(Column::new("order", 0)),
        arrow::compute::SortOptions {
            descending: false,
            nulls_first: true,
        },
    )];
    let start = if rows_frame {
        ScalarValue::UInt64(preceding.map(|n| n - 1))
    } else {
        ScalarValue::Int64(preceding.map(|n| n as i64))
    };
    let frame = Arc::new(WindowFrame::new_bounds(
        if rows_frame {
            WindowFrameUnits::Rows
        } else {
            WindowFrameUnits::Range
        },
        WindowFrameBound::Preceding(start),
        WindowFrameBound::CurrentRow,
    ));
    let mut results = Vec::with_capacity(calls.len());
    for (i, call) in calls.iter().enumerate() {
        let udf = if call.distinct {
            Arc::new(AggregateUDF::from(FlinkAggregate::new(call)))
        } else {
            match call.function {
                proto::AggregateFunction::Count | proto::AggregateFunction::CountStar => {
                    count_udaf()
                }
                proto::AggregateFunction::Sum
                    if call.input_type == Some(DataType::Int64)
                        && call.output_type == DataType::Int64 =>
                {
                    sum_udaf()
                }
                proto::AggregateFunction::Min => min_udaf(),
                proto::AggregateFunction::Max => max_udaf(),
                _ => Arc::new(AggregateUDF::from(FlinkAggregate::new(call))),
            }
        };
        let args: Vec<Arc<dyn PhysicalExpr>> =
            vec![Arc::new(Column::new(&format!("value_{i}"), i + 1))];
        let aggregate = Arc::new(
            AggregateExprBuilder::new(udf, args)
                .schema(batch.schema())
                .alias(format!("over_{i}"))
                .build()?,
        );
        let window: Box<dyn WindowExpr> = if preceding.is_some() {
            Box::new(SlidingAggregateWindowExpr::new(
                aggregate,
                &[],
                &order_by,
                frame.clone(),
                None,
            ))
        } else {
            Box::new(PlainAggregateWindowExpr::new(
                aggregate,
                &[],
                &order_by,
                frame.clone(),
                None,
            ))
        };
        results.push(window.evaluate(&batch)?);
    }
    let mut changed = Vec::new();
    for (index, row) in state.rows.values_mut().flatten().enumerate() {
        let values = results
            .iter()
            .map(|array| aggregate_value(array.as_ref(), index))
            .collect::<Result<Vec<_>>>()?;
        update_row_if_changed(row, values, &mut changed);
    }
    Ok(changed)
}
