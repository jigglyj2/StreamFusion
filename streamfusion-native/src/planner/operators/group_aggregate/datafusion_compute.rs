// Copyright 2026 StreamFusion Authors
// Licensed under the Apache License, Version 2.0

//! DataFusion computes Arrow aggregate batches; Flink's canonical state remains the checkpoint
//! contract. Floating-point arithmetic, decimal overflow and DISTINCT sums/retractable extrema retain
//! the ordered Flink transitions: reassociating those operations can change observable results.

use super::*;
use arrow::compute::{cast, filter, take};
use datafusion::functions_aggregate::{
    count::count_udaf,
    min_max::{max_udaf, min_udaf},
    sum::sum_udaf,
};
use datafusion::physical_expr::{
    aggregate::{AggregateExprBuilder, AggregateFunctionExpr},
    expressions::Column,
};

pub(super) fn reusable(call: &Call) -> bool {
    if call.distinct {
        return false;
    }
    match call.function {
        proto::AggregateFunction::Count | proto::AggregateFunction::CountStar => true,
        proto::AggregateFunction::Sum
        | proto::AggregateFunction::Sum0
        | proto::AggregateFunction::Avg => {
            matches!(
                call.input_type,
                Some(DataType::Int8 | DataType::Int16 | DataType::Int32 | DataType::Int64)
            ) && matches!(
                call.output_type,
                DataType::Int8 | DataType::Int16 | DataType::Int32 | DataType::Int64
            )
        }
        proto::AggregateFunction::Min | proto::AggregateFunction::Max => {
            !call.retractable
                && !matches!(call.input_type, Some(DataType::Float32 | DataType::Float64))
        }
        _ => false,
    }
}

/// Physical expressions are prepared once per incoming batch, never once per group or row.
#[derive(Debug)]
pub(in crate::planner::operators) struct Kernels(pub(super) Vec<Option<AggregateFunctionExpr>>);
impl Kernels {
    pub(in crate::planner::operators) fn new(calls: &[Call]) -> Result<Self> {
        Ok(Self(
            calls
                .iter()
                .map(|call| {
                    if !reusable(call) && !datafusion_distinct_count::supports(call) {
                        return Ok(None);
                    }
                    let udf = match call.function {
                        proto::AggregateFunction::Count | proto::AggregateFunction::CountStar => {
                            count_udaf()
                        }
                        proto::AggregateFunction::Min => min_udaf(),
                        proto::AggregateFunction::Max => max_udaf(),
                        _ => sum_udaf(),
                    };
                    let data_type = if datafusion_distinct_count::supports(call) {
                        DataType::Int8
                    } else if matches!(
                        call.function,
                        proto::AggregateFunction::Sum
                            | proto::AggregateFunction::Sum0
                            | proto::AggregateFunction::Avg
                    ) {
                        DataType::Int64
                    } else {
                        call.input_type.clone().unwrap_or(DataType::Int8)
                    };
                    let schema = Arc::new(Schema::new(vec![Field::new("value", data_type, true)]));
                    AggregateExprBuilder::new(udf, vec![Arc::new(Column::new("value", 0))])
                        .schema(schema)
                        .alias("flink_batch_contribution")
                        .build()
                        .map(Some)
                })
                .collect::<Result<Vec<_>>>()?,
        ))
    }
}

/// Apply an append-only group/run without suppressing a Flink bundle boundary. The caller admits
/// the batch workspace and performs keyed state reads/writes once per incoming Arrow batch.
impl AccumulatorState {
    pub(in crate::planner::operators) fn apply_append_batch(
        &mut self,
        calls: &[Call],
        kernels: &Kernels,
        batch: &RecordBatch,
        rows: &[usize],
    ) -> Result<()> {
        if rows.is_empty() {
            return Ok(());
        }
        let selection = UInt32Array::from(
            rows.iter()
                .map(|&row| {
                    u32::try_from(row).map_err(|_| {
                        DataFusionError::Execution("aggregate selection exceeds u32".into())
                    })
                })
                .collect::<Result<Vec<_>>>()?,
        );
        for (index, call) in calls.iter().enumerate() {
            if datafusion_distinct_count::supports(call) {
                datafusion_distinct_count::apply_append_batch(
                    &mut self.accumulators[index],
                    call,
                    kernels.0[index]
                        .as_ref()
                        .expect("distinct count was prepared"),
                    batch,
                    rows,
                )?;
                continue;
            }
            if !reusable(call) {
                let mut state = Self {
                    row_count: self.row_count,
                    accumulators: vec![std::mem::replace(
                        &mut self.accumulators[index],
                        Accumulator::Count(0),
                    )],
                };
                for &row in rows {
                    state.apply(std::slice::from_ref(call), batch, row, true)?;
                }
                self.accumulators[index] = state.accumulators.remove(0);
                continue;
            }
            let input = match call.input_index {
                Some(column) => take(batch.column(column).as_ref(), &selection, None)?,
                None => Arc::new(Int8Array::from(vec![1; rows.len()])) as ArrayRef,
            };
            let input = if let Some(column) = call.filter_index {
                let mask = take(batch.column(column).as_ref(), &selection, None)?;
                let mask = mask
                    .as_any()
                    .downcast_ref::<BooleanArray>()
                    .ok_or_else(|| {
                        DataFusionError::Execution("aggregate FILTER is not boolean".into())
                    })?;
                filter(input.as_ref(), mask)?
            } else {
                input
            };
            let arithmetic = matches!(
                call.function,
                proto::AggregateFunction::Sum
                    | proto::AggregateFunction::Sum0
                    | proto::AggregateFunction::Avg
            );
            let input = if arithmetic {
                cast(input.as_ref(), &DataType::Int64)?
            } else {
                input
            };
            let non_null = (input.len() - input.null_count()) as i64;
            let mut native = kernels.0[index]
                .as_ref()
                .expect("reusable aggregate was prepared")
                .create_accumulator()?;
            native.update_batch(&[input])?;
            let result = native.evaluate()?.to_array()?;
            let value = aggregate_value(result.as_ref(), 0)?;
            let contribution = match call.function {
                proto::AggregateFunction::Count | proto::AggregateFunction::CountStar => {
                    Accumulator::Count(match value {
                        Some(AggregateValue::Int(count)) => count as i64,
                        _ => {
                            return Err(DataFusionError::Internal(
                                "DataFusion COUNT returned a non-integer".into(),
                            ))
                        }
                    })
                }
                proto::AggregateFunction::Min | proto::AggregateFunction::Max => {
                    Accumulator::AppendExtremum(value)
                }
                proto::AggregateFunction::Avg => Accumulator::Average {
                    value,
                    count: non_null,
                },
                _ => Accumulator::Sum {
                    value: value
                        .as_ref()
                        .map(|value| {
                            aggregate_add(&zero_value(&call.output_type), value, &call.output_type)
                        })
                        .transpose()?
                        .flatten(),
                    count: non_null,
                },
            };
            // Preserve Flink's integer width, SUM0/null result and accumulator serialization.
            let mut state = Self {
                row_count: 0,
                accumulators: vec![std::mem::replace(
                    &mut self.accumulators[index],
                    Accumulator::Count(0),
                )],
            };
            state.merge(
                std::slice::from_ref(call),
                &Self {
                    row_count: 0,
                    accumulators: vec![contribution],
                },
            )?;
            self.accumulators[index] = state.accumulators.remove(0);
        }
        self.row_count = self.row_count.wrapping_add(rows.len() as i64);
        Ok(())
    }

    pub(in crate::planner::operators) fn apply_input_rows(
        &mut self,
        calls: &[Call],
        kernels: &Kernels,
        batch: &RecordBatch,
        rows: &[usize],
        accumulate: &[bool],
        row_inputs: Option<&datafusion_rows::RowInputs>,
        ignore_empty_retract: bool,
    ) -> Result<()> {
        let mut start = 0;
        let mut row_kernels = None;
        while start < rows.len() {
            if accumulate[rows[start]] {
                let end = (start + 1..rows.len())
                    .find(|&i| !accumulate[rows[i]])
                    .unwrap_or(rows.len());
                self.apply_append_batch(calls, kernels, batch, &rows[start..end])?;
                row_kernels = None;
                start = end;
            } else {
                if !ignore_empty_retract || self.row_count != 0 {
                    if row_kernels.is_none() {
                        row_kernels = Some(datafusion_rows::RowKernels::new(calls, kernels, self)?);
                    }
                    row_kernels.as_mut().unwrap().apply(
                        self,
                        calls,
                        row_inputs.expect("retracting input columns"),
                        batch,
                        rows[start],
                        false,
                    )?;
                }
                start += 1;
            }
        }
        Ok(())
    }
}
