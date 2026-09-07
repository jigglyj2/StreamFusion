// Copyright 2026 StreamFusion Authors
// Licensed under the Apache License, Version 2.0

//! Per-record changelogs cannot be reduced to one aggregate result per Arrow batch. Prepare the
//! columns once and retain DataFusion accumulators for each loaded group throughout the batch,
//! evaluating each required intermediate result without rebuilding expressions or keyed state.
use super::datafusion_compute::Kernels;
use super::*;
use datafusion::logical_expr::Accumulator as NativeAccumulator;

pub(in crate::planner::operators) struct RowInputs {
    columns: Vec<Option<(ArrayRef, Option<ArrayRef>)>>,
}
impl RowInputs {
    pub(in crate::planner::operators) fn new(calls: &[Call], batch: &RecordBatch) -> Result<Self> {
        Ok(Self {
            columns: calls
                .iter()
                .map(|call| {
                    if !datafusion_compute::reusable(call) {
                        return Ok(None);
                    }
                    let input = match call.input_index {
                        Some(i) => batch.column(i).clone(),
                        None => Arc::new(Int8Array::from(vec![1; batch.num_rows()])) as ArrayRef,
                    };
                    let arithmetic = matches!(
                        call.function,
                        proto::AggregateFunction::Sum
                            | proto::AggregateFunction::Sum0
                            | proto::AggregateFunction::Avg
                    );
                    if arithmetic {
                        let input = arrow::compute::cast(input.as_ref(), &DataType::Int64)?;
                        let values = input
                            .as_any()
                            .downcast_ref::<Int64Array>()
                            .expect("cast to i64");
                        let negative =
                            Arc::new(arrow::compute::unary::<_, _, arrow::datatypes::Int64Type>(
                                values,
                                |value| value.wrapping_neg(),
                            )) as ArrayRef;
                        Ok(Some((input, Some(negative))))
                    } else {
                        Ok(Some((input, None)))
                    }
                })
                .collect::<Result<Vec<_>>>()?,
        })
    }
}

pub(super) struct RowKernels {
    native: Vec<Option<Box<dyn NativeAccumulator>>>,
}
impl RowKernels {
    pub(super) fn new(calls: &[Call], kernels: &Kernels, state: &AccumulatorState) -> Result<Self> {
        let native = calls
            .iter()
            .zip(&state.accumulators)
            .enumerate()
            .map(|(index, (call, state))| {
                let Some(expr) = kernels.0[index].as_ref() else {
                    return Ok(None);
                };
                let mut native = expr.create_accumulator()?;
                let (value, data_type) = match state {
                    Accumulator::Count(count) => {
                        (Some(AggregateValue::Int(*count as i128)), DataType::Int64)
                    }
                    Accumulator::Sum { value, .. } | Accumulator::Average { value, .. } => {
                        (value.clone(), DataType::Int64)
                    }
                    Accumulator::AppendExtremum(value) => (value.clone(), call.output_type.clone()),
                    _ => {
                        return Err(DataFusionError::Internal(
                            "unexpected DataFusion accumulator adapter".into(),
                        ))
                    }
                };
                native.merge_batch(&[aggregate_array(&[value], &data_type)?])?;
                Ok(Some(native))
            })
            .collect::<Result<Vec<_>>>()?;
        Ok(Self { native })
    }

    pub(super) fn apply(
        &mut self,
        state: &mut AccumulatorState,
        calls: &[Call],
        inputs: &RowInputs,
        batch: &RecordBatch,
        row: usize,
        accumulate: bool,
    ) -> Result<()> {
        let delta = if accumulate { 1 } else { -1 };
        for (index, call) in calls.iter().enumerate() {
            let Some(native) = &mut self.native[index] else {
                let mut single = AccumulatorState {
                    row_count: state.row_count,
                    accumulators: vec![std::mem::replace(
                        &mut state.accumulators[index],
                        Accumulator::Count(0),
                    )],
                };
                let result = single.apply(std::slice::from_ref(call), batch, row, accumulate);
                state.accumulators[index] = single.accumulators.remove(0);
                result?;
                continue;
            };
            if !aggregate_filter(call, batch, row)? {
                continue;
            }
            let (input, negative) = inputs.columns[index]
                .as_ref()
                .expect("native input was prepared");
            if !accumulate
                && matches!(
                    call.function,
                    proto::AggregateFunction::Min | proto::AggregateFunction::Max
                )
            {
                continue;
            }
            if accumulate {
                native.update_batch(&[input.slice(row, 1)])?;
            } else if let Some(negative) = negative {
                native.update_batch(&[negative.slice(row, 1)])?;
            } else {
                native.retract_batch(&[input.slice(row, 1)])?;
            }
            let array = native.evaluate()?.to_array()?;
            let value = aggregate_value(array.as_ref(), 0)?;
            match &mut state.accumulators[index] {
                Accumulator::Count(count) => {
                    *count = match value {
                        Some(AggregateValue::Int(v)) => v as i64,
                        _ => {
                            return Err(DataFusionError::Internal(
                                "DataFusion COUNT returned a non-integer".into(),
                            ))
                        }
                    };
                }
                Accumulator::Sum { value: sum, count } => {
                    *sum = value
                        .as_ref()
                        .map(|v| {
                            aggregate_add(&zero_value(&call.output_type), v, &call.output_type)
                        })
                        .transpose()?
                        .flatten();
                    if !input.is_null(row) {
                        *count = count.wrapping_add(delta);
                    }
                }
                Accumulator::Average { value: sum, count } => {
                    *sum = value;
                    if !input.is_null(row) {
                        *count = count.wrapping_add(delta);
                    }
                }
                Accumulator::AppendExtremum(current) => *current = value,
                _ => unreachable!("validated native accumulator adapter"),
            }
        }
        state.row_count = state.row_count.wrapping_add(delta);
        Ok(())
    }
}
