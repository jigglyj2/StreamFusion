// Copyright 2026 StreamFusion Authors
// Licensed under the Apache License, Version 2.0.

//! Vectorized canonical partial merging. SQL FILTER has already been applied locally.
//! The caller owns batch admission and the Flink empty-group/timer transitions.

use super::*;

pub(in crate::planner::operators) struct GroupedMerge(GroupedCompute);

impl GroupedMerge {
    pub(in crate::planner::operators) fn new(calls: &[Call]) -> Result<Option<Self>> {
        if calls.iter().any(|call| {
            call.distinct
                || !matches!(
                    call.function,
                    proto::AggregateFunction::Count
                        | proto::AggregateFunction::CountStar
                        | proto::AggregateFunction::Min
                        | proto::AggregateFunction::Max
                )
                || !super::super::datafusion_compute::reusable(call)
        }) {
            return Ok(None);
        }
        // COUNT partials are additive BIGINT values. DataFusion SUM uses wrapping addition,
        // matching Flink even in debug builds; its COUNT merge uses checked debug arithmetic.
        let sum = Call {
            function: proto::AggregateFunction::Sum,
            input_index: Some(0),
            input_type: Some(DataType::Int64),
            output_type: DataType::Int64,
            retractable: false,
            filter_index: None,
            distinct: false,
        };
        let merge_calls = calls
            .iter()
            .map(|call| {
                if matches!(
                    call.function,
                    proto::AggregateFunction::Count | proto::AggregateFunction::CountStar
                ) {
                    sum.clone()
                } else {
                    call.clone()
                }
            })
            .collect::<Vec<_>>();
        let kernels = Kernels::new(&merge_calls)?;
        if kernels.0.iter().any(|kernel| {
            kernel
                .as_ref()
                .is_none_or(|kernel| !kernel.groups_accumulator_supported())
        }) {
            return Ok(None);
        }
        let count = Kernels::new(&[sum])?;
        Ok(Some(Self(GroupedCompute {
            values: kernels
                .0
                .iter()
                .map(|kernel| kernel.as_ref().unwrap().create_groups_accumulator())
                .collect::<Result<_>>()?,
            non_null: calls.iter().map(|_| None).collect(),
            row_count: count.0[0].as_ref().unwrap().create_groups_accumulator()?,
        })))
    }

    pub(in crate::planner::operators) fn merge(
        &mut self,
        calls: &[Call],
        states: &[&AccumulatorState],
        groups: &[usize],
        total_groups: usize,
    ) -> Result<()> {
        if states.len() != groups.len()
            || groups.iter().any(|&group| group >= total_groups)
            || calls.len() != self.0.values.len()
            || states
                .iter()
                .any(|state| state.accumulators.len() != calls.len())
        {
            return Err(DataFusionError::Execution(
                "grouped partial shape differs from its groups or calls".into(),
            ));
        }
        let counts = Arc::new(Int64Array::from_iter_values(
            states.iter().map(|state| state.row_count),
        )) as ArrayRef;
        self.0
            .row_count
            .merge_batch(&[counts], groups, total_groups)?;
        for (index, call) in calls.iter().enumerate() {
            let values = states
                .iter()
                .map(|state| match &state.accumulators[index] {
                    Accumulator::Count(value) => Ok(Some(AggregateValue::Int(*value as i128))),
                    Accumulator::AppendExtremum(value) => Ok(value.clone()),
                    _ => Err(DataFusionError::Execution(
                        "unsupported grouped partial accumulator".into(),
                    )),
                })
                .collect::<Result<Vec<_>>>()?;
            let data_type = if matches!(
                call.function,
                proto::AggregateFunction::Count | proto::AggregateFunction::CountStar
            ) {
                &DataType::Int64
            } else {
                &call.output_type
            };
            let array = aggregate_array(&values, data_type)?;
            self.0.values[index].merge_batch(&[array], groups, total_groups)?;
        }
        Ok(())
    }

    pub(in crate::planner::operators) fn finish(&mut self) -> Result<GroupedOutput> {
        self.0.finish()
    }
}

#[cfg(test)]
mod tests;
