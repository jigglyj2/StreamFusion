// Copyright 2026 StreamFusion Authors
// Licensed under the Apache License, Version 2.0.

//! Compact DataFusion state for append-only groups that emit only at a buffer boundary.
//! State is held in DataFusion's group vectors, not a Rust accumulator object per key.
//! Flink's canonical accumulator is constructed only while serializing an output partial.

use super::datafusion_compute::Kernels;
use super::*;
use datafusion::logical_expr::{EmitTo, GroupsAccumulator};

pub(in crate::planner::operators) struct GroupedCompute {
    values: Vec<Box<dyn GroupsAccumulator>>,
    non_null: Vec<Option<Box<dyn GroupsAccumulator>>>,
    row_count: Box<dyn GroupsAccumulator>,
}

pub(in crate::planner::operators) struct GroupedOutput {
    values: Vec<ArrayRef>,
    non_null: Vec<Option<ArrayRef>>,
    row_count: ArrayRef,
}

impl GroupedCompute {
    pub(in crate::planner::operators) fn new(calls: &[Call]) -> Result<Option<Self>> {
        let kernels = Kernels::new(calls)?;
        if kernels.0.iter().any(|kernel| {
            kernel
                .as_ref()
                .is_none_or(|kernel| !kernel.groups_accumulator_supported())
        }) {
            return Ok(None);
        }
        let count_call = Call {
            function: proto::AggregateFunction::CountStar,
            input_index: None,
            input_type: None,
            output_type: DataType::Int64,
            retractable: false,
            filter_index: None,
            distinct: false,
        };
        let count = Kernels::new(&[count_call])?;
        let count = count.0[0]
            .as_ref()
            .expect("COUNT supports grouped execution");
        let values = kernels
            .0
            .iter()
            .map(|kernel| kernel.as_ref().unwrap().create_groups_accumulator())
            .collect::<Result<Vec<_>>>()?;
        let non_null = calls
            .iter()
            .map(|call| {
                if matches!(
                    call.function,
                    proto::AggregateFunction::Sum
                        | proto::AggregateFunction::Sum0
                        | proto::AggregateFunction::Avg
                ) {
                    count.create_groups_accumulator().map(Some)
                } else {
                    Ok(None)
                }
            })
            .collect::<Result<Vec<_>>>()?;
        Ok(Some(Self {
            values,
            non_null,
            row_count: count.create_groups_accumulator()?,
        }))
    }

    pub(in crate::planner::operators) fn update(
        &mut self,
        calls: &[Call],
        batch: &RecordBatch,
        groups: &[usize],
        valid: Option<&BooleanArray>,
        total_groups: usize,
    ) -> Result<()> {
        if groups.len() != batch.num_rows() || valid.is_some_and(|mask| mask.len() != groups.len())
        {
            return Err(DataFusionError::Execution(
                "grouped aggregate selection differs from its input".into(),
            ));
        }
        if groups.iter().any(|&group| group >= total_groups) {
            return Err(DataFusionError::Execution(
                "grouped aggregate index exceeds its state".into(),
            ));
        }
        let ones = Arc::new(Int8Array::from(vec![1; batch.num_rows()])) as ArrayRef;
        self.row_count
            .update_batch(std::slice::from_ref(&ones), groups, valid, total_groups)?;
        for (index, call) in calls.iter().enumerate() {
            let column = call
                .input_index
                .map_or_else(|| ones.clone(), |column| batch.column(column).clone());
            let column = if self.non_null[index].is_some() {
                arrow::compute::cast(column.as_ref(), &DataType::Int64)?
            } else {
                column
            };
            let filter = call
                .filter_index
                .map(|column| {
                    batch
                        .column(column)
                        .as_any()
                        .downcast_ref::<BooleanArray>()
                        .ok_or_else(|| {
                            DataFusionError::Execution(
                                "grouped aggregate FILTER is not boolean".into(),
                            )
                        })
                })
                .transpose()?;
            let combined = match (valid, filter) {
                (Some(valid), Some(filter)) => Some(arrow::compute::and_kleene(valid, filter)?),
                _ => None,
            };
            let filter = combined.as_ref().or(filter).or(valid);
            self.values[index].update_batch(
                std::slice::from_ref(&column),
                groups,
                filter,
                total_groups,
            )?;
            if let Some(count) = &mut self.non_null[index] {
                count.update_batch(std::slice::from_ref(&column), groups, filter, total_groups)?;
            }
        }
        Ok(())
    }

    pub(in crate::planner::operators) fn finish(&mut self) -> Result<GroupedOutput> {
        Ok(GroupedOutput {
            values: self
                .values
                .iter_mut()
                .map(|value| value.evaluate(EmitTo::All))
                .collect::<Result<Vec<_>>>()?,
            non_null: self
                .non_null
                .iter_mut()
                .map(|count| {
                    count
                        .as_mut()
                        .map(|count| count.evaluate(EmitTo::All))
                        .transpose()
                })
                .collect::<Result<Vec<_>>>()?,
            row_count: self.row_count.evaluate(EmitTo::All)?,
        })
    }

    pub(in crate::planner::operators) fn size(&self) -> usize {
        self.values.iter().map(|value| value.size()).sum::<usize>()
            + self
                .non_null
                .iter()
                .flatten()
                .map(|count| count.size())
                .sum::<usize>()
            + self.row_count.size()
    }
}

impl GroupedOutput {
    pub(in crate::planner::operators) fn size(&self) -> usize {
        self.values
            .iter()
            .map(|value| value.get_array_memory_size())
            .sum::<usize>()
            + self
                .non_null
                .iter()
                .flatten()
                .map(|count| count.get_array_memory_size())
                .sum::<usize>()
            + self.row_count.get_array_memory_size()
    }

    pub(in crate::planner::operators) fn state(
        &self,
        calls: &[Call],
        row: usize,
    ) -> Result<AccumulatorState> {
        let accumulators = calls
            .iter()
            .enumerate()
            .map(|(index, call)| {
                let value = aggregate_value(self.values[index].as_ref(), row)?;
                Ok(match call.function {
                    proto::AggregateFunction::Count | proto::AggregateFunction::CountStar => {
                        Accumulator::Count(count(&self.values[index], row)?)
                    }
                    proto::AggregateFunction::Min | proto::AggregateFunction::Max => {
                        Accumulator::AppendExtremum(value)
                    }
                    proto::AggregateFunction::Avg => Accumulator::Average {
                        value,
                        count: count(self.non_null[index].as_ref().unwrap(), row)?,
                    },
                    _ => Accumulator::Sum {
                        value: value
                            .as_ref()
                            .map(|value| {
                                aggregate_add(
                                    &zero_value(&call.output_type),
                                    value,
                                    &call.output_type,
                                )
                            })
                            .transpose()?
                            .flatten(),
                        count: count(self.non_null[index].as_ref().unwrap(), row)?,
                    },
                })
            })
            .collect::<Result<Vec<_>>>()?;
        Ok(AccumulatorState {
            row_count: count(&self.row_count, row)?,
            accumulators,
        })
    }
}
fn count(values: &ArrayRef, row: usize) -> Result<i64> {
    let values = values
        .as_any()
        .downcast_ref::<Int64Array>()
        .ok_or_else(|| {
            DataFusionError::Internal("DataFusion grouped COUNT must return BIGINT".into())
        })?;
    if row >= values.len() || values.is_null(row) {
        return Err(DataFusionError::Internal(
            "DataFusion grouped COUNT is missing a group".into(),
        ));
    }
    Ok(values.value(row))
}

#[cfg(test)]
mod tests;
