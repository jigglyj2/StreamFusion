// Copyright 2026 StreamFusion Authors
// Licensed under the Apache License, Version 2.0

use super::*;

impl AccumulatorState {
    pub(in crate::planner::operators) fn new(calls: &[Call]) -> Self {
        Self {
            row_count: 0,
            accumulators: calls
                .iter()
                .map(|call| match call.function {
                    proto::AggregateFunction::CountStar | proto::AggregateFunction::Count => {
                        if call.distinct {
                            Accumulator::DistinctCount {
                                count: 0,
                                values: BTreeMap::new(),
                            }
                        } else {
                            Accumulator::Count(0)
                        }
                    }
                    proto::AggregateFunction::Sum | proto::AggregateFunction::Sum0 => {
                        if call.distinct {
                            Accumulator::DistinctSum {
                                value: None,
                                count: 0,
                                values: BTreeMap::new(),
                            }
                        } else {
                            Accumulator::Sum {
                                value: None,
                                count: 0,
                            }
                        }
                    }
                    proto::AggregateFunction::Avg => {
                        if call.distinct {
                            Accumulator::DistinctAverage {
                                value: Some(zero_value(&call.average_accumulator_type())),
                                count: 0,
                                values: BTreeMap::new(),
                            }
                        } else {
                            Accumulator::Average {
                                value: Some(zero_value(&call.average_accumulator_type())),
                                count: 0,
                            }
                        }
                    }
                    proto::AggregateFunction::Min | proto::AggregateFunction::Max => {
                        if call.retractable {
                            Accumulator::Extremum(BTreeMap::new())
                        } else {
                            Accumulator::AppendExtremum(None)
                        }
                    }
                    _ => unreachable!("validated aggregate function"),
                })
                .collect(),
        }
    }

    pub(in crate::planner::operators) fn estimated_dynamic_bytes(&self) -> usize {
        self.accumulators
            .capacity()
            .saturating_mul(std::mem::size_of::<Accumulator>())
            .saturating_add(self.accumulators.iter().fold(0usize, |bytes, accumulator| {
                bytes.saturating_add(match accumulator {
                    Accumulator::Count(_) => 0,
                    Accumulator::DistinctCount { values, .. } | Accumulator::Extremum(values) => {
                        estimated_value_map_bytes(values)
                    }
                    Accumulator::Sum { value, .. }
                    | Accumulator::Average { value, .. }
                    | Accumulator::AppendExtremum(value) => {
                        value.as_ref().map_or(0, AggregateValue::dynamic_bytes)
                    }
                    Accumulator::DistinctSum { value, values, .. }
                    | Accumulator::DistinctAverage { value, values, .. } => value
                        .as_ref()
                        .map_or(0, AggregateValue::dynamic_bytes)
                        .saturating_add(estimated_value_map_bytes(values)),
                })
            }))
    }

    pub(in crate::planner::operators) fn has_delta(&self) -> bool {
        self.row_count != 0
            || self
                .accumulators
                .iter()
                .any(|accumulator| match accumulator {
                    Accumulator::Count(count) => *count != 0,
                    Accumulator::DistinctCount { count, values } => {
                        *count != 0 || !values.is_empty()
                    }
                    Accumulator::Sum { value, count } | Accumulator::Average { value, count } => {
                        *count != 0 || value.as_ref().is_some_and(|value| !value.is_zero())
                    }
                    Accumulator::DistinctSum {
                        value,
                        count,
                        values,
                    }
                    | Accumulator::DistinctAverage {
                        value,
                        count,
                        values,
                    } => {
                        *count != 0
                            || !values.is_empty()
                            || value.as_ref().is_some_and(|value| !value.is_zero())
                    }
                    Accumulator::AppendExtremum(value) => value.is_some(),
                    Accumulator::Extremum(values) => !values.is_empty(),
                })
    }

    pub(in crate::planner::operators) fn incremental_persistent_only(
        &self,
        calls: &[Call],
    ) -> Self {
        let neutral = Self::new(calls);
        Self {
            row_count: 0,
            accumulators: calls
                .iter()
                .zip(&self.accumulators)
                .zip(neutral.accumulators)
                .map(|((call, current), empty)| {
                    if incremental_call_requires_persistence(call) {
                        current.clone()
                    } else {
                        empty
                    }
                })
                .collect(),
        }
    }

    pub(in crate::planner::operators) fn has_incremental_persistent_state(
        &self,
        calls: &[Call],
    ) -> bool {
        calls
            .iter()
            .zip(&self.accumulators)
            .any(|(call, accumulator)| {
                incremental_call_requires_persistence(call) && !accumulator_is_neutral(accumulator)
            })
    }

    /// Creates an accumulator seed from the result at the end of an already processed prefix.
    /// OVER aggregation uses this only to accumulate later rows; it never retracts from this
    /// compact seed. Keeping just the current extremum is therefore sufficient and avoids storing
    /// a complete accumulator alongside every ordered row.
    pub(in crate::planner::operators) fn from_prefix_values(
        calls: &[Call],
        values: &[Option<AggregateValue>],
    ) -> Result<Self> {
        if calls.len() != values.len() {
            return Err(DataFusionError::Internal(
                "aggregate prefix value count does not match its calls".to_string(),
            ));
        }
        let accumulators = calls
            .iter()
            .zip(values)
            .map(|(call, value)| match call.function {
                proto::AggregateFunction::CountStar | proto::AggregateFunction::Count => {
                    let count = match value {
                        Some(AggregateValue::Int(value)) => {
                            i64::try_from(*value).map_err(|_| {
                                DataFusionError::Execution(
                                    "OVER COUNT prefix does not fit i64".to_string(),
                                )
                            })?
                        }
                        None => 0,
                        Some(_) => {
                            return Err(DataFusionError::Internal(
                                "OVER COUNT prefix has a non-integer value".to_string(),
                            ));
                        }
                    };
                    Ok(Accumulator::Count(count))
                }
                proto::AggregateFunction::Sum | proto::AggregateFunction::Sum0 => {
                    Ok(Accumulator::Sum {
                        value: value.clone(),
                        // Only zero versus non-zero affects accumulation output. This seed is never
                        // retracted, so the exact historical non-null count is not needed.
                        count: i64::from(value.is_some()),
                    })
                }
                proto::AggregateFunction::Avg => Err(DataFusionError::Internal(
                    "OVER AVG cannot reconstruct its sum and count from a compact prefix"
                        .to_string(),
                )),
                proto::AggregateFunction::Min | proto::AggregateFunction::Max => {
                    if call.retractable {
                        let mut extrema = BTreeMap::new();
                        if let Some(value) = value.clone() {
                            extrema.insert(value, 1);
                        }
                        Ok(Accumulator::Extremum(extrema))
                    } else {
                        Ok(Accumulator::AppendExtremum(value.clone()))
                    }
                }
                _ => unreachable!("validated aggregate function"),
            })
            .collect::<Result<Vec<_>>>()?;
        Ok(Self {
            row_count: 1,
            accumulators,
        })
    }

    /// Builds the accumulator delta produced when an incremental aggregate replaces one partial
    /// accumulator value. Unlike ordinary row accumulation, COUNT consumes the numeric partial
    /// count rather than counting the partial row itself.
    #[cfg(test)]
    pub(in crate::planner::operators) fn from_partial_values(
        calls: &[Call],
        values: &[Option<AggregateValue>],
        row_count: i64,
        accumulate: bool,
    ) -> Result<Self> {
        if calls.iter().any(|call| call.distinct) {
            return Err(DataFusionError::Internal(
                "incremental final aggregate cannot merge a DISTINCT call".to_string(),
            ));
        }
        if calls.len() != values.len() {
            return Err(DataFusionError::Internal(
                "incremental partial value count does not match its calls".to_string(),
            ));
        }
        let accumulators = calls
            .iter()
            .zip(values)
            .map(|(call, value)| partial_value_accumulator(call, value.as_ref(), accumulate))
            .collect::<Result<Vec<_>>>()?;
        Ok(Self {
            row_count: if accumulate {
                row_count
            } else {
                row_count.wrapping_neg()
            },
            accumulators,
        })
    }

    /// Builds a final-stage delta directly from partial accumulators. This avoids allocating the
    /// three temporary value vectors that a split-DISTINCT bundle would otherwise create for
    /// every partial key.
    pub(in crate::planner::operators) fn from_mapped_partial(
        calls: &[Call],
        partial_calls: &[Call],
        persistent: &Self,
        bundle: &Self,
        value_indices: &[u32],
        persistent_only: bool,
        row_count: i64,
        accumulate: bool,
    ) -> Result<Self> {
        if calls.len() != value_indices.len()
            || partial_calls.len() != persistent.accumulators.len()
            || partial_calls.len() != bundle.accumulators.len()
        {
            return Err(DataFusionError::Internal(
                "incremental partial mapping does not match its accumulator plan".to_string(),
            ));
        }
        let accumulators = calls
            .iter()
            .zip(value_indices)
            .map(|(call, &index)| {
                let value = if index == u32::MAX {
                    None
                } else {
                    let index = index as usize;
                    let partial_call = partial_calls.get(index).ok_or_else(|| {
                        DataFusionError::Internal(
                            "incremental partial mapping index is out of bounds".to_string(),
                        )
                    })?;
                    if persistent_only && !incremental_call_requires_persistence(partial_call) {
                        None
                    } else {
                        let source = if incremental_call_requires_persistence(partial_call) {
                            persistent
                        } else {
                            bundle
                        };
                        Some(accumulator_value(partial_call, &source.accumulators[index]))
                    }
                    .flatten()
                };
                partial_value_accumulator(call, value.as_ref(), accumulate)
            })
            .collect::<Result<Vec<_>>>()?;
        Ok(Self {
            row_count: if accumulate {
                row_count
            } else {
                row_count.wrapping_neg()
            },
            accumulators,
        })
    }

    pub(in crate::planner::operators) fn apply(
        &mut self,
        calls: &[Call],
        batch: &RecordBatch,
        row: usize,
        accumulate: bool,
    ) -> Result<()> {
        let values = row_aggregate_values(calls, batch, row)?;
        let active = calls
            .iter()
            .map(|call| aggregate_filter(call, batch, row))
            .collect::<Result<Vec<_>>>()?;
        self.apply_values_with_activity(calls, &values, &active, accumulate)
    }

    pub(in crate::planner::operators) fn apply_values(
        &mut self,
        calls: &[Call],
        values: &[Option<AggregateValue>],
        accumulate: bool,
    ) -> Result<()> {
        let active = vec![true; calls.len()];
        self.apply_values_with_activity(calls, values, &active, accumulate)
    }

    fn apply_values_with_activity(
        &mut self,
        calls: &[Call],
        values: &[Option<AggregateValue>],
        active: &[bool],
        accumulate: bool,
    ) -> Result<()> {
        if values.len() != calls.len() {
            return Err(DataFusionError::Internal(format!(
                "aggregate contribution has {} values for {} calls",
                values.len(),
                calls.len()
            )));
        }
        let delta = if accumulate { 1 } else { -1 };
        self.row_count = self.row_count.wrapping_add(delta);
        for (((call, accumulator), value), active) in calls
            .iter()
            .zip(&mut self.accumulators)
            .zip(values)
            .zip(active)
        {
            if !active {
                continue;
            }
            match accumulator {
                Accumulator::Count(count) => {
                    let present = call.input_index.is_none() || value.is_some();
                    if present {
                        *count = count.wrapping_add(delta);
                    }
                }
                Accumulator::DistinctCount { count, values } => {
                    if let Some(value) = value.as_ref() {
                        let previous = values.get(value).copied().unwrap_or_default();
                        let current = previous.wrapping_add(delta);
                        if current == 0 {
                            values.remove(value);
                        } else {
                            values.insert(value.clone(), current);
                        }
                        if (accumulate && previous == 0) || (!accumulate && current == 0) {
                            *count = count.wrapping_add(delta);
                        }
                    }
                }
                Accumulator::Sum { value: sum, count } => {
                    if let Some(value) = value.as_ref() {
                        *sum = if let Some(current) = sum.as_ref() {
                            if accumulate {
                                aggregate_add(current, value, &call.output_type)?
                            } else {
                                aggregate_sub(current, value, &call.output_type)?
                            }
                        } else if accumulate {
                            Some(value.clone())
                        } else {
                            aggregate_sub(&zero_value(&call.output_type), value, &call.output_type)?
                        };
                        *count = count.wrapping_add(delta);
                    }
                }
                Accumulator::DistinctSum {
                    value: sum,
                    count,
                    values,
                } => {
                    if let Some(value) = value.as_ref() {
                        let previous = values.get(value).copied().unwrap_or_default();
                        let current = previous.wrapping_add(delta);
                        if current == 0 {
                            values.remove(value);
                        } else {
                            values.insert(value.clone(), current);
                        }
                        if (accumulate && previous == 0) || (!accumulate && current == 0) {
                            *sum = if let Some(current_sum) = sum.as_ref() {
                                if accumulate {
                                    aggregate_add(current_sum, value, &call.output_type)?
                                } else {
                                    aggregate_sub(current_sum, value, &call.output_type)?
                                }
                            } else if accumulate {
                                Some(value.clone())
                            } else {
                                aggregate_sub(
                                    &zero_value(&call.output_type),
                                    value,
                                    &call.output_type,
                                )?
                            };
                            *count = count.wrapping_add(delta);
                        }
                    }
                }
                Accumulator::Average { value: sum, count } => {
                    if let Some(value) = value.as_ref() {
                        let contribution = average_contribution(value, call)?;
                        *sum = if let Some(current) = sum.as_ref() {
                            if accumulate {
                                aggregate_add(
                                    current,
                                    &contribution,
                                    &call.average_accumulator_type(),
                                )?
                            } else {
                                aggregate_sub(
                                    current,
                                    &contribution,
                                    &call.average_accumulator_type(),
                                )?
                            }
                        } else {
                            None
                        };
                        *count = count.wrapping_add(delta);
                    }
                }
                Accumulator::DistinctAverage {
                    value: sum,
                    count,
                    values,
                } => {
                    if let Some(value) = value.as_ref() {
                        let previous = values.get(value).copied().unwrap_or_default();
                        let current = previous.wrapping_add(delta);
                        if current == 0 {
                            values.remove(value);
                        } else {
                            values.insert(value.clone(), current);
                        }
                        if (accumulate && previous == 0) || (!accumulate && current == 0) {
                            let contribution = average_contribution(value, call)?;
                            *sum = if let Some(current_sum) = sum.as_ref() {
                                if accumulate {
                                    aggregate_add(
                                        current_sum,
                                        &contribution,
                                        &call.average_accumulator_type(),
                                    )?
                                } else {
                                    aggregate_sub(
                                        current_sum,
                                        &contribution,
                                        &call.average_accumulator_type(),
                                    )?
                                }
                            } else {
                                None
                            };
                            *count = count.wrapping_add(delta);
                        }
                    }
                }
                Accumulator::AppendExtremum(extremum) => {
                    if let Some(value) = value.as_ref() {
                        *extremum = Some(match (extremum.as_ref(), call.function) {
                            (Some(current), proto::AggregateFunction::Min) => {
                                flink_append_extremum(current, value, true)
                            }
                            (Some(current), proto::AggregateFunction::Max) => {
                                flink_append_extremum(current, value, false)
                            }
                            (None, _) => value.clone(),
                            _ => unreachable!("validated extremum function"),
                        });
                    }
                }
                Accumulator::Extremum(values) => {
                    if let Some(value) = value.as_ref() {
                        let count = values.entry(value.clone()).or_default();
                        *count = count.wrapping_add(delta);
                        if *count == 0 {
                            values.remove(&value);
                        }
                    }
                }
            }
        }
        Ok(())
    }

    /// Merge another accumulator namespace into this one using the same arithmetic and
    /// extremum semantics as row-by-row accumulation. Session windows use this when Flink's
    /// merging-window set coalesces namespaces; replaying every historical input row here is
    /// both unnecessary and quadratic for long-lived sessions.
    pub(in crate::planner::operators) fn merge(
        &mut self,
        calls: &[Call],
        other: &Self,
    ) -> Result<()> {
        if self.accumulators.len() != calls.len() || other.accumulators.len() != calls.len() {
            return Err(DataFusionError::Internal(
                "aggregate accumulator shape does not match its calls".to_string(),
            ));
        }
        self.row_count = self.row_count.wrapping_add(other.row_count);
        for ((call, accumulator), other) in calls
            .iter()
            .zip(&mut self.accumulators)
            .zip(&other.accumulators)
        {
            match (accumulator, other) {
                (Accumulator::Count(value), Accumulator::Count(other)) => {
                    *value = value.wrapping_add(*other);
                }
                (
                    Accumulator::DistinctCount { count, values },
                    Accumulator::DistinctCount {
                        values: other_values,
                        ..
                    },
                ) => {
                    for (value, delta) in other_values {
                        let previous = values.get(value).copied().unwrap_or_default();
                        let current = previous.wrapping_add(*delta);
                        if current == 0 {
                            values.remove(value);
                        } else {
                            values.insert(value.clone(), current);
                        }
                        *count =
                            count.wrapping_add(i64::from(current > 0) - i64::from(previous > 0));
                    }
                }
                (
                    Accumulator::DistinctSum {
                        value,
                        count,
                        values,
                    },
                    Accumulator::DistinctSum {
                        values: other_values,
                        ..
                    },
                ) => {
                    for (other_value, delta) in other_values {
                        let previous = values.get(other_value).copied().unwrap_or_default();
                        let current = previous.wrapping_add(*delta);
                        if current == 0 {
                            values.remove(other_value);
                        } else {
                            values.insert(other_value.clone(), current);
                        }
                        match (previous > 0, current > 0) {
                            (false, true) => {
                                *value = match value.as_ref() {
                                    Some(sum) => {
                                        aggregate_add(sum, other_value, &call.output_type)?
                                    }
                                    None if *count == 0 => Some(other_value.clone()),
                                    None => None,
                                };
                                *count = count.wrapping_add(1);
                            }
                            (true, false) => {
                                *value = match value.as_ref() {
                                    Some(sum) => {
                                        aggregate_sub(sum, other_value, &call.output_type)?
                                    }
                                    None => None,
                                };
                                *count = count.wrapping_sub(1);
                            }
                            _ => {}
                        }
                    }
                }
                (
                    Accumulator::DistinctAverage {
                        value,
                        count,
                        values,
                    },
                    Accumulator::DistinctAverage {
                        values: other_values,
                        ..
                    },
                ) => {
                    for (other_value, delta) in other_values {
                        let previous = values.get(other_value).copied().unwrap_or_default();
                        let current = previous.wrapping_add(*delta);
                        if current == 0 {
                            values.remove(other_value);
                        } else {
                            values.insert(other_value.clone(), current);
                        }
                        let contribution = average_contribution(other_value, call)?;
                        match (previous > 0, current > 0) {
                            (false, true) => {
                                *value = if let Some(sum) = value.as_ref() {
                                    aggregate_add(
                                        sum,
                                        &contribution,
                                        &call.average_accumulator_type(),
                                    )?
                                } else {
                                    None
                                };
                                *count = count.wrapping_add(1);
                            }
                            (true, false) => {
                                *value = if let Some(sum) = value.as_ref() {
                                    aggregate_sub(
                                        sum,
                                        &contribution,
                                        &call.average_accumulator_type(),
                                    )?
                                } else {
                                    None
                                };
                                *count = count.wrapping_sub(1);
                            }
                            _ => {}
                        }
                    }
                }
                (
                    Accumulator::Sum { value, count },
                    Accumulator::Sum {
                        value: other_value,
                        count: other_count,
                    },
                ) => {
                    // Incremental aggregation can replace one partial value with another. The
                    // cardinality delta is then zero while the SUM delta is non-zero.
                    if other_value.is_some() || *other_count != 0 {
                        *value = match (value.as_ref(), other_value.as_ref()) {
                            (Some(left), Some(right)) => {
                                aggregate_add(left, right, &call.output_type)?
                            }
                            (None, Some(right)) if *count == 0 => Some(right.clone()),
                            // A populated SUM with no value represents an overflowed decimal.
                            // Preserve that state rather than incorrectly resurrecting a value.
                            _ => None,
                        };
                        *count = count.wrapping_add(*other_count);
                    }
                }
                (
                    Accumulator::Average { value, count },
                    Accumulator::Average {
                        value: other_value,
                        count: other_count,
                    },
                ) => {
                    // As with SUM, a partial replacement can change the AVG numerator without
                    // changing its contribution count.
                    if other_value.is_some() || *other_count != 0 {
                        *value = match (value.as_ref(), other_value.as_ref()) {
                            (Some(left), Some(right)) => {
                                aggregate_add(left, right, &call.average_accumulator_type())?
                            }
                            _ => None,
                        };
                        *count = count.wrapping_add(*other_count);
                    }
                }
                (Accumulator::AppendExtremum(value), Accumulator::AppendExtremum(other_value)) => {
                    if let Some(other_value) = other_value {
                        *value = Some(match (value.as_ref(), call.function) {
                            (Some(current), proto::AggregateFunction::Min) => {
                                flink_append_extremum(current, other_value, true)
                            }
                            (Some(current), proto::AggregateFunction::Max) => {
                                flink_append_extremum(current, other_value, false)
                            }
                            (None, _) => other_value.clone(),
                            _ => unreachable!("validated extremum function"),
                        });
                    }
                }
                (Accumulator::Extremum(values), Accumulator::Extremum(other_values)) => {
                    for (value, other_count) in other_values {
                        let count = values.entry(value.clone()).or_default();
                        *count = count.wrapping_add(*other_count);
                        if *count == 0 {
                            values.remove(value);
                        }
                    }
                }
                _ => {
                    return Err(DataFusionError::Internal(
                        "aggregate accumulator variants do not match their calls".to_string(),
                    ));
                }
            }
        }
        Ok(())
    }

    pub(in crate::planner::operators) fn values(
        &self,
        calls: &[Call],
    ) -> Vec<Option<AggregateValue>> {
        calls
            .iter()
            .zip(&self.accumulators)
            .map(|(call, accumulator)| accumulator_value(call, accumulator))
            .collect()
    }

    /// Heap payload cloned into one visible result, not the retained B-tree history. Keep
    /// positive-count selection identical to accumulator_value (incremental state can be sparse).
    pub(super) fn visible_payload_bytes(&self, calls: &[Call]) -> usize {
        calls
            .iter()
            .zip(&self.accumulators)
            .fold(0usize, |bytes, (call, accumulator)| {
                let value = match accumulator {
                    Accumulator::Count(_) | Accumulator::DistinctCount { .. } => None,
                    Accumulator::Sum { value, .. }
                    | Accumulator::DistinctSum { value, .. }
                    | Accumulator::Average { value, .. }
                    | Accumulator::DistinctAverage { value, .. }
                    | Accumulator::AppendExtremum(value) => value.as_ref(),
                    Accumulator::Extremum(values) => {
                        if call.function == proto::AggregateFunction::Max {
                            values
                                .iter()
                                .rev()
                                .find_map(|(value, count)| (*count > 0).then_some(value))
                        } else {
                            values
                                .iter()
                                .find_map(|(value, count)| (*count > 0).then_some(value))
                        }
                    }
                };
                bytes.saturating_add(value.map_or(0, AggregateValue::dynamic_bytes))
            })
    }
}

impl AggregateValue {
    fn is_zero(&self) -> bool {
        match self {
            Self::Boolean(value) => !value,
            Self::Int(value) => *value == 0,
            Self::Float32(value) => f32::from_bits(*value) == 0.0,
            Self::Float64(value) => f64::from_bits(*value) == 0.0,
            Self::Bytes(value) => value.iter().all(|byte| *byte == 0),
        }
    }
}

pub(in crate::planner::operators) fn incremental_call_requires_persistence(call: &Call) -> bool {
    call.distinct
        || (call.retractable
            && matches!(
                call.function,
                proto::AggregateFunction::Min | proto::AggregateFunction::Max
            ))
}

pub(in crate::planner::operators) fn partial_value_accumulator(
    call: &Call,
    value: Option<&AggregateValue>,
    accumulate: bool,
) -> Result<Accumulator> {
    let direction = if accumulate { 1 } else { -1 };
    match call.function {
        proto::AggregateFunction::CountStar | proto::AggregateFunction::Count => {
            let value = match value {
                Some(AggregateValue::Int(value)) => i64::try_from(*value).map_err(|_| {
                    DataFusionError::Execution(
                        "incremental COUNT partial does not fit i64".to_string(),
                    )
                })?,
                None => 0,
                Some(_) => {
                    return Err(DataFusionError::Internal(
                        "incremental COUNT partial has a non-integer value".to_string(),
                    ));
                }
            };
            Ok(Accumulator::Count(value.wrapping_mul(direction)))
        }
        proto::AggregateFunction::Sum | proto::AggregateFunction::Sum0 => {
            let value = match (value, accumulate) {
                (Some(value), true) => Some(value.clone()),
                (Some(value), false) => {
                    aggregate_sub(&zero_value(&call.output_type), value, &call.output_type)?
                }
                (None, _) => None,
            };
            let count = i64::from(value.is_some()).wrapping_mul(direction);
            Ok(Accumulator::Sum { value, count })
        }
        proto::AggregateFunction::Avg if accumulate => Ok(Accumulator::Average {
            value: value.cloned(),
            count: i64::from(value.is_some()),
        }),
        proto::AggregateFunction::Avg => Err(DataFusionError::Internal(
            "incremental final AVG accumulator cannot be subtracted".to_string(),
        )),
        proto::AggregateFunction::Min | proto::AggregateFunction::Max if call.retractable => {
            let mut values = BTreeMap::new();
            if let Some(value) = value.cloned() {
                values.insert(value, direction);
            }
            Ok(Accumulator::Extremum(values))
        }
        proto::AggregateFunction::Min | proto::AggregateFunction::Max if accumulate => {
            Ok(Accumulator::AppendExtremum(value.cloned()))
        }
        proto::AggregateFunction::Min | proto::AggregateFunction::Max => {
            if value.is_none() {
                Ok(Accumulator::AppendExtremum(None))
            } else {
                Err(DataFusionError::Internal(
                    "incremental final extremum replacement requires a retractable accumulator"
                        .to_string(),
                ))
            }
        }
        proto::AggregateFunction::Unspecified => unreachable!("validated aggregate function"),
    }
}

pub(in crate::planner::operators) fn accumulator_value(
    call: &Call,
    accumulator: &Accumulator,
) -> Option<AggregateValue> {
    match accumulator {
        Accumulator::Count(value) => Some(AggregateValue::Int(*value as i128)),
        Accumulator::DistinctCount { count, .. } => Some(AggregateValue::Int(*count as i128)),
        Accumulator::Sum { value, count } => {
            if *count == 0 {
                (call.function == proto::AggregateFunction::Sum0)
                    .then(|| zero_value(&call.output_type))
            } else {
                value.clone()
            }
        }
        Accumulator::DistinctSum { value, count, .. } => {
            if *count == 0 {
                None
            } else {
                value.clone()
            }
        }
        Accumulator::Average { value, count }
        | Accumulator::DistinctAverage { value, count, .. } => {
            average_value(value.as_ref(), *count, call)
        }
        Accumulator::AppendExtremum(value) => value.clone(),
        Accumulator::Extremum(values) => match call.function {
            proto::AggregateFunction::Min => values
                .iter()
                .find_map(|(value, count)| (*count > 0).then(|| value.clone())),
            proto::AggregateFunction::Max => values
                .iter()
                .rev()
                .find_map(|(value, count)| (*count > 0).then(|| value.clone())),
            _ => unreachable!("validated extremum function"),
        },
    }
}

pub(in crate::planner::operators) fn estimated_value_map_bytes(
    values: &BTreeMap<AggregateValue, i64>,
) -> usize {
    values
        .iter()
        .fold(COUNTED_MAP_BASE_BYTES, |bytes, (value, _)| {
            bytes
                .saturating_add(counted_map_entry_bytes())
                .saturating_add(value.dynamic_bytes())
        })
}

// Even one entry allocates a whole node. Per-entry occupancy slack alone undercounts
// sparse maps. Removing the last entry can retain that node, and BTreeMap has no public
// capacity API; keep the base even for empty maps. Measured-allocation tests cover first
// insertion, splits, dense trees, and shrink-to-empty retractions.
pub(super) const COUNTED_MAP_BASE_BYTES: usize = 1024;
pub(super) fn counted_map_entry_bytes() -> usize {
    std::mem::size_of::<(AggregateValue, i64)>() + 64
}
