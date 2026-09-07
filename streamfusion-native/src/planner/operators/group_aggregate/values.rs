// Copyright 2026 StreamFusion Authors
// Licensed under the Apache License, Version 2.0

use super::*;

impl AggregateValue {
    pub(super) fn dynamic_bytes(&self) -> usize {
        match self {
            Self::Bytes(value) => value.capacity(),
            _ => 0,
        }
    }
}

pub(in crate::planner::operators) fn row_aggregate_values(
    calls: &[Call],
    batch: &RecordBatch,
    row: usize,
) -> Result<Vec<Option<AggregateValue>>> {
    calls
        .iter()
        .map(|call| match call.input_index {
            Some(index) if call.function == proto::AggregateFunction::Count && !call.distinct => {
                Ok((!batch.column(index).is_null(row)).then_some(AggregateValue::Boolean(true)))
            }
            Some(index) => aggregate_value(batch.column(index).as_ref(), row),
            None => Ok(None),
        })
        .collect()
}

pub(in crate::planner::operators) fn aggregate_filter(
    call: &Call,
    batch: &RecordBatch,
    row: usize,
) -> Result<bool> {
    let Some(index) = call.filter_index else {
        return Ok(true);
    };
    let filter = batch
        .column(index)
        .as_any()
        .downcast_ref::<BooleanArray>()
        .ok_or_else(|| {
            DataFusionError::Execution(format!(
                "aggregate FILTER input {index} is not Arrow Boolean"
            ))
        })?;
    Ok(!filter.is_null(row) && filter.value(row))
}

pub(in crate::planner::operators) fn aggregate_value(
    array: &dyn Array,
    row: usize,
) -> Result<Option<AggregateValue>> {
    if array.is_null(row) {
        return Ok(None);
    }
    let value = match array.data_type() {
        DataType::Int8 => array
            .as_any()
            .downcast_ref::<Int8Array>()
            .unwrap()
            .value(row) as i128,
        DataType::Int16 => array
            .as_any()
            .downcast_ref::<Int16Array>()
            .unwrap()
            .value(row) as i128,
        DataType::Int32 => array
            .as_any()
            .downcast_ref::<Int32Array>()
            .unwrap()
            .value(row) as i128,
        DataType::Int64 => array
            .as_any()
            .downcast_ref::<Int64Array>()
            .unwrap()
            .value(row) as i128,
        DataType::Decimal128(_, _) => array
            .as_any()
            .downcast_ref::<Decimal128Array>()
            .unwrap()
            .value(row),
        DataType::Float32 => {
            return Ok(Some(float32_value(
                array
                    .as_any()
                    .downcast_ref::<Float32Array>()
                    .unwrap()
                    .value(row),
            )))
        }
        DataType::Float64 => {
            return Ok(Some(float64_value(
                array
                    .as_any()
                    .downcast_ref::<Float64Array>()
                    .unwrap()
                    .value(row),
            )))
        }
        DataType::Boolean => {
            return Ok(Some(AggregateValue::Boolean(
                array
                    .as_any()
                    .downcast_ref::<BooleanArray>()
                    .unwrap()
                    .value(row),
            )))
        }
        DataType::Utf8 => {
            return Ok(Some(AggregateValue::Bytes(
                array
                    .as_any()
                    .downcast_ref::<StringArray>()
                    .unwrap()
                    .value(row)
                    .as_bytes()
                    .to_vec(),
            )))
        }
        DataType::Date32 => array
            .as_any()
            .downcast_ref::<Date32Array>()
            .unwrap()
            .value(row) as i128,
        DataType::Time32(TimeUnit::Second) => array
            .as_any()
            .downcast_ref::<Time32SecondArray>()
            .unwrap()
            .value(row) as i128,
        DataType::Time32(TimeUnit::Millisecond) => array
            .as_any()
            .downcast_ref::<Time32MillisecondArray>()
            .unwrap()
            .value(row) as i128,
        DataType::Time64(TimeUnit::Microsecond) => array
            .as_any()
            .downcast_ref::<Time64MicrosecondArray>()
            .unwrap()
            .value(row) as i128,
        DataType::Time64(TimeUnit::Nanosecond) => array
            .as_any()
            .downcast_ref::<Time64NanosecondArray>()
            .unwrap()
            .value(row) as i128,
        DataType::Timestamp(TimeUnit::Second, _) => array
            .as_any()
            .downcast_ref::<TimestampSecondArray>()
            .unwrap()
            .value(row) as i128,
        DataType::Timestamp(TimeUnit::Millisecond, _) => array
            .as_any()
            .downcast_ref::<TimestampMillisecondArray>()
            .unwrap()
            .value(row) as i128,
        DataType::Timestamp(TimeUnit::Microsecond, _) => array
            .as_any()
            .downcast_ref::<TimestampMicrosecondArray>()
            .unwrap()
            .value(row) as i128,
        DataType::Timestamp(TimeUnit::Nanosecond, _) => array
            .as_any()
            .downcast_ref::<TimestampNanosecondArray>()
            .unwrap()
            .value(row) as i128,
        other => {
            return Err(DataFusionError::Execution(format!(
                "group aggregate cannot read input type {other}"
            )));
        }
    };
    Ok(Some(AggregateValue::Int(value)))
}

pub(in crate::planner::operators) fn aggregate_array(
    values: &[Option<AggregateValue>],
    data_type: &DataType,
) -> Result<ArrayRef> {
    Ok(match data_type {
        DataType::Int8 => Arc::new(Int8Array::from_iter(
            values
                .iter()
                .map(|value| value.as_ref().map(int_value).transpose())
                .collect::<Result<Vec<_>>>()?
                .into_iter()
                .map(|value| value.map(|value| value as i8)),
        )),
        DataType::Int16 => Arc::new(Int16Array::from_iter(
            values
                .iter()
                .map(|value| value.as_ref().map(int_value).transpose())
                .collect::<Result<Vec<_>>>()?
                .into_iter()
                .map(|value| value.map(|value| value as i16)),
        )),
        DataType::Int32 => Arc::new(Int32Array::from_iter(
            values
                .iter()
                .map(|value| value.as_ref().map(int_value).transpose())
                .collect::<Result<Vec<_>>>()?
                .into_iter()
                .map(|value| value.map(|value| value as i32)),
        )),
        DataType::Int64 => Arc::new(Int64Array::from_iter(
            values
                .iter()
                .map(|value| value.as_ref().map(int_value).transpose())
                .collect::<Result<Vec<_>>>()?
                .into_iter()
                .map(|value| value.map(|value| value as i64)),
        )),
        DataType::Decimal128(precision, scale) => Arc::new(
            Decimal128Array::from_iter(
                values
                    .iter()
                    .map(|value| value.as_ref().map(int_value).transpose())
                    .collect::<Result<Vec<_>>>()?,
            )
            .with_precision_and_scale(*precision, *scale)?,
        ),
        DataType::Float32 => Arc::new(Float32Array::from_iter(float32_options(values)?)),
        DataType::Float64 => Arc::new(Float64Array::from_iter(float64_options(values)?)),
        DataType::Boolean => Arc::new(BooleanArray::from_iter(boolean_options(values)?)),
        DataType::Utf8 => {
            let strings = values
                .iter()
                .map(|value| match value {
                    None => Ok(None),
                    Some(AggregateValue::Bytes(value)) => std::str::from_utf8(value)
                        .map(Some)
                        .map_err(|error| DataFusionError::External(Box::new(error))),
                    Some(other) => Err(value_type_error("Utf8", other)),
                })
                .collect::<Result<Vec<_>>>()?;
            Arc::new(StringArray::from(strings))
        }
        DataType::Date32 => Arc::new(Date32Array::from_iter(int32_options(values)?)),
        DataType::Time32(TimeUnit::Second) => {
            Arc::new(Time32SecondArray::from_iter(int32_options(values)?))
        }
        DataType::Time32(TimeUnit::Millisecond) => {
            Arc::new(Time32MillisecondArray::from_iter(int32_options(values)?))
        }
        DataType::Time64(TimeUnit::Microsecond) => {
            Arc::new(Time64MicrosecondArray::from_iter(int64_options(values)?))
        }
        DataType::Time64(TimeUnit::Nanosecond) => {
            Arc::new(Time64NanosecondArray::from_iter(int64_options(values)?))
        }
        DataType::Timestamp(TimeUnit::Second, _) => {
            Arc::new(TimestampSecondArray::from_iter(int64_options(values)?))
        }
        DataType::Timestamp(TimeUnit::Millisecond, _) => {
            Arc::new(TimestampMillisecondArray::from_iter(int64_options(values)?))
        }
        DataType::Timestamp(TimeUnit::Microsecond, _) => {
            Arc::new(TimestampMicrosecondArray::from_iter(int64_options(values)?))
        }
        DataType::Timestamp(TimeUnit::Nanosecond, _) => {
            Arc::new(TimestampNanosecondArray::from_iter(int64_options(values)?))
        }
        other => {
            return Err(DataFusionError::Execution(format!(
                "group aggregate cannot emit type {other}"
            )));
        }
    })
}

pub(in crate::planner::operators) fn zero_value(data_type: &DataType) -> AggregateValue {
    match data_type {
        DataType::Float32 => float32_value(0.0),
        DataType::Float64 => float64_value(0.0),
        _ => AggregateValue::Int(0),
    }
}

pub(in crate::planner::operators) fn average_contribution(
    value: &AggregateValue,
    call: &Call,
) -> Result<AggregateValue> {
    let accumulator_type = call.average_accumulator_type();
    match (value, &call.input_type, &accumulator_type) {
        (AggregateValue::Int(value), Some(input), DataType::Int64)
            if matches!(
                input,
                DataType::Int8 | DataType::Int16 | DataType::Int32 | DataType::Int64
            ) =>
        {
            Ok(AggregateValue::Int(*value))
        }
        (AggregateValue::Float32(value), Some(DataType::Float32), DataType::Float64) => {
            Ok(float64_value(f32::from_bits(*value) as f64))
        }
        (AggregateValue::Float64(value), Some(DataType::Float64), DataType::Float64) => {
            Ok(AggregateValue::Float64(*value))
        }
        (
            AggregateValue::Int(value),
            Some(DataType::Decimal128(_, input_scale)),
            DataType::Decimal128(38, sum_scale),
        ) if input_scale == sum_scale => Ok(AggregateValue::Int(*value)),
        _ => Err(DataFusionError::Internal(format!(
            "AVG cannot convert {value:?} from {:?} into {}",
            call.input_type, accumulator_type
        ))),
    }
}

pub(in crate::planner::operators) fn average_value(
    value: Option<&AggregateValue>,
    count: i64,
    call: &Call,
) -> Option<AggregateValue> {
    if count == 0 {
        return None;
    }
    let accumulator_type = call.average_accumulator_type();
    match (value?, &accumulator_type, &call.output_type) {
        (AggregateValue::Int(sum), DataType::Int64, DataType::Int8) => Some(AggregateValue::Int(
            (*sum as i64).wrapping_div(count) as i8 as i128,
        )),
        (AggregateValue::Int(sum), DataType::Int64, DataType::Int16) => Some(AggregateValue::Int(
            (*sum as i64).wrapping_div(count) as i16 as i128,
        )),
        (AggregateValue::Int(sum), DataType::Int64, DataType::Int32) => Some(AggregateValue::Int(
            (*sum as i64).wrapping_div(count) as i32 as i128,
        )),
        (AggregateValue::Int(sum), DataType::Int64, DataType::Int64) => Some(AggregateValue::Int(
            (*sum as i64).wrapping_div(count) as i128,
        )),
        (AggregateValue::Float64(sum), DataType::Float64, DataType::Float32) => {
            Some(float32_value((f64::from_bits(*sum) / count as f64) as f32))
        }
        (AggregateValue::Float64(sum), DataType::Float64, DataType::Float64) => {
            Some(float64_value(f64::from_bits(*sum) / count as f64))
        }
        (
            AggregateValue::Int(sum),
            DataType::Decimal128(_, sum_scale),
            DataType::Decimal128(precision, output_scale),
        ) => crate::planner::expressions::decimal::flink_divide_nonzero(
            *sum,
            *sum_scale,
            count as i128,
            0,
            *precision,
            *output_scale,
        )
        .map(AggregateValue::Int),
        _ => None,
    }
}

pub(in crate::planner::operators) fn aggregate_add(
    left: &AggregateValue,
    right: &AggregateValue,
    data_type: &DataType,
) -> Result<Option<AggregateValue>> {
    Ok(Some(match (left, right, data_type) {
        (AggregateValue::Int(left), AggregateValue::Int(right), DataType::Int8) => {
            AggregateValue::Int((*left as i8).wrapping_add(*right as i8) as i128)
        }
        (AggregateValue::Int(left), AggregateValue::Int(right), DataType::Int16) => {
            AggregateValue::Int((*left as i16).wrapping_add(*right as i16) as i128)
        }
        (AggregateValue::Int(left), AggregateValue::Int(right), DataType::Int32) => {
            AggregateValue::Int((*left as i32).wrapping_add(*right as i32) as i128)
        }
        (AggregateValue::Int(left), AggregateValue::Int(right), DataType::Int64) => {
            AggregateValue::Int((*left as i64).wrapping_add(*right as i64) as i128)
        }
        (AggregateValue::Int(left), AggregateValue::Int(right), DataType::Decimal128(_, _)) => {
            return Ok(decimal_operation(
                *left,
                *right,
                data_type,
                i128::checked_add,
            ));
        }
        (AggregateValue::Float32(left), AggregateValue::Float32(right), DataType::Float32) => {
            float32_value(f32::from_bits(*left) + f32::from_bits(*right))
        }
        (AggregateValue::Float64(left), AggregateValue::Float64(right), DataType::Float64) => {
            float64_value(f64::from_bits(*left) + f64::from_bits(*right))
        }
        _ => return Err(value_operation_error("add", left, right, data_type)),
    }))
}

pub(in crate::planner::operators) fn aggregate_sub(
    left: &AggregateValue,
    right: &AggregateValue,
    data_type: &DataType,
) -> Result<Option<AggregateValue>> {
    Ok(Some(match (left, right, data_type) {
        (AggregateValue::Int(left), AggregateValue::Int(right), DataType::Int8) => {
            AggregateValue::Int((*left as i8).wrapping_sub(*right as i8) as i128)
        }
        (AggregateValue::Int(left), AggregateValue::Int(right), DataType::Int16) => {
            AggregateValue::Int((*left as i16).wrapping_sub(*right as i16) as i128)
        }
        (AggregateValue::Int(left), AggregateValue::Int(right), DataType::Int32) => {
            AggregateValue::Int((*left as i32).wrapping_sub(*right as i32) as i128)
        }
        (AggregateValue::Int(left), AggregateValue::Int(right), DataType::Int64) => {
            AggregateValue::Int((*left as i64).wrapping_sub(*right as i64) as i128)
        }
        (AggregateValue::Int(left), AggregateValue::Int(right), DataType::Decimal128(_, _)) => {
            return Ok(decimal_operation(
                *left,
                *right,
                data_type,
                i128::checked_sub,
            ));
        }
        (AggregateValue::Float32(left), AggregateValue::Float32(right), DataType::Float32) => {
            float32_value(f32::from_bits(*left) - f32::from_bits(*right))
        }
        (AggregateValue::Float64(left), AggregateValue::Float64(right), DataType::Float64) => {
            float64_value(f64::from_bits(*left) - f64::from_bits(*right))
        }
        _ => return Err(value_operation_error("subtract", left, right, data_type)),
    }))
}

pub(in crate::planner::operators) fn decimal_operation(
    left: i128,
    right: i128,
    data_type: &DataType,
    operation: fn(i128, i128) -> Option<i128>,
) -> Option<AggregateValue> {
    let DataType::Decimal128(precision, _) = data_type else {
        unreachable!("decimal operation has a decimal output type")
    };
    let maximum = 10_i128.pow(*precision as u32) - 1;
    operation(left, right)
        .filter(|value| *value >= -maximum && *value <= maximum)
        .map(AggregateValue::Int)
}

pub(in crate::planner::operators) fn flink_append_extremum(
    current: &AggregateValue,
    candidate: &AggregateValue,
    minimum: bool,
) -> AggregateValue {
    let replace = match (current, candidate) {
        (AggregateValue::Float32(current), AggregateValue::Float32(candidate)) => {
            let current = f32::from_bits(*current);
            let candidate = f32::from_bits(*candidate);
            if minimum {
                candidate < current
            } else {
                candidate > current
            }
        }
        (AggregateValue::Float64(current), AggregateValue::Float64(candidate)) => {
            let current = f64::from_bits(*current);
            let candidate = f64::from_bits(*candidate);
            if minimum {
                candidate < current
            } else {
                candidate > current
            }
        }
        _ => {
            if minimum {
                candidate < current
            } else {
                candidate > current
            }
        }
    };
    if replace {
        candidate.clone()
    } else {
        current.clone()
    }
}

pub(in crate::planner::operators) fn float32_value(value: f32) -> AggregateValue {
    AggregateValue::Float32(if value.is_nan() {
        f32::NAN.to_bits()
    } else {
        value.to_bits()
    })
}

pub(in crate::planner::operators) fn float64_value(value: f64) -> AggregateValue {
    AggregateValue::Float64(if value.is_nan() {
        f64::NAN.to_bits()
    } else {
        value.to_bits()
    })
}

pub(in crate::planner::operators) fn flink_f32_cmp(left: u32, right: u32) -> Ordering {
    let left_value = f32::from_bits(left);
    let right_value = f32::from_bits(right);
    if left_value < right_value {
        Ordering::Less
    } else if left_value > right_value {
        Ordering::Greater
    } else {
        (left as i32).cmp(&(right as i32))
    }
}

pub(in crate::planner::operators) fn flink_f64_cmp(left: u64, right: u64) -> Ordering {
    let left_value = f64::from_bits(left);
    let right_value = f64::from_bits(right);
    if left_value < right_value {
        Ordering::Less
    } else if left_value > right_value {
        Ordering::Greater
    } else {
        (left as i64).cmp(&(right as i64))
    }
}

pub(in crate::planner::operators) fn value_tag(value: &AggregateValue) -> u8 {
    match value {
        AggregateValue::Boolean(_) => 1,
        AggregateValue::Int(_) => 2,
        AggregateValue::Float32(_) => 3,
        AggregateValue::Float64(_) => 4,
        AggregateValue::Bytes(_) => 5,
    }
}

pub(in crate::planner::operators) fn int_value(value: &AggregateValue) -> Result<i128> {
    match value {
        AggregateValue::Int(value) => Ok(*value),
        other => Err(value_type_error("integer", other)),
    }
}

pub(in crate::planner::operators) fn float32_options(
    values: &[Option<AggregateValue>],
) -> Result<Vec<Option<f32>>> {
    values
        .iter()
        .map(|value| match value {
            Some(AggregateValue::Float32(value)) => Ok(Some(f32::from_bits(*value))),
            None => Ok(None),
            Some(other) => Err(value_type_error("Float32", other)),
        })
        .collect()
}

pub(in crate::planner::operators) fn float64_options(
    values: &[Option<AggregateValue>],
) -> Result<Vec<Option<f64>>> {
    values
        .iter()
        .map(|value| match value {
            Some(AggregateValue::Float64(value)) => Ok(Some(f64::from_bits(*value))),
            None => Ok(None),
            Some(other) => Err(value_type_error("Float64", other)),
        })
        .collect()
}

pub(in crate::planner::operators) fn boolean_options(
    values: &[Option<AggregateValue>],
) -> Result<Vec<Option<bool>>> {
    values
        .iter()
        .map(|value| match value {
            Some(AggregateValue::Boolean(value)) => Ok(Some(*value)),
            None => Ok(None),
            Some(other) => Err(value_type_error("Boolean", other)),
        })
        .collect()
}

pub(in crate::planner::operators) fn int32_options(
    values: &[Option<AggregateValue>],
) -> Result<Vec<Option<i32>>> {
    values
        .iter()
        .map(|value| {
            value
                .as_ref()
                .map(int_value)
                .transpose()
                .map(|value| value.map(|value| value as i32))
        })
        .collect()
}

pub(in crate::planner::operators) fn int64_options(
    values: &[Option<AggregateValue>],
) -> Result<Vec<Option<i64>>> {
    values
        .iter()
        .map(|value| {
            value
                .as_ref()
                .map(int_value)
                .transpose()
                .map(|value| value.map(|value| value as i64))
        })
        .collect()
}

pub(in crate::planner::operators) fn value_type_error(
    expected: &str,
    actual: &AggregateValue,
) -> DataFusionError {
    DataFusionError::Internal(format!(
        "group aggregate expected {expected} accumulator value, got {actual:?}"
    ))
}

pub(in crate::planner::operators) fn value_operation_error(
    operation: &str,
    left: &AggregateValue,
    right: &AggregateValue,
    data_type: &DataType,
) -> DataFusionError {
    DataFusionError::Internal(format!(
        "group aggregate cannot {operation} {left:?} and {right:?} as {data_type}"
    ))
}
