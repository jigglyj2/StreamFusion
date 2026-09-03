// Copyright 2026 StreamFusion Authors
// Licensed under the Apache License, Version 2.0

use std::cmp::Ordering;

use arrow::array::{
    Array, BinaryArray, BooleanArray, Date32Array, Date64Array, Decimal128Array,
    FixedSizeBinaryArray, FixedSizeListArray, Float32Array, Float64Array, Int16Array, Int32Array,
    Int64Array, Int8Array, LargeBinaryArray, LargeListArray, LargeStringArray, ListArray, MapArray,
    StringArray, StructArray, Time32MillisecondArray, Time32SecondArray, Time64MicrosecondArray,
    Time64NanosecondArray, TimestampMicrosecondArray, TimestampMillisecondArray,
    TimestampNanosecondArray, TimestampSecondArray, UInt16Array, UInt32Array, UInt64Array,
    UInt8Array,
};
use arrow::datatypes::{DataType, TimeUnit};
use arrow::record_batch::RecordBatch;
use datafusion::error::{DataFusionError, Result};

pub(crate) fn compare_rows(
    left: &RecordBatch,
    left_row: usize,
    right: &RecordBatch,
    right_row: usize,
    indices: &[u32],
    ascending: &[bool],
    nulls_last: &[bool],
) -> Result<Ordering> {
    for ((&index, &ascending), &nulls_last) in indices.iter().zip(ascending).zip(nulls_last) {
        let index = index as usize;
        let ordering = compare_values(
            left.column(index).as_ref(),
            left_row,
            right.column(index).as_ref(),
            right_row,
            nulls_last,
        )?;
        if ordering != Ordering::Equal {
            return Ok(if ascending {
                ordering
            } else {
                ordering.reverse()
            });
        }
    }
    Ok(Ordering::Equal)
}

pub(crate) fn equal_rows(
    left: &RecordBatch,
    left_row: usize,
    right: &RecordBatch,
    right_row: usize,
    indices: impl IntoIterator<Item = usize>,
) -> Result<bool> {
    equal_rows_with_float_mode(
        left,
        left_row,
        right,
        right_row,
        indices,
        FloatEquality::Ieee,
    )
}

pub(crate) fn record_equaliser_rows(
    left: &RecordBatch,
    left_row: usize,
    right: &RecordBatch,
    right_row: usize,
    indices: impl IntoIterator<Item = usize>,
) -> Result<bool> {
    equal_rows_with_float_mode(
        left,
        left_row,
        right,
        right_row,
        indices,
        FloatEquality::CanonicalBits,
    )
}

fn equal_rows_with_float_mode(
    left: &RecordBatch,
    left_row: usize,
    right: &RecordBatch,
    right_row: usize,
    indices: impl IntoIterator<Item = usize>,
    float_equality: FloatEquality,
) -> Result<bool> {
    for index in indices {
        if !equal_values(
            left.column(index).as_ref(),
            left_row,
            right.column(index).as_ref(),
            right_row,
            float_equality,
        )? {
            return Ok(false);
        }
    }
    Ok(true)
}

#[derive(Clone, Copy)]
enum FloatEquality {
    Ieee,
    CanonicalBits,
}

pub(super) fn row_has_nan(batch: &RecordBatch, row: usize, indices: &[u32]) -> Result<bool> {
    for &index in indices {
        if value_has_nan(batch.column(index as usize).as_ref(), row)? {
            return Ok(true);
        }
    }
    Ok(false)
}

pub(super) fn data_type_can_have_nan(data_type: &DataType) -> bool {
    match data_type {
        DataType::Float32 | DataType::Float64 => true,
        DataType::List(field) | DataType::LargeList(field) | DataType::FixedSizeList(field, _) => {
            data_type_can_have_nan(field.data_type())
        }
        DataType::Struct(fields) => fields
            .iter()
            .any(|field| data_type_can_have_nan(field.data_type())),
        _ => false,
    }
}

fn value_has_nan(array: &dyn Array, row: usize) -> Result<bool> {
    if array.is_null(row) {
        return Ok(false);
    }
    Ok(match array.data_type() {
        DataType::Float32 => downcast::<Float32Array>(array)?.value(row).is_nan(),
        DataType::Float64 => downcast::<Float64Array>(array)?.value(row).is_nan(),
        DataType::List(_) => {
            let array = downcast::<ListArray>(array)?;
            range_has_nan(
                array.values().as_ref(),
                array.value_offsets()[row] as usize,
                array.value_length(row) as usize,
            )?
        }
        DataType::LargeList(_) => {
            let array = downcast::<LargeListArray>(array)?;
            range_has_nan(
                array.values().as_ref(),
                array.value_offsets()[row] as usize,
                array.value_length(row) as usize,
            )?
        }
        DataType::FixedSizeList(_, _) => {
            let array = downcast::<FixedSizeListArray>(array)?;
            range_has_nan(
                array.values().as_ref(),
                array.value_offset(row) as usize,
                array.value_length() as usize,
            )?
        }
        DataType::Struct(_) => {
            let array = downcast::<StructArray>(array)?;
            let mut found = false;
            for child in array.columns() {
                if value_has_nan(child.as_ref(), row)? {
                    found = true;
                    break;
                }
            }
            found
        }
        _ => false,
    })
}

fn range_has_nan(array: &dyn Array, start: usize, len: usize) -> Result<bool> {
    for row in start..start + len {
        if value_has_nan(array, row)? {
            return Ok(true);
        }
    }
    Ok(false)
}

fn compare_values(
    left: &dyn Array,
    left_row: usize,
    right: &dyn Array,
    right_row: usize,
    nulls_last: bool,
) -> Result<Ordering> {
    let left_null = left.is_null(left_row);
    let right_null = right.is_null(right_row);
    match (left_null, right_null) {
        (true, true) => return Ok(Ordering::Equal),
        (true, false) => {
            return Ok(if nulls_last {
                Ordering::Greater
            } else {
                Ordering::Less
            })
        }
        (false, true) => {
            return Ok(if nulls_last {
                Ordering::Less
            } else {
                Ordering::Greater
            })
        }
        (false, false) => {}
    }
    if left.data_type() != right.data_type() {
        return Err(DataFusionError::Execution(format!(
            "top-n compared different Arrow types {} and {}",
            left.data_type(),
            right.data_type()
        )));
    }
    macro_rules! primitive {
        ($array:ty) => {{
            let left = downcast::<$array>(left)?.value(left_row);
            let right = downcast::<$array>(right)?.value(right_row);
            left.cmp(&right)
        }};
    }
    macro_rules! partial {
        ($array:ty) => {{
            let left = downcast::<$array>(left)?.value(left_row);
            let right = downcast::<$array>(right)?.value(right_row);
            // Flink's generated comparator uses `>` and `<`: NaN and signed zero compare equal.
            left.partial_cmp(&right).unwrap_or(Ordering::Equal)
        }};
    }
    let result = match left.data_type() {
        DataType::Null => Ordering::Equal,
        DataType::Boolean => primitive!(BooleanArray),
        DataType::Int8 => primitive!(Int8Array),
        DataType::Int16 => primitive!(Int16Array),
        DataType::Int32 => primitive!(Int32Array),
        DataType::Int64 => primitive!(Int64Array),
        DataType::UInt8 => primitive!(UInt8Array),
        DataType::UInt16 => primitive!(UInt16Array),
        DataType::UInt32 => primitive!(UInt32Array),
        DataType::UInt64 => primitive!(UInt64Array),
        DataType::Float32 => partial!(Float32Array),
        DataType::Float64 => partial!(Float64Array),
        DataType::Utf8 => downcast::<StringArray>(left)?
            .value(left_row)
            .as_bytes()
            .cmp(downcast::<StringArray>(right)?.value(right_row).as_bytes()),
        DataType::LargeUtf8 => downcast::<LargeStringArray>(left)?
            .value(left_row)
            .as_bytes()
            .cmp(
                downcast::<LargeStringArray>(right)?
                    .value(right_row)
                    .as_bytes(),
            ),
        DataType::Binary => downcast::<BinaryArray>(left)?
            .value(left_row)
            .cmp(downcast::<BinaryArray>(right)?.value(right_row)),
        DataType::LargeBinary => downcast::<LargeBinaryArray>(left)?
            .value(left_row)
            .cmp(downcast::<LargeBinaryArray>(right)?.value(right_row)),
        DataType::FixedSizeBinary(_) => downcast::<FixedSizeBinaryArray>(left)?
            .value(left_row)
            .cmp(downcast::<FixedSizeBinaryArray>(right)?.value(right_row)),
        DataType::Decimal128(_, _) => primitive!(Decimal128Array),
        DataType::Date32 => primitive!(Date32Array),
        DataType::Date64 => primitive!(Date64Array),
        DataType::Time32(TimeUnit::Second) => primitive!(Time32SecondArray),
        DataType::Time32(TimeUnit::Millisecond) => primitive!(Time32MillisecondArray),
        DataType::Time64(TimeUnit::Microsecond) => primitive!(Time64MicrosecondArray),
        DataType::Time64(TimeUnit::Nanosecond) => primitive!(Time64NanosecondArray),
        DataType::Timestamp(TimeUnit::Second, _) => primitive!(TimestampSecondArray),
        DataType::Timestamp(TimeUnit::Millisecond, _) => primitive!(TimestampMillisecondArray),
        DataType::Timestamp(TimeUnit::Microsecond, _) => primitive!(TimestampMicrosecondArray),
        DataType::Timestamp(TimeUnit::Nanosecond, _) => primitive!(TimestampNanosecondArray),
        DataType::List(_) => compare_list(
            downcast::<ListArray>(left)?,
            left_row,
            downcast::<ListArray>(right)?,
            right_row,
        )?,
        DataType::LargeList(_) => compare_large_list(
            downcast::<LargeListArray>(left)?,
            left_row,
            downcast::<LargeListArray>(right)?,
            right_row,
        )?,
        DataType::FixedSizeList(_, _) => compare_fixed_list(
            downcast::<FixedSizeListArray>(left)?,
            left_row,
            downcast::<FixedSizeListArray>(right)?,
            right_row,
        )?,
        DataType::Struct(_) => compare_struct(
            downcast::<StructArray>(left)?,
            left_row,
            downcast::<StructArray>(right)?,
            right_row,
        )?,
        other => {
            return Err(DataFusionError::Plan(format!(
                "Flink Top-N ordering type {other} is not implemented natively"
            )))
        }
    };
    Ok(result)
}

fn equal_values(
    left: &dyn Array,
    left_row: usize,
    right: &dyn Array,
    right_row: usize,
    float_equality: FloatEquality,
) -> Result<bool> {
    let left_null = left.is_null(left_row);
    let right_null = right.is_null(right_row);
    if left_null || right_null {
        return Ok(left_null == right_null);
    }
    if left.data_type() != right.data_type() {
        return Ok(false);
    }
    let equal = match left.data_type() {
        DataType::Float32 => float32_equal(
            downcast::<Float32Array>(left)?.value(left_row),
            downcast::<Float32Array>(right)?.value(right_row),
            float_equality,
        ),
        DataType::Float64 => float64_equal(
            downcast::<Float64Array>(left)?.value(left_row),
            downcast::<Float64Array>(right)?.value(right_row),
            float_equality,
        ),
        DataType::List(_) => {
            let left = downcast::<ListArray>(left)?;
            let right = downcast::<ListArray>(right)?;
            equal_ranges(
                left.values().as_ref(),
                left.value_offsets()[left_row] as usize,
                left.value_length(left_row) as usize,
                right.values().as_ref(),
                right.value_offsets()[right_row] as usize,
                right.value_length(right_row) as usize,
                float_equality,
            )?
        }
        DataType::LargeList(_) => {
            let left = downcast::<LargeListArray>(left)?;
            let right = downcast::<LargeListArray>(right)?;
            equal_ranges(
                left.values().as_ref(),
                left.value_offsets()[left_row] as usize,
                left.value_length(left_row) as usize,
                right.values().as_ref(),
                right.value_offsets()[right_row] as usize,
                right.value_length(right_row) as usize,
                float_equality,
            )?
        }
        DataType::FixedSizeList(_, _) => {
            let left = downcast::<FixedSizeListArray>(left)?;
            let right = downcast::<FixedSizeListArray>(right)?;
            equal_ranges(
                left.values().as_ref(),
                left.value_offset(left_row) as usize,
                left.value_length() as usize,
                right.values().as_ref(),
                right.value_offset(right_row) as usize,
                right.value_length() as usize,
                float_equality,
            )?
        }
        DataType::Struct(_) => {
            let left = downcast::<StructArray>(left)?;
            let right = downcast::<StructArray>(right)?;
            let mut equal = left.num_columns() == right.num_columns();
            for (left, right) in left.columns().iter().zip(right.columns()) {
                equal &= equal_values(
                    left.as_ref(),
                    left_row,
                    right.as_ref(),
                    right_row,
                    float_equality,
                )?;
                if !equal {
                    break;
                }
            }
            equal
        }
        DataType::Map(_, _) => equal_maps(
            downcast::<MapArray>(left)?,
            left_row,
            downcast::<MapArray>(right)?,
            right_row,
            float_equality,
        )?,
        _ => compare_values(left, left_row, right, right_row, false)? == Ordering::Equal,
    };
    Ok(equal)
}

fn equal_ranges(
    left: &dyn Array,
    left_start: usize,
    left_len: usize,
    right: &dyn Array,
    right_start: usize,
    right_len: usize,
    float_equality: FloatEquality,
) -> Result<bool> {
    if left_len != right_len {
        return Ok(false);
    }
    for offset in 0..left_len {
        if !equal_values(
            left,
            left_start + offset,
            right,
            right_start + offset,
            float_equality,
        )? {
            return Ok(false);
        }
    }
    Ok(true)
}

fn equal_maps(
    left: &MapArray,
    left_row: usize,
    right: &MapArray,
    right_row: usize,
    float_equality: FloatEquality,
) -> Result<bool> {
    let left_start = left.value_offsets()[left_row] as usize;
    let left_len = left.value_length(left_row) as usize;
    let right_start = right.value_offsets()[right_row] as usize;
    let right_len = right.value_length(right_row) as usize;
    if left_len != right_len {
        return Ok(false);
    }
    let left_entries = left.entries();
    let right_entries = right.entries();
    let left_keys = left_entries.column(0).as_ref();
    let left_values = left_entries.column(1).as_ref();
    let right_keys = right_entries.column(0).as_ref();
    let right_values = right_entries.column(1).as_ref();
    for left_offset in 0..left_len {
        let left_index = left_start + left_offset;
        let mut found = false;
        for right_offset in 0..right_len {
            let right_index = right_start + right_offset;
            if equal_values(
                left_keys,
                left_index,
                right_keys,
                right_index,
                float_equality,
            )? {
                if !equal_values(
                    left_values,
                    left_index,
                    right_values,
                    right_index,
                    float_equality,
                )? {
                    return Ok(false);
                }
                found = true;
                break;
            }
        }
        if !found {
            return Ok(false);
        }
    }
    Ok(true)
}

fn float32_equal(left: f32, right: f32, mode: FloatEquality) -> bool {
    match mode {
        FloatEquality::Ieee => left == right,
        FloatEquality::CanonicalBits => {
            (left.is_nan() && right.is_nan()) || left.to_bits() == right.to_bits()
        }
    }
}

fn float64_equal(left: f64, right: f64, mode: FloatEquality) -> bool {
    match mode {
        FloatEquality::Ieee => left == right,
        FloatEquality::CanonicalBits => {
            (left.is_nan() && right.is_nan()) || left.to_bits() == right.to_bits()
        }
    }
}

fn compare_list(
    left: &ListArray,
    left_row: usize,
    right: &ListArray,
    right_row: usize,
) -> Result<Ordering> {
    compare_ranges(
        left.values().as_ref(),
        left.value_offsets()[left_row] as usize,
        left.value_length(left_row) as usize,
        right.values().as_ref(),
        right.value_offsets()[right_row] as usize,
        right.value_length(right_row) as usize,
    )
}

fn compare_large_list(
    left: &LargeListArray,
    left_row: usize,
    right: &LargeListArray,
    right_row: usize,
) -> Result<Ordering> {
    compare_ranges(
        left.values().as_ref(),
        left.value_offsets()[left_row] as usize,
        left.value_length(left_row) as usize,
        right.values().as_ref(),
        right.value_offsets()[right_row] as usize,
        right.value_length(right_row) as usize,
    )
}

fn compare_fixed_list(
    left: &FixedSizeListArray,
    left_row: usize,
    right: &FixedSizeListArray,
    right_row: usize,
) -> Result<Ordering> {
    compare_ranges(
        left.values().as_ref(),
        left.value_offset(left_row) as usize,
        left.value_length() as usize,
        right.values().as_ref(),
        right.value_offset(right_row) as usize,
        right.value_length() as usize,
    )
}

fn compare_ranges(
    left: &dyn Array,
    left_start: usize,
    left_len: usize,
    right: &dyn Array,
    right_start: usize,
    right_len: usize,
) -> Result<Ordering> {
    for offset in 0..left_len.min(right_len) {
        let ordering = compare_values(
            left,
            left_start + offset,
            right,
            right_start + offset,
            false,
        )?;
        if ordering != Ordering::Equal {
            return Ok(ordering);
        }
    }
    Ok(left_len.cmp(&right_len))
}

fn compare_struct(
    left: &StructArray,
    left_row: usize,
    right: &StructArray,
    right_row: usize,
) -> Result<Ordering> {
    for (left, right) in left.columns().iter().zip(right.columns()) {
        let ordering = compare_values(left.as_ref(), left_row, right.as_ref(), right_row, false)?;
        if ordering != Ordering::Equal {
            return Ok(ordering);
        }
    }
    Ok(Ordering::Equal)
}

fn downcast<T: Array + 'static>(array: &dyn Array) -> Result<&T> {
    array.as_any().downcast_ref::<T>().ok_or_else(|| {
        DataFusionError::Execution(format!(
            "top-n expected {}, got {}",
            std::any::type_name::<T>(),
            array.data_type()
        ))
    })
}
