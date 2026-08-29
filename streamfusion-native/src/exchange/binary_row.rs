// Copyright 2026 StreamFusion Authors
// Licensed under the Apache License, Version 2.0.

use arrow::array::{
    Array, BinaryArray, BooleanArray, Date32Array, Decimal128Array, Float32Array, Float64Array,
    Int16Array, Int32Array, Int64Array, Int8Array, StringArray, Time32MillisecondArray,
    Time32SecondArray, TimestampMicrosecondArray, TimestampMillisecondArray,
    TimestampNanosecondArray, TimestampSecondArray,
};
use arrow::datatypes::{DataType, TimeUnit};
use arrow::error::{ArrowError, Result};
use arrow::record_batch::RecordBatch;

/// Flink logical key types whose BinaryRow encoding has been proven independently.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum KeyField {
    Boolean,
    TinyInt,
    SmallInt,
    Integer,
    BigInt,
    Float,
    Double,
    String,
    Binary,
    Date,
    Time,
    Timestamp { precision: u8 },
    Decimal { precision: u8 },
}

/// Encodes selected Arrow values as the exact bytes hashed by Flink's BinaryRowData key selector.
pub fn encode_binary_row(
    batch: &RecordBatch,
    row: usize,
    fields: &[(usize, KeyField)],
) -> Result<Vec<u8>> {
    if row >= batch.num_rows() {
        return Err(ArrowError::InvalidArgumentError(format!(
            "key row {row} is outside a {}-row batch",
            batch.num_rows()
        )));
    }
    let mut writer = BinaryRowWriter::new(fields.len());
    for (position, (column_index, kind)) in fields.iter().copied().enumerate() {
        let array = batch.column(column_index);
        if array.is_null(row) {
            writer.set_null(position);
            continue;
        }
        match kind {
            KeyField::Boolean => writer.write_fixed(
                position,
                &[u8::from(value::<BooleanArray>(array, row)?.value(row))],
            ),
            KeyField::TinyInt => writer.write_fixed(
                position,
                &value::<Int8Array>(array, row)?.value(row).to_le_bytes(),
            ),
            KeyField::SmallInt => writer.write_fixed(
                position,
                &value::<Int16Array>(array, row)?.value(row).to_le_bytes(),
            ),
            KeyField::Integer => writer.write_fixed(
                position,
                &value::<Int32Array>(array, row)?.value(row).to_le_bytes(),
            ),
            KeyField::BigInt => writer.write_fixed(
                position,
                &value::<Int64Array>(array, row)?.value(row).to_le_bytes(),
            ),
            KeyField::Float => writer.write_fixed(
                position,
                &value::<Float32Array>(array, row)?.value(row).to_le_bytes(),
            ),
            KeyField::Double => writer.write_fixed(
                position,
                &value::<Float64Array>(array, row)?.value(row).to_le_bytes(),
            ),
            KeyField::String => writer.write_bytes(
                position,
                value::<StringArray>(array, row)?.value(row).as_bytes(),
            ),
            KeyField::Binary => {
                writer.write_bytes(position, value::<BinaryArray>(array, row)?.value(row))
            }
            KeyField::Date => writer.write_fixed(
                position,
                &value::<Date32Array>(array, row)?.value(row).to_le_bytes(),
            ),
            KeyField::Time => writer.write_fixed(position, &time_millis(array, row)?.to_le_bytes()),
            KeyField::Timestamp { precision } => {
                let (millis, nanos) = timestamp_parts(array, row)?;
                if precision <= 3 {
                    writer.write_fixed(position, &millis.to_le_bytes());
                } else {
                    writer.write_noncompact_timestamp(position, millis, nanos);
                }
            }
            KeyField::Decimal { precision } => {
                let unscaled = value::<Decimal128Array>(array, row)?.value(row);
                if precision <= 18 {
                    let compact = i64::try_from(unscaled).map_err(|_| {
                        ArrowError::InvalidArgumentError(format!(
                            "DECIMAL({precision}) key does not fit Flink's compact representation"
                        ))
                    })?;
                    writer.write_fixed(position, &compact.to_le_bytes());
                } else {
                    writer.write_noncompact_decimal(position, unscaled);
                }
            }
        }
    }
    Ok(writer.finish())
}

fn time_millis(array: &dyn Array, row: usize) -> Result<i32> {
    match array.data_type() {
        DataType::Time32(TimeUnit::Second) => value::<Time32SecondArray>(array, row)?
            .value(row)
            .checked_mul(1_000)
            .ok_or_else(|| {
                ArrowError::InvalidArgumentError("TIME key milliseconds overflow".to_string())
            }),
        DataType::Time32(TimeUnit::Millisecond) => {
            Ok(value::<Time32MillisecondArray>(array, row)?.value(row))
        }
        other => Err(ArrowError::CastError(format!(
            "Flink TIME key requires Arrow Time32 second or millisecond, got {other}"
        ))),
    }
}

fn timestamp_parts(array: &dyn Array, row: usize) -> Result<(i64, i32)> {
    let (millis, nanos) = match array.data_type() {
        DataType::Timestamp(TimeUnit::Second, _) => (
            value::<TimestampSecondArray>(array, row)?
                .value(row)
                .checked_mul(1_000)
                .ok_or_else(|| {
                    ArrowError::InvalidArgumentError(
                        "timestamp key milliseconds overflow".to_string(),
                    )
                })?,
            0,
        ),
        DataType::Timestamp(TimeUnit::Millisecond, _) => (
            value::<TimestampMillisecondArray>(array, row)?.value(row),
            0,
        ),
        DataType::Timestamp(TimeUnit::Microsecond, _) => {
            let micros = value::<TimestampMicrosecondArray>(array, row)?.value(row);
            (
                micros.div_euclid(1_000),
                (micros.rem_euclid(1_000) * 1_000) as i32,
            )
        }
        DataType::Timestamp(TimeUnit::Nanosecond, _) => {
            let nanos = value::<TimestampNanosecondArray>(array, row)?.value(row);
            (
                nanos.div_euclid(1_000_000),
                nanos.rem_euclid(1_000_000) as i32,
            )
        }
        other => {
            return Err(ArrowError::CastError(format!(
                "Flink TIMESTAMP key requires an Arrow timestamp, got {other}"
            )));
        }
    };
    Ok((millis, nanos))
}

fn value<'a, T: Array + 'static>(array: &'a dyn Array, row: usize) -> Result<&'a T> {
    array.as_any().downcast_ref::<T>().ok_or_else(|| {
        ArrowError::CastError(format!(
            "key row {row} expected {}, got {}",
            std::any::type_name::<T>(),
            array.data_type()
        ))
    })
}

struct BinaryRowWriter {
    null_bytes: usize,
    bytes: Vec<u8>,
}

impl BinaryRowWriter {
    fn new(arity: usize) -> Self {
        let null_bytes = (arity + 63 + 8) / 64 * 8;
        Self {
            null_bytes,
            bytes: vec![0; null_bytes + arity * 8],
        }
    }

    fn field_offset(&self, position: usize) -> usize {
        self.null_bytes + position * 8
    }

    fn set_null(&mut self, position: usize) {
        let bit = position + 8;
        self.bytes[bit / 8] |= 1 << (bit % 8);
    }

    fn write_fixed(&mut self, position: usize, value: &[u8]) {
        let offset = self.field_offset(position);
        self.bytes[offset..offset + value.len()].copy_from_slice(value);
    }

    fn write_bytes(&mut self, position: usize, value: &[u8]) {
        let field_offset = self.field_offset(position);
        if value.len() <= 7 {
            self.bytes[field_offset..field_offset + value.len()].copy_from_slice(value);
            self.bytes[field_offset + 7] = 0x80 | value.len() as u8;
            return;
        }
        let variable_offset = self.bytes.len();
        self.bytes.extend_from_slice(value);
        self.bytes.resize(self.bytes.len().next_multiple_of(8), 0);
        let offset_and_size = ((variable_offset as u64) << 32) | value.len() as u64;
        self.bytes[field_offset..field_offset + 8].copy_from_slice(&offset_and_size.to_le_bytes());
    }

    fn write_noncompact_timestamp(&mut self, position: usize, millis: i64, nanos: i32) {
        let variable_offset = self.bytes.len();
        self.bytes.extend_from_slice(&millis.to_le_bytes());
        let offset_and_nanos = ((variable_offset as u64) << 32) | nanos as u32 as u64;
        self.write_fixed(position, &offset_and_nanos.to_le_bytes());
    }

    fn write_noncompact_decimal(&mut self, position: usize, unscaled: i128) {
        let encoded = minimal_twos_complement(unscaled);
        let variable_offset = self.bytes.len();
        self.bytes.resize(variable_offset + 16, 0);
        self.bytes[variable_offset..variable_offset + encoded.len()].copy_from_slice(&encoded);
        let offset_and_size = ((variable_offset as u64) << 32) | encoded.len() as u64;
        self.write_fixed(position, &offset_and_size.to_le_bytes());
    }

    fn finish(self) -> Vec<u8> {
        self.bytes
    }
}

fn minimal_twos_complement(value: i128) -> Vec<u8> {
    let bytes = value.to_be_bytes();
    let mut start = 0;
    while start < bytes.len() - 1 {
        let current = bytes[start];
        let next = bytes[start + 1];
        if (current == 0 && next & 0x80 == 0) || (current == 0xff && next & 0x80 != 0) {
            start += 1;
        } else {
            break;
        }
    }
    bytes[start..].to_vec()
}

#[cfg(test)]
mod tests {
    use std::sync::Arc;

    use arrow::array::{
        ArrayRef, Date32Array, Decimal128Array, Int32Array, StringArray, Time32MillisecondArray,
        TimestampMicrosecondArray,
    };
    use arrow::datatypes::{DataType, Field, Schema};

    use super::*;

    #[test]
    fn matches_flink_fixed_and_inline_variable_layout() {
        let schema = Arc::new(Schema::new(vec![
            Field::new("id", DataType::Int32, false),
            Field::new("name", DataType::Utf8, false),
        ]));
        let batch = RecordBatch::try_new(
            schema,
            vec![
                Arc::new(Int32Array::from(vec![42])) as ArrayRef,
                Arc::new(StringArray::from(vec!["abc"])) as ArrayRef,
            ],
        )
        .unwrap();

        let bytes =
            encode_binary_row(&batch, 0, &[(0, KeyField::Integer), (1, KeyField::String)]).unwrap();

        assert_eq!(
            bytes,
            [
                0, 0, 0, 0, 0, 0, 0, 0, 42, 0, 0, 0, 0, 0, 0, 0, b'a', b'b', b'c', 0, 0, 0, 0,
                0x83,
            ]
        );
    }

    #[test]
    fn encodes_null_bits_and_long_strings_like_flink() {
        let schema = Arc::new(Schema::new(vec![
            Field::new("id", DataType::Int32, true),
            Field::new("name", DataType::Utf8, false),
        ]));
        let batch = RecordBatch::try_new(
            schema,
            vec![
                Arc::new(Int32Array::from(vec![None])) as ArrayRef,
                Arc::new(StringArray::from(vec!["long-key"])) as ArrayRef,
            ],
        )
        .unwrap();
        let bytes =
            encode_binary_row(&batch, 0, &[(0, KeyField::Integer), (1, KeyField::String)]).unwrap();

        assert_eq!(bytes[1], 1);
        assert_eq!(&bytes[24..32], b"long-key");
        assert_eq!(
            u64::from_le_bytes(bytes[16..24].try_into().unwrap()),
            (24_u64 << 32) | 8
        );
    }

    #[test]
    fn matches_flink_temporal_and_decimal_layout() {
        let schema = Arc::new(Schema::new(vec![
            Field::new("date", DataType::Date32, false),
            Field::new("time", DataType::Time32(TimeUnit::Millisecond), false),
            Field::new(
                "timestamp",
                DataType::Timestamp(TimeUnit::Microsecond, None),
                false,
            ),
            Field::new("decimal", DataType::Decimal128(25, 0), false),
        ]));
        let decimal = Decimal128Array::from(vec![i64::MAX as i128 + 1])
            .with_precision_and_scale(25, 0)
            .unwrap();
        let batch = RecordBatch::try_new(
            schema,
            vec![
                Arc::new(Date32Array::from(vec![20_000])) as ArrayRef,
                Arc::new(Time32MillisecondArray::from(vec![45_678])) as ArrayRef,
                Arc::new(TimestampMicrosecondArray::from(vec![-1_001])) as ArrayRef,
                Arc::new(decimal) as ArrayRef,
            ],
        )
        .unwrap();

        let bytes = encode_binary_row(
            &batch,
            0,
            &[
                (0, KeyField::Date),
                (1, KeyField::Time),
                (2, KeyField::Timestamp { precision: 6 }),
                (3, KeyField::Decimal { precision: 25 }),
            ],
        )
        .unwrap();

        assert_eq!(i32::from_le_bytes(bytes[8..12].try_into().unwrap()), 20_000);
        assert_eq!(
            i32::from_le_bytes(bytes[16..20].try_into().unwrap()),
            45_678
        );
        assert_eq!(
            u64::from_le_bytes(bytes[24..32].try_into().unwrap()),
            (40_u64 << 32) | 999_000
        );
        assert_eq!(i64::from_le_bytes(bytes[40..48].try_into().unwrap()), -2);
        assert_eq!(
            u64::from_le_bytes(bytes[32..40].try_into().unwrap()),
            (48_u64 << 32) | 9
        );
        assert_eq!(&bytes[48..57], &[0, 0x80, 0, 0, 0, 0, 0, 0, 0]);
        assert_eq!(bytes.len(), 64);
    }
}
