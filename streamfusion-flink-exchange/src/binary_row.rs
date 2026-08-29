// Copyright 2026 StreamFusion Authors
// Licensed under the Apache License, Version 2.0

use arrow::array::{
    Array, BinaryArray, BooleanArray, Float32Array, Float64Array, Int16Array, Int32Array,
    Int64Array, Int8Array, StringArray,
};
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
        }
    }
    Ok(writer.finish())
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

    fn finish(self) -> Vec<u8> {
        self.bytes
    }
}

#[cfg(test)]
mod tests {
    use std::sync::Arc;

    use arrow::array::{ArrayRef, Int32Array, StringArray};
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
}
