// Copyright 2026 StreamFusion Authors
// Licensed under the Apache License, Version 2.0.

//! Flink BinaryArrayData/BinaryMapData and nested BinaryRowData layouts. Offsets are
//! relative to each nested container, not the enclosing row. Encode into the caller's
//! scratch buffer without materializing child Arrow arrays or temporary row buffers.

use arrow::array::{ListArray, MapArray, StructArray};

use super::*;

#[cfg(test)]
mod tests;

pub(super) fn write_nested(
    writer: &mut BinaryRowWriter<'_>,
    position: usize,
    array: &dyn Array,
    row: usize,
) -> Result<()> {
    let start = writer.bytes.len();
    match array.data_type() {
        DataType::List(_) => {
            let list = value::<ListArray>(array, row)?;
            let offsets = list.value_offsets();
            append_array(
                writer.bytes,
                list.values().as_ref(),
                offsets[row] as usize,
                offsets[row + 1] as usize,
            )?;
        }
        DataType::Map(_, _) => {
            let map = value::<MapArray>(array, row)?;
            let offsets = map.value_offsets();
            let from = offsets[row] as usize;
            let to = offsets[row + 1] as usize;
            writer.bytes.extend_from_slice(&[0; 4]);
            append_array(writer.bytes, map.keys().as_ref(), from, to)?;
            let key_size = writer.bytes.len() - start - 4;
            writer.bytes[start..start + 4].copy_from_slice(&(key_size as i32).to_le_bytes());
            append_array(writer.bytes, map.values().as_ref(), from, to)?;
        }
        DataType::Struct(fields) => {
            let structure = value::<StructArray>(array, row)?;
            let null_bytes = (fields.len() + 71) / 64 * 8;
            writer
                .bytes
                .resize(start + null_bytes + fields.len() * 8, 0);
            let mut child = BinaryRowWriter {
                base: start,
                stride: 8,
                null_bit_offset: 8,
                null_bytes,
                bytes: writer.bytes,
            };
            for (index, field) in fields.iter().enumerate() {
                write_value(
                    &mut child,
                    index,
                    structure.column(index).as_ref(),
                    row,
                    KeyField::from_arrow_type(field.data_type())?,
                )?;
            }
        }
        other => {
            return Err(ArrowError::InvalidArgumentError(format!(
                "expected nested key, got {other}"
            )))
        }
    }
    let size = writer.bytes.len() - start;
    let offset_and_size = (((start - writer.base) as u64) << 32) | size as u64;
    writer.write_fixed(position, &offset_and_size.to_le_bytes());
    writer.bytes.resize(
        writer.base + (writer.bytes.len() - writer.base).next_multiple_of(8),
        0,
    );
    Ok(())
}

fn append_array(bytes: &mut Vec<u8>, array: &dyn Array, from: usize, to: usize) -> Result<()> {
    let count = to - from;
    let kind = KeyField::from_arrow_type(array.data_type())?;
    let stride = match kind {
        KeyField::Boolean | KeyField::TinyInt => 1,
        KeyField::SmallInt => 2,
        KeyField::Integer | KeyField::Float | KeyField::Date | KeyField::Time => 4,
        _ => 8,
    };
    let base = bytes.len();
    let null_bytes = 4 + count.div_ceil(32) * 4;
    bytes.resize(base + (null_bytes + count * stride).next_multiple_of(8), 0);
    bytes[base..base + 4].copy_from_slice(&(count as i32).to_le_bytes());
    let mut writer = BinaryRowWriter {
        base,
        stride,
        null_bit_offset: 32,
        null_bytes,
        bytes,
    };
    for index in from..to {
        // BinaryArrayWriter canonicalizes NaNs, unlike BinaryRowWriter.
        if !array.is_null(index) && kind == KeyField::Float {
            let number = value::<Float32Array>(array, index)?.value(index);
            let bits = if number.is_nan() {
                0x7fc0_0000
            } else {
                number.to_bits()
            };
            writer.write_fixed(index - from, &bits.to_le_bytes());
        } else if !array.is_null(index) && kind == KeyField::Double {
            let number = value::<Float64Array>(array, index)?.value(index);
            let bits = if number.is_nan() {
                0x7ff8_0000_0000_0000
            } else {
                number.to_bits()
            };
            writer.write_fixed(index - from, &bits.to_le_bytes());
        } else {
            write_value(&mut writer, index - from, array, index, kind)?;
        }
    }
    Ok(())
}
