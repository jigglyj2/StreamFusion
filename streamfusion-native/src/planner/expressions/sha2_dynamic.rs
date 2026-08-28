// Copyright 2026 StreamFusion Authors
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
// http://www.apache.org/licenses/LICENSE-2.0

use std::hash::{Hash, Hasher};
use std::sync::Arc;

use arrow::array::{Array, Int32Array, StringArray};
use arrow::datatypes::{DataType, Field, FieldRef, Schema};
use arrow::record_batch::RecordBatch;
use datafusion::error::{DataFusionError, Result};
use datafusion::logical_expr::ColumnarValue;
use datafusion::physical_expr::PhysicalExpr;
use sha2::{Digest, Sha224, Sha256, Sha384, Sha512};

#[derive(Debug, Eq)]
struct Sha2DynamicExpr {
    value: Arc<dyn PhysicalExpr>,
    bit_length: Arc<dyn PhysicalExpr>,
}

impl PartialEq for Sha2DynamicExpr {
    fn eq(&self, other: &Self) -> bool {
        self.value.eq(&other.value) && self.bit_length.eq(&other.bit_length)
    }
}

impl Hash for Sha2DynamicExpr {
    fn hash<H: Hasher>(&self, state: &mut H) {
        self.value.hash(state);
        self.bit_length.hash(state);
    }
}

impl std::fmt::Display for Sha2DynamicExpr {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(formatter, "SHA2({}, {})", self.value, self.bit_length)
    }
}

impl PhysicalExpr for Sha2DynamicExpr {
    fn data_type(&self, _input_schema: &Schema) -> Result<DataType> {
        Ok(DataType::Utf8)
    }

    fn nullable(&self, _input_schema: &Schema) -> Result<bool> {
        Ok(true)
    }

    fn evaluate(&self, batch: &RecordBatch) -> Result<ColumnarValue> {
        let values = self.value.evaluate(batch)?.into_array(batch.num_rows())?;
        let bit_lengths = self
            .bit_length
            .evaluate(batch)?
            .into_array(batch.num_rows())?;
        let values = values
            .as_any()
            .downcast_ref::<StringArray>()
            .ok_or_else(|| DataFusionError::Execution("SHA2 expected Utf8 input".to_string()))?;
        let bit_lengths = bit_lengths
            .as_any()
            .downcast_ref::<Int32Array>()
            .ok_or_else(|| {
                DataFusionError::Execution("SHA2 expected Int32 bit length".to_string())
            })?;
        let mut output = Vec::with_capacity(batch.num_rows());
        for index in 0..batch.num_rows() {
            output.push(match (values.is_null(index), bit_lengths.is_null(index)) {
                (true, _) | (_, true) => None,
                (false, false) => Some(sha2_hex(values.value(index), bit_lengths.value(index))?),
            });
        }
        Ok(ColumnarValue::Array(Arc::new(StringArray::from(output))))
    }

    fn return_field(&self, input_schema: &Schema) -> Result<FieldRef> {
        let source = self.value.return_field(input_schema)?;
        Ok(Arc::new(Field::new(source.name(), DataType::Utf8, true)))
    }

    fn children(&self) -> Vec<&Arc<dyn PhysicalExpr>> {
        vec![&self.value, &self.bit_length]
    }

    fn with_new_children(
        self: Arc<Self>,
        children: Vec<Arc<dyn PhysicalExpr>>,
    ) -> Result<Arc<dyn PhysicalExpr>> {
        Ok(Arc::new(Self {
            value: Arc::clone(&children[0]),
            bit_length: Arc::clone(&children[1]),
        }))
    }

    fn fmt_sql(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(formatter, "SHA2(")?;
        self.value.fmt_sql(formatter)?;
        write!(formatter, ", ")?;
        self.bit_length.fmt_sql(formatter)?;
        write!(formatter, ")")
    }
}

fn sha2_hex(value: &str, bit_length: i32) -> Result<String> {
    let bytes = match bit_length {
        224 => Sha224::digest(value.as_bytes()).to_vec(),
        256 => Sha256::digest(value.as_bytes()).to_vec(),
        384 => Sha384::digest(value.as_bytes()).to_vec(),
        512 => Sha512::digest(value.as_bytes()).to_vec(),
        _ => {
            return Err(DataFusionError::Execution(format!(
                "Unsupported algorithm: SHA-{bit_length}"
            )));
        }
    };
    const HEX: &[u8; 16] = b"0123456789abcdef";
    let mut encoded = String::with_capacity(bytes.len() * 2);
    for byte in bytes {
        encoded.push(HEX[(byte >> 4) as usize] as char);
        encoded.push(HEX[(byte & 0x0f) as usize] as char);
    }
    Ok(encoded)
}

pub(crate) fn create(
    value: Arc<dyn PhysicalExpr>,
    bit_length: Arc<dyn PhysicalExpr>,
    schema: &Schema,
) -> Result<Arc<dyn PhysicalExpr>> {
    if value.data_type(schema)? != DataType::Utf8
        || bit_length.data_type(schema)? != DataType::Int32
    {
        return Err(DataFusionError::Plan(
            "SHA2 requires Utf8 input and Int32 bit length".to_string(),
        ));
    }
    Ok(Arc::new(Sha2DynamicExpr { value, bit_length }))
}
