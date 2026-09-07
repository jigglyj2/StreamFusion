// Copyright 2026 StreamFusion Authors
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
// http://www.apache.org/licenses/LICENSE-2.0

use std::sync::Arc;

use arrow::array::{make_array, Array, ArrayRef, StructArray};
use arrow::buffer::NullBuffer;
use arrow::datatypes::{DataType, Schema};
use datafusion::common::config::ConfigOptions;
use datafusion::error::{DataFusionError, Result};
use datafusion::logical_expr::{
    ColumnarValue, ScalarFunctionArgs, ScalarUDF, ScalarUDFImpl, Signature, Volatility,
};
use datafusion::physical_expr::{PhysicalExpr, ScalarFunctionExpr};
use datafusion::scalar::ScalarValue;

pub(crate) fn create(
    operand: Arc<dyn PhysicalExpr>,
    field_name: &str,
    schema: &Schema,
) -> Result<Arc<dyn PhysicalExpr>> {
    let DataType::Struct(fields) = operand.data_type(schema)? else {
        return Err(DataFusionError::Plan(
            "ROW field access requires a struct".into(),
        ));
    };
    let index = fields
        .iter()
        .position(|field| field.name() == field_name)
        .ok_or_else(|| DataFusionError::Plan(format!("ROW field {field_name} does not exist")))?;
    Ok(Arc::new(ScalarFunctionExpr::try_new(
        Arc::new(ScalarUDF::new_from_impl(FlinkStructField {
            index,
            output_type: fields[index].data_type().clone(),
            signature: Signature::any(1, Volatility::Immutable),
        })),
        vec![operand],
        schema,
        Arc::new(ConfigOptions::new()),
    )?))
}

#[derive(Debug, PartialEq, Eq, Hash)]
struct FlinkStructField {
    index: usize,
    output_type: DataType,
    signature: Signature,
}

impl ScalarUDFImpl for FlinkStructField {
    fn name(&self) -> &str {
        "streamfusion_struct_field"
    }

    fn signature(&self) -> &Signature {
        &self.signature
    }

    fn return_type(&self, _args: &[DataType]) -> Result<DataType> {
        Ok(self.output_type.clone())
    }

    fn invoke_with_args(&self, args: ScalarFunctionArgs) -> Result<ColumnarValue> {
        let operand = args
            .args
            .first()
            .ok_or_else(|| DataFusionError::Execution("Missing ROW operand".into()))?;
        match operand {
            ColumnarValue::Array(parent) => Ok(ColumnarValue::Array(self.field(parent)?)),
            ColumnarValue::Scalar(parent) => Ok(ColumnarValue::Scalar(
                ScalarValue::try_from_array(&self.field(&parent.to_array()?)?, 0)?,
            )),
        }
    }
}

impl FlinkStructField {
    fn field(&self, parent: &ArrayRef) -> Result<ArrayRef> {
        let parent = parent
            .as_any()
            .downcast_ref::<StructArray>()
            .ok_or_else(|| {
                DataFusionError::Execution("ROW field access requires a struct".into())
            })?;
        let child = parent
            .columns()
            .get(self.index)
            .ok_or_else(|| DataFusionError::Execution("ROW field index is out of range".into()))?;
        if parent.null_count() == 0 || child.data_type() == &DataType::Null {
            return Ok(Arc::clone(child));
        }
        // DataFusion 55 get_field omits parent validity. Evaluate the operand once, share
        // every child value buffer, and combine only the parent/child validity bitmaps.
        let nulls = NullBuffer::union(parent.nulls(), child.nulls());
        Ok(make_array(
            child.to_data().into_builder().nulls(nulls).build()?,
        ))
    }
}

#[cfg(test)]
#[path = "struct_field_tests.rs"]
mod tests;
