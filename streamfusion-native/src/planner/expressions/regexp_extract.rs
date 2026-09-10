// Copyright 2026 StreamFusion Authors. Licensed under the Apache License, Version 2.0.
//! Compose DataFusion regexp_match and array_element under one Flink memory reservation.
use arrow::array::RecordBatch;
use arrow::datatypes::{DataType, Field, Schema};
use datafusion::common::config::ConfigOptions;
use datafusion::error::{DataFusionError, Result};
use datafusion::logical_expr::{
    ColumnarValue, ScalarFunctionArgs, ScalarUDF, ScalarUDFImpl, Signature, Volatility,
};
use datafusion::physical_expr::expressions::{Column, Literal};
use datafusion::physical_expr::{PhysicalExpr, ScalarFunctionExpr};
use datafusion::scalar::ScalarValue;
use std::hash::{Hash, Hasher};
use std::sync::Arc;
pub(super) mod memory;
mod pattern;
#[cfg(test)]
mod tests;

#[derive(Debug)]
pub(super) struct RegexExtract {
    pattern: String,
    signature: Signature,
    schema: Arc<Schema>,
    kernel: Arc<dyn PhysicalExpr>,
}
impl PartialEq for RegexExtract {
    fn eq(&self, other: &Self) -> bool {
        self.pattern == other.pattern
    }
}
impl Eq for RegexExtract {}
impl Hash for RegexExtract {
    fn hash<H: Hasher>(&self, state: &mut H) {
        self.pattern.hash(state);
    }
}
impl ScalarUDFImpl for RegexExtract {
    fn name(&self) -> &str {
        "flink_regexp_extract"
    }
    fn signature(&self) -> &Signature {
        &self.signature
    }
    fn return_type(&self, types: &[DataType]) -> Result<DataType> {
        if types != [DataType::Utf8] {
            return Err(DataFusionError::Plan(
                "REGEXP_EXTRACT requires VARCHAR".into(),
            ));
        }
        Ok(DataType::Utf8)
    }
    fn is_strict(&self) -> bool {
        true
    }
    fn invoke_with_args(&self, args: ScalarFunctionArgs) -> Result<ColumnarValue> {
        let value = args.args.first().ok_or_else(|| {
            DataFusionError::Execution("REGEXP_EXTRACT requires one value".into())
        })?;
        let scalar = matches!(value, ColumnarValue::Scalar(_));
        let rows = if scalar { 1 } else { args.number_rows };
        let batch =
            RecordBatch::try_new(self.schema.clone(), vec![value.clone().into_array(rows)?])?;
        let result = self.kernel.evaluate(&batch)?.into_array(rows)?;
        if scalar {
            Ok(ColumnarValue::Scalar(ScalarValue::try_from_array(
                &result, 0,
            )?))
        } else {
            Ok(ColumnarValue::Array(result))
        }
    }
}

pub(crate) fn create(
    value: Arc<dyn PhysicalExpr>,
    pattern: &str,
    schema: &Schema,
) -> Result<Arc<dyn PhysicalExpr>> {
    pattern::validate(pattern)?;
    let inner_schema = Arc::new(Schema::new(vec![Field::new("value", DataType::Utf8, true)]));
    let captures = Arc::new(ScalarFunctionExpr::try_new(
        datafusion_functions::regex::regexp_match(),
        vec![
            Arc::new(Column::new("value", 0)),
            Arc::new(Literal::new(ScalarValue::Utf8(Some(pattern.into())))),
        ],
        &inner_schema,
        Arc::new(ConfigOptions::new()),
    )?);
    // Arrow omits unmatched groups. Exactly one projected capture prevents index shifting.
    // DataFusion returns NULL for an empty list and preserves a participating empty string.
    let kernel = super::array_element::create(captures, 1, &inner_schema)?;
    let function = RegexExtract {
        pattern: pattern.into(),
        signature: Signature::exact(vec![DataType::Utf8], Volatility::Immutable),
        schema: inner_schema,
        kernel,
    };
    function.return_type(&[value.data_type(schema)?])?;
    Ok(Arc::new(ScalarFunctionExpr::try_new(
        Arc::new(ScalarUDF::new_from_impl(function)),
        vec![value],
        schema,
        Arc::new(ConfigOptions::new()),
    )?))
}
