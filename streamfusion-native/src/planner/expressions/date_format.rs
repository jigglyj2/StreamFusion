// Copyright 2026 StreamFusion Authors
// Licensed under the Apache License, Version 2.0.
//! One managed scalar boundary around DataFusion's full-range numeric formatting tree.
use arrow::datatypes::{DataType, Field, Schema, TimeUnit};
use arrow::record_batch::RecordBatch;
use datafusion::common::config::ConfigOptions;
use datafusion::error::{DataFusionError, Result};
use datafusion::logical_expr::{
    ColumnarValue, ScalarFunctionArgs, ScalarUDF, ScalarUDFImpl, Signature, Volatility,
};
use datafusion::physical_expr::expressions::Column;
use datafusion::physical_expr::{PhysicalExpr, ScalarFunctionExpr};
use datafusion::scalar::ScalarValue;
use std::hash::{Hash, Hasher};
use std::sync::Arc;
mod kernel;
mod pattern;
#[cfg(test)]
mod tests;

#[derive(Debug)]
pub(super) struct NumericDateFormat {
    pattern: String,
    signature: Signature,
    schema: Arc<Schema>,
    kernel: Arc<dyn PhysicalExpr>,
}
impl PartialEq for NumericDateFormat {
    fn eq(&self, other: &Self) -> bool {
        self.pattern == other.pattern
    }
}
impl Eq for NumericDateFormat {}
impl Hash for NumericDateFormat {
    fn hash<H: Hasher>(&self, state: &mut H) {
        self.pattern.hash(state);
    }
}
impl NumericDateFormat {
    pub(super) fn bytes_per_row(&self) -> Result<usize> {
        // yyyy can expand from 4 to 10 ASCII bytes. Include all concurrently retained
        // format parts, concatenation, numeric adaptation arrays and scalar broadcasting.
        // The compiled expression tree is small plan metadata; this is one batch allowance.
        self.pattern
            .len()
            .checked_mul(24)
            .and_then(|n| n.checked_add(512))
            .ok_or_else(|| {
                DataFusionError::ResourcesExhausted("DATE_FORMAT workspace overflow".into())
            })
    }
}
impl ScalarUDFImpl for NumericDateFormat {
    fn name(&self) -> &str {
        "flink_date_format_numeric"
    }
    fn signature(&self) -> &Signature {
        &self.signature
    }
    fn return_type(&self, types: &[DataType]) -> Result<DataType> {
        if types != [DataType::Timestamp(TimeUnit::Millisecond, None)] {
            return Err(DataFusionError::Plan(
                "DATE_FORMAT numeric patterns require timezone-free TIMESTAMP(3)".into(),
            ));
        }
        Ok(DataType::Utf8)
    }
    fn is_strict(&self) -> bool {
        true
    }
    fn invoke_with_args(&self, args: ScalarFunctionArgs) -> Result<ColumnarValue> {
        let value = args.args.first().ok_or_else(|| {
            DataFusionError::Execution("DATE_FORMAT requires one timestamp".into())
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
    let inner_schema = Arc::new(Schema::new(vec![Field::new(
        "timestamp",
        DataType::Timestamp(TimeUnit::Millisecond, None),
        true,
    )]));
    let expression = kernel::create(
        Arc::new(Column::new("timestamp", 0)),
        pattern,
        &inner_schema,
    )?;
    let function = NumericDateFormat {
        pattern: pattern.into(),
        signature: Signature::exact(
            vec![DataType::Timestamp(TimeUnit::Millisecond, None)],
            Volatility::Immutable,
        ),
        schema: inner_schema,
        kernel: expression,
    };
    function.return_type(&[value.data_type(schema)?])?;
    Ok(Arc::new(ScalarFunctionExpr::try_new(
        Arc::new(ScalarUDF::new_from_impl(function)),
        vec![value],
        schema,
        Arc::new(ConfigOptions::new()),
    )?))
}
