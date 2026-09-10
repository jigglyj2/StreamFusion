// Copyright 2026 StreamFusion Authors. Licensed under the Apache License, Version 2.0.
//! Flink's zero-based SPLIT_INDEX using DataFusion splitting and list extraction.
use arrow::array::RecordBatch;
use arrow::datatypes::{DataType, Field, Schema};
use datafusion::common::config::ConfigOptions;
use datafusion::error::{DataFusionError, Result};
use datafusion::logical_expr::{
    ColumnarValue, Operator, ScalarFunctionArgs, ScalarUDF, ScalarUDFImpl, Signature, Volatility,
};
use datafusion::physical_expr::expressions::{BinaryExpr, CaseExpr, CastExpr, Column, Literal};
use datafusion::physical_expr::{PhysicalExpr, ScalarFunctionExpr};
use datafusion::scalar::ScalarValue;
use std::hash::{Hash, Hasher};
use std::sync::Arc;
pub(super) mod memory;
#[cfg(test)]
mod tests;

#[derive(Debug)]
pub(super) struct SplitIndex {
    delimiter: String,
    signature: Signature,
    schema: Arc<Schema>,
    kernel: Arc<dyn PhysicalExpr>,
}
impl PartialEq for SplitIndex {
    fn eq(&self, other: &Self) -> bool {
        self.delimiter == other.delimiter
    }
}
impl Eq for SplitIndex {}
impl Hash for SplitIndex {
    fn hash<H: Hasher>(&self, state: &mut H) {
        self.delimiter.hash(state);
    }
}
impl ScalarUDFImpl for SplitIndex {
    fn name(&self) -> &str {
        "flink_split_index"
    }
    fn signature(&self) -> &Signature {
        &self.signature
    }
    fn return_type(&self, types: &[DataType]) -> Result<DataType> {
        if types != [DataType::Utf8, DataType::Int64] {
            return Err(DataFusionError::Plan(
                "SPLIT_INDEX requires Utf8 and Int64".into(),
            ));
        }
        Ok(DataType::Utf8)
    }
    fn is_strict(&self) -> bool {
        true
    }
    fn invoke_with_args(&self, args: ScalarFunctionArgs) -> Result<ColumnarValue> {
        let scalar = args
            .args
            .iter()
            .all(|value| matches!(value, ColumnarValue::Scalar(_)));
        let rows = if scalar { 1 } else { args.number_rows };
        let columns = args
            .args
            .into_iter()
            .map(|value| value.into_array(rows))
            .collect::<Result<Vec<_>>>()?;
        let batch = RecordBatch::try_new(self.schema.clone(), columns)?;
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
    delimiter: &str,
    index: Arc<dyn PhysicalExpr>,
    schema: &Schema,
) -> Result<Arc<dyn PhysicalExpr>> {
    if delimiter.is_empty() {
        return Err(DataFusionError::Plan(
            "SPLIT_INDEX delimiter must be nonempty".into(),
        ));
    }
    if value.data_type(schema)? != DataType::Utf8 || index.data_type(schema)? != DataType::Int32 {
        return Err(DataFusionError::Plan(
            "SPLIT_INDEX requires VARCHAR and INTEGER".into(),
        ));
    }
    let inner_schema = Arc::new(Schema::new(vec![
        Field::new("value", DataType::Utf8, true),
        Field::new("index", DataType::Int64, true),
    ]));
    let parts = Arc::new(ScalarFunctionExpr::try_new(
        datafusion_functions_nested::string::string_to_array_udf(),
        vec![
            Arc::new(Column::new("value", 0)),
            Arc::new(Literal::new(ScalarValue::Utf8(Some(delimiter.into())))),
        ],
        &inner_schema,
        Arc::new(ConfigOptions::new()),
    )?);
    // Arrow List uses i32 offsets. DF rejects index i32::MAX + 1 instead of returning
    // NULL. Flink's zero-based INTEGER maximum can never address such a list; reject
    // it along with negative indices before invoking DF's one-based extraction.
    let one_based = Arc::new(BinaryExpr::new(
        Arc::new(Column::new("index", 1)),
        Operator::Plus,
        Arc::new(Literal::new(ScalarValue::Int64(Some(1)))),
    ));
    let index_column = Arc::new(Column::new("index", 1)) as Arc<dyn PhysicalExpr>;
    let valid = Arc::new(BinaryExpr::new(
        Arc::new(BinaryExpr::new(
            index_column.clone(),
            Operator::GtEq,
            Arc::new(Literal::new(ScalarValue::Int64(Some(0)))),
        )),
        Operator::And,
        Arc::new(BinaryExpr::new(
            index_column,
            Operator::Lt,
            Arc::new(Literal::new(ScalarValue::Int64(Some(i32::MAX as i64)))),
        )),
    ));
    let element = Arc::new(ScalarFunctionExpr::try_new(
        datafusion_functions_nested::extract::array_element_udf(),
        vec![parts, one_based],
        &inner_schema,
        Arc::new(ConfigOptions::new()),
    )?);
    let kernel = Arc::new(CaseExpr::try_new(
        None,
        vec![(valid, element)],
        Some(Arc::new(Literal::new(ScalarValue::Utf8(None)))),
    )?);
    let function = SplitIndex {
        delimiter: delimiter.into(),
        signature: Signature::exact(vec![DataType::Utf8, DataType::Int64], Volatility::Immutable),
        schema: inner_schema,
        kernel,
    };
    Ok(Arc::new(ScalarFunctionExpr::try_new(
        Arc::new(ScalarUDF::new_from_impl(function)),
        vec![value, Arc::new(CastExpr::new(index, DataType::Int64, None))],
        schema,
        Arc::new(ConfigOptions::new()),
    )?))
}
