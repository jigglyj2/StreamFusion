// Copyright 2026 StreamFusion Authors
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
// http://www.apache.org/licenses/LICENSE-2.0

use std::sync::Arc;

use arrow::datatypes::{DataType, Schema};
use datafusion::common::config::ConfigOptions;
use datafusion::error::Result;
use datafusion::logical_expr::Operator;
use datafusion::physical_expr::expressions::{BinaryExpr, CaseExpr, CastExpr, Literal};
use datafusion::physical_expr::{PhysicalExpr, ScalarFunctionExpr};
use datafusion::scalar::ScalarValue;

pub(crate) fn create(
    array: Arc<dyn PhysicalExpr>,
    index: i64,
    schema: &Schema,
) -> Result<Arc<dyn PhysicalExpr>> {
    let index = Arc::new(Literal::new(ScalarValue::Int64(Some(index)))) as Arc<dyn PhysicalExpr>;
    Ok(Arc::new(ScalarFunctionExpr::try_new(
        datafusion_functions_nested::extract::array_element_udf(),
        vec![array, index],
        schema,
        Arc::new(ConfigOptions::new()),
    )?))
}

pub(crate) fn create_dynamic(
    array: Arc<dyn PhysicalExpr>,
    index: Arc<dyn PhysicalExpr>,
    schema: &Schema,
) -> Result<Arc<dyn PhysicalExpr>> {
    let index = Arc::new(CastExpr::new(index, DataType::Int64, None)) as Arc<dyn PhysicalExpr>;
    let positive = Arc::new(BinaryExpr::new(
        Arc::clone(&index),
        Operator::Gt,
        Arc::new(Literal::new(ScalarValue::Int64(Some(0)))),
    )) as Arc<dyn PhysicalExpr>;
    let element = Arc::new(ScalarFunctionExpr::try_new(
        datafusion_functions_nested::extract::array_element_udf(),
        vec![Arc::clone(&array), index],
        schema,
        Arc::new(ConfigOptions::new()),
    )?) as Arc<dyn PhysicalExpr>;
    let array_field = array.return_field(schema)?;
    let element_type = match array_field.data_type() {
        DataType::List(field) => field.data_type().clone(),
        other => {
            return Err(datafusion::error::DataFusionError::Plan(format!(
                "array element requires List input, got {other}"
            )))
        }
    };
    let null =
        Arc::new(Literal::new(ScalarValue::try_from(&element_type)?)) as Arc<dyn PhysicalExpr>;
    Ok(Arc::new(CaseExpr::try_new(
        None,
        vec![(positive, element)],
        Some(null),
    )?))
}

#[cfg(test)]
mod tests {
    use std::sync::Arc;

    use arrow::array::{Array, Int32Array, ListArray};
    use arrow::datatypes::{Field, Int32Type, Schema};
    use arrow::record_batch::RecordBatch;
    use datafusion::physical_expr::expressions::Column;

    use super::create_dynamic;

    #[test]
    fn computed_indexes_follow_flinks_positive_one_based_contract() {
        let arrays = Arc::new(ListArray::from_iter_primitive::<Int32Type, _, _>(vec![
            Some(vec![Some(10), None, Some(30)]),
            Some(vec![Some(10), None, Some(30)]),
            Some(vec![Some(10), None, Some(30)]),
            Some(vec![Some(10), None, Some(30)]),
            None,
        ]));
        let indexes = Arc::new(Int32Array::from(vec![
            Some(1),
            Some(0),
            Some(-1),
            Some(4),
            None,
        ]));
        let schema = Arc::new(Schema::new(vec![
            Field::new("arrays", arrays.data_type().clone(), true),
            Field::new("indexes", indexes.data_type().clone(), true),
        ]));
        let batch = RecordBatch::try_new(Arc::clone(&schema), vec![arrays, indexes]).unwrap();
        let output = create_dynamic(
            Arc::new(Column::new("arrays", 0)),
            Arc::new(Column::new("indexes", 1)),
            schema.as_ref(),
        )
        .unwrap()
        .evaluate(&batch)
        .unwrap()
        .into_array(batch.num_rows())
        .unwrap();
        let output = output.as_any().downcast_ref::<Int32Array>().unwrap();

        assert_eq!(output.value(0), 10);
        assert!(output.is_null(1));
        assert!(output.is_null(2));
        assert!(output.is_null(3));
        assert!(output.is_null(4));
    }
}
