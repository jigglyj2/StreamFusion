// Copyright 2026 StreamFusion Authors
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0

use std::sync::Arc;

use arrow::array::{new_empty_array, ArrayRef, RecordBatch, RecordBatchOptions};
use arrow::datatypes::{Field, Schema};
use datafusion::datasource::memory::MemorySourceConfig;
use datafusion::error::{DataFusionError, Result};
use datafusion::physical_plan::ExecutionPlan;
use datafusion::scalar::ScalarValue;

use super::calc::literal_scalar;
use crate::{planner::expressions::null_literal, proto};

pub(crate) fn create(values: &proto::Values) -> Result<Arc<dyn ExecutionPlan>> {
    let declared_schema = values
        .schema
        .as_ref()
        .ok_or_else(|| DataFusionError::Plan("VALUES has no declared schema".to_string()))?;
    let fields = declared_schema
        .fields
        .iter()
        .map(|field| {
            let logical_type = field.r#type.as_ref().ok_or_else(|| {
                DataFusionError::Plan(format!("VALUES field {} has no declared type", field.name))
            })?;
            Ok(Field::new(
                &field.name,
                null_literal::data_type(logical_type)?,
                logical_type.nullable,
            ))
        })
        .collect::<Result<Vec<_>>>()?;
    let schema = Arc::new(Schema::new(fields));
    let arrays = columns(values, schema.as_ref())?;
    let options = RecordBatchOptions::new().with_row_count(Some(values.rows.len()));
    let batch = RecordBatch::try_new_with_options(Arc::clone(&schema), arrays, &options)?;
    Ok(MemorySourceConfig::try_new_exec(
        &[vec![batch]],
        schema,
        None,
    )?)
}

fn columns(values: &proto::Values, schema: &Schema) -> Result<Vec<ArrayRef>> {
    if values.rows.is_empty() {
        return Ok(schema
            .fields()
            .iter()
            .map(|field| new_empty_array(field.data_type()))
            .collect());
    }
    let width = schema.fields().len();
    let mut columns = vec![Vec::with_capacity(values.rows.len()); width];
    for (row_index, row) in values.rows.iter().enumerate() {
        if row.values.len() != width {
            return Err(DataFusionError::Plan(format!(
                "VALUES row {row_index} has {} fields, expected {width}",
                row.values.len()
            )));
        }
        for (column_index, expression) in row.values.iter().enumerate() {
            let scalar = literal_scalar(expression)?.ok_or_else(|| {
                DataFusionError::Plan(format!(
                    "VALUES row {row_index} field {column_index} is not a literal expression"
                ))
            })?;
            if scalar.data_type() != *schema.field(column_index).data_type() {
                return Err(DataFusionError::Plan(format!(
                    "VALUES row {row_index} field {column_index} has type {}, expected {}",
                    scalar.data_type(),
                    schema.field(column_index).data_type()
                )));
            }
            columns[column_index].push(scalar);
        }
    }
    columns
        .into_iter()
        .map(ScalarValue::iter_to_array)
        .collect()
}

#[cfg(test)]
mod tests {
    use arrow::array::{Int32Array, StringArray};
    use datafusion::physical_plan::collect;
    use datafusion::prelude::SessionContext;

    use super::*;

    #[tokio::test]
    async fn creates_nullable_literal_columns_without_an_external_input() {
        let integer = logical_type(
            proto::logical_type::Type::Integer(proto::EmptyType {}),
            true,
        );
        let string = logical_type(
            proto::logical_type::Type::Varchar(proto::EmptyType {}),
            true,
        );
        let values = proto::Values {
            schema: Some(proto::Schema {
                fields: vec![field("id", integer.clone()), field("name", string.clone())],
            }),
            rows: vec![
                row(integer_literal(1), string_literal("one")),
                row(null_literal(integer), string_literal("two")),
            ],
        };

        let output = collect(create(&values).unwrap(), SessionContext::new().task_ctx())
            .await
            .unwrap();
        let ids = output[0]
            .column(0)
            .as_any()
            .downcast_ref::<Int32Array>()
            .unwrap();
        let names = output[0]
            .column(1)
            .as_any()
            .downcast_ref::<StringArray>()
            .unwrap();

        assert_eq!(ids.iter().collect::<Vec<_>>(), vec![Some(1), None]);
        assert_eq!(
            names.iter().collect::<Vec<_>>(),
            vec![Some("one"), Some("two")]
        );
    }

    #[tokio::test]
    async fn preserves_the_row_count_of_a_zero_column_seed() {
        let values = proto::Values {
            schema: Some(proto::Schema { fields: vec![] }),
            rows: vec![proto::ValuesRow { values: vec![] }],
        };

        let output = collect(create(&values).unwrap(), SessionContext::new().task_ctx())
            .await
            .unwrap();

        assert_eq!(output.len(), 1);
        assert_eq!(output[0].num_columns(), 0);
        assert_eq!(output[0].num_rows(), 1);
    }

    fn logical_type(r#type: proto::logical_type::Type, nullable: bool) -> proto::LogicalType {
        proto::LogicalType {
            nullable,
            r#type: Some(r#type),
        }
    }

    fn field(name: &str, r#type: proto::LogicalType) -> proto::Field {
        proto::Field {
            name: name.to_string(),
            r#type: Some(r#type),
        }
    }

    fn row(first: proto::Expression, second: proto::Expression) -> proto::ValuesRow {
        proto::ValuesRow {
            values: vec![first, second],
        }
    }

    fn integer_literal(value: i32) -> proto::Expression {
        proto::Expression {
            expression: Some(proto::expression::Expression::IntegerLiteral(
                proto::IntegerLiteral { value },
            )),
        }
    }

    fn string_literal(value: &str) -> proto::Expression {
        proto::Expression {
            expression: Some(proto::expression::Expression::StringLiteral(
                proto::StringLiteral {
                    value: value.to_string(),
                },
            )),
        }
    }

    fn null_literal(r#type: proto::LogicalType) -> proto::Expression {
        proto::Expression {
            expression: Some(proto::expression::Expression::NullLiteral(
                proto::NullLiteral {
                    r#type: Some(r#type),
                },
            )),
        }
    }
}
