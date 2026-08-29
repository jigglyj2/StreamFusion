// Copyright 2026 StreamFusion Authors
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0

use std::sync::Arc;

use datafusion::error::{DataFusionError, Result};
use datafusion::physical_expr::expressions::Column;
use datafusion::physical_plan::projection::ProjectionExec;
use datafusion::physical_plan::union::UnionExec;
use datafusion::physical_plan::ExecutionPlan;

use super::calc::create_expression;
use crate::proto;

pub(crate) fn create(
    expand: &proto::Expand,
    child: Arc<dyn ExecutionPlan>,
) -> Result<Arc<dyn ExecutionPlan>> {
    if expand.projections.is_empty() {
        return Err(DataFusionError::Plan(
            "StreamFusion Expand requires at least one projection".to_string(),
        ));
    }
    let output_width = expand.projections[0].expressions.len();
    let child_schema = child.schema();
    let ordinal_index = child_schema.fields().len().checked_sub(1).ok_or_else(|| {
        DataFusionError::Plan("StreamFusion Expand input has no selection ordinal".to_string())
    })?;
    let ordinal_name = child_schema.field(ordinal_index).name().clone();
    let projections = expand
        .projections
        .iter()
        .enumerate()
        .map(|(projection_index, projection)| {
            if projection.expressions.len() != output_width {
                return Err(DataFusionError::Plan(format!(
                    "Expand projection {projection_index} has {} expressions, expected {output_width}",
                    projection.expressions.len()
                )));
            }
            let mut expressions = projection
                .expressions
                .iter()
                .enumerate()
                .map(|(index, expression)| {
                    Ok((
                        create_expression(expression, child_schema.as_ref())?,
                        format!("expand_{index}"),
                    ))
                })
                .collect::<Result<Vec<_>>>()?;
            expressions.push((
                Arc::new(Column::new(&ordinal_name, ordinal_index)),
                ordinal_name.clone(),
            ));
            Ok(Arc::new(ProjectionExec::try_new(expressions, Arc::clone(&child))?)
                as Arc<dyn ExecutionPlan>)
        })
        .collect::<Result<Vec<_>>>()?;
    UnionExec::try_new(projections)
}

#[cfg(test)]
mod tests {
    use arrow::array::{Int32Array, RecordBatch};
    use arrow::datatypes::{DataType, Field, Schema};
    use datafusion::datasource::memory::MemorySourceConfig;
    use datafusion::physical_plan::collect;
    use datafusion::prelude::SessionContext;

    use super::*;

    #[tokio::test]
    async fn emits_every_projection_and_preserves_selection_ordinals() {
        let schema = Arc::new(Schema::new(vec![
            Field::new("id", DataType::Int32, false),
            Field::new("__streamfusion_input_row", DataType::Int32, false),
        ]));
        let batch = RecordBatch::try_new(
            Arc::clone(&schema),
            vec![
                Arc::new(Int32Array::from(vec![10, 20])),
                Arc::new(Int32Array::from(vec![0, 1])),
            ],
        )
        .unwrap();
        let child = MemorySourceConfig::try_new_exec(&[vec![batch]], schema, None).unwrap();
        let expand = proto::Expand {
            input: None,
            projections: vec![projection(0), projection(1)],
        };

        let output = collect(
            create(&expand, child).unwrap(),
            SessionContext::new().task_ctx(),
        )
        .await
        .unwrap();

        assert_eq!(output.len(), 2);
        assert_eq!(int_values(&output[0], 0), &[10, 20]);
        assert_eq!(int_values(&output[0], 1), &[0, 0]);
        assert_eq!(int_values(&output[0], 2), &[0, 1]);
        assert_eq!(int_values(&output[1], 1), &[1, 1]);
        assert_eq!(int_values(&output[1], 2), &[0, 1]);
    }

    fn projection(grouping_id: i32) -> proto::ExpandProjection {
        proto::ExpandProjection {
            expressions: vec![
                proto::Expression {
                    expression: Some(proto::expression::Expression::InputReference(
                        proto::InputReference {
                            index: 0,
                            r#type: None,
                        },
                    )),
                },
                proto::Expression {
                    expression: Some(proto::expression::Expression::IntegerLiteral(
                        proto::IntegerLiteral { value: grouping_id },
                    )),
                },
            ],
        }
    }

    fn int_values(batch: &RecordBatch, index: usize) -> &[i32] {
        batch
            .column(index)
            .as_any()
            .downcast_ref::<Int32Array>()
            .unwrap()
            .values()
    }
}
