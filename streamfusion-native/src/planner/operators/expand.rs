// Copyright 2026 StreamFusion Authors
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0

use std::fmt::{Debug, Formatter};
use std::sync::Arc;

use arrow::array::{Array, ArrayRef};
use arrow::compute::interleave;
use arrow::datatypes::{Schema, SchemaRef};
use arrow::record_batch::RecordBatch;
use datafusion::common::tree_node::TreeNodeRecursion;
use datafusion::error::{DataFusionError, Result};
use datafusion::execution::memory_pool::MemoryConsumer;
use datafusion::execution::TaskContext;
use datafusion::physical_expr::{EquivalenceProperties, PhysicalExpr};
use datafusion::physical_plan::execution_plan::EmissionType;
use datafusion::physical_plan::stream::RecordBatchStreamAdapter;
use datafusion::physical_plan::{
    DisplayAs, DisplayFormatType, ExecutionPlan, ExecutionPlanProperties, PlanProperties,
    SendableRecordBatchStream,
};
use futures::StreamExt;

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
    let ordinal_field = Arc::clone(&child_schema.fields()[ordinal_index]);
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
            projection
                .expressions
                .iter()
                .map(|expression| create_expression(expression, child_schema.as_ref()))
                .collect::<Result<Vec<_>>>()
        })
        .collect::<Result<Vec<_>>>()?;
    let mut fields = projections[0]
        .iter()
        .enumerate()
        .map(|(index, expression)| {
            expression
                .return_field(child_schema.as_ref())
                .map(|field| field.as_ref().clone().with_name(format!("expand_{index}")))
        })
        .collect::<Result<Vec<_>>>()?;
    fields.push(ordinal_field.as_ref().clone());
    Ok(Arc::new(ExpandExec::new(
        child,
        projections,
        Arc::new(Schema::new(fields)),
    )))
}

/// Evaluates every projection for one Arrow input batch, then interleaves their rows in Flink's
/// input-row-major order. DataFusion's `UnionExec` emits projection-major batches, which changes
/// downstream mini-batch boundaries and therefore changes observable streaming changelogs.
struct ExpandExec {
    input: Arc<dyn ExecutionPlan>,
    projections: Vec<Vec<Arc<dyn PhysicalExpr>>>,
    schema: SchemaRef,
    properties: Arc<PlanProperties>,
}

impl ExpandExec {
    fn new(
        input: Arc<dyn ExecutionPlan>,
        projections: Vec<Vec<Arc<dyn PhysicalExpr>>>,
        schema: SchemaRef,
    ) -> Self {
        let properties = Arc::new(PlanProperties::new(
            EquivalenceProperties::new(Arc::clone(&schema)),
            input.output_partitioning().clone(),
            EmissionType::Incremental,
            input.boundedness(),
        ));
        Self {
            input,
            projections,
            schema,
            properties,
        }
    }
}

impl Debug for ExpandExec {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("ExpandExec")
            .field("projection_count", &self.projections.len())
            .field("schema", &self.schema)
            .finish_non_exhaustive()
    }
}

impl DisplayAs for ExpandExec {
    fn fmt_as(&self, _: DisplayFormatType, f: &mut Formatter<'_>) -> std::fmt::Result {
        write!(f, "StreamFusionExpandExec")
    }
}

impl ExecutionPlan for ExpandExec {
    fn name(&self) -> &'static str {
        "StreamFusionExpandExec"
    }

    fn properties(&self) -> &Arc<PlanProperties> {
        &self.properties
    }

    fn children(&self) -> Vec<&Arc<dyn ExecutionPlan>> {
        vec![&self.input]
    }

    fn apply_expressions(
        &self,
        f: &mut dyn FnMut(&Arc<dyn PhysicalExpr>) -> Result<TreeNodeRecursion>,
    ) -> Result<TreeNodeRecursion> {
        for expression in self.projections.iter().flatten() {
            if f(expression)? == TreeNodeRecursion::Stop {
                return Ok(TreeNodeRecursion::Stop);
            }
        }
        Ok(TreeNodeRecursion::Continue)
    }

    fn with_new_children(
        self: Arc<Self>,
        mut children: Vec<Arc<dyn ExecutionPlan>>,
    ) -> Result<Arc<dyn ExecutionPlan>> {
        if children.len() != 1 {
            return Err(DataFusionError::Internal(format!(
                "Expand expected one child, got {}",
                children.len()
            )));
        }
        Ok(Arc::new(Self::new(
            children.remove(0),
            self.projections.clone(),
            Arc::clone(&self.schema),
        )))
    }

    fn execute(
        &self,
        partition: usize,
        context: Arc<TaskContext>,
    ) -> Result<SendableRecordBatchStream> {
        let schema = Arc::clone(&self.schema);
        let output_schema = Arc::clone(&schema);
        let projections = self.projections.clone();
        let reservation = MemoryConsumer::new("StreamFusionExpandExec")
            .register(&context.runtime_env().memory_pool);
        let stream = self.input.execute(partition, context)?.map(move |batch| {
            let output = expand_batch(batch?, &projections, Arc::clone(&output_schema))?;
            reservation.try_resize(output.get_array_memory_size())?;
            Ok(output)
        });
        Ok(Box::pin(RecordBatchStreamAdapter::new(schema, stream)))
    }
}

fn expand_batch(
    batch: RecordBatch,
    projections: &[Vec<Arc<dyn PhysicalExpr>>],
    schema: SchemaRef,
) -> Result<RecordBatch> {
    let row_count = batch.num_rows();
    let projection_count = projections.len();
    let ordinal = Arc::clone(batch.column(batch.num_columns() - 1));
    let mut evaluated = projections
        .iter()
        .map(|projection| {
            let mut columns = projection
                .iter()
                .map(|expression| expression.evaluate(&batch)?.into_array(row_count))
                .collect::<Result<Vec<_>>>()?;
            columns.push(Arc::clone(&ordinal));
            Ok(columns)
        })
        .collect::<Result<Vec<Vec<ArrayRef>>>>()?;
    let indices = (0..row_count)
        .flat_map(|row| (0..projection_count).map(move |projection| (projection, row)))
        .collect::<Vec<_>>();
    let columns = (0..schema.fields().len())
        .map(|column| {
            let values = evaluated
                .iter()
                .map(|projection| projection[column].as_ref() as &dyn Array)
                .collect::<Vec<_>>();
            interleave(&values, &indices).map_err(DataFusionError::from)
        })
        .collect::<Result<Vec<_>>>()?;
    evaluated.clear();
    Ok(RecordBatch::try_new(schema, columns)?)
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

        assert_eq!(output.len(), 1);
        assert_eq!(int_values(&output[0], 0), &[10, 10, 20, 20]);
        assert_eq!(int_values(&output[0], 1), &[0, 1, 0, 1]);
        assert_eq!(int_values(&output[0], 2), &[0, 0, 1, 1]);
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
