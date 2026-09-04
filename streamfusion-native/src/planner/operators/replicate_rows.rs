// Copyright 2026 StreamFusion Authors
//
// Licensed under the Apache License, Version 2.0

use std::fmt::{Debug, Formatter};
use std::sync::Arc;

use arrow::array::{Array, ArrayRef, Int64Array, RecordBatch, UInt32Array};
use arrow::compute::take;
use arrow::datatypes::{Schema, SchemaRef};
use datafusion::common::tree_node::TreeNodeRecursion;
use datafusion::error::{DataFusionError, Result};
use datafusion::execution::memory_pool::{MemoryConsumer, MemoryReservation};
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

const INPUT_ROW_COLUMN: &str = "__streamfusion_input_row";
const MAX_OUTPUT_ROWS_PER_BATCH: usize = 16_384;

pub(crate) fn create(
    replicate: &proto::ReplicateRows,
    child: Arc<dyn ExecutionPlan>,
) -> Result<Arc<dyn ExecutionPlan>> {
    let child_schema = child.schema();
    let ordinal_index = child_schema.fields().len().checked_sub(1).ok_or_else(|| {
        DataFusionError::Plan("replicate rows input has no input-row ordinal".to_string())
    })?;
    if child_schema.field(ordinal_index).name() != INPUT_ROW_COLUMN {
        return Err(DataFusionError::Plan(
            "replicate rows input-row ordinal must be the final column".to_string(),
        ));
    }
    let repetition = create_expression(
        replicate.repetition.as_ref().ok_or_else(|| {
            DataFusionError::Plan("replicate rows has no repetition expression".to_string())
        })?,
        child_schema.as_ref(),
    )?;
    if repetition.return_field(child_schema.as_ref())?.data_type()
        != &arrow::datatypes::DataType::Int64
    {
        return Err(DataFusionError::Plan(
            "replicate rows repetition expression must return BIGINT".to_string(),
        ));
    }
    let values = replicate
        .values
        .iter()
        .map(|value| create_expression(value, child_schema.as_ref()))
        .collect::<Result<Vec<_>>>()?;
    if values.is_empty() {
        return Err(DataFusionError::Plan(
            "replicate rows requires at least one value expression".to_string(),
        ));
    }
    let mut fields = child_schema.fields()[..ordinal_index]
        .iter()
        .map(|field| field.as_ref().clone())
        .collect::<Vec<_>>();
    for (index, value) in values.iter().enumerate() {
        fields.push(
            value
                .return_field(child_schema.as_ref())?
                .as_ref()
                .clone()
                .with_name(format!("__streamfusion_replicate_value_{index}")),
        );
    }
    fields.push(child_schema.field(ordinal_index).as_ref().clone());
    Ok(Arc::new(ReplicateRowsExec::new(
        child,
        repetition,
        values,
        Arc::new(Schema::new(fields)),
    )))
}

struct ReplicateRowsExec {
    input: Arc<dyn ExecutionPlan>,
    repetition: Arc<dyn PhysicalExpr>,
    values: Vec<Arc<dyn PhysicalExpr>>,
    schema: SchemaRef,
    properties: Arc<PlanProperties>,
}

impl ReplicateRowsExec {
    fn new(
        input: Arc<dyn ExecutionPlan>,
        repetition: Arc<dyn PhysicalExpr>,
        values: Vec<Arc<dyn PhysicalExpr>>,
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
            repetition,
            values,
            schema,
            properties,
        }
    }
}

impl Debug for ReplicateRowsExec {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("ReplicateRowsExec")
            .field("value_count", &self.values.len())
            .field("schema", &self.schema)
            .finish_non_exhaustive()
    }
}

impl DisplayAs for ReplicateRowsExec {
    fn fmt_as(&self, _: DisplayFormatType, f: &mut Formatter<'_>) -> std::fmt::Result {
        write!(f, "StreamFusionReplicateRowsExec")
    }
}

impl ExecutionPlan for ReplicateRowsExec {
    fn name(&self) -> &'static str {
        "StreamFusionReplicateRowsExec"
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
        if f(&self.repetition)? == TreeNodeRecursion::Stop {
            return Ok(TreeNodeRecursion::Stop);
        }
        for value in &self.values {
            if f(value)? == TreeNodeRecursion::Stop {
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
                "ReplicateRows expected one child, got {}",
                children.len()
            )));
        }
        Ok(Arc::new(Self::new(
            children.remove(0),
            Arc::clone(&self.repetition),
            self.values.clone(),
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
        let repetition = Arc::clone(&self.repetition);
        let values = self.values.clone();
        let reservation = MemoryConsumer::new("StreamFusionReplicateRowsExec")
            .register(&context.runtime_env().memory_pool);
        let stream = self.input.execute(partition, context)?.flat_map(
            move |batch| -> futures::stream::BoxStream<'static, Result<RecordBatch>> {
                match batch.and_then(|batch| {
                    ReplicationWork::new(
                        batch,
                        repetition.as_ref(),
                        &values,
                        Arc::clone(&output_schema),
                        reservation.new_empty(),
                    )
                }) {
                    Ok(work) => futures::stream::unfold(Some(work), |work| async move {
                        let mut work = work?;
                        let output = work.next_batch();
                        let next = if work.is_finished() { None } else { Some(work) };
                        Some((output, next))
                    })
                    .boxed(),
                    Err(error) => futures::stream::once(async move { Err(error) }).boxed(),
                }
            },
        );
        Ok(Box::pin(RecordBatchStreamAdapter::new(schema, stream)))
    }
}

struct ReplicationWork {
    schema: SchemaRef,
    sources: Vec<ArrayRef>,
    counts: Int64Array,
    input_rows: usize,
    row: usize,
    emitted_for_row: usize,
    reservation: MemoryReservation,
}

impl ReplicationWork {
    fn new(
        batch: RecordBatch,
        repetition: &dyn PhysicalExpr,
        values: &[Arc<dyn PhysicalExpr>],
        schema: SchemaRef,
        reservation: MemoryReservation,
    ) -> Result<Self> {
        let input_rows = batch.num_rows();
        if input_rows > u32::MAX as usize {
            return Err(DataFusionError::Execution(
                "replicate rows input exceeds u32 rows".to_string(),
            ));
        }
        let counts = repetition.evaluate(&batch)?.into_array(input_rows)?;
        let counts = counts
            .as_any()
            .downcast_ref::<Int64Array>()
            .ok_or_else(|| {
                DataFusionError::Execution("replicate rows count is not BIGINT".to_string())
            })?
            .clone();
        for row in 0..input_rows {
            if counts.is_null(row) {
                return Err(DataFusionError::Execution(
                    "replicate rows count cannot be NULL".to_string(),
                ));
            }
        }
        let visible_count = batch.num_columns() - 1;
        let mut sources = batch.columns()[..visible_count].to_vec();
        sources.extend(
            values
                .iter()
                .map(|value| value.evaluate(&batch)?.into_array(input_rows))
                .collect::<Result<Vec<ArrayRef>>>()?,
        );
        sources.push(Arc::clone(batch.column(visible_count)));
        let mut work = Self {
            schema,
            sources,
            counts,
            input_rows,
            row: 0,
            emitted_for_row: 0,
            reservation,
        };
        work.skip_empty_rows();
        Ok(work)
    }

    fn is_finished(&self) -> bool {
        self.row == self.input_rows
    }

    fn skip_empty_rows(&mut self) {
        while self.row < self.input_rows && self.counts.value(self.row) <= 0 {
            self.row += 1;
        }
    }

    fn next_batch(&mut self) -> Result<RecordBatch> {
        let mut indices = Vec::with_capacity(MAX_OUTPUT_ROWS_PER_BATCH);
        while self.row < self.input_rows && indices.len() < MAX_OUTPUT_ROWS_PER_BATCH {
            let repetitions = self.counts.value(self.row).max(0) as usize;
            let remaining = repetitions - self.emitted_for_row;
            let selected = remaining.min(MAX_OUTPUT_ROWS_PER_BATCH - indices.len());
            indices.extend(std::iter::repeat_n(self.row as u32, selected));
            self.emitted_for_row += selected;
            if self.emitted_for_row == repetitions {
                self.row += 1;
                self.emitted_for_row = 0;
                self.skip_empty_rows();
            }
        }
        let indices = UInt32Array::from(indices);
        let estimated_arrays = if self.input_rows == 0 {
            0
        } else {
            self.sources
                .iter()
                .map(|source| source.get_array_memory_size())
                .sum::<usize>()
                .saturating_mul(indices.len())
                .div_ceil(self.input_rows)
        };
        self.reservation.try_resize(
            estimated_arrays
                .saturating_add(indices.len().saturating_mul(std::mem::size_of::<u32>())),
        )?;
        let columns = self
            .sources
            .iter()
            .map(|source| take(source.as_ref(), &indices, None).map_err(DataFusionError::from))
            .collect::<Result<Vec<_>>>()?;
        let output = RecordBatch::try_new(Arc::clone(&self.schema), columns)?;
        self.reservation
            .try_resize(output.get_array_memory_size())?;
        Ok(output)
    }
}

#[cfg(test)]
mod tests {
    use arrow::array::{Int32Array, Int64Array};
    use arrow::datatypes::{DataType, Field, Schema};
    use datafusion::datasource::memory::MemorySourceConfig;
    use datafusion::physical_plan::collect;
    use datafusion::prelude::SessionContext;

    use super::*;

    #[tokio::test]
    async fn repeats_complete_rows_and_preserves_ordinals() {
        let schema = Arc::new(Schema::new(vec![
            Field::new("count", DataType::Int64, false),
            Field::new("value", DataType::Int32, true),
            Field::new(INPUT_ROW_COLUMN, DataType::Int32, false),
        ]));
        let batch = RecordBatch::try_new(
            Arc::clone(&schema),
            vec![
                Arc::new(Int64Array::from(vec![2, 0, 1])),
                Arc::new(Int32Array::from(vec![Some(7), None, Some(9)])),
                Arc::new(Int32Array::from(vec![0, 1, 2])),
            ],
        )
        .unwrap();
        let child = MemorySourceConfig::try_new_exec(&[vec![batch]], schema, None).unwrap();
        let plan = proto::ReplicateRows {
            input: None,
            repetition: Some(input(0)),
            values: vec![input(1)],
        };
        let output = collect(
            create(&plan, child).unwrap(),
            SessionContext::new().task_ctx(),
        )
        .await
        .unwrap();
        assert_eq!(
            output[0]
                .column(1)
                .as_any()
                .downcast_ref::<Int32Array>()
                .unwrap()
                .values(),
            &[7, 7, 9]
        );
        assert_eq!(
            output[0]
                .column(2)
                .as_any()
                .downcast_ref::<Int32Array>()
                .unwrap()
                .values(),
            &[7, 7, 9]
        );
        assert_eq!(
            output[0]
                .column(3)
                .as_any()
                .downcast_ref::<Int32Array>()
                .unwrap()
                .values(),
            &[0, 0, 2]
        );
    }

    #[tokio::test]
    async fn bounds_expansion_batch_size() {
        let schema = Arc::new(Schema::new(vec![
            Field::new("count", DataType::Int64, false),
            Field::new("value", DataType::Int32, false),
            Field::new(INPUT_ROW_COLUMN, DataType::Int32, false),
        ]));
        let batch = RecordBatch::try_new(
            Arc::clone(&schema),
            vec![
                Arc::new(Int64Array::from(vec![
                    (MAX_OUTPUT_ROWS_PER_BATCH + 1) as i64,
                ])),
                Arc::new(Int32Array::from(vec![7])),
                Arc::new(Int32Array::from(vec![0])),
            ],
        )
        .unwrap();
        let child = MemorySourceConfig::try_new_exec(&[vec![batch]], schema, None).unwrap();
        let plan = proto::ReplicateRows {
            input: None,
            repetition: Some(input(0)),
            values: vec![input(1)],
        };

        let output = collect(
            create(&plan, child).unwrap(),
            SessionContext::new().task_ctx(),
        )
        .await
        .unwrap();

        assert_eq!(output.len(), 2);
        assert_eq!(output[0].num_rows(), MAX_OUTPUT_ROWS_PER_BATCH);
        assert_eq!(output[1].num_rows(), 1);
    }

    fn input(index: u32) -> proto::Expression {
        proto::Expression {
            expression: Some(proto::expression::Expression::InputReference(
                proto::InputReference {
                    index,
                    r#type: None,
                },
            )),
        }
    }
}
