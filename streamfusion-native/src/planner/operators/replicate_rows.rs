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
use super::envelope::Envelope;
use crate::proto;

#[cfg(test)]
const INPUT_ROW_COLUMN: &str = "__streamfusion_input_row";
const MAX_OUTPUT_ROWS_PER_BATCH: usize = 16_384;
// A vectorization target, not an independent deployment memory budget. One unusually wide
// row is still admitted against the Flink pool; otherwise wide rows produce smaller batches.
const TARGET_OUTPUT_WORKSPACE_BYTES: usize = 8 << 20;

#[cfg(test)]
mod memory_tests;

pub(crate) fn create(
    replicate: &proto::ReplicateRows,
    child: Arc<dyn ExecutionPlan>,
) -> Result<Arc<dyn ExecutionPlan>> {
    let child_schema = child.schema();
    let envelope = Envelope::from_schema(child_schema.as_ref())?;
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
    let mut fields = child_schema.fields()[..envelope.payload_width]
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
    fields.extend(
        envelope
            .indices()
            .map(|index| child_schema.field(index).as_ref().clone()),
    );
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
        let registry = crate::memory_pool::buffer_registry(context.memory_pool());
        let stream = self.input.execute(partition, context)?.flat_map(
            move |batch| -> futures::stream::BoxStream<'static, Result<RecordBatch>> {
                match batch.and_then(|batch| {
                    ReplicationWork::new(
                        batch,
                        repetition.as_ref(),
                        &values,
                        Arc::clone(&output_schema),
                        reservation.new_empty(),
                        registry.clone(),
                    )
                }) {
                    Ok(work) => futures::stream::unfold(Some(work), |work| async move {
                        let mut work = work?;
                        let output = work.next_batch();
                        let next = if output.is_err() || work.is_finished() {
                            None
                        } else {
                            Some(work)
                        };
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
    _sources_memory: MemoryReservation,
    reservation: MemoryReservation,
    registry: Option<Arc<crate::memory_pool::arrow_lease::Registry>>,
}

impl ReplicationWork {
    fn new(
        batch: RecordBatch,
        repetition: &dyn PhysicalExpr,
        values: &[Arc<dyn PhysicalExpr>],
        schema: SchemaRef,
        reservation: MemoryReservation,
        registry: Option<Arc<crate::memory_pool::arrow_lease::Registry>>,
    ) -> Result<Self> {
        let input_rows = batch.num_rows();
        if input_rows > u32::MAX as usize {
            return Err(DataFusionError::Execution(
                "replicate rows input exceeds u32 rows".to_string(),
            ));
        }
        let sources_memory = reservation.new_empty();
        sources_memory.try_grow(crate::memory_pool::selection::add(
            4096,
            crate::memory_pool::selection::multiply(schema.fields().len(), 64)?,
        )?)?;
        let counts = repetition.evaluate(&batch)?.into_array(input_rows)?;
        sources_memory.try_grow(counts.get_array_memory_size())?;
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
        let envelope = Envelope::from_schema(batch.schema().as_ref())?;
        let visible_count = envelope.payload_width;
        let mut sources = batch.columns()[..visible_count].to_vec();
        sources.extend(
            values
                .iter()
                .map(|value| {
                    let array = value.evaluate(&batch)?.into_array(input_rows)?;
                    sources_memory.try_grow(array.get_array_memory_size())?;
                    Ok(array)
                })
                .collect::<Result<Vec<ArrayRef>>>()?,
        );
        sources.extend(
            envelope
                .indices()
                .map(|index| Arc::clone(batch.column(index))),
        );
        let mut work = Self {
            schema,
            sources,
            counts,
            input_rows,
            row: 0,
            emitted_for_row: 0,
            _sources_memory: sources_memory,
            reservation,
            registry,
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
        use crate::memory_pool::selection;
        let fixed = selection::add(
            selection::fixed_allowance(&self.sources)?,
            MAX_OUTPUT_ROWS_PER_BATCH * std::mem::size_of::<u32>(),
        )?;
        self.reservation.try_resize(fixed)?;
        let mut indices = Vec::with_capacity(MAX_OUTPUT_ROWS_PER_BATCH);
        let mut row = self.row;
        let mut emitted_for_row = self.emitted_for_row;
        let mut selected_bytes = 0;
        while row < self.input_rows && indices.len() < MAX_OUTPUT_ROWS_PER_BATCH {
            let repetitions = self.counts.value(row).max(0) as usize;
            if repetitions == 0 {
                row += 1;
                continue;
            }
            let row_bytes = selection::row_allowance(&self.sources, row)?.max(1);
            let remaining_bytes = TARGET_OUTPUT_WORKSPACE_BYTES.saturating_sub(selected_bytes);
            let byte_rows = remaining_bytes / row_bytes;
            if byte_rows == 0 && !indices.is_empty() {
                break;
            }
            let remaining = repetitions - emitted_for_row;
            let selected = remaining
                .min(MAX_OUTPUT_ROWS_PER_BATCH - indices.len())
                .min(byte_rows.max(1));
            indices.extend(std::iter::repeat_n(row as u32, selected));
            selected_bytes =
                selection::add(selected_bytes, selection::multiply(row_bytes, selected)?)?;
            emitted_for_row += selected;
            if emitted_for_row == repetitions {
                row += 1;
                emitted_for_row = 0;
            }
        }
        // Admit the whole gather at once, never through per-input-row JNI callbacks. On
        // pressure, shorten the already admitted selection vector and recompute its runs.
        loop {
            match self
                .reservation
                .try_resize(selection::add(fixed, selected_bytes)?)
            {
                Ok(()) => break,
                Err(DataFusionError::ResourcesExhausted(_)) if indices.len() > 1 => {
                    indices.truncate(indices.len() / 2);
                    selected_bytes =
                        indices.chunk_by(|a, b| a == b).try_fold(0, |bytes, run| {
                            selection::add(
                                bytes,
                                selection::multiply(
                                    selection::row_allowance(&self.sources, run[0] as usize)?,
                                    run.len(),
                                )?,
                            )
                        })?;
                }
                Err(error) => return Err(error),
            }
        }
        // Derive the committed cursor from the admitted prefix, not the optimistic selection.
        if let Some(&last) = indices.last() {
            row = last as usize;
            emitted_for_row = if row == self.row {
                self.emitted_for_row
            } else {
                0
            };
            emitted_for_row += indices
                .iter()
                .rev()
                .take_while(|&&index| index == last)
                .count();
            if emitted_for_row == self.counts.value(row) as usize {
                row += 1;
                emitted_for_row = 0;
            }
        }
        let indices = UInt32Array::from(indices);
        let columns = self
            .sources
            .iter()
            .map(|source| take(source.as_ref(), &indices, None).map_err(DataFusionError::from))
            .collect::<Result<Vec<_>>>()?;
        let output = RecordBatch::try_new(Arc::clone(&self.schema), columns)?;
        drop(indices);
        self.reservation
            .try_resize(output.get_array_memory_size())?;
        let memory = self.reservation.split(output.get_array_memory_size());
        let output = crate::memory_pool::arrow_lease::datafusion_batch_registered(
            output,
            memory,
            self.registry.clone(),
        )?;
        self.row = row;
        self.emitted_for_row = emitted_for_row;
        self.skip_empty_rows();
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
