// Copyright 2026 StreamFusion Authors
//
// Licensed under the Apache License, Version 2.0

use std::fmt::{Debug, Formatter};
use std::sync::{Arc, Mutex};

use arrow::array::RecordBatch;
use arrow::datatypes::SchemaRef;
use datafusion::common::tree_node::TreeNodeRecursion;
use datafusion::error::{DataFusionError, Result};
use datafusion::execution::TaskContext;
use datafusion::physical_expr::{EquivalenceProperties, PhysicalExpr};
use datafusion::physical_plan::execution_plan::{Boundedness, EmissionType};
use datafusion::physical_plan::memory::MemoryStream;
use datafusion::physical_plan::{
    DisplayAs, DisplayFormatType, ExecutionPlan, Partitioning, PlanProperties,
    SendableRecordBatchStream,
};

/// An Arrow input whose batch can be replaced while its surrounding physical plan is reused.
pub(crate) struct ReusableInputExec {
    schema: SchemaRef,
    batch: Mutex<Option<RecordBatch>>,
    properties: Arc<PlanProperties>,
}

impl ReusableInputExec {
    pub(crate) fn new(schema: SchemaRef) -> Self {
        let properties = Arc::new(PlanProperties::new(
            EquivalenceProperties::new(Arc::clone(&schema)),
            Partitioning::UnknownPartitioning(1),
            EmissionType::Incremental,
            Boundedness::Bounded,
        ));
        Self {
            schema,
            batch: Mutex::new(None),
            properties,
        }
    }

    pub(crate) fn replace_batch(&self, batch: RecordBatch) -> Result<()> {
        if batch.schema().as_ref() != self.schema.as_ref() {
            return Err(DataFusionError::Execution(format!(
                "native input schema changed from {:?} to {:?}",
                self.schema,
                batch.schema()
            )));
        }
        *self
            .batch
            .lock()
            .map_err(|_| DataFusionError::Internal("reusable input lock poisoned".to_string()))? =
            Some(batch);
        Ok(())
    }

    pub(crate) fn clear(&self) {
        if let Ok(mut batch) = self.batch.lock() {
            *batch = None;
        }
    }
}

impl Debug for ReusableInputExec {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("ReusableInputExec")
            .field("schema", &self.schema)
            .finish_non_exhaustive()
    }
}

impl DisplayAs for ReusableInputExec {
    fn fmt_as(&self, _: DisplayFormatType, f: &mut Formatter<'_>) -> std::fmt::Result {
        write!(f, "StreamFusionReusableInputExec")
    }
}

impl ExecutionPlan for ReusableInputExec {
    fn name(&self) -> &'static str {
        "StreamFusionReusableInputExec"
    }

    fn properties(&self) -> &Arc<PlanProperties> {
        &self.properties
    }

    fn children(&self) -> Vec<&Arc<dyn ExecutionPlan>> {
        Vec::new()
    }

    fn apply_expressions(
        &self,
        _: &mut dyn FnMut(&Arc<dyn PhysicalExpr>) -> Result<TreeNodeRecursion>,
    ) -> Result<TreeNodeRecursion> {
        Ok(TreeNodeRecursion::Continue)
    }

    fn with_new_children(
        self: Arc<Self>,
        children: Vec<Arc<dyn ExecutionPlan>>,
    ) -> Result<Arc<dyn ExecutionPlan>> {
        if !children.is_empty() {
            return Err(DataFusionError::Internal(format!(
                "reusable input expected no children, got {}",
                children.len()
            )));
        }
        Ok(self)
    }

    fn execute(&self, partition: usize, _: Arc<TaskContext>) -> Result<SendableRecordBatchStream> {
        if partition != 0 {
            return Err(DataFusionError::Execution(format!(
                "reusable input has no partition {partition}"
            )));
        }
        let batch = self
            .batch
            .lock()
            .map_err(|_| DataFusionError::Internal("reusable input lock poisoned".to_string()))?
            .clone()
            .ok_or_else(|| {
                DataFusionError::Execution("reusable input has no current Arrow batch".to_string())
            })?;
        Ok(Box::pin(MemoryStream::try_new(
            vec![batch],
            Arc::clone(&self.schema),
            None,
        )?))
    }
}

#[cfg(test)]
mod tests {
    use arrow::array::Int32Array;
    use arrow::datatypes::{DataType, Field, Schema};
    use datafusion::physical_plan::collect;
    use datafusion::prelude::SessionContext;

    use super::*;

    #[tokio::test]
    async fn executes_multiple_replacement_batches_through_the_same_node() {
        let schema = Arc::new(Schema::new(vec![Field::new("id", DataType::Int32, false)]));
        let input = Arc::new(ReusableInputExec::new(Arc::clone(&schema)));
        for values in [vec![1, 2], vec![3, 4]] {
            input
                .replace_batch(
                    RecordBatch::try_new(
                        Arc::clone(&schema),
                        vec![Arc::new(Int32Array::from(values.clone()))],
                    )
                    .unwrap(),
                )
                .unwrap();
            let output = collect(input.clone(), SessionContext::new().task_ctx())
                .await
                .unwrap();
            assert_eq!(
                output[0]
                    .column(0)
                    .as_any()
                    .downcast_ref::<Int32Array>()
                    .unwrap()
                    .values(),
                values.as_slice()
            );
        }
    }
}
