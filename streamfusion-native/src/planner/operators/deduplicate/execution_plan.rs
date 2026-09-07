// Copyright 2026 StreamFusion Authors
// Licensed under the Apache License, Version 2.0

use std::sync::{Arc, Mutex};

use arrow::datatypes::{Schema, SchemaRef};
use arrow::record_batch::RecordBatch;
use datafusion::error::{DataFusionError, Result};
use datafusion::physical_plan::ExecutionPlan;

use super::DeduplicateProcessor;
use crate::memory_pool::HostMemoryReservation;
use crate::planner::persistent::unary::{InvocationState, UnaryBatchProcessor, UnaryExec};
use crate::planner::persistent::PersistentOperatorFactory;
use crate::proto;

pub(crate) struct DeduplicateFactory(pub(crate) Arc<Mutex<DeduplicateProcessor>>);
impl PersistentOperatorFactory for DeduplicateFactory {
    fn supports_owned_envelope(&self) -> bool {
        true
    }
    fn snapshot(&self, key_group: u32) -> Result<crate::state::SnapshotBytes> {
        self.0
            .lock()
            .map_err(|_| poisoned())?
            .snapshot_key_group(key_group)
    }
    fn restore(&self, key_group: u32, bytes: &[u8]) -> Result<()> {
        self.0
            .lock()
            .map_err(|_| poisoned())?
            .restore_key_group(key_group, bytes)
    }
    fn checkpoint(&self, directory: &std::path::Path) -> Result<()> {
        self.0.lock().map_err(|_| poisoned())?.checkpoint(directory)
    }
    fn build(
        &self,
        node: &proto::Operator,
        mut children: Vec<Arc<dyn ExecutionPlan>>,
    ) -> Result<Arc<dyn ExecutionPlan>> {
        if !matches!(
            node.operator,
            Some(proto::operator::Operator::Deduplicate(_))
        ) || children.len() != 1
        {
            return Err(DataFusionError::Plan(
                "deduplicate binding requires a Deduplicate node with one child".into(),
            ));
        }
        Ok(Arc::new(
            DeduplicateExec::new(self.0.clone(), children.remove(0))?
                .with_node_id(node.plan_node_id),
        ))
    }
}

/// Only the kernel is specific to deduplication; stream and invocation mechanics are shared.
pub(crate) type DeduplicateExec = UnaryExec<DeduplicateProcessor>;

/// Admitted output waiting to be attached to its Arrow buffer owners by the shared lease helper.
pub(super) struct NativeOutput {
    pub(crate) batch: RecordBatch,
    pub(crate) _memory: HostMemoryReservation,
}

impl UnaryBatchProcessor for DeduplicateProcessor {
    const NAME: &'static str = "StreamFusionDeduplicateExec";

    fn invocation(&mut self) -> &mut InvocationState {
        &mut self.invocation
    }
    fn prepare_output_schema(&mut self, input: SchemaRef) -> Result<SchemaRef> {
        // SQL dedup consumes inserts. Changelog normalization has a separate storage contract.
        if self.plan.input_changelog {
            return Err(DataFusionError::Plan("DeduplicateExec requires insert-only SQL input; changelog normalization needs its own native node".into()));
        }
        self.prepare_schema(input.clone(), input.fields().len())?;
        let count = self.visible_count.expect("prepared schema");
        let fields = crate::planner::operators::envelope::selected_output_fields(&input, count)?;
        Ok(Arc::new(Schema::new(fields)))
    }
    fn process_batch(&mut self, input: RecordBatch) -> Result<RecordBatch> {
        let output = self.process_native(input)?;
        crate::memory_pool::arrow_lease::host_batch(output.batch, output._memory)
    }
}

pub(super) fn poisoned() -> DataFusionError {
    DataFusionError::Execution("deduplicate native state lock is poisoned".into())
}
