// Copyright 2026 StreamFusion Authors
// Licensed under the Apache License, Version 2.0

use std::fmt;
use std::sync::{Arc, Mutex};

use datafusion::common::tree_node::TreeNodeRecursion;
use datafusion::error::{DataFusionError, Result};
use datafusion::execution::TaskContext;
use datafusion::physical_expr::{EquivalenceProperties, PhysicalExpr};
use datafusion::physical_plan::execution_plan::{Boundedness, EmissionType};
use datafusion::physical_plan::{
    DisplayAs, DisplayFormatType, ExecutionPlan, Partitioning, PlanProperties,
    SendableRecordBatchStream,
};

/// An already opened, finite Arrow snapshot supplied by a task-local source adapter.
/// Consuming the stream transfers its batch ownership; this node retains no second snapshot.
/// Re-execution is an error: the enclosing DataFusion CollectLeft join must reuse its cache.
pub(crate) struct LookupSnapshotExec {
    stream: Mutex<Option<SendableRecordBatchStream>>,
    properties: Arc<PlanProperties>,
}

impl LookupSnapshotExec {
    pub(crate) fn new(stream: SendableRecordBatchStream) -> Arc<Self> {
        let properties = Arc::new(PlanProperties::new(
            EquivalenceProperties::new(stream.schema()),
            Partitioning::UnknownPartitioning(1),
            EmissionType::Incremental,
            Boundedness::Bounded,
        ));
        Arc::new(Self {
            stream: Mutex::new(Some(stream)),
            properties,
        })
    }
}

impl fmt::Debug for LookupSnapshotExec {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("StreamFusionLookupSnapshotExec")
            .finish_non_exhaustive()
    }
}

impl DisplayAs for LookupSnapshotExec {
    fn fmt_as(&self, _: DisplayFormatType, f: &mut fmt::Formatter) -> fmt::Result {
        write!(f, "StreamFusionLookupSnapshotExec")
    }
}

impl ExecutionPlan for LookupSnapshotExec {
    fn name(&self) -> &'static str {
        "StreamFusionLookupSnapshotExec"
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
            return Err(DataFusionError::Plan(
                "lookup snapshot has no children".into(),
            ));
        }
        Ok(self)
    }

    fn execute(&self, partition: usize, _: Arc<TaskContext>) -> Result<SendableRecordBatchStream> {
        if partition != 0 {
            return Err(DataFusionError::Plan(
                "lookup snapshot has one task-local partition".into(),
            ));
        }
        self.stream
            .lock()
            .map_err(|_| DataFusionError::Internal("lookup snapshot lock poisoned".into()))?
            .take()
            .ok_or_else(|| {
                DataFusionError::Execution(
                    "lookup snapshot was already consumed; reload only when the task reopens"
                        .into(),
                )
            })
    }
}
