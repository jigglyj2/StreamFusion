// Copyright 2026 StreamFusion Authors
// Licensed under the Apache License, Version 2.0.

use super::NativeExecutionContext;
use crate::planner::persistent::control::ControlEvent;
use datafusion::error::{DataFusionError, Result};

impl NativeExecutionContext {
    pub(crate) fn clock_input_bindings(&self) -> &[(u64, usize)] {
        &self.clock_inputs
    }

    pub(super) fn validate_clock_inputs(
        &self,
        batches: &[arrow::record_batch::RecordBatch],
    ) -> Result<()> {
        use crate::planner::operators::envelope::processing_time;
        for (port, batch) in batches.iter().enumerate() {
            let expected = self.clock_inputs.iter().any(|&(_, input)| input == port);
            if processing_time::column(batch)?.is_some() != expected {
                return Err(DataFusionError::Plan(format!(
                    "processing-time clock input at port {port} differs from negotiated capability"
                )));
            }
        }
        if self
            .clock_inputs
            .iter()
            .any(|&(_, port)| port >= batches.len())
        {
            return Err(DataFusionError::Plan(
                "processing-time input port is missing".into(),
            ));
        }
        Ok(())
    }

    /// One read per idle edge, not one JNI callback per native timer or row.
    pub(crate) fn processing_time_deadlines(&self) -> Result<Vec<i64>> {
        self.require_idle()?;
        // Two scalar descriptors per already-admitted plan binding fit its bounded control
        // headroom. Do not add per-poll host reservations for these ephemeral descriptors.
        let mut deadlines = Vec::with_capacity(self.persistent.len().saturating_mul(2));
        for (id, factory) in &self.persistent {
            if !factory.supports_control(ControlEvent::ProcessingTime(0)) {
                continue;
            }
            if let Some(deadline) = factory.next_processing_time_timer()? {
                deadlines.push(i64::try_from(*id).map_err(|_| {
                    DataFusionError::Plan(
                        "processing-time stage ID exceeds Java's positive long range".into(),
                    )
                })?);
                deadlines.push(deadline);
            }
        }
        Ok(deadlines)
    }
}

/// Resolve once while resources are bound, retaining only bounded per-plan scalar descriptors.
/// A shared region's anonymous input slots resolve through references, not through copied trees.
pub(super) fn resolve_inputs<'a>(
    plan: &super::Definition,
    bindings: impl Iterator<Item = &'a crate::planner::persistent::PersistentBinding>,
) -> Result<Vec<(u64, usize)>> {
    use crate::planner::persistent;
    use crate::proto;
    let mut result = Vec::new();
    for (id, factory) in bindings {
        let Some(local) = factory.processing_time_input() else {
            continue;
        };
        if plan.protocol_version() < crate::RECORD_POLICY_PLAN_PROTOCOL_VERSION
            || !factory.supports_owned_envelope()
            || !factory.supports_control(ControlEvent::ProcessingTime(0))
        {
            return Err(DataFusionError::Plan(
                "per-record clock inputs require owned Arrow envelopes and processing-time capability".into()));
        }
        let node = plan.find_unique(|node| node.plan_node_id == *id)?;
        let children = persistent::children(node)?;
        let Some(child) = children.get(local) else {
            return Err(DataFusionError::Plan(
                "processing-time clock input slot is out of range".into(),
            ));
        };
        let Some(proto::operator::Operator::Input(input)) = &child.operator else {
            return Err(DataFusionError::Plan(
                "processing-time clock owner must directly consume a native input edge".into(),
            ));
        };
        let port = match plan {
            super::Definition::Tree(_) => input.input_index as usize,
            super::Definition::Region(region) => {
                let stage = region
                    .message
                    .stages
                    .iter()
                    .position(|stage| {
                        stage
                            .operator
                            .as_ref()
                            .is_some_and(|node| node.plan_node_id == *id)
                    })
                    .unwrap();
                match region.inputs[stage].get(input.input_index as usize) {
                    Some(crate::planner::region::RegionInput::External(port)) => *port,
                    _ => return Err(DataFusionError::Plan(
                        "processing-time clock owner must directly consume an external region edge"
                            .into(),
                    )),
                }
            }
        };
        if port > i32::MAX as usize || result.iter().any(|&(_, previous)| previous == port) {
            return Err(DataFusionError::Plan(
                "per-record clock owners require distinct external input ports in Java's range"
                    .into(),
            ));
        }
        result.push((*id, port));
    }
    result.sort_unstable_by_key(|&(_, port)| port);
    Ok(result)
}
