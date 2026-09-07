// Copyright 2026 StreamFusion Authors
// Licensed under the Apache License, Version 2.0

use super::execution_plan::{poisoned, DeduplicateFactory};
use super::DeduplicateProcessor;
use crate::execution_context::NativeExecutionContext;
use crate::memory_pool::HostMemoryReservation;
use crate::{planner::persistent::find_unique, proto};
use arrow::record_batch::RecordBatch;
use datafusion::error::Result;
use prost::Message;
use std::sync::{Arc, Mutex};

/// Compatibility state-lifecycle facade. Fusion, lowering, execution and metrics all belong to
/// the shared NativeExecutionContext; this adapter knows only how to construct dedup state.
pub(crate) struct DeduplicateHandle {
    pub(crate) processor: Arc<Mutex<DeduplicateProcessor>>,
    pub(crate) context: Arc<NativeExecutionContext>,
    control: HostMemoryReservation,
}
impl DeduplicateHandle {
    pub(crate) fn new(
        bytes: &[u8],
        control: HostMemoryReservation,
        create: impl FnOnce(&[u8]) -> Result<DeduplicateProcessor>,
    ) -> Result<Self> {
        let pool = control.datafusion_pool(control.available_capacity()?.unwrap_or(usize::MAX));
        let mut context = NativeExecutionContext::new(bytes, pool)?;
        let node = find_unique(context.plan(), |node| {
            matches!(
                node.operator,
                Some(proto::operator::Operator::Deduplicate(_))
            )
        })?;
        let id = node.plan_node_id;
        let mut copy = control.sibling("deduplicate lifecycle plan copy");
        copy.resize(context.plan().encoded_len())?;
        let bare = proto::NativePlan {
            protocol_version: context.plan().protocol_version,
            root: Some(node.clone()),
        }
        .encode_to_vec();
        let processor = Arc::new(Mutex::new(create(&bare)?));
        context.bind_persistent(vec![(id, Arc::new(DeduplicateFactory(processor.clone())))])?;
        Ok(Self {
            processor,
            context: Arc::new(context),
            control,
        })
    }
    pub(crate) fn process_arrow(&mut self, batch: RecordBatch) -> Result<RecordBatch> {
        self.processor
            .lock()
            .map_err(|_| poisoned())?
            .require_idle()?;
        self.context.execute_single_batch(
            vec![batch],
            self.control.sibling("native region Arrow export"),
        )
    }
    pub(crate) fn metrics(&self) -> Result<Vec<i64>> {
        self.context.metric_snapshot()
    }
}

#[cfg(test)]
mod tests;
