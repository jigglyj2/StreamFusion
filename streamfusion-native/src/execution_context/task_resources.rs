// Copyright 2026 StreamFusion Authors
// Licensed under the Apache License, Version 2.0.

use super::*;
use crate::memory_pool::HostMemoryReservation;
use crate::planner::operators::local_window_aggregate::buffered::execution_plan::LocalWindowFactory;
use crate::planner::persistent::find_unique;
use prost::Message;
use std::collections::HashSet;

impl NativeExecutionContext {
    /// Bind non-keyed resources once, transactionally, before lowering or invocation. The
    /// wire contains resolved Flink capacities, not SQL plan fragments or native budgets.
    pub(crate) fn install_task_resources(
        &mut self,
        bytes: &[u8],
        memory: HostMemoryReservation,
    ) -> Result<()> {
        if self.task_resources_installed
            || self
                .physical_plan
                .get_mut()
                .map_err(|_| invalid("poisoned native plan"))?
                .is_some()
        {
            return Err(invalid(
                "native task resources must be installed once before lowering",
            ));
        }
        let credit = self.reservation("native task resource decode and setup");
        credit.try_grow(
            bytes
                .len()
                .checked_mul(64)
                .and_then(|n| n.checked_add(64 * 1024))
                .ok_or_else(|| invalid("native task resource admission overflow"))?,
        )?;
        let options = proto::NativeTaskBindings::decode(bytes)
            .map_err(|error| invalid(format!("invalid native task resource protobuf: {error}")))?;
        if options.protocol_version != 1
            || options.bindings.is_empty()
            || self.plan.protocol_version < crate::ENVELOPE_PLAN_PROTOCOL_VERSION
        {
            return Err(invalid(
                "unsupported or empty native task resource protocol",
            ));
        }
        let mut ids = HashSet::new();
        let mut bindings: Vec<PersistentBinding> = Vec::new();
        for binding in &options.bindings {
            let id = binding.plan_node_id;
            if id == 0
                || !ids.insert(id)
                || self.persistent.iter().any(|(existing, _)| *existing == id)
            {
                return Err(invalid(
                    "native task resources require unique positive unbound node IDs",
                ));
            }
            let node = find_unique(&self.plan, |node| node.plan_node_id == id)?;
            let Some(proto::native_task_binding::Resource::LocalWindowBuffer(buffer)) =
                &binding.resource
            else {
                return Err(invalid("missing or unsupported native task resource kind"));
            };
            if buffer.flink_buffer_memory_bytes == 0
                || buffer.flink_buffer_memory_bytes > i64::MAX as u64
            {
                return Err(invalid(
                    "Flink window buffer capacity must be a positive Java long",
                ));
            }
            let capacity = usize::try_from(buffer.flink_buffer_memory_bytes)
                .map_err(|_| invalid("Flink window buffer capacity exceeds native usize"))?;
            bindings.push((
                id,
                Arc::new(LocalWindowFactory::new(
                    node,
                    memory.sibling("local window shared state"),
                    capacity,
                    buffer.flink_page_bytes as usize,
                )?),
            ));
        }
        // A local window must never silently run without its original Flink capacity.
        fn required(node: &proto::Operator, ids: &HashSet<u64>) -> Result<()> {
            if matches!(
                node.operator,
                Some(proto::operator::Operator::LocalWindowAggregate(_))
            ) && !ids.contains(&node.plan_node_id)
            {
                return Err(invalid("native task resources omit a local window stage"));
            }
            for child in crate::planner::persistent::children(node)? {
                required(child, ids)?;
            }
            Ok(())
        }
        if let Some(root) = &self.plan.root {
            required(root, &ids)?;
        }
        self.bind_persistent(bindings)?;
        self.task_resources_installed = true;
        Ok(())
    }
}
fn invalid(message: impl Into<String>) -> DataFusionError {
    DataFusionError::Plan(message.into())
}
