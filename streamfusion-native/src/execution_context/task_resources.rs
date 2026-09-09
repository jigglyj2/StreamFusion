// Copyright 2026 StreamFusion Authors
// Licensed under the Apache License, Version 2.0.

use super::*;
use crate::memory_pool::HostMemoryReservation;
use crate::planner::operators::local_window_aggregate::buffered::execution_plan::LocalWindowFactory;
use prost::Message;
use std::collections::HashSet;

#[derive(Clone, Copy)]
pub(crate) struct WindowBuffer {
    pub(crate) capacity: usize,
    pub(crate) page: usize,
}

fn requires_buffer(node: &proto::Operator) -> bool {
    matches!(
        &node.operator,
        Some(proto::operator::Operator::LocalWindowAggregate(_))
    ) || matches!(&node.operator, Some(proto::operator::Operator::WindowAggregate(plan)) if plan.processing_time)
}

impl NativeExecutionContext {
    /// Bind buffer resources once, transactionally, before their keyed owner or lowering. The
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
        if !matches!(options.protocol_version, 1 | 2)
            || options.bindings.is_empty()
            || self.protocol_version() < crate::ENVELOPE_PLAN_PROTOCOL_VERSION
        {
            return Err(invalid(
                "unsupported or empty native task resource protocol",
            ));
        }
        let mut ids = HashSet::new();
        let mut bindings: Vec<PersistentBinding> = Vec::new();
        let mut processing_buffers = Vec::new();
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
            let node = self.plan.find_unique(|node| node.plan_node_id == id)?;
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
            if matches!(&node.operator, Some(proto::operator::Operator::WindowAggregate(plan)) if plan.processing_time)
            {
                if options.protocol_version < 2 {
                    return Err(invalid(
                        "processing-time buffer resources require protocol 2",
                    ));
                }
                crate::planner::operators::window_aggregate::shared_execution::validate_node(
                    node, 1,
                )?;
                crate::planner::operators::local_window_aggregate::buffered::BufferedWindow::validate_capacity(capacity, buffer.flink_page_bytes as usize)?;
                processing_buffers.push((
                    id,
                    WindowBuffer {
                        capacity,
                        page: buffer.flink_page_bytes as usize,
                    },
                ));
            } else {
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
        }
        // A local window must never silently run without its original Flink capacity.
        fn required(node: &proto::Operator, ids: &HashSet<u64>) -> Result<()> {
            if requires_buffer(node) && !ids.contains(&node.plan_node_id) {
                return Err(invalid("native task resources omit a local window stage"));
            }
            for child in crate::planner::persistent::children(node)? {
                required(child, ids)?;
            }
            Ok(())
        }
        for root in self.plan.roots() {
            required(root, &ids)?;
        }
        self.bind_persistent(bindings)?;
        self.processing_window_buffers = processing_buffers;
        self.task_resources_installed = true;
        Ok(())
    }
}
fn invalid(message: impl Into<String>) -> DataFusionError {
    DataFusionError::Plan(message.into())
}
