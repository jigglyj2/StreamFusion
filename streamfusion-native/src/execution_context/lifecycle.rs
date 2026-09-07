// Copyright 2026 StreamFusion Authors
// Licensed under the Apache License, Version 2.0.

//! Construct non-keyed task-lifetime resources before capability negotiation. This is an
//! operator constructor registry, not a registry of neighboring operator combinations.
//! Keyed resources are supplied separately by Flink's state-binding phase and merged here.

use super::*;
use crate::planner::operators::local_group_aggregate::execution_plan::LocalGroupAggregateFactory;

pub(super) fn task_local_bindings(
    plan: &proto::NativePlan,
    pool: &Arc<dyn MemoryPool>,
) -> Result<Vec<PersistentBinding>> {
    fn visit(
        node: &proto::Operator,
        pool: &Arc<dyn MemoryPool>,
        bindings: &mut Vec<PersistentBinding>,
    ) -> Result<()> {
        if let Some(proto::operator::Operator::LocalGroupAggregate(_)) = &node.operator {
            if node.plan_node_id == 0 || bindings.iter().any(|(id, _)| *id == node.plan_node_id) {
                return Err(DataFusionError::Plan(
                    "task-local native stages require unique positive plan-node IDs".into(),
                ));
            }
            let memory = crate::memory_pool::host_reservation(pool, "local aggregate bundle")?;
            bindings.push((
                node.plan_node_id,
                Arc::new(LocalGroupAggregateFactory::new(node, memory)?),
            ));
        }
        for child in crate::planner::persistent::children(node)? {
            visit(child, pool, bindings)?;
        }
        Ok(())
    }
    let mut bindings = Vec::new();
    if plan.protocol_version >= crate::ENVELOPE_PLAN_PROTOCOL_VERSION {
        if let Some(root) = &plan.root {
            visit(root, pool, &mut bindings)?;
        }
    }
    Ok(bindings)
}
