// Copyright 2026 StreamFusion Authors
// Licensed under the Apache License, Version 2.0.

use arrow::array::RecordBatchReader;
use arrow::ffi_stream::{ArrowArrayStreamReader, FFI_ArrowArrayStream};

use super::*;
use crate::planner::operators::lookup_join::{LookupJoinFactory, LookupTable};

/// Addresses exist only for this synchronous task-open call, never in the portable plan.
pub(crate) struct LookupSource {
    pub(crate) node_id: u64,
    pub(crate) stream: *mut FFI_ArrowArrayStream,
}

impl NativeExecutionContext {
    /// Install all immutable sources transactionally before publishing the context. The caller
    /// keeps each C struct valid through this call and releases any stream not consumed on error.
    /// Producers must use this task's Flink-accounted allocator and retain credit in array owners.
    pub(crate) unsafe fn install_lookup_sources(&mut self, sources: &[LookupSource]) -> Result<()> {
        if self.protocol_version() < crate::RECORD_POLICY_PLAN_PROTOCOL_VERSION {
            return Err(invalid("lookup sources require native plan protocol 3"));
        }
        let metadata = self.reservation("lookup task resource bindings");
        metadata.try_grow(
            sources
                .len()
                .checked_mul(1024)
                .and_then(|n| n.checked_add(4096))
                .ok_or_else(|| invalid("lookup source binding admission overflow"))?,
        )?;
        // Validate every identity/address before moving a stream or invoking a source callback.
        for (index, source) in sources.iter().enumerate() {
            if source.node_id == 0
                || source.stream.is_null()
                || sources[..index]
                    .iter()
                    .any(|prior| prior.node_id == source.node_id || prior.stream == source.stream)
                || self.persistent.iter().any(|(id, _)| *id == source.node_id)
            {
                return Err(invalid(
                    "lookup sources require distinct streams and positive unbound node identities",
                ));
            }
            let node = self
                .plan
                .find_unique(|node| node.plan_node_id == source.node_id)?;
            if !matches!(&node.operator, Some(proto::operator::Operator::LookupJoin(spec)) if spec.kind == proto::LookupJoinKind::Inner as i32)
            {
                return Err(invalid("lookup source requires its own inner lookup node"));
            }
        }
        fn required(node: &proto::Operator, sources: &[LookupSource]) -> Result<()> {
            if matches!(
                node.operator,
                Some(proto::operator::Operator::LookupJoin(_))
            ) && !sources
                .iter()
                .any(|source| source.node_id == node.plan_node_id)
            {
                return Err(invalid("lookup source bindings omit a lookup stage"));
            }
            for child in crate::planner::persistent::children(node)? {
                required(child, sources)?;
            }
            Ok(())
        }
        for root in self.plan.roots() {
            required(root, sources)?;
        }
        let mut bindings: Vec<PersistentBinding> = Vec::new();
        for source in sources {
            let node = self
                .plan
                .find_unique(|node| node.plan_node_id == source.node_id)?;
            let Some(proto::operator::Operator::LookupJoin(spec)) = &node.operator else {
                unreachable!()
            };
            // Moving the C Stream nulls its release callback in the caller's struct. Reader drop
            // releases it exactly once, including get_schema/get_next errors and admission denial.
            let reader = unsafe { ArrowArrayStreamReader::from_raw(source.stream) }?;
            let expected = crate::planner::arrow_schema(
                spec.side_schema
                    .as_ref()
                    .ok_or_else(|| invalid("lookup source requires its planned schema"))?,
            )?;
            if reader.schema() != expected {
                return Err(invalid(
                    "lookup C Stream schema differs from the physical plan",
                ));
            }
            let table = LookupTable::from_source(
                reader,
                spec.side_keys.iter().map(|&key| key as usize).collect(),
                self.memory_pool.clone(),
            )?;
            bindings.push((
                source.node_id,
                Arc::new(LookupJoinFactory::new(node, table)?),
            ));
        }
        self.bind_persistent(bindings)
    }
}

fn invalid(message: impl Into<String>) -> DataFusionError {
    DataFusionError::Plan(message.into())
}
