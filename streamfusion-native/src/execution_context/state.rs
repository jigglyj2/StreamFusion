// Copyright 2026 StreamFusion Authors
// Licensed under the Apache License, Version 2.0

use super::{invocation::InvocationGuard, NativeExecutionContext};
use crate::memory_pool::HostMemoryReservation;
use crate::planner::operators::{
    deduplicate::{execution_plan::DeduplicateFactory, DeduplicateProcessor},
    group_aggregate::{
        execution_plan::{validate_native_node, GroupAggregateFactory},
        GroupAggregateProcessor,
    },
    regular_join::{execution_plan::RegularJoinFactory, RegularJoinProcessor},
    top_n::{
        execution_plan::{self as top_n_execution, TopNFactory},
        TopNProcessor,
    },
    window_aggregate::shared_execution::{self as shared_window, WindowFactory},
};
use crate::planner::persistent::{PersistentBinding, PersistentOperatorFactory};
use crate::{proto, state::SnapshotBytes};
use datafusion::error::{DataFusionError, Result};
use datafusion::execution::memory_pool::MemoryReservation;
use prost::Message;
use std::collections::HashSet;
use std::path::Path;
use std::sync::{Arc, Mutex};

pub(super) struct StateResources {
    options: proto::NativeStateBindings,
    memory: HostMemoryReservation,
    // Setup admission is transactional and follows the decoded bindings after success.
    // Drop options before returning their credit, including on constructor failure.
    _control_memory: MemoryReservation,
}

impl NativeExecutionContext {
    /// One resource-binding phase for the complete native tree, before execution or restore.
    pub(crate) fn install_state(
        &mut self,
        bytes: &[u8],
        memory: HostMemoryReservation,
    ) -> Result<()> {
        if self.state_resources.is_some()
            || self
                .physical_plan
                .get_mut()
                .map_err(|_| invalid("poisoned plan"))?
                .is_some()
        {
            return Err(invalid(
                "state bindings must be installed once before lowering",
            ));
        }
        let control_memory = self.reservation("native state-binding configuration and setup");
        control_memory.try_grow(
            bytes
                .len()
                .checked_mul(4)
                .and_then(|n| n.checked_add(4096))
                .ok_or_else(|| invalid("state-binding reservation overflow"))?,
        )?;
        let options = proto::NativeStateBindings::decode(bytes)
            .map_err(|error| invalid(format!("invalid state-binding protobuf: {error}")))?;
        if !matches!(options.protocol_version, 1 | 2 | 3)
            || self.protocol_version() < crate::ENVELOPE_PLAN_PROTOCOL_VERSION
            || options.bindings.is_empty()
        {
            return Err(invalid("unsupported state-binding protocol version"));
        }
        let mut ids = HashSet::new();
        let mut directories = HashSet::new();
        // Validate the entire request before opening any database or constructing operator state.
        for binding in &options.bindings {
            if binding.plan_node_id == 0 || !ids.insert(binding.plan_node_id) {
                return Err(invalid(
                    "state bindings require unique positive plan-node IDs",
                ));
            }
            if self
                .persistent
                .iter()
                .any(|(id, _)| *id == binding.plan_node_id)
            {
                return Err(invalid("state node already has a lifecycle binding"));
            }
            if binding.max_parallelism == 0
                || binding.max_parallelism > 32768
                || binding.first_key_group > binding.last_key_group
                || binding.last_key_group >= binding.max_parallelism
            {
                return Err(invalid(
                    "state binding has an invalid Flink key-group range",
                ));
            }
            let node = self
                .plan
                .find_unique(|node| node.plan_node_id == binding.plan_node_id)?;
            if !matches!(
                node.operator,
                Some(
                    proto::operator::Operator::Deduplicate(_)
                        | proto::operator::Operator::GroupAggregate(_)
                        | proto::operator::Operator::GlobalGroupAggregate(_)
                        | proto::operator::Operator::RegularJoin(_)
                        | proto::operator::Operator::WindowAggregate(_)
                        | proto::operator::Operator::TopN(_)
                )
            ) {
                return Err(invalid(format!(
                    "node {} has no shared state constructor",
                    binding.plan_node_id
                )));
            }
            if matches!(
                node.operator,
                Some(
                    proto::operator::Operator::GroupAggregate(_)
                        | proto::operator::Operator::GlobalGroupAggregate(_)
                )
            ) {
                validate_native_node(node)?;
            }
            if matches!(node.operator, Some(proto::operator::Operator::TopN(_))) {
                top_n_execution::validate_node(node, binding.max_parallelism)?;
            }
            let window = matches!(
                node.operator,
                Some(proto::operator::Operator::WindowAggregate(_))
            );
            if binding.restored_watermark.is_some() && (options.protocol_version < 3 || !window) {
                return Err(invalid(
                    "restored operator watermarks require a window binding and protocol 3",
                ));
            }
            if window {
                shared_window::validate_node(node, binding.max_parallelism)?;
                if matches!(&node.operator, Some(proto::operator::Operator::WindowAggregate(plan)) if plan.processing_time)
                    && !self
                        .processing_window_buffers
                        .iter()
                        .any(|(id, _)| *id == binding.plan_node_id)
                {
                    return Err(invalid("processing-time state requires its original Flink buffer resource before binding"));
                }
            }
            match binding.backend.as_ref() {
                Some(proto::native_state_binding::Backend::Memory(_)) => {}
                Some(proto::native_state_binding::Backend::Rocksdb(rocks)) => {
                    if let Some(directory) = &rocks.log_directory {
                        if options.protocol_version < 2 {
                            return Err(invalid(
                                "RocksDB log directory requires state-binding protocol 2",
                            ));
                        }
                        if !Path::new(directory).is_absolute() || directory.contains('\0') {
                            return Err(invalid(
                                "RocksDB log directory must be an absolute path without NUL bytes",
                            ));
                        }
                    }
                    if rocks.memory_limit == 0
                        || usize::try_from(rocks.memory_limit).is_err()
                        || !Path::new(&rocks.plugin_path).is_absolute()
                        || !Path::new(&rocks.database_path).is_absolute()
                        || !directories.insert(rocks.database_path.as_str())
                    {
                        return Err(invalid("RocksDB bindings need unique absolute database paths, an absolute plugin path and positive Flink memory leases"));
                    }
                }
                None => return Err(invalid("state binding has no supported backend")),
            }
        }
        for root in self.plan.roots() {
            require_bindings(root, &ids)?;
        }
        let mut bindings: Vec<PersistentBinding> = Vec::with_capacity(options.bindings.len());
        for binding in &options.bindings {
            let node = self
                .plan
                .find_unique(|node| node.plan_node_id == binding.plan_node_id)?;
            let mut plan_memory = memory.sibling("native state constructor plan");
            plan_memory.resize(super::operator_spec::admission(node)?)?;
            let bare = proto::NativePlan {
                protocol_version: self.protocol_version(),
                root: Some(super::operator_spec::without_children(node)),
            }
            .encode_to_vec();
            let state_memory =
                memory.sibling(format!("native state node {}", binding.plan_node_id));
            let buffer = self
                .processing_window_buffers
                .iter()
                .find(|(id, _)| *id == binding.plan_node_id)
                .map(|(_, buffer)| *buffer);
            let factory = create(node, &bare, binding, state_memory, buffer)?;
            bindings.push((binding.plan_node_id, factory));
        }
        self.bind_persistent(bindings)?;
        self.state_resources = Some(StateResources {
            options,
            memory,
            _control_memory: control_memory,
        });
        Ok(())
    }

    fn state_control<T>(
        &self,
        id: u64,
        mutating: bool,
        action: impl FnOnce(&dyn PersistentOperatorFactory) -> Result<T>,
    ) -> Result<T> {
        let mut invocation = InvocationGuard::begin(self)?;
        let (_, owner) = self
            .persistent
            .iter()
            .find(|(node, _)| *node == id)
            .ok_or_else(|| invalid(format!("native state node {id} is not bound")))?;
        if mutating {
            invocation.executing();
        }
        let result = action(owner.as_ref());
        invocation.successful = result.is_ok();
        result
    }

    pub(crate) fn snapshot_state(&self, id: u64, key_group: u32) -> Result<SnapshotBytes> {
        self.state_control(id, false, |owner| owner.snapshot(key_group))
    }

    pub(crate) fn restore_state(&self, id: u64, key_group: u32, bytes: &[u8]) -> Result<()> {
        self.state_control(id, true, |owner| owner.restore(key_group, bytes))
    }

    pub(crate) fn checkpoint_state(&self, id: u64, directory: &Path) -> Result<()> {
        self.state_control(id, false, |owner| owner.checkpoint(directory))
    }

    /// Import a Flink-materialized checkpoint through the same canonical key-group contract,
    /// independent of the destination backend and SQL operator family.
    pub(crate) fn import_state_checkpoint(
        &self,
        id: u64,
        plugin: &Path,
        directory: &Path,
        first: u32,
        last: u32,
        reader_limit: usize,
    ) -> Result<()> {
        use crate::state::RocksPluginKeyedState;
        let resources = self
            .state_resources
            .as_ref()
            .ok_or_else(|| invalid("checkpoint import requires shared state resources"))?;
        let binding = resources
            .options
            .bindings
            .iter()
            .find(|binding| binding.plan_node_id == id)
            .ok_or_else(|| invalid("checkpoint import node is not bound"))?;
        if first > last
            || first < binding.first_key_group
            || last > binding.last_key_group
            || reader_limit == 0
            || !plugin.is_absolute()
            || !directory.is_absolute()
        {
            return Err(invalid(
                "invalid checkpoint import range, path or memory lease",
            ));
        }
        self.state_control(id, true, |owner| {
            let mut memory = resources.memory.sibling("native region checkpoint reader");
            // Charge the temporary RocksDB reader before opening it. Canonical snapshot buffers
            // acquire additional sibling reservations and retain them until each restore finishes.
            memory.resize(reader_limit)?;
            // The plugin's normal open also serves new task databases. Recovery must never
            // silently turn a missing/incomplete checkpoint into a newly created empty database.
            if !directory.join("CURRENT").is_file() {
                return Err(invalid(
                    "native state import is missing the RocksDB CURRENT file",
                ));
            }
            let log_directory = match binding.backend.as_ref() {
                Some(proto::native_state_binding::Backend::Rocksdb(rocks)) => {
                    rocks.log_directory.clone()
                }
                _ => None,
            };
            let reader = proto::NativeRocksDbState {
                plugin_path: plugin
                    .to_str()
                    .ok_or_else(|| invalid("checkpoint plugin path is not UTF-8"))?
                    .to_owned(),
                database_path: directory
                    .to_str()
                    .ok_or_else(|| invalid("checkpoint database path is not UTF-8"))?
                    .to_owned(),
                memory_limit: reader_limit as u64,
                log_directory,
            };
            let source = RocksPluginKeyedState::open_configured(&reader, first, last, None)?;
            for group in first..=last {
                owner.restore_from_checkpoint(group, &source, &memory)?;
            }
            Ok(())
        })
    }
}

// Backend configuration is bound once, independently of the operator family. New shared
// persistent operators receive this configured state object instead of reopening a backend.
fn create(
    node: &proto::Operator,
    bytes: &[u8],
    binding: &proto::NativeStateBinding,
    memory: HostMemoryReservation,
    buffer: Option<super::task_resources::WindowBuffer>,
) -> Result<Arc<dyn PersistentOperatorFactory>> {
    use crate::state::{KeyedState, MemoryKeyedState, RocksPluginKeyedState};
    let max = binding.max_parallelism;
    let first = binding.first_key_group;
    let last = binding.last_key_group;
    let scratch = memory.sibling("native state batch scratch and output");
    let state: Box<dyn KeyedState> = match binding.backend.as_ref() {
        Some(proto::native_state_binding::Backend::Memory(_)) => {
            if matches!(
                node.operator,
                Some(
                    proto::operator::Operator::WindowAggregate(_)
                        | proto::operator::Operator::TopN(_)
                )
            ) {
                Box::new(crate::state::OrderedMemoryKeyedState::new(
                    first, last, memory,
                )?)
            } else {
                Box::new(MemoryKeyedState::new(first, last, memory)?)
            }
        }
        Some(proto::native_state_binding::Backend::Rocksdb(rocks)) => Box::new(
            RocksPluginKeyedState::open_configured(rocks, first, last, Some(&memory))?,
        ),
        None => return Err(invalid("unsupported native state binding")),
    };
    match &node.operator {
        Some(proto::operator::Operator::WindowAggregate(_)) => Ok(Arc::new(WindowFactory::new(
            node, bytes, binding, state, scratch, buffer,
        )?)),
        Some(
            proto::operator::Operator::GroupAggregate(_)
            | proto::operator::Operator::GlobalGroupAggregate(_),
        ) => Ok(Arc::new(GroupAggregateFactory(Arc::new(Mutex::new(
            GroupAggregateProcessor::with_state(bytes, max, first, last, state, scratch)?,
        ))))),
        Some(proto::operator::Operator::TopN(_)) => {
            Ok(Arc::new(TopNFactory(Arc::new(Mutex::new(
                TopNProcessor::with_state_with_range(bytes, max, first, last, state, scratch)?,
            )))))
        }
        Some(proto::operator::Operator::Deduplicate(_)) => {
            Ok(Arc::new(DeduplicateFactory(Arc::new(Mutex::new(
                DeduplicateProcessor::with_state(bytes, max, state, scratch)?,
            )))))
        }
        Some(proto::operator::Operator::RegularJoin(_)) => {
            Ok(Arc::new(RegularJoinFactory(Arc::new(Mutex::new(
                RegularJoinProcessor::with_state(bytes, max, first, last, state, scratch)?,
            )))))
        }
        _ => Err(invalid("unsupported native state binding")),
    }
}

fn invalid(message: impl Into<String>) -> DataFusionError {
    DataFusionError::Plan(message.into())
}

fn require_bindings(node: &proto::Operator, ids: &HashSet<u64>) -> Result<()> {
    if matches!(
        node.operator,
        Some(
            proto::operator::Operator::Deduplicate(_)
                | proto::operator::Operator::RegularJoin(_)
                | proto::operator::Operator::GroupAggregate(_)
                | proto::operator::Operator::GlobalGroupAggregate(_)
                | proto::operator::Operator::WindowAggregate(_)
                | proto::operator::Operator::TopN(_)
        )
    ) && !ids.contains(&node.plan_node_id)
    {
        return Err(invalid(format!(
            "native state node {} is missing its resource binding",
            node.plan_node_id
        )));
    }
    for child in crate::planner::persistent::children(node)? {
        require_bindings(child, ids)?;
    }
    Ok(())
}
