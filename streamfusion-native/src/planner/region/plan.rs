// Copyright 2026 StreamFusion Authors
// Licensed under the Apache License, Version 2.0.

use crate::execution_context::wire_memory::PlanMemory;
use crate::planner::persistent;
use crate::proto;
use datafusion::error::{DataFusionError, Result};
use datafusion::execution::memory_pool::{MemoryConsumer, MemoryPool, MemoryReservation};
use prost::Message;
use std::collections::{HashMap, HashSet};
use std::sync::Arc;

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(crate) enum RegionInput {
    External(usize),
    Stage(usize),
}

/// The decoded graph and resolved references remain under one coarse control-plan reservation.
pub(crate) struct RegionPlan {
    pub(crate) message: proto::NativeRegionPlan,
    pub(crate) inputs: Vec<Vec<RegionInput>>,
    pub(crate) outputs: Vec<usize>,
    pub(crate) consumers: Vec<usize>,
    pub(crate) physical_bytes: usize,
    _memory: MemoryReservation,
}
impl RegionPlan {
    pub(crate) fn decode(bytes: &[u8], pool: &Arc<dyn MemoryPool>) -> Result<Self> {
        let memory = MemoryConsumer::new("native region plan and references").register(pool);
        let estimate = PlanMemory::scan_region(bytes)?;
        memory.try_grow(estimate.decoded()?)?;
        let physical_bytes = estimate.physical()?;
        let message =
            proto::NativeRegionPlan::decode(bytes).map_err(|error| invalid(error.to_string()))?;
        let (inputs, outputs, consumers) = validate(&message)?;
        Ok(Self {
            message,
            inputs,
            outputs,
            consumers,
            physical_bytes,
            _memory: memory,
        })
    }
}

fn validate(
    plan: &proto::NativeRegionPlan,
) -> Result<(Vec<Vec<RegionInput>>, Vec<usize>, Vec<usize>)> {
    if plan.protocol_version != 1 {
        return Err(invalid("unsupported protocol version"));
    }
    if plan.input_count == 0
        || plan.input_count > i32::MAX as u32
        || plan.stages.is_empty()
        || plan.output_stage_ids.is_empty()
    {
        return Err(invalid("requires external inputs, stages and outputs"));
    }
    let mut seen = HashMap::new();
    let mut external = HashSet::new();
    let mut uids = HashSet::new();
    let mut inputs = Vec::new();
    let mut consumers = vec![0usize; plan.stages.len()];
    for (index, stage) in plan.stages.iter().enumerate() {
        let operator = stage
            .operator
            .as_ref()
            .ok_or_else(|| invalid("missing operator"))?;
        let id = operator.plan_node_id;
        if id == 0 || id > i64::MAX as u64 {
            return Err(invalid(
                "stage identity must be a positive signed 64-bit ID",
            ));
        }
        if seen.contains_key(&id) {
            return Err(invalid("duplicate stage identity"));
        }
        if operator
            .metric_uid
            .as_ref()
            .is_some_and(|uid| !uids.insert(uid))
        {
            return Err(invalid("duplicate metric UID"));
        }
        let children = persistent::children(operator)?;
        if children.is_empty() || children.len() != stage.inputs.len() {
            return Err(invalid("stage input arity mismatch"));
        }
        let mut slots = HashSet::new();
        for child in children {
            let Some(proto::operator::Operator::Input(input)) = &child.operator else {
                return Err(invalid("stage fragment contains nested physical operators"));
            };
            if child.plan_node_id != 0
                || !child.metric_name.is_empty()
                || child.metric_uid.is_some()
                || child.clear_record_timestamps
            {
                return Err(invalid(
                    "local Input slots must have no physical identity or record policy",
                ));
            }
            if input.input_index as usize >= stage.inputs.len() || !slots.insert(input.input_index)
            {
                return Err(invalid(
                    "local Input slots must be unique and contiguous from zero",
                ));
            }
        }
        let mut references = Vec::new();
        for input in &stage.inputs {
            references.push(match input.source {
                Some(proto::native_region_input_reference::Source::ExternalInput(port)) => {
                    if port >= plan.input_count || !external.insert(port) {
                        return Err(invalid(
                            "external input ports must be distinct and in range",
                        ));
                    }
                    RegionInput::External(port as usize)
                }
                Some(proto::native_region_input_reference::Source::StageId(id)) => {
                    let previous = *seen.get(&id).ok_or_else(|| {
                        invalid("stage reference must name an earlier definition")
                    })?;
                    consumers[previous] += 1;
                    RegionInput::Stage(previous)
                }
                None => return Err(invalid("input reference has no source")),
            });
        }
        inputs.push(references);
        seen.insert(id, index);
    }
    if external.len() != plan.input_count as usize {
        return Err(invalid("external input port is unused"));
    }
    let mut reachable = HashSet::new();
    let mut outputs = Vec::new();
    for id in &plan.output_stage_ids {
        let index = *seen
            .get(id)
            .ok_or_else(|| invalid("output IDs must be distinct stage definitions"))?;
        if !reachable.insert(index) {
            return Err(invalid("output IDs must be distinct stage definitions"));
        }
        outputs.push(index);
        consumers[index] += 1;
    }
    let mut pending = outputs.clone();
    while let Some(index) = pending.pop() {
        for input in &inputs[index] {
            if let RegionInput::Stage(previous) = input {
                if reachable.insert(*previous) {
                    pending.push(*previous);
                }
            }
        }
    }
    if reachable.len() != plan.stages.len() {
        return Err(invalid("stage definition is unreachable from outputs"));
    }
    Ok((inputs, outputs, consumers))
}

fn invalid(reason: impl Into<String>) -> DataFusionError {
    DataFusionError::Plan(format!("Invalid native region plan: {}", reason.into()))
}

#[cfg(test)]
mod tests;
