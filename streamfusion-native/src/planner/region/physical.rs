// Copyright 2026 StreamFusion Authors
// Licensed under the Apache License, Version 2.0.

use super::shared::SharedStage;
use super::{RegionInput, RegionPlan};
use crate::planner::operators::{envelope, identified::IdentifiedExec};
use crate::planner::{create_operator, persistent::PersistentBinding, LoweringResources};
use datafusion::error::{DataFusionError, Result};
use datafusion::execution::memory_pool::{MemoryConsumer, MemoryPool, MemoryReservation};
use datafusion::physical_plan::{ExecutionPlan, ExecutionPlanProperties};
use std::collections::HashSet;
use std::sync::{atomic::AtomicU8, Arc};

/// One retained physical DAG, with ordinary DataFusion operators for all computation.
/// The flat stage list is also the metric enumeration: a shared physical stage is counted once.
pub(crate) struct PhysicalRegion {
    pub(super) stages: Vec<Arc<dyn ExecutionPlan>>,
    pub(super) outputs: Vec<Arc<dyn ExecutionPlan>>,
    pub(super) shared: Vec<Arc<SharedStage>>,
    pub(super) invocation: AtomicU8,
    pub(super) pool: Arc<dyn MemoryPool>,
    _memory: MemoryReservation,
    _contract: RegionPlan,
}
impl PhysicalRegion {
    pub(crate) fn lower(
        plan: RegionPlan,
        external: Vec<Arc<dyn ExecutionPlan>>,
        persistent: &[PersistentBinding],
        pool: Arc<dyn MemoryPool>,
    ) -> Result<Arc<Self>> {
        let memory =
            MemoryConsumer::new("native region physical graph and stream control").register(&pool);
        memory.try_grow(plan.physical_bytes.checked_add(64 * 1024).ok_or_else(|| {
            DataFusionError::ResourcesExhausted("native region physical admission overflow".into())
        })?)?;
        if external.len() != plan.message.input_count as usize {
            return Err(invalid("external input count changed after planning"));
        }
        for input in &external {
            owned_envelope(input)?;
        }
        // A bounded broadcast must not feed two inputs that one downstream operator may
        // drain sequentially. Admit divergent exits now; reconvergence requires a proven
        // cooperative multi-input operator contract, not an unbounded broadcast queue.
        for &output in &plan.outputs {
            let mut pending = vec![output];
            let mut seen = HashSet::new();
            while let Some(index) = pending.pop() {
                if !seen.insert(index) {
                    return Err(invalid("shared branches reconverge within an output; cooperative multi-input draining is not established"));
                }
                pending.extend(plan.inputs[index].iter().filter_map(|input| match input {
                    RegionInput::Stage(index) => Some(*index),
                    _ => None,
                }));
            }
        }
        let mut bindings = HashSet::new();
        for (id, _) in persistent {
            if !bindings.insert(*id)
                || !plan
                    .message
                    .stages
                    .iter()
                    .any(|s| s.operator.as_ref().unwrap().plan_node_id == *id)
            {
                return Err(invalid("duplicate or unused persistent binding"));
            }
        }
        let resources = LoweringResources {
            persistent,
            memory: Some(pool.clone()),
        };
        let mut stages = Vec::<Arc<dyn ExecutionPlan>>::new();
        let mut sharing = Vec::<Option<Arc<SharedStage>>>::new();
        let mut used = vec![0; plan.message.stages.len()];
        for (index, stage) in plan.message.stages.iter().enumerate() {
            let inputs = plan.inputs[index]
                .iter()
                .map(|reference| match reference {
                    RegionInput::External(port) => external[*port].clone(),
                    RegionInput::Stage(stage) => reader(*stage, &stages, &sharing, &mut used),
                })
                .collect::<Vec<_>>();
            let physical = create_operator(stage.operator.as_ref().unwrap(), &inputs, &resources)?;
            owned_envelope(&physical)?;
            sharing.push(
                (plan.consumers[index] > 1)
                    .then(|| SharedStage::new(physical.clone(), plan.consumers[index])),
            );
            stages.push(physical);
        }
        let outputs = plan
            .outputs
            .iter()
            .map(|index| reader(*index, &stages, &sharing, &mut used))
            .collect();
        debug_assert_eq!(used, plan.consumers);
        Ok(Arc::new(Self {
            stages,
            outputs,
            shared: sharing.into_iter().flatten().collect(),
            invocation: AtomicU8::new(0),
            pool,
            _memory: memory,
            _contract: plan,
        }))
    }
    pub(crate) fn metrics(&self) -> Vec<i64> {
        self.stages
            .iter()
            .flat_map(|plan| {
                let stage = plan
                    .downcast_ref::<IdentifiedExec>()
                    .expect("lowered physical stage must retain its identity");
                [
                    stage.plan_node_id() as i64,
                    stage.input_rows().min(i64::MAX as u64) as i64,
                    stage.output_rows().min(i64::MAX as u64) as i64,
                ]
            })
            .collect()
    }
}
fn reader(
    index: usize,
    stages: &[Arc<dyn ExecutionPlan>],
    sharing: &[Option<Arc<SharedStage>>],
    used: &mut [usize],
) -> Arc<dyn ExecutionPlan> {
    let slot = used[index];
    used[index] += 1;
    sharing[index]
        .as_ref()
        .map_or_else(|| stages[index].clone(), |shared| shared.reader(slot))
}
fn owned_envelope(plan: &Arc<dyn ExecutionPlan>) -> Result<()> {
    let schema = plan.schema();
    let envelope = envelope::Envelope::from_schema(&schema)?;
    if schema.fields().len() != envelope.payload_width + 3
        || schema.field(envelope.payload_width).name() != envelope::OWNED_TIMESTAMP_V1
    {
        return Err(invalid(
            "every region edge requires the owned Arrow envelope",
        ));
    }
    if plan.output_partitioning().partition_count() != 1 {
        return Err(invalid(
            "region inputs and stages require one task-local partition",
        ));
    }
    Ok(())
}
fn invalid(reason: &str) -> DataFusionError {
    DataFusionError::Plan(format!("Invalid native region lowering: {reason}"))
}
