// Copyright 2026 StreamFusion Authors
// Licensed under the Apache License, Version 2.0

use std::sync::Arc;

use arrow::datatypes::SchemaRef;
use arrow::record_batch::RecordBatch;
use datafusion::error::{DataFusionError, Result};
use datafusion::execution::runtime_env::RuntimeEnvBuilder;
use datafusion::execution::TaskContext;
use datafusion::physical_plan::{collect, ExecutionPlan};
use datafusion::prelude::{SessionConfig, SessionContext};

use crate::memory_pool::HostMemoryReservation;
use crate::proto;

use super::reusable_input::ReusableInputExec;

const PIPELINE_CONTROL_BYTES: usize = 256 * 1024;
const CALC_STAGE_CONTROL_BYTES: usize = 64 * 1024;

/// A reusable DataFusion Calc tail embedded in a persistent native stateful operator.
///
/// Stateful processors own their lifecycle and checkpoint contract, while this object lowers the
/// adjacent stateless tail once and replaces only its Arrow input for each emitted batch. It is
/// the same one-tree/one-boundary model used by the normal native execution context, without
/// teaching DataFusion how to own Flink keyed state.
pub(crate) struct FusedCalcPipeline {
    input: Arc<ReusableInputExec>,
    plan: Arc<dyn ExecutionPlan>,
    runtime: tokio::runtime::Runtime,
    task_context: Arc<TaskContext>,
    stage_count: usize,
    _control_reservation: HostMemoryReservation,
}

impl FusedCalcPipeline {
    pub(crate) fn new(
        input_schema: SchemaRef,
        stages: Vec<proto::Calc>,
        mut control_reservation: HostMemoryReservation,
    ) -> Result<Self> {
        if stages.is_empty() {
            return Err(DataFusionError::Plan(
                "a fused Calc pipeline must contain at least one stage".to_string(),
            ));
        }
        let memory_limit = control_reservation
            .available_capacity()?
            .unwrap_or(usize::MAX);
        control_reservation.try_grow(
            PIPELINE_CONTROL_BYTES
                .saturating_add(stages.len().saturating_mul(CALC_STAGE_CONTROL_BYTES)),
        )?;
        let memory_pool = control_reservation.datafusion_pool(memory_limit);
        let runtime_env = RuntimeEnvBuilder::new()
            .with_memory_pool(memory_pool)
            .build()
            .map(Arc::new)?;
        let task_context =
            SessionContext::new_with_config_rt(SessionConfig::new(), runtime_env).task_ctx();
        let runtime = tokio::runtime::Builder::new_current_thread()
            .build()
            .map_err(|error| DataFusionError::External(Box::new(error)))?;
        let input = Arc::new(ReusableInputExec::new(input_schema));
        let mut plan = Arc::clone(&input) as Arc<dyn ExecutionPlan>;
        let stage_count = stages.len();
        for stage in &stages {
            plan = super::calc::create(stage, plan)?;
        }
        Ok(Self {
            input,
            plan,
            runtime,
            task_context,
            stage_count,
            _control_reservation: control_reservation,
        })
    }

    pub(crate) fn execute(&self, batch: RecordBatch) -> Result<RecordBatch> {
        self.input.replace_batch(batch)?;
        let result = self.runtime.block_on(collect(
            Arc::clone(&self.plan),
            Arc::clone(&self.task_context),
        ));
        self.input.clear();
        let mut batches = result?;
        match batches.len() {
            0 => Ok(self.empty_output()),
            1 => Ok(batches.pop().expect("one fused Calc output batch")),
            count => Err(DataFusionError::Internal(format!(
                "a fused Calc tail produced {count} batches for one reusable input batch"
            ))),
        }
    }

    pub(crate) fn empty_output(&self) -> RecordBatch {
        RecordBatch::new_empty(self.plan.schema())
    }

    pub(crate) fn stage_count(&self) -> usize {
        self.stage_count
    }
}
