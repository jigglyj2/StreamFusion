// Copyright 2026 StreamFusion Authors
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
// http://www.apache.org/licenses/LICENSE-2.0

use std::collections::HashMap;
use std::sync::atomic::{AtomicI64, AtomicU8, Ordering};
use std::sync::{Arc, Mutex, OnceLock};

use arrow::datatypes::SchemaRef;
use datafusion::error::{DataFusionError, Result};
use datafusion::execution::memory_pool::{MemoryConsumer, MemoryPool, MemoryReservation};
use datafusion::execution::runtime_env::RuntimeEnvBuilder;
use datafusion::execution::TaskContext;
use datafusion::physical_plan::ExecutionPlan;
use datafusion::prelude::SessionConfig;
use jni::objects::{Global, JObject};
use jni::JavaVM;
#[cfg(test)]
use prost::Message;

use crate::memory_pool::{FlinkMemoryPool, JvmMemoryReservationBroker};
use crate::planner::operators::reusable_input::ReusableInputExec;
use crate::planner::{create_plan_with_memory, persistent::PersistentBinding};
use crate::proto;

// Rust, Tokio, and DataFusion allocate control structures outside Arrow's buffer allocator. Keep
// a conservative task-lifetime envelope for those allocations in Flink's pool, then add explicit
// reservations for cached schemas and the lowered physical tree before they are retained.
const EXECUTION_CONTEXT_CONTROL_BYTES: usize = 256 * 1024;
const CACHED_SCHEMA_BASE_BYTES: usize = 4 * 1024;
const PHYSICAL_PLAN_BASE_BYTES: usize = 64 * 1024;
pub(crate) mod wire_memory;

#[cfg(test)]
mod control_tests;
mod gauges;
mod invocation;
mod lifecycle;
pub(crate) mod operator_spec;
#[cfg(test)]
mod partition_tests;
#[cfg(test)]
mod persistent_tests;
mod state;
#[cfg(test)]
mod state_conformance_tests;
pub(crate) mod stream;
#[cfg(test)]
mod stream_tests;

pub(crate) struct NativeExecutionContext {
    plan: proto::NativePlan,
    runtime: Arc<tokio::runtime::Runtime>,
    task_context: Arc<TaskContext>,
    memory_pool: Arc<dyn MemoryPool>,
    // Execution streams can retain expressions and input owners; drop them before plan credit.
    retained_stream: Mutex<Option<datafusion::physical_plan::SendableRecordBatchStream>>,
    physical_plan: Mutex<Option<CachedPhysicalPlan>>,
    stream_creations: std::sync::atomic::AtomicUsize,
    input_schemas: Mutex<Vec<SchemaRef>>,
    schema_reservation: Mutex<MemoryReservation>,
    persistent: Vec<PersistentBinding>,
    state_resources: Option<state::StateResources>,
    invocation: AtomicU8,
    controls: Arc<crate::planner::persistent::control::ControlEvents>,
    _control_reservation: MemoryReservation,
    physical_control_bytes: usize,
}

struct CachedPhysicalPlan {
    plan: Arc<dyn ExecutionPlan>,
    inputs: Vec<Arc<ReusableInputExec>>,
    _reservation: MemoryReservation,
}

impl NativeExecutionContext {
    pub(crate) fn requires_input_envelope(&self) -> bool {
        self.plan().protocol_version >= crate::RECORD_POLICY_PLAN_PROTOCOL_VERSION
            || !self.persistent.is_empty()
    }

    pub(crate) fn new(bytes: &[u8], memory_pool: Arc<dyn MemoryPool>) -> Result<Self> {
        let control_reservation =
            MemoryConsumer::new("native execution context and decoded plan").register(&memory_pool);
        control_reservation.try_grow(EXECUTION_CONTEXT_CONTROL_BYTES)?;
        let plan_memory = wire_memory::PlanMemory::scan(bytes)?;
        control_reservation.try_grow(plan_memory.decoded()?)?;
        let physical_control_bytes = plan_memory.physical()?;
        let plan = crate::decode_plan(bytes)?;
        let persistent = lifecycle::task_local_bindings(&plan, &memory_pool)?;
        let schema_reservation =
            MemoryConsumer::new("native cached input schemas").register(&memory_pool);
        let runtime_env = RuntimeEnvBuilder::new()
            .with_memory_pool(Arc::clone(&memory_pool))
            .build()
            .map(Arc::new)?;
        let controls = Arc::new(crate::planner::persistent::control::ControlEvents::default());
        // Flink has already planned this query. Native lowering constructs physical expressions
        // directly; creating a SQL SessionContext here would populate unused function/catalog
        // registries and initialize process-global UDF caches for every physical context.
        let task_context = Arc::new(TaskContext::new(
            None,
            "streamfusion".into(),
            SessionConfig::new().with_extension(controls.clone()),
            HashMap::new(),
            HashMap::new(),
            HashMap::new(),
            HashMap::new(),
            runtime_env,
        ));
        let runtime = Arc::new(
            tokio::runtime::Builder::new_current_thread()
                .build()
                .map_err(|error| DataFusionError::External(Box::new(error)))?,
        );
        Ok(Self {
            plan,
            runtime,
            task_context,
            memory_pool,
            physical_plan: Mutex::new(None),
            retained_stream: Mutex::new(None),
            stream_creations: std::sync::atomic::AtomicUsize::new(0),
            input_schemas: Mutex::new(Vec::new()),
            schema_reservation: Mutex::new(schema_reservation),
            persistent,
            state_resources: None,
            invocation: AtomicU8::new(0),
            controls,
            _control_reservation: control_reservation,
            physical_control_bytes,
        })
    }

    pub(crate) fn plan(&self) -> &proto::NativePlan {
        &self.plan
    }

    /// Register lifecycle-owned state before the tree is lowered. Recursive planner factories
    /// compose these nodes exactly like stateless DataFusion physical operators.
    pub(crate) fn bind_persistent(&mut self, bindings: Vec<PersistentBinding>) -> Result<()> {
        if self
            .physical_plan
            .get_mut()
            .map_err(|_| DataFusionError::Internal("native physical-plan lock poisoned".into()))?
            .is_some()
        {
            return Err(DataFusionError::Plan(
                "persistent bindings must be installed before physical lowering".into(),
            ));
        }
        for (index, (id, _)) in bindings.iter().enumerate() {
            if *id == 0
                || self.persistent.iter().any(|(existing, _)| existing == id)
                || bindings[..index].iter().any(|(existing, _)| existing == id)
            {
                return Err(DataFusionError::Plan(
                    "native lifecycle bindings require unique positive plan-node IDs".into(),
                ));
            }
        }
        self.persistent.extend(bindings);
        Ok(())
    }

    pub(crate) fn require_idle(&self) -> Result<()> {
        if self.invocation.load(Ordering::Acquire) != 0 {
            return Err(DataFusionError::Execution(
                "native plan invocation is active or failed; failed state requires recovery".into(),
            ));
        }
        Ok(())
    }

    pub(crate) fn runtime(&self) -> &tokio::runtime::Runtime {
        &self.runtime
    }

    pub(crate) fn task_context(&self) -> Arc<TaskContext> {
        Arc::clone(&self.task_context)
    }

    pub(crate) fn reservation(&self, consumer: impl Into<String>) -> MemoryReservation {
        MemoryConsumer::new(consumer).register(&self.memory_pool)
    }

    pub(crate) fn remember_input_schema(&self, input: usize, schema: SchemaRef) -> Result<()> {
        let mut schemas = self.input_schemas.lock().map_err(|_| {
            DataFusionError::Internal("native input-schema cache lock poisoned".to_string())
        })?;
        if input < schemas.len() {
            if schemas[input].as_ref() != schema.as_ref() {
                return Err(DataFusionError::Execution(format!(
                    "native input {input} schema changed after C Data negotiation"
                )));
            }
            return Ok(());
        }
        if input != schemas.len() {
            return Err(DataFusionError::Internal(format!(
                "native input schema {input} arrived before input {}",
                schemas.len()
            )));
        }
        let fields_bytes = schema
            .fields()
            .iter()
            .try_fold(0usize, |bytes, field| bytes.checked_add(field.size()));
        let metadata_bytes = schema
            .metadata()
            .iter()
            .try_fold(0usize, |bytes, (key, value)| {
                bytes
                    .checked_add(key.capacity())?
                    .checked_add(value.capacity())?
                    .checked_add(128)
            });
        let schema_bytes = fields_bytes
            .and_then(|bytes| bytes.checked_add(metadata_bytes?))
            .and_then(|bytes| bytes.checked_mul(2))
            .and_then(|bytes| bytes.checked_add(CACHED_SCHEMA_BASE_BYTES))
            .ok_or_else(|| {
                DataFusionError::ResourcesExhausted(
                    "native input-schema reservation overflowed usize".to_string(),
                )
            })?;
        self.schema_reservation
            .lock()
            .map_err(|_| {
                DataFusionError::Internal(
                    "native input-schema reservation lock poisoned".to_string(),
                )
            })?
            .try_grow(schema_bytes)?;
        schemas.push(schema);
        Ok(())
    }

    pub(crate) fn input_schema(&self, input: usize) -> Result<SchemaRef> {
        self.input_schemas
            .lock()
            .map_err(|_| {
                DataFusionError::Internal("native input-schema cache lock poisoned".to_string())
            })?
            .get(input)
            .cloned()
            .ok_or_else(|| {
                DataFusionError::Execution(format!(
                    "native input {input} omitted its schema before C Data negotiation"
                ))
            })
    }

    pub(crate) fn execute_plan<T>(
        &self,
        batches: Vec<arrow::array::RecordBatch>,
        execute: impl FnOnce(Arc<dyn ExecutionPlan>) -> Result<T>,
    ) -> Result<T> {
        let mut invocation = invocation::InvocationGuard::begin(self)?;
        let plan = self.prepare_plan(batches)?;
        self.retained_stream
            .lock()
            .map_err(|_| DataFusionError::Internal("native stream lock poisoned".into()))?
            .take();
        self.set_input_streaming(false)?;
        invocation.executing();
        let result = execute(plan);
        invocation.successful = result.is_ok();
        result
    }

    fn set_input_streaming(&self, enabled: bool) -> Result<()> {
        let cached = self
            .physical_plan
            .lock()
            .map_err(|_| DataFusionError::Internal("native plan lock poisoned".into()))?;
        if let Some(cached) = cached.as_ref() {
            for input in &cached.inputs {
                input.streaming(enabled);
            }
        }
        Ok(())
    }

    // A synchronous Calc chain reaches Pending only when its single input needs another
    // batch. Multi-input scheduling and persistent control drains require an explicit barrier
    // contract before they may use this mode; EOF is never inferred from temporary idleness.
    fn can_retain_stream(&self, plan: &Arc<dyn ExecutionPlan>) -> bool {
        fn synchronous(node: &proto::Operator) -> bool {
            match node.operator.as_ref() {
                Some(proto::operator::Operator::Input(_)) => true,
                Some(proto::operator::Operator::Calc(calc)) => {
                    calc.input.as_deref().is_some_and(synchronous)
                }
                _ => false,
            }
        }
        fn synchronous_physical(plan: &Arc<dyn ExecutionPlan>) -> bool {
            matches!(
                plan.name(),
                "ProjectionExec"
                    | "FilterExec"
                    | "ManagedFilterExec"
                    | "StreamFusionReusableInputExec"
            ) && plan
                .children()
                .iter()
                .all(|child| synchronous_physical(child))
        }
        self.persistent.is_empty()
            && self.plan.root.as_ref().is_some_and(synchronous)
            && synchronous_physical(plan)
    }

    // Caller must hold the invocation claim before touching reusable input slots. DataFusion
    // may open children lazily during stream polling, so slots live until invocation teardown.
    fn prepare_plan(
        &self,
        batches: Vec<arrow::array::RecordBatch>,
    ) -> Result<Arc<dyn ExecutionPlan>> {
        let mut cached = self.physical_plan.lock().map_err(|_| {
            DataFusionError::Internal("native physical-plan cache lock poisoned".to_string())
        })?;
        if cached.is_none() {
            let physical_plan_bytes =
                self.physical_control_bytes
                    .checked_add(PHYSICAL_PLAN_BASE_BYTES)
                    .and_then(|bytes| {
                        batches.iter().try_fold(bytes, |bytes, batch| {
                            let schema = batch.schema();
                            let fields = schema
                                .fields()
                                .iter()
                                .try_fold(0usize, |sum, field| sum.checked_add(field.size()))?;
                            let metadata = schema.metadata().iter().try_fold(
                                0usize,
                                |sum, (key, value)| {
                                    sum.checked_add(key.capacity())?
                                        .checked_add(value.capacity())?
                                        .checked_add(128)
                                },
                            )?;
                            bytes
                                .checked_add(fields.checked_add(metadata)?.checked_mul(2)?)?
                                .checked_add(CACHED_SCHEMA_BASE_BYTES)
                        })
                    })
                    .ok_or_else(|| {
                        DataFusionError::ResourcesExhausted(
                            "native physical-plan reservation overflowed usize".to_string(),
                        )
                    })?;
            // The prospective tree owns its allowance locally until lowering succeeds. A failed
            // first batch must not accumulate another reservation on every retry.
            let reservation = self.reservation("native lowered physical plan");
            reservation.try_grow(physical_plan_bytes)?;
            // DataFusion schema projection/equivalence construction temporarily overlaps
            // input/output expression maps. This common allowance is released after lowering,
            // independent of the physical operator families present in the tree.
            let construction = self.reservation("native physical-plan construction workspace");
            construction.try_grow(physical_plan_bytes)?;
            let inputs = batches
                .iter()
                .map(|batch| Arc::new(ReusableInputExec::new(batch.schema())))
                .collect::<Vec<_>>();
            let external_inputs = inputs
                .iter()
                .map(|input| Arc::clone(input) as Arc<dyn ExecutionPlan>)
                .collect();
            let plan = create_plan_with_memory(
                &self.plan,
                external_inputs,
                &self.persistent,
                Some(self.memory_pool.clone()),
            )?;
            *cached = Some(CachedPhysicalPlan {
                plan,
                inputs,
                _reservation: reservation,
            });
        }
        let cached = cached.as_ref().expect("physical plan was initialized");
        if cached.inputs.len() != batches.len() {
            return Err(DataFusionError::Execution(format!(
                "native plan expects {} inputs, received {}",
                cached.inputs.len(),
                batches.len()
            )));
        }
        for (input, batch) in cached.inputs.iter().zip(batches) {
            if let Err(error) = input.replace_batch(batch) {
                for input in &cached.inputs {
                    input.clear();
                }
                return Err(error);
            }
        }
        Ok(Arc::clone(&cached.plan))
    }

    pub(crate) fn metric_value(&self, plan_node_id: u64, name: &str) -> Result<usize> {
        fn sum_all(plan: &Arc<dyn ExecutionPlan>, name: &str) -> usize {
            let current = plan
                .metrics()
                .and_then(|metrics| metrics.sum_by_name(name))
                .map(|value| value.as_usize())
                .unwrap_or(0);
            current.saturating_add(
                plan.children()
                    .into_iter()
                    .map(|child| sum_all(child, name))
                    .sum(),
            )
        }

        fn sum_stage(plan: &Arc<dyn ExecutionPlan>, name: &str) -> usize {
            if plan
                .downcast_ref::<crate::planner::operators::identified::IdentifiedExec>()
                .is_some()
            {
                return 0;
            }
            let current = plan
                .metrics()
                .and_then(|metrics| metrics.sum_by_name(name))
                .map(|value| value.as_usize())
                .unwrap_or(0);
            current.saturating_add(
                plan.children()
                    .into_iter()
                    .map(|child| sum_stage(child, name))
                    .sum(),
            )
        }

        fn identified_metric(
            plan: &Arc<dyn ExecutionPlan>,
            plan_node_id: u64,
            name: &str,
        ) -> Option<usize> {
            if let Some(identified) =
                plan.downcast_ref::<crate::planner::operators::identified::IdentifiedExec>()
            {
                if identified.plan_node_id() == plan_node_id {
                    return Some(sum_stage(identified.input(), name));
                }
            }
            plan.children()
                .into_iter()
                .find_map(|child| identified_metric(child, plan_node_id, name))
        }

        let cached = self.physical_plan.lock().map_err(|_| {
            DataFusionError::Internal("native physical-plan cache lock poisoned".to_string())
        })?;
        let Some(cached) = cached.as_ref() else {
            return Ok(0);
        };
        if plan_node_id == 0 {
            return Ok(sum_all(&cached.plan, name));
        }
        identified_metric(&cached.plan, plan_node_id, name).ok_or_else(|| {
            DataFusionError::Execution(format!(
                "native metric requested unknown plan node {plan_node_id}"
            ))
        })
    }

    pub(crate) fn metric_snapshot(&self) -> Result<Vec<i64>> {
        let cached = self.physical_plan.lock().map_err(|_| {
            DataFusionError::Internal("native physical-plan cache lock poisoned".to_string())
        })?;
        Ok(cached.as_ref().map_or_else(Vec::new, |cached| {
            crate::plan_metrics::snapshot(&cached.plan)
        }))
    }

    pub(crate) fn input_batch_count(&self, ids: &[u64]) -> Result<u64> {
        fn count(plan: &Arc<dyn ExecutionPlan>, ids: &[u64]) -> u64 {
            let own = plan
                .downcast_ref::<crate::planner::operators::identified::IdentifiedExec>()
                .filter(|stage| ids.contains(&stage.plan_node_id()))
                .map_or(0, |stage| stage.input_batches());
            plan.children()
                .into_iter()
                .fold(own, |total, child| total.saturating_add(count(child, ids)))
        }
        let cached = self
            .physical_plan
            .lock()
            .map_err(|_| DataFusionError::Internal("native metric-tree lock poisoned".into()))?;
        Ok(cached.as_ref().map_or(0, |cached| count(&cached.plan, ids)))
    }
}

static NEXT_CONTEXT_HANDLE: AtomicI64 = AtomicI64::new(1);
static EXECUTION_CONTEXTS: OnceLock<Mutex<HashMap<i64, Arc<NativeExecutionContext>>>> =
    OnceLock::new();

fn contexts() -> &'static Mutex<HashMap<i64, Arc<NativeExecutionContext>>> {
    EXECUTION_CONTEXTS.get_or_init(|| Mutex::new(HashMap::new()))
}

pub(crate) fn register(
    bytes: &[u8],
    java_vm: JavaVM,
    memory_manager: Global<JObject<'static>>,
    memory_limit: usize,
) -> Result<i64> {
    register_with_state(bytes, None, java_vm, memory_manager, memory_limit)
}

pub(crate) fn register_with_state(
    bytes: &[u8],
    bindings: Option<&[u8]>,
    java_vm: JavaVM,
    memory_manager: Global<JObject<'static>>,
    memory_limit: usize,
) -> Result<i64> {
    let broker = Arc::new(JvmMemoryReservationBroker::new(java_vm, memory_manager));
    let memory_pool: Arc<dyn MemoryPool> =
        Arc::new(FlinkMemoryPool::new(broker.clone(), memory_limit));
    let mut context = NativeExecutionContext::new(bytes, memory_pool)?;
    if let Some(bindings) = bindings {
        context.install_state(
            bindings,
            crate::memory_pool::HostMemoryReservation::new(broker, "native region state bindings"),
        )?;
    }
    let context = Arc::new(context);
    let handle = NEXT_CONTEXT_HANDLE.fetch_add(1, Ordering::Relaxed);
    if handle <= 0 {
        return Err(DataFusionError::Internal(
            "native execution-context handle overflowed".to_string(),
        ));
    }
    contexts()
        .lock()
        .map_err(|_| {
            DataFusionError::Internal("execution-context registry lock poisoned".to_string())
        })?
        .insert(handle, context);
    Ok(handle)
}

pub(crate) fn get(handle: i64) -> Result<Arc<NativeExecutionContext>> {
    contexts()
        .lock()
        .map_err(|_| {
            DataFusionError::Internal("execution-context registry lock poisoned".to_string())
        })?
        .get(&handle)
        .cloned()
        .ok_or_else(|| {
            DataFusionError::Execution(format!(
                "native execution context {handle} is missing or closed"
            ))
        })
}

pub(crate) fn close(handle: i64) -> Result<()> {
    let removed = contexts()
        .lock()
        .map_err(|_| {
            DataFusionError::Internal("execution-context registry lock poisoned".to_string())
        })?
        .remove(&handle);
    if removed.is_none() {
        return Err(DataFusionError::Execution(format!(
            "native execution context {handle} is missing or already closed"
        )));
    }
    Ok(())
}

#[cfg(test)]
mod allocation_tests;
#[cfg(test)]
mod expression_memory_tests;
#[cfg(test)]
mod memory_tests;
