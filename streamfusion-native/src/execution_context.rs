// Copyright 2026 StreamFusion Authors
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
// http://www.apache.org/licenses/LICENSE-2.0

use std::collections::HashMap;
use std::sync::atomic::{AtomicI64, Ordering};
use std::sync::{Arc, Mutex, OnceLock};

use datafusion::error::{DataFusionError, Result};
use datafusion::execution::memory_pool::{MemoryConsumer, MemoryPool, MemoryReservation};
use datafusion::execution::runtime_env::RuntimeEnvBuilder;
use datafusion::execution::TaskContext;
use datafusion::physical_plan::ExecutionPlan;
use datafusion::prelude::{SessionConfig, SessionContext};
use jni::objects::{Global, JObject};
use jni::JavaVM;
use prost::Message;

use crate::memory_pool::{FlinkMemoryPool, JvmMemoryReservationBroker};
use crate::planner::create_plan_from_decoded;
use crate::planner::operators::reusable_input::ReusableInputExec;
use crate::proto;

pub(crate) struct NativeExecutionContext {
    plan: proto::NativePlan,
    runtime: tokio::runtime::Runtime,
    task_context: Arc<TaskContext>,
    memory_pool: Arc<dyn MemoryPool>,
    physical_plan: Mutex<Option<CachedPhysicalPlan>>,
    _plan_reservation: MemoryReservation,
}

struct CachedPhysicalPlan {
    plan: Arc<dyn ExecutionPlan>,
    inputs: Vec<Arc<ReusableInputExec>>,
}

impl NativeExecutionContext {
    fn new(
        plan: proto::NativePlan,
        java_vm: JavaVM,
        memory_manager: Global<JObject<'static>>,
        memory_limit: usize,
    ) -> Result<Self> {
        let broker = Arc::new(JvmMemoryReservationBroker::new(java_vm, memory_manager));
        let memory_pool: Arc<dyn MemoryPool> = Arc::new(FlinkMemoryPool::new(broker, memory_limit));
        let plan_reservation = MemoryConsumer::new("native protobuf plan").register(&memory_pool);
        plan_reservation.try_grow(plan.encoded_len())?;
        let runtime_env = RuntimeEnvBuilder::new()
            .with_memory_pool(Arc::clone(&memory_pool))
            .build()
            .map(Arc::new)?;
        let task_context =
            SessionContext::new_with_config_rt(SessionConfig::new(), runtime_env).task_ctx();
        let runtime = tokio::runtime::Builder::new_current_thread()
            .build()
            .map_err(|error| DataFusionError::External(Box::new(error)))?;
        Ok(Self {
            plan,
            runtime,
            task_context,
            memory_pool,
            physical_plan: Mutex::new(None),
            _plan_reservation: plan_reservation,
        })
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

    pub(crate) fn execute_plan<T>(
        &self,
        batches: Vec<arrow::array::RecordBatch>,
        execute: impl FnOnce(Arc<dyn ExecutionPlan>) -> Result<T>,
    ) -> Result<T> {
        let mut cached = self.physical_plan.lock().map_err(|_| {
            DataFusionError::Internal("native physical-plan cache lock poisoned".to_string())
        })?;
        if cached.is_none() {
            let inputs = batches
                .iter()
                .map(|batch| Arc::new(ReusableInputExec::new(batch.schema())))
                .collect::<Vec<_>>();
            let external_inputs = inputs
                .iter()
                .map(|input| Arc::clone(input) as Arc<dyn ExecutionPlan>)
                .collect();
            let plan = create_plan_from_decoded(&self.plan, external_inputs)?;
            *cached = Some(CachedPhysicalPlan { plan, inputs });
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
        let result = execute(Arc::clone(&cached.plan));
        for input in &cached.inputs {
            input.clear();
        }
        result
    }
}

static NEXT_CONTEXT_HANDLE: AtomicI64 = AtomicI64::new(1);
static EXECUTION_CONTEXTS: OnceLock<Mutex<HashMap<i64, Arc<NativeExecutionContext>>>> =
    OnceLock::new();

fn contexts() -> &'static Mutex<HashMap<i64, Arc<NativeExecutionContext>>> {
    EXECUTION_CONTEXTS.get_or_init(|| Mutex::new(HashMap::new()))
}

pub(crate) fn register(
    plan: proto::NativePlan,
    java_vm: JavaVM,
    memory_manager: Global<JObject<'static>>,
    memory_limit: usize,
) -> Result<i64> {
    let context = Arc::new(NativeExecutionContext::new(
        plan,
        java_vm,
        memory_manager,
        memory_limit,
    )?);
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
