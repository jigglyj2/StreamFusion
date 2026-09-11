// Copyright 2026 StreamFusion Authors. Licensed under the Apache License, Version 2.0.
use super::*;
use crate::exchange::output_bindings::OutputBindings;

impl NativeExecutionContext {
    pub(crate) fn bind_exchange_outputs(&self, bindings: OutputBindings) -> Result<()> {
        let mut invocation = invocation::InvocationGuard::begin(self)?;
        let count = match &self.plan {
            Definition::Tree(_) => 1,
            Definition::Region(plan) => plan.message.output_stage_ids.len(),
        };
        if self.protocol_version() < crate::RECORD_POLICY_PLAN_PROTOCOL_VERSION
            || bindings.ports.len() != count
        {
            return Err(DataFusionError::Plan(
                "exchange outputs require all ports of an owned-envelope plan".into(),
            ));
        }
        let mut outputs = self.exchange_outputs.lock().map_err(|_| poisoned())?;
        if outputs.is_some() {
            return Err(DataFusionError::Plan(
                "exchange outputs are already bound".into(),
            ));
        }
        *outputs = Some(Arc::new(bindings));
        invocation.successful = true;
        Ok(())
    }
    pub(crate) fn exchange_output_bindings(&self) -> Result<Option<Arc<OutputBindings>>> {
        Ok(self
            .exchange_outputs
            .lock()
            .map_err(|_| poisoned())?
            .clone())
    }
}
fn poisoned() -> DataFusionError {
    DataFusionError::Execution("exchange output binding lock is poisoned".into())
}
