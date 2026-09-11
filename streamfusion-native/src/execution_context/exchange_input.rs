// Copyright 2026 StreamFusion Authors
// Licensed under the Apache License, Version 2.0.

use super::*;

pub(super) struct PreparedInput {
    schema: SchemaRef,
    _memory: MemoryReservation,
}

impl NativeExecutionContext {
    pub(crate) fn bind_exchange_input(
        &self,
        port: usize,
        bytes: &[u8],
        memory: MemoryReservation,
    ) -> Result<()> {
        let mut invocation = invocation::InvocationGuard::begin(self)?;
        let valid = match &self.plan {
            Definition::Region(plan) => port < plan.message.input_count as usize,
            Definition::Tree(plan) => match &plan.root {
                Some(root) => has_input(root, port)?,
                None => false,
            },
        };
        if !valid {
            return Err(DataFusionError::Plan(
                "exchange binding addresses an unknown native input port".into(),
            ));
        }
        let plan = crate::exchange::decode_exchange_plan(bytes)?;
        let schema = crate::exchange::transport_schema(&plan)?;
        let mut inputs = self.exchange_inputs.lock().map_err(|_| poisoned())?;
        if inputs.contains_key(&port) {
            return Err(DataFusionError::Plan(
                "exchange input is already bound".into(),
            ));
        }
        inputs.insert(
            port,
            PreparedInput {
                schema,
                _memory: memory,
            },
        );
        invocation.successful = true;
        Ok(())
    }

    pub(crate) fn exchange_input_schema(&self, port: usize) -> Result<SchemaRef> {
        self.exchange_inputs
            .lock()
            .map_err(|_| poisoned())?
            .get(&port)
            .map(|input| input.schema.clone())
            .ok_or_else(|| DataFusionError::Plan("exchange input has not been bound".into()))
    }
}

fn has_input(node: &proto::Operator, port: usize) -> Result<bool> {
    if let Some(proto::operator::Operator::Input(input)) = &node.operator {
        return Ok(input.input_index as usize == port);
    }
    for child in crate::planner::persistent::children(node)? {
        if has_input(child, port)? {
            return Ok(true);
        }
    }
    Ok(false)
}
fn poisoned() -> DataFusionError {
    DataFusionError::Execution("exchange input binding lock is poisoned".into())
}

#[cfg(test)]
mod tests;
