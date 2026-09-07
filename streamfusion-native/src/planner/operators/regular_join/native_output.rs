// Copyright 2026 StreamFusion Authors
// Licensed under the Apache License, Version 2.0

use super::*;

/// Keeps a produced batch admitted while its native consumer is processing it. Only the outer
/// Arrow C Stream reader transfers accounting to Java; an intermediate ExecutionPlan must not.
pub(crate) struct NativeJoinOutput {
    pub(crate) batch: RecordBatch,
    pub(crate) _memory: HostMemoryReservation,
}

#[cfg(test)]
impl NativeJoinOutput {
    pub(crate) fn into_arrow(self) -> Result<RecordBatch> {
        let Self {
            batch,
            _memory: mut memory,
        } = self;
        memory.transfer_to_arrow(batch.get_array_memory_size())?;
        Ok(batch)
    }
}
