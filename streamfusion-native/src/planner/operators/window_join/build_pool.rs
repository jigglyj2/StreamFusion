// Copyright 2026 StreamFusion Authors
// Licensed under the Apache License, Version 2.0.

//! DataFusion's single-batch INNER build retains buffers already owned by ClosedWindow.
//! This pool exposes that existing allowance without a second Flink reservation. It is
//! private to this exact physical plan; other consumers and additional storage are rejected.

use arrow::record_batch::RecordBatch;
use datafusion::error::{DataFusionError, Result};
use datafusion::execution::memory_pool::{
    GreedyMemoryPool, MemoryLimit, MemoryPool, MemoryReservation,
};
use std::fmt::{Debug, Display, Formatter};

pub(super) struct WindowBuildPool {
    logical: GreedyMemoryPool,
    // Retain the already leased input even if the plan/context drop in a different order.
    _input: RecordBatch,
}

impl WindowBuildPool {
    pub(super) fn new(leased_input: RecordBatch) -> Self {
        Self {
            logical: GreedyMemoryPool::new(leased_input.get_array_memory_size()),
            _input: leased_input,
        }
    }
}

impl Debug for WindowBuildPool {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("WindowBuildPool")
            .field("logical", &self.logical)
            .finish_non_exhaustive()
    }
}
impl Display for WindowBuildPool {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        write!(f, "WindowBuildPool({})", self.logical)
    }
}
impl MemoryPool for WindowBuildPool {
    fn name(&self) -> &str {
        "window-join-leased-build"
    }
    fn grow(&self, reservation: &MemoryReservation, additional: usize) {
        self.try_grow(reservation, additional)
            .expect("unexpected infallible window build growth");
    }
    fn shrink(&self, reservation: &MemoryReservation, shrink: usize) {
        self.logical.shrink(reservation, shrink);
    }
    fn try_grow(&self, reservation: &MemoryReservation, additional: usize) -> Result<()> {
        if reservation.consumer().name() != "NestedLoopJoinLoad[0]" {
            return Err(DataFusionError::ResourcesExhausted(
                "window join input allowance covers only the single INNER build consumer".into(),
            ));
        }
        self.logical.try_grow(reservation, additional)
    }
    fn reserved(&self) -> usize {
        self.logical.reserved()
    }
    fn memory_limit(&self) -> MemoryLimit {
        self.logical.memory_limit()
    }
}
